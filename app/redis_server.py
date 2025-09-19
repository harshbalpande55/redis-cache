"""
Main Redis server implementation.
Orchestrates all components using proper separation of concerns.
"""
import asyncio
from typing import Optional, List

from .storage import StorageBackend, InMemoryStorage
from .protocol import RedisProtocolParser, RedisResponseFormatter
from .command_registry import CommandRegistry
from .blocking_manager import BlockingManager
from .commands import (
    PingCommand, EchoCommand, SetCommand, GetCommand, 
    RpushCommand, DelCommand, ExistsCommand, LrangeCommand, LpushCommand,
    LlenCommand, LpopCommand, BlpopCommand, XaddCommand, XrangeCommand, XreadCommand, TypeCommand, IncrCommand
)


class RedisServer:
    """Main Redis server class that orchestrates all components."""
    
    def __init__(self, storage: Optional[StorageBackend] = None):
        # Initialize storage backend
        self.storage = storage or InMemoryStorage()
        
        # Initialize protocol components
        self.protocol_parser = RedisProtocolParser()
        self.response_formatter = RedisResponseFormatter()
        
        # Initialize blocking manager
        self.blocking_manager = BlockingManager()
        
        # Initialize command registry
        self.command_registry = CommandRegistry()
        self._register_commands()
        
        # Transaction state management - track per client
        self.client_transactions = {}  # client_id -> {'in_transaction': bool, 'commands': list}
    
    def _register_commands(self) -> None:
        """Register all available commands."""
        commands = [
            PingCommand(self.storage),
            EchoCommand(self.storage),
            SetCommand(self.storage),
            GetCommand(self.storage),
            RpushCommand(self.storage, self.blocking_manager),
            LrangeCommand(self.storage),
            LpushCommand(self.storage),
            LlenCommand(self.storage),
            LpopCommand(self.storage),
            BlpopCommand(self.storage, self.blocking_manager),
            DelCommand(self.storage),
            ExistsCommand(self.storage),
            XaddCommand(self.storage),
            XrangeCommand(self.storage),
            XreadCommand(self.storage),
            TypeCommand(self.storage),
            IncrCommand(self.storage),
            # MULTI and EXEC are now handled directly in handle_client
        ]
        
        for command in commands:
            self.command_registry.register(command)
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a single client connection using async I/O."""
        # Get client identifier for transaction tracking
        client_id = id(writer)
        if client_id not in self.client_transactions:
            self.client_transactions[client_id] = {'in_transaction': False, 'commands': []}
        
        try:
            # Loop to handle multiple commands on the same connection
            while True:
                try:
                    # Read data from the client
                    data = await reader.read(1024)
                    if not data:
                        # Client disconnected
                        break
                    
                    # Parse the Redis command
                    command, args = self.protocol_parser.parse_command(data)
                    
                    if command is None:
                        # Invalid command format
                        response = self.response_formatter.error("invalid command format")
                        writer.write(response)
                        await writer.drain()
                    else:
                        # Handle transaction commands
                        if command == "MULTI":
                            self.client_transactions[client_id]['in_transaction'] = True
                            self.client_transactions[client_id]['commands'] = []
                            response = self.response_formatter.simple_string("OK")
                        elif command == "EXEC":
                            if self.client_transactions[client_id]['in_transaction']:
                                # Execute all queued commands
                                results = []
                                for cmd, cmd_args in self.client_transactions[client_id]['commands']:
                                    cmd_response = self.command_registry.execute_command(cmd, cmd_args)
                                    results.append(cmd_response)
                                
                                # Reset transaction state
                                self.client_transactions[client_id]['in_transaction'] = False
                                self.client_transactions[client_id]['commands'] = []
                                
                                # Return array of results
                                response = self.response_formatter.array(results)
                            else:
                                response = self.response_formatter.error("ERR EXEC without MULTI")
                        elif command == "DISCARD":
                            if self.client_transactions[client_id]['in_transaction']:
                                # Cancel the transaction
                                self.client_transactions[client_id]['in_transaction'] = False
                                self.client_transactions[client_id]['commands'] = []
                                response = self.response_formatter.simple_string("OK")
                            else:
                                response = self.response_formatter.error("ERR DISCARD without MULTI")
                        else:
                            # Regular command execution
                            if self.client_transactions[client_id]['in_transaction']:
                                # Queue command for later execution
                                self.client_transactions[client_id]['commands'].append((command, args))
                                response = self.response_formatter.simple_string("QUEUED")
                            else:
                                # Execute command immediately
                                response = self.command_registry.execute_command(command, args)
                        
                        # Check if this is a blocking command that needs special handling
                        if response.startswith(b"BLOCKING_REQUIRED"):
                            if command == "BLPOP":
                                # Handle blocking BLPOP with timeout
                                timeout_str = response.decode('utf-8').split(':')[1]
                                timeout = float(timeout_str)
                                await self._handle_blocking_blpop(writer, args, timeout)
                            elif command == "XREAD":
                                # Handle blocking XREAD with timeout
                                parts = response.decode('utf-8').split(':')
                                timeout = int(parts[1])
                                keys = parts[2].split(':')
                                ids = parts[3].split(':')
                                
                                # Resolve $ IDs to actual max IDs for blocking
                                resolved_ids = []
                                for i, stream_id in enumerate(ids):
                                    if stream_id == "$":
                                        # Find the max ID in this stream
                                        stream = self.storage.get_stream(keys[i])
                                        if stream and stream:
                                            max_id = None
                                            max_time = -1
                                            max_seq = -1
                                            
                                            for entry_id in stream.keys():
                                                try:
                                                    if '-' not in entry_id:
                                                        continue
                                                    
                                                    time_part, seq_part = entry_id.split('-', 1)
                                                    time_ms = int(time_part)
                                                    seq_num = int(seq_part)
                                                    
                                                    if (time_ms > max_time or 
                                                        (time_ms == max_time and seq_num > max_seq)):
                                                        max_time = time_ms
                                                        max_seq = seq_num
                                                        max_id = entry_id
                                                        
                                                except ValueError:
                                                    continue
                                            
                                            resolved_ids.append(max_id if max_id else "0-0")
                                        else:
                                            resolved_ids.append("0-0")
                                    else:
                                        resolved_ids.append(stream_id)
                                
                                await self._handle_blocking_xread(writer, keys, resolved_ids, timeout)
                        else:
                            # Send normal response
                            writer.write(response)
                            await writer.drain()
                    
                except ConnectionResetError:
                    # Client disconnected unexpectedly
                    break
                except Exception as e:
                    # Handle unexpected errors
                    error_response = self.response_formatter.error(f"ERR {str(e)}")
                    writer.write(error_response)
                    await writer.drain()
                    break
                    
        finally:
            # Clean up blocking client if it exists
            self.blocking_manager.remove_blocking_client(writer)
            
            # Clean up transaction state for this client
            if client_id in self.client_transactions:
                del self.client_transactions[client_id]
            
            writer.close()
            await writer.wait_closed()
    
    async def _handle_blocking_blpop(self, writer: asyncio.StreamWriter, args: List[str], timeout: float) -> None:
        """Handle blocking BLPOP command with timeout."""
        import time
        
        keys = args[:-1]  # All arguments except the last one are keys
        start_time = time.time()
        
        # Add client to blocking manager
        self.blocking_manager.add_blocking_client(writer, keys, "BLPOP")
        
        try:
            # Wait for an element to become available or timeout
            while True:
                # Check if timeout has expired
                if timeout > 0 and (time.time() - start_time) >= timeout:
                    # Timeout reached, send null response
                    response = self.response_formatter.array(None)  # *-1\r\n
                    writer.write(response)
                    await writer.drain()
                    return
                
                # Check if any of the keys have elements
                for key in keys:
                    value = self.storage.blpop(key)
                    if value is not None:
                        # Found an element, send response to the oldest waiting client
                        oldest_client = self.blocking_manager.get_oldest_waiting_client(key)
                        if oldest_client and oldest_client.writer == writer:
                            # This client gets the element
                            response = self.response_formatter.array([
                                self.response_formatter.bulk_string(key),
                                self.response_formatter.bulk_string(value)
                            ])
                            writer.write(response)
                            await writer.drain()
                            return
                
                # No elements available, wait a bit before checking again
                await asyncio.sleep(0.01)  # Small delay to prevent busy waiting
                
        finally:
            # Always clean up the client from blocking manager
            self.blocking_manager.remove_blocking_client(writer)
    
    async def _handle_blocking_xread(self, writer: asyncio.StreamWriter, keys: List[str], ids: List[str], timeout: int) -> None:
        """Handle blocking XREAD command with timeout."""
        import time
        
        start_time = time.time()
        
        # Add client to blocking manager for stream keys
        self.blocking_manager.add_blocking_client(writer, keys, "XREAD")
        
        try:
            # Wait for new entries in any of the streams or timeout
            while True:
                # Check if timeout has expired
                if timeout > 0 and (time.time() - start_time) * 1000 >= timeout:
                    # Timeout reached, send null response
                    response = self.response_formatter.array(None)  # *-1\r\n
                    writer.write(response)
                    await writer.drain()
                    return
                
                # Check if any of the streams have new entries
                streams = list(zip(keys, ids))
                result = self.storage.xread(streams)
                
                if result:
                    # Found new entries, send response
                    entries = []
                    for stream_key, stream_entries in result:
                        # Create stream entry array
                        stream_array = []
                        for entry_id, fields in stream_entries:
                            # Create field-value pairs array
                            field_value_pairs = []
                            for field, value in fields.items():
                                field_value_pairs.append(self.response_formatter.bulk_string(field))
                                field_value_pairs.append(self.response_formatter.bulk_string(value))
                            
                            # Create entry array: [id, [field1, value1, field2, value2, ...]]
                            entry_array = [
                                self.response_formatter.bulk_string(entry_id),
                                self.response_formatter.array(field_value_pairs)
                            ]
                            stream_array.append(self.response_formatter.array(entry_array))
                        
                        # Create stream array: [stream_key, [entries...]]
                        stream_entry = [
                            self.response_formatter.bulk_string(stream_key),
                            self.response_formatter.array(stream_array)
                        ]
                        entries.append(self.response_formatter.array(stream_entry))
                    
                    response = self.response_formatter.array(entries)
                    writer.write(response)
                    await writer.drain()
                    return
                
                # No new entries available, wait a bit before checking again
                await asyncio.sleep(0.01)  # Small delay to prevent busy waiting
                
        finally:
            # Always clean up the client from blocking manager
            self.blocking_manager.remove_blocking_client(writer)
    
    async def start_server(self, host: str = "localhost", port: int = 6379) -> None:
        """Start the Redis server."""
        print(f"Starting Redis server on {host}:{port}")
        
        # Create async server that handles multiple clients concurrently
        server = await asyncio.start_server(
            self.handle_client, 
            host, 
            port
        )
        
        # Start serving clients
        async with server:
            print(f"Redis server is running on {host}:{port}")
            await server.serve_forever()
    
    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "registered_commands": self.command_registry.list_commands(),
            "storage_size": self.storage.size(),
            "storage_type": type(self.storage).__name__
        }
