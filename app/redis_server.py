"""
Main Redis server implementation.
Orchestrates all components using proper separation of concerns.
"""
import asyncio
import time
from typing import Optional, List

from .storage import StorageBackend, InMemoryStorage
from .protocol import RedisProtocolParser, RedisResponseFormatter
from .command_registry import CommandRegistry
from .blocking_manager import BlockingManager
from .commands import (
    PingCommand, EchoCommand, SetCommand, GetCommand, 
    RpushCommand, DelCommand, ExistsCommand, LrangeCommand, LpushCommand,
    LlenCommand, LpopCommand, BlpopCommand, XaddCommand, XrangeCommand, XreadCommand, TypeCommand, IncrCommand, InfoCommand, ReplconfCommand, PsyncCommand
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
        
        # Replica configuration
        self.is_replica = False
        self.master_host = None
        self.master_port = None
        
        # Replication state
        self.master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"  # Default replication ID
        self.master_repl_offset = 0  # Replication offset
        
        # Connected replicas for command propagation
        self.connected_replicas = []  # List of (reader, writer, offset) tuples for connected replicas
        
        # Replication backlog for partial resynchronization
        self.replication_backlog = []  # List of (offset, command_bytes) tuples
        self.replication_backlog_size = 1000000  # 1MB backlog size
    
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
            InfoCommand(self.storage, self),
            ReplconfCommand(self.storage, self),
            PsyncCommand(self.storage, self),
            # MULTI and EXEC are now handled directly in handle_client
        ]
        
        for command in commands:
            self.command_registry.register(command)
    
    def set_replica_config(self, master_host: str, master_port: int) -> None:
        """Set this server as a replica of the specified master."""
        self.is_replica = True
        self.master_host = master_host
        self.master_port = master_port
    
    def add_replica_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Add a replica connection for command propagation."""
        self.connected_replicas.append((reader, writer, 0))  # Start with offset 0
        print(f"Added replica connection. Total replicas: {len(self.connected_replicas)}")
        
        # Log replica connection details
        replica_addr = writer.get_extra_info('peername')
        if replica_addr:
            print(f"Replica connected from {replica_addr[0]}:{replica_addr[1]}")
    
    def remove_replica_connection(self, writer: asyncio.StreamWriter) -> None:
        """Remove a replica connection."""
        self.connected_replicas = [(r, w, o) for r, w, o in self.connected_replicas if w != writer]
        print(f"Removed replica connection. Total replicas: {len(self.connected_replicas)}")
    
    def update_replica_offset(self, writer: asyncio.StreamWriter, offset: int) -> None:
        """Update the replication offset for a specific replica."""
        for i, (reader, w, old_offset) in enumerate(self.connected_replicas):
            if w == writer:
                self.connected_replicas[i] = (reader, w, offset)
                print(f"Updated replica offset to {offset} (was {old_offset})")
                break
    
    async def propagate_command_to_replicas(self, command: str, args: List[str]) -> None:
        """Propagate a command to all connected replicas."""
        if not self.connected_replicas:
            return
        
        # Build the command in RESP format
        command_parts = [command] + args
        resp_command = f"*{len(command_parts)}\r\n"
        for part in command_parts:
            resp_command += f"${len(part)}\r\n{part}\r\n"
        
        # Add to replication backlog
        self.add_to_replication_backlog(resp_command.encode())
        
        # Send to all replicas
        failed_replicas = []
        for reader, writer, offset in self.connected_replicas:
            try:
                writer.write(resp_command.encode())
                await writer.drain()
                print(f"Propagated command to replica (offset: {offset})")
            except Exception as e:
                print(f"Failed to propagate command to replica: {e}")
                failed_replicas.append(writer)
        
        # Remove failed replica connections
        for writer in failed_replicas:
            self.remove_replica_connection(writer)
    
    def increment_replication_offset(self, command_bytes: int) -> None:
        """Increment the replication offset by the number of bytes in the command."""
        self.master_repl_offset += command_bytes
    
    def add_to_replication_backlog(self, command_bytes: bytes) -> None:
        """Add a command to the replication backlog."""
        # Add to backlog
        self.replication_backlog.append((self.master_repl_offset, command_bytes))
        
        # Maintain backlog size limit
        current_size = sum(len(cmd) for _, cmd in self.replication_backlog)
        while current_size > self.replication_backlog_size and self.replication_backlog:
            self.replication_backlog.pop(0)
            current_size = sum(len(cmd) for _, cmd in self.replication_backlog)
    
    async def send_backlog_to_replica(self, writer: asyncio.StreamWriter, from_offset: int) -> None:
        """Send backlog commands to a replica starting from the specified offset."""
        for offset, command_bytes in self.replication_backlog:
            if offset > from_offset:
                try:
                    writer.write(command_bytes)
                    await writer.drain()
                except Exception as e:
                    print(f"Failed to send backlog command to replica: {e}")
                    break
    
    async def start_replication_handshake(self, replica_port: int = 6380) -> None:
        """Start the replication handshake with the master."""
        if not self.is_replica:
            return
        
        try:
            # Connect to master
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            
            # Send PING command first
            ping_command = f"*1\r\n$4\r\nPING\r\n"
            writer.write(ping_command.encode())
            await writer.drain()
            
            # Read response
            response = await reader.read(1024)
            print(f"Master response to PING: {response.decode()}")
            
            # Send REPLCONF listening-port command
            port_command = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(replica_port))}\r\n{replica_port}\r\n"
            writer.write(port_command.encode())
            await writer.drain()
            
            # Read response
            response = await reader.read(1024)
            print(f"Master response to listening-port: {response.decode()}")
            
            # Send REPLCONF capa command
            capa_command = f"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
            writer.write(capa_command.encode())
            await writer.drain()
            
            # Read response
            response = await reader.read(1024)
            print(f"Master response to capa: {response.decode()}")
            
            # Send PSYNC command
            psync_command = f"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
            writer.write(psync_command.encode())
            await writer.drain()
            
            # Read PSYNC response - this contains both FULLRESYNC and RDB file
            # First, read the FULLRESYNC response line
            fullresync_line = await reader.readline()
            print(f"Master response to PSYNC: {fullresync_line.decode().strip()}")
            
            # Then read the RDB file length line
            rdb_length_line = await reader.readline()
            if rdb_length_line.startswith(b'$'):
                # Parse the length
                rdb_length = int(rdb_length_line[1:-2])  # Remove $ and \r\n
                
                # Read the RDB file content
                rdb_content = await reader.read(rdb_length)
                print(f"Received RDB file of {len(rdb_content)} bytes")
                
                # The RDB file is now loaded, we can continue with replication
                # In a full implementation, we would parse and load the RDB file
                
                # Keep connection alive for ongoing replication
                # Start a replication loop to receive commands from master
                print("Starting replication loop...")
                try:
                    buffer = b""
                    last_ack_time = time.time()
                    ack_interval = 1.0  # Send ACK every 1 second
                    replica_offset = 0  # Track our replication offset
                    
                    while True:
                        # Read data from master
                        data = await reader.read(1024)
                        if not data:
                            print("Master disconnected")
                            break
                        
                        buffer += data
                        
                        # Process complete commands from buffer
                        while buffer:
                            try:
                                # Try to parse a command from the buffer
                                command, args = self.protocol_parser.parse_command(buffer)
                                if command:
                                    print(f"Received command from master: {command} {' '.join(args)}")
                                    
                                    # Calculate the length of the processed command for offset tracking
                                    command_bytes = f"*{len([command] + args)}\r\n"
                                    for part in [command] + args:
                                        command_bytes += f"${len(part)}\r\n{part}\r\n"
                                    
                                    # Handle REPLCONF GETACK command specially - it needs a response
                                    if command == "REPLCONF" and args and args[0] == "GETACK":
                                        # Send ACK response to master with current offset (before processing this command)
                                        ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n"
                                        writer.write(ack_response.encode())
                                        await writer.drain()
                                        print(f"Sent ACK response to GETACK command with offset {replica_offset}")
                                        
                                        # Now update offset to include this REPLCONF command
                                        replica_offset += len(command_bytes.encode())
                                    else:
                                        # Execute other commands silently (no response)
                                        self.command_registry.execute_command(command, args)
                                        
                                        # Update replica offset with the command length
                                        # Count all propagated commands except handshake-specific ones
                                        if command not in ["PSYNC", "INFO"]:
                                            replica_offset += len(command_bytes.encode())
                                    
                                    # Remove the processed command from buffer
                                    buffer = buffer[len(command_bytes.encode()):]
                                    
                                    # Send periodic ACK to master
                                    current_time = time.time()
                                    if current_time - last_ack_time >= ack_interval:
                                        # Send REPLCONF ACK with current replication offset
                                        ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n"
                                        writer.write(ack_command.encode())
                                        await writer.drain()
                                        last_ack_time = current_time
                                        print(f"Sent ACK to master with offset {replica_offset}")
                                else:
                                    # Incomplete command, wait for more data
                                    break
                            except Exception as e:
                                print(f"Error parsing command: {e}")
                                break
                except Exception as e:
                    print(f"Replication loop error: {e}")
                finally:
                    writer.close()
                    await writer.wait_closed()
            else:
                print("Unexpected response format after PSYNC")
                writer.close()
                await writer.wait_closed()
            
        except Exception as e:
            print(f"Replication handshake failed: {e}")
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a single client connection using async I/O."""
        # Get client identifier for transaction tracking
        client_id = id(writer)
        if client_id not in self.client_transactions:
            self.client_transactions[client_id] = {'in_transaction': False, 'commands': []}
        
        # Track if this is a replica connection
        is_replica_connection = False
        
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
                            
                            # Check if this is a replica connection
                            is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                            
                            if not is_replica:
                                response = self.response_formatter.simple_string("OK")
                            else:
                                # Replica MULTI commands don't send responses
                                response = None
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
                                
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    # Return array of results
                                    response = self.response_formatter.array(results)
                                else:
                                    # Replica EXEC commands don't send responses
                                    response = None
                            else:
                                response = self.response_formatter.error("ERR EXEC without MULTI")
                        elif command == "DISCARD":
                            if self.client_transactions[client_id]['in_transaction']:
                                # Cancel the transaction
                                self.client_transactions[client_id]['in_transaction'] = False
                                self.client_transactions[client_id]['commands'] = []
                                
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    response = self.response_formatter.simple_string("OK")
                                else:
                                    # Replica DISCARD commands don't send responses
                                    response = None
                            else:
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    response = self.response_formatter.error("ERR DISCARD without MULTI")
                                else:
                                    # Replica DISCARD commands don't send responses
                                    response = None
                        elif command == "PSYNC":
                            # Handle PSYNC command and mark this as a replica connection
                            response = self.command_registry.execute_command(command, args)
                            is_replica_connection = True
                            # Add this connection to replica connections
                            self.add_replica_connection(reader, writer)
                            # Mark this client as a replica for the entire session
                            self.client_transactions[client_id]['is_replica'] = True
                            
                            # Send the response
                            if response is not None:
                                writer.write(response)
                                await writer.drain()
                            
                            # Continue the loop to handle replica commands directly
                            # instead of starting a separate task
                            print(f"PSYNC completed for client {client_id}, continuing as replica")
                            continue
                        elif command == "REPLCONF" and len(args) > 0 and args[0].lower() == "ack":
                            # Handle REPLCONF ACK command and update replica offset
                            response = self.command_registry.execute_command(command, args)
                            # Update the replica's offset
                            if len(args) == 2:
                                try:
                                    replica_offset = int(args[1])
                                    self.update_replica_offset(writer, replica_offset)
                                except ValueError:
                                    pass  # Invalid offset, ignore
                        else:
                            # Regular command execution
                            if self.client_transactions[client_id]['in_transaction']:
                                # Queue command for later execution
                                self.client_transactions[client_id]['commands'].append((command, args))
                                
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    response = self.response_formatter.simple_string("QUEUED")
                                else:
                                    # Replica commands in transactions don't send responses
                                    response = None
                            else:
                                # Execute command immediately
                                response = self.command_registry.execute_command(command, args)
                                
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    # This is a regular client, propagate commands to replicas
                                    # Propagate all commands except replication-specific ones
                                    if command not in ["REPLCONF", "PSYNC", "INFO"]:
                                        await self.propagate_command_to_replicas(command, args)
                                    
                                    # Increment replication offset only for data-modifying commands
                                    if command not in ["PING", "REPLCONF", "PSYNC", "INFO", "GET", "LRANGE", "LLEN", "TYPE", "EXISTS"]:
                                        command_bytes = len(data)
                                        self.increment_replication_offset(command_bytes)
                                else:
                                    # This is a replica receiving commands from master
                                    # Don't send response back to master, just process the command silently
                                    response = None
                        
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
                            # Send normal response (skip if response is None for replica commands)
                            if response is not None:
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
            
            # Only clean up if this is not a replica connection
            # Replica connections are handled by the separate _handle_replica_connection task
            if not is_replica_connection:
                # Clean up transaction state for this client
                if client_id in self.client_transactions:
                    del self.client_transactions[client_id]
                
                writer.close()
                await writer.wait_closed()
    
    async def _handle_replica_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client_id: int) -> None:
        """Handle a replica connection by continuously reading and processing commands from the master."""
        print(f"Replica connection task started for client {client_id}")
        try:
            buffer = b""
            last_ack_time = time.time()
            ack_interval = 1.0  # Send ACK every 1 second
            replica_offset = 0  # Track our replication offset
            
            while True:
                print(f"Replica {client_id} waiting for data from master...")
                # Read data from master
                data = await reader.read(1024)
                if not data:
                    print("Master disconnected")
                    break
                
                buffer += data
                
                # Process complete commands from buffer
                while buffer:
                    try:
                        # Try to parse a command from the buffer
                        command, args = self.protocol_parser.parse_command(buffer)
                        if command:
                            print(f"Received command from master: {command} {' '.join(args)}")
                            
                            # Calculate the length of the processed command for offset tracking
                            command_bytes = f"*{len([command] + args)}\r\n"
                            for part in [command] + args:
                                command_bytes += f"${len(part)}\r\n{part}\r\n"
                            
                            # Handle REPLCONF GETACK command specially - it needs a response
                            if command == "REPLCONF" and args and args[0] == "GETACK":
                                # Send ACK response to master with current offset (before processing this command)
                                ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n"
                                writer.write(ack_response.encode())
                                await writer.drain()
                                print(f"Sent ACK response to GETACK command with offset {replica_offset}")
                                
                                # Now update offset to include this REPLCONF command
                                replica_offset += len(command_bytes.encode())
                            else:
                                # Execute other commands silently (no response)
                                self.command_registry.execute_command(command, args)
                                
                                # Update replica offset with the command length
                                # Count all propagated commands except handshake-specific ones
                                if command not in ["PSYNC", "INFO"]:
                                    replica_offset += len(command_bytes.encode())
                            
                            # Remove the processed command from buffer
                            buffer = buffer[len(command_bytes.encode()):]
                            
                            # Send periodic ACK to master
                            current_time = time.time()
                            if current_time - last_ack_time >= ack_interval:
                                # Send REPLCONF ACK with current replication offset
                                ack_command = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n"
                                writer.write(ack_command.encode())
                                await writer.drain()
                                last_ack_time = current_time
                                print(f"Sent ACK to master with offset {replica_offset}")
                        else:
                            # Incomplete command, wait for more data
                            break
                    except Exception as e:
                        print(f"Error parsing command: {e}")
                        break
        except Exception as e:
            print(f"Replica connection error: {e}")
        finally:
            # Remove replica connection when it disconnects
            self.remove_replica_connection(writer)
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
            
            # If this is a replica, start the replication handshake
            if self.is_replica:
                # Start handshake in background
                asyncio.create_task(self.start_replication_handshake(port))
            
            await server.serve_forever()
    
    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "registered_commands": self.command_registry.list_commands(),
            "storage_size": self.storage.size(),
            "storage_type": type(self.storage).__name__
        }
