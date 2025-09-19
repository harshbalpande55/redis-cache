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
    LlenCommand, LpopCommand, BlpopCommand, XaddCommand, XrangeCommand, TypeCommand
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
            TypeCommand(self.storage),
        ]
        
        for command in commands:
            self.command_registry.register(command)
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle a single client connection using async I/O."""
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
                        # Execute the command
                        response = self.command_registry.execute_command(command, args)
                        
                        # Check if this is a blocking command that needs special handling
                        if response.startswith(b"BLOCKING_REQUIRED") and command == "BLPOP":
                            # Handle blocking BLPOP with timeout
                            timeout_str = response.decode('utf-8').split(':')[1]
                            timeout = float(timeout_str)
                            await self._handle_blocking_blpop(writer, args, timeout)
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
