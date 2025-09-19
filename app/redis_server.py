"""
Main Redis server implementation.
Orchestrates all components using proper separation of concerns.
"""
import asyncio
from typing import Optional

from .storage import StorageBackend, InMemoryStorage
from .protocol import RedisProtocolParser, RedisResponseFormatter
from .command_registry import CommandRegistry
from .commands import (
    PingCommand, EchoCommand, SetCommand, GetCommand, 
    RpushCommand, DelCommand, ExistsCommand
)


class RedisServer:
    """Main Redis server class that orchestrates all components."""
    
    def __init__(self, storage: Optional[StorageBackend] = None):
        # Initialize storage backend
        self.storage = storage or InMemoryStorage()
        
        # Initialize protocol components
        self.protocol_parser = RedisProtocolParser()
        self.response_formatter = RedisResponseFormatter()
        
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
            RpushCommand(self.storage),
            DelCommand(self.storage),
            ExistsCommand(self.storage),
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
                    else:
                        # Execute the command
                        response = self.command_registry.execute_command(command, args)
                    
                    # Send response to client
                    writer.write(response)
                    await writer.drain()  # Ensure data is sent
                    
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
            writer.close()
            await writer.wait_closed()
    
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
