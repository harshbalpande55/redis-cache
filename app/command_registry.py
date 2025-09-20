"""
Command registry for managing and routing Redis commands.
Uses Registry pattern for extensible command handling.
"""
from typing import Dict, Optional

from app.commands import Command
from app.protocol import RedisResponseFormatter


class CommandRegistry:
    """Registry for managing Redis commands."""
    
    def __init__(self):
        self._commands: Dict[str, Command] = {}
        self.formatter = RedisResponseFormatter()
    
    def register(self, command: Command) -> None:
        """Register a command handler."""
        command_name = command.get_name().upper()
        self._commands[command_name] = command
    
    def get_command(self, name: str) -> Optional[Command]:
        """Get a command handler by name."""
        return self._commands.get(name.upper())
    
    def execute_command(self, command_name: str, args: list) -> bytes:
        """Execute a command with given arguments."""
        command = self.get_command(command_name)
        
        if command is None:
            return self.formatter.error(f"unknown command '{command_name}'")
        
        try:
            return command.execute(args)
        except Exception as e:
            return self.formatter.error(f"ERR {str(e)}")
    
    def list_commands(self) -> list:
        """Get list of registered command names."""
        return list(self._commands.keys())
    
    def is_registered(self, command_name: str) -> bool:
        """Check if a command is registered."""
        return command_name.upper() in self._commands
