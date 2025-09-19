"""
Redis command implementations using Command pattern.
Each command is a separate class for better organization and extensibility.
"""
from abc import ABC, abstractmethod
from typing import List, Optional
import time

from .protocol import RedisResponseFormatter
from .storage import StorageBackend


class Command(ABC):
    """Abstract base class for Redis commands."""
    
    def __init__(self, storage: StorageBackend):
        self.storage = storage
        self.formatter = RedisResponseFormatter()
    
    @abstractmethod
    def execute(self, args: List[str]) -> bytes:
        """Execute the command with given arguments."""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Get the command name."""
        pass
    
    def validate_args(self, args: List[str], expected_count: int) -> Optional[bytes]:
        """Validate argument count and return error if invalid."""
        if len(args) != expected_count:
            return self.formatter.error(
                f"wrong number of arguments for '{self.get_name().lower()}' command"
            )
        return None


class PingCommand(Command):
    """PING command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) == 0:
            return self.formatter.simple_string("PONG")
        elif len(args) == 1:
            message = args[0]
            return self.formatter.bulk_string(message)
        else:
            return self.formatter.error("wrong number of arguments for 'ping' command")
    
    def get_name(self) -> str:
        return "PING"


class EchoCommand(Command):
    """ECHO command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        error = self.validate_args(args, 1)
        if error:
            return error
        
        message = args[0]
        return self.formatter.bulk_string(message)
    
    def get_name(self) -> str:
        return "ECHO"


class SetCommand(Command):
    """SET command implementation with PX support."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'set' command")
        
        key = args[0]
        value = args[1]
        
        # Check for PX option (expiration in milliseconds)
        expires_at = None
        if len(args) == 4 and args[2].upper() == "PX":
            try:
                px_milliseconds = int(args[3])
                expires_at = time.time() + (px_milliseconds / 1000.0)  # Convert ms to seconds
            except ValueError:
                return self.formatter.error("value is not an integer or out of range")
        
        # Store the key-value pair with optional expiration
        self.storage.set(key, value, expires_at)
        return self.formatter.simple_string("OK")
    
    def get_name(self) -> str:
        return "SET"


class GetCommand(Command):
    """GET command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        error = self.validate_args(args, 1)
        if error:
            return error
        
        key = args[0]
        value = self.storage.get(key)
        return self.formatter.bulk_string(value)
    
    def get_name(self) -> str:
        return "GET"


class DelCommand(Command):
    """DEL command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 1:
            return self.formatter.error("wrong number of arguments for 'del' command")
        
        deleted_count = 0
        for key in args:
            if self.storage.delete(key):
                deleted_count += 1
        
        return self.formatter.integer(deleted_count)
    
    def get_name(self) -> str:
        return "DEL"


class ExistsCommand(Command):
    """EXISTS command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 1:
            return self.formatter.error("wrong number of arguments for 'exists' command")
        
        existing_count = 0
        for key in args:
            if self.storage.exists(key):
                existing_count += 1
        
        return self.formatter.integer(existing_count)
    
    def get_name(self) -> str:
        return "EXISTS"

class RpushCommand(Command):
    """RPUSH command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'rpush' command")
        
        key = args[0]
        values = args[1:]
        
        # Use the rpush method from storage
        new_length = self.storage.rpush(key, *values)
        return self.formatter.integer(new_length)
    
    def get_name(self) -> str:
        return "RPUSH"


class LrangeCommand(Command):
    """LRANGE command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        error = self.validate_args(args, 3)
        if error:
            return error
        
        key = args[0]
        try:
            start = int(args[1])
            stop = int(args[2])
        except ValueError:
            return self.formatter.error("value is not an integer or out of range")
        
        # Get the range from storage
        result = self.storage.lrange(key, start, stop)
        
        if result is None:
            # List doesn't exist, return empty array
            return self.formatter.array([])
        
        # Convert list elements to bulk strings
        bulk_strings = [self.formatter.bulk_string(item) for item in result]
        return self.formatter.array(bulk_strings)
    
    def get_name(self) -> str:
        return "LRANGE"


class LpushCommand(Command):
    """LPUSH command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'lpush' command")
        
        key = args[0]
        values = args[1:]
        
        # Use the lpush method from storage
        new_length = self.storage.lpush(key, *values)
        return self.formatter.integer(new_length)
    
    def get_name(self) -> str:
        return "LPUSH"

class LlenCommand(Command):
    """LLEN command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 1:
            return self.formatter.error("wrong number of arguments for 'llen' command")
        
        key = args[0]
        length = self.storage.llen(key)
        return self.formatter.integer(length)
    
    def get_name(self) -> str:
        return "LLEN"

class LpopCommand(Command):
    """LPOP command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 1 or len(args) > 2:
            return self.formatter.error("wrong number of arguments for 'lpop' command")
        
        key = args[0]
        count = 1  # Default count
        
        # Parse optional count parameter
        if len(args) == 2:
            try:
                count = int(args[1])
                if count < 0:
                    return self.formatter.error("value is not an integer or out of range")
            except ValueError:
                return self.formatter.error("value is not an integer or out of range")
        
        # Get the popped elements
        popped_elements = self.storage.lpop(key, count)
        
        if popped_elements is None:
            # Key doesn't exist, is not a list, or is empty
            if count == 1:
                return self.formatter.bulk_string(None)  # Single element: return null
            else:
                return self.formatter.array([])  # Multiple elements: return empty array
        
        # Return the popped elements
        if count == 1:
            # Single element: return as bulk string
            return self.formatter.bulk_string(popped_elements[0])
        else:
            # Multiple elements: return as array
            bulk_strings = [self.formatter.bulk_string(item) for item in popped_elements]
            return self.formatter.array(bulk_strings)
    
    def get_name(self) -> str:
        return "LPOP"