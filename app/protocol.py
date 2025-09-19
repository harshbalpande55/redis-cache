"""
Redis protocol parser and response formatter.
Handles RESP (Redis Serialization Protocol) parsing and formatting.
"""
from typing import List, Optional, Tuple


class RedisProtocolError(Exception):
    """Exception raised for Redis protocol parsing errors."""
    pass


class RedisProtocolParser:
    """Parser for Redis protocol (RESP) commands."""
    
    @staticmethod
    def parse_command(data: bytes) -> Tuple[Optional[str], List[str]]:
        """
        Parse Redis protocol command from bytes.
        
        Args:
            data: Raw bytes from client
            
        Returns:
            Tuple of (command, args) or (None, []) if parsing fails
        """
        try:
            # Convert bytes to string and split by \r\n
            lines = data.decode('utf-8').strip().split('\r\n')
            
            if not lines or lines[0][0] != '*':
                return None, []
            
            # First line should be *<number of arguments>
            num_args = int(lines[0][1:])
            
            # Parse arguments
            args = []
            i = 1
            for _ in range(num_args):
                if i >= len(lines) or lines[i][0] != '$':
                    return None, []
                
                # $<length of string>
                str_length = int(lines[i][1:])
                i += 1
                
                if i >= len(lines):
                    return None, []
                
                # The actual string
                args.append(lines[i])
                i += 1
            
            if len(args) == 0:
                return None, []
            
            command = args[0].upper()
            return command, args[1:]
        
        except (ValueError, IndexError, UnicodeDecodeError):
            return None, []


class RedisResponseFormatter:
    """Formatter for Redis protocol responses."""
    
    @staticmethod
    def simple_string(value: str) -> bytes:
        """Format a simple string response (+OK, +PONG, etc.)."""
        return f"+{value}\r\n".encode('utf-8')
    
    @staticmethod
    def bulk_string(value: Optional[str]) -> bytes:
        """
        Format a bulk string response.
        Returns $-1\r\n for None (null response).
        """
        if value is None:
            return b"$-1\r\n"
        return f"${len(value)}\r\n{value}\r\n".encode('utf-8')
    
    @staticmethod
    def error(message: str) -> bytes:
        """Format an error response."""
        return f"-{message}\r\n".encode('utf-8')
    
    @staticmethod
    def integer(value: int) -> bytes:
        """Format an integer response."""
        return f":{value}\r\n".encode('utf-8')
    
    @staticmethod
    def array(values: List[bytes]) -> bytes:
        """Format an array response."""
        if values is None:
            return b"*-1\r\n"
        
        result = f"*{len(values)}\r\n".encode('utf-8')
        for value in values:
            result += value
        return result
