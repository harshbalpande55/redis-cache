"""
Redis command implementations using Command pattern.
Each command is a separate class for better organization and extensibility.
"""
from abc import ABC, abstractmethod
from typing import List, Optional
import time

from app.protocol import RedisResponseFormatter
from app.storage import StorageBackend


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
    
    def __init__(self, storage, blocking_manager=None):
        super().__init__(storage)
        self.blocking_manager = blocking_manager
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'rpush' command")
        
        key = args[0]
        values = args[1:]
        
        # Use the rpush method from storage
        new_length = self.storage.rpush(key, *values)
        
        # Notify waiting clients if there are any
        if self.blocking_manager and self.blocking_manager.has_waiting_clients(key):
            # This is a simplified notification - in a real implementation,
            # this would trigger the blocking clients to wake up
            pass
        
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


class BlpopCommand(Command):
    """BLPOP command implementation with blocking support."""
    
    def __init__(self, storage, blocking_manager=None):
        super().__init__(storage)
        self.blocking_manager = blocking_manager
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'blpop' command")
        
        keys = args[:-1]  # All arguments except the last one are keys
        timeout_str = args[-1]  # Last argument is timeout
        
        try:
            timeout = float(timeout_str)
            if timeout < 0:
                return self.formatter.error("timeout is negative")
        except ValueError:
            return self.formatter.error("timeout is not a float or out of range")
        
        # Try to pop from each key in order
        for key in keys:
            value = self.storage.blpop(key)
            if value is not None:
                # Found an element, return it
                return self.formatter.array([
                    self.formatter.bulk_string(key),
                    self.formatter.bulk_string(value)
                ])
        
        # No elements available, this should trigger blocking behavior
        # Return a special response that indicates blocking is needed with timeout
        return f"BLOCKING_REQUIRED:{timeout}".encode('utf-8')
    
    def get_name(self) -> str:
        return "BLPOP"


class XaddCommand(Command):
    """XADD command implementation for Redis streams."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 3:
            return self.formatter.error("wrong number of arguments for 'xadd' command")
        
        key = args[0]
        entry_id = args[1]
        
        # Parse field-value pairs
        if len(args) % 2 != 0:
            return self.formatter.error("wrong number of arguments for 'xadd' command")
        
        fields = {}
        for i in range(2, len(args), 2):
            field = args[i]
            value = args[i + 1]
            fields[field] = value
        
        # Validate entry ID and handle auto-generation
        validation_result = self._validate_entry_id(key, entry_id)
        if validation_result is None:
            # Explicit ID is valid, use it
            final_id = entry_id
        elif validation_result.startswith("ERR"):
            # Validation error
            return self.formatter.error(validation_result)
        else:
            # Auto-generated ID
            final_id = validation_result
        
        # Add entry to stream
        try:
            result_id = self.storage.xadd(key, final_id, fields)
            return self.formatter.bulk_string(result_id)
        except Exception as e:
            return self.formatter.error(f"ERR {str(e)}")
    
    def _validate_entry_id(self, key: str, entry_id: str) -> Optional[str]:
        """Validate entry ID according to Redis rules."""
        # Handle auto-generation cases
        if entry_id == "*":
            # Auto-generate both time and sequence
            return self._generate_auto_id(key, None, None)
        elif entry_id.endswith("-*"):
            # Auto-generate sequence number for given time
            try:
                time_part = entry_id[:-2]  # Remove "-*"
                time_ms = int(time_part)
                return self._generate_auto_id(key, time_ms, None)
            except ValueError:
                return "ERR Invalid entry ID format"
        
        # Parse the entry ID (format: millisecondsTime-sequenceNumber)
        try:
            if '-' not in entry_id:
                return "ERR Invalid entry ID format"
            
            time_part, seq_part = entry_id.split('-', 1)
            time_ms = int(time_part)
            seq_num = int(seq_part)
        except ValueError:
            return "ERR Invalid entry ID format"
        
        # Check minimum ID requirement (must be greater than 0-0)
        if time_ms == 0 and seq_num == 0:
            return "ERR The ID specified in XADD must be greater than 0-0"
        
        # Get the stream to check for existing entries
        stream = self.storage.get_stream(key)
        if stream is None:
            # Stream doesn't exist, any valid ID is acceptable
            return None
        
        if not stream:
            # Stream is empty, any valid ID is acceptable
            return None
        
        # Find the highest entry ID in the stream
        highest_id = None
        highest_time = -1
        highest_seq = -1
        
        for existing_id in stream.keys():
            try:
                if '-' not in existing_id:
                    continue
                
                existing_time_part, existing_seq_part = existing_id.split('-', 1)
                existing_time = int(existing_time_part)
                existing_seq = int(existing_seq_part)
                
                # Check if this is the highest ID so far
                if (existing_time > highest_time or 
                    (existing_time == highest_time and existing_seq > highest_seq)):
                    highest_time = existing_time
                    highest_seq = existing_seq
                    highest_id = existing_id
                    
            except ValueError:
                continue
        
        # If no valid entries found, any valid ID is acceptable
        if highest_id is None:
            return None
        
        # Validate that the new ID is greater than the highest existing ID
        if time_ms < highest_time:
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        elif time_ms == highest_time and seq_num <= highest_seq:
            return "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        
        return None
    
    def _generate_auto_id(self, key: str, time_ms: Optional[int], seq_num: Optional[int]) -> Optional[str]:
        """Generate an auto ID for the given parameters."""
        import time
        
        # If time is not specified, use current timestamp
        if time_ms is None:
            time_ms = int(time.time() * 1000)  # Convert to milliseconds
        
        # Get the stream to find the next sequence number
        stream = self.storage.get_stream(key)
        
        if stream is None or not stream:
            # Stream doesn't exist or is empty, start with sequence 1 (since 0-0 is invalid)
            if time_ms == 0:
                return "0-1"
            else:
                return f"{time_ms}-0"
        
        # Find the highest sequence number for the given time
        max_seq = -1
        for existing_id in stream.keys():
            try:
                if '-' not in existing_id:
                    continue
                
                existing_time_part, existing_seq_part = existing_id.split('-', 1)
                existing_time = int(existing_time_part)
                existing_seq = int(existing_seq_part)
                
                # If this entry has the same time, track the max sequence
                if existing_time == time_ms and existing_seq > max_seq:
                    max_seq = existing_seq
                    
            except ValueError:
                continue
        
        # Return the next sequence number
        next_seq = max_seq + 1
        return f"{time_ms}-{next_seq}"
    
    def get_name(self) -> str:
        return "XADD"


class XrangeCommand(Command):
    """XRANGE command implementation for Redis streams."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 3:
            return self.formatter.error("wrong number of arguments for 'xrange' command")
        
        key = args[0]
        start = args[1]
        end = args[2]
        count = None
        
        # Parse optional COUNT argument
        if len(args) >= 5 and args[3].upper() == "COUNT":
            try:
                count = int(args[4])
                if count < 0:
                    return self.formatter.error("COUNT must be a positive integer")
            except ValueError:
                return self.formatter.error("COUNT must be a positive integer")
        
        # Get the range from storage
        result = self.storage.xrange(key, start, end, count)
        
        if result is None:
            # Stream doesn't exist, return empty array
            return self.formatter.array([])
        
        # Convert result to RESP array format
        # Each entry is an array: [id, [field1, value1, field2, value2, ...]]
        entries = []
        for entry_id, fields in result:
            # Create field-value pairs array
            field_value_pairs = []
            for field, value in fields.items():
                field_value_pairs.append(self.formatter.bulk_string(field))
                field_value_pairs.append(self.formatter.bulk_string(value))
            
            # Create entry array: [id, [field1, value1, field2, value2, ...]]
            entry_array = [
                self.formatter.bulk_string(entry_id),
                self.formatter.array(field_value_pairs)
            ]
            entries.append(self.formatter.array(entry_array))
        
        return self.formatter.array(entries)
    
    def get_name(self) -> str:
        return "XRANGE"


class XreadCommand(Command):
    """XREAD command implementation for Redis streams."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 3:
            return self.formatter.error("wrong number of arguments for 'xread' command")
        
        # Parse arguments: XREAD [BLOCK timeout] STREAMS key1 key2 ... id1 id2 ...
        block_timeout = None
        streams_start_idx = 0
        
        # Check for BLOCK option
        if len(args) >= 3 and args[0].upper() == "BLOCK":
            try:
                block_timeout = int(args[1])
                if block_timeout < 0:
                    return self.formatter.error("timeout is negative")
                streams_start_idx = 2
            except ValueError:
                return self.formatter.error("timeout is not an integer or out of range")
        
        # Parse STREAMS section
        if args[streams_start_idx].upper() != "STREAMS":
            return self.formatter.error("syntax error")
        
        # Find the split point between keys and IDs
        streams_section = args[streams_start_idx + 1:]
        if len(streams_section) % 2 != 0:
            return self.formatter.error("syntax error")
        
        # Split into keys and IDs
        num_streams = len(streams_section) // 2
        keys = streams_section[:num_streams]
        ids = streams_section[num_streams:]
        
        # Create list of (stream_key, start_id) tuples
        streams = list(zip(keys, ids))
        
        # Read from streams
        result = self.storage.xread(streams)
        
        if not result:
            # No data available
            if block_timeout is not None:
                # This should trigger blocking behavior
                return f"BLOCKING_REQUIRED:{block_timeout}:{':'.join(keys)}:{':'.join(ids)}".encode('utf-8')
            else:
                # Return null array
                return self.formatter.array(None)
        
        # Convert result to RESP array format
        # Format: [[stream_key, [[id, [field1, value1, field2, value2, ...]], ...]], ...]
        entries = []
        for stream_key, stream_entries in result:
            # Create stream entry array
            stream_array = []
            for entry_id, fields in stream_entries:
                # Create field-value pairs array
                field_value_pairs = []
                for field, value in fields.items():
                    field_value_pairs.append(self.formatter.bulk_string(field))
                    field_value_pairs.append(self.formatter.bulk_string(value))
                
                # Create entry array: [id, [field1, value1, field2, value2, ...]]
                entry_array = [
                    self.formatter.bulk_string(entry_id),
                    self.formatter.array(field_value_pairs)
                ]
                stream_array.append(self.formatter.array(entry_array))
            
            # Create stream array: [stream_key, [entries...]]
            stream_entry = [
                self.formatter.bulk_string(stream_key),
                self.formatter.array(stream_array)
            ]
            entries.append(self.formatter.array(stream_entry))
        
        return self.formatter.array(entries)
    
    def get_name(self) -> str:
        return "XREAD"


class TypeCommand(Command):
    """TYPE command implementation."""
    
    def execute(self, args: List[str]) -> bytes:
        error = self.validate_args(args, 1)
        if error:
            return error
        
        key = args[0]
        key_type = self.storage.type(key)
        
        # Return "none" if key doesn't exist, otherwise return the type
        if key_type is None:
            return self.formatter.simple_string("none")
        else:
            return self.formatter.simple_string(key_type)
    
    def get_name(self) -> str:
        return "TYPE"


class IncrCommand(Command):
    """INCR command implementation for Redis."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 1:
            return self.formatter.error("wrong number of arguments for 'incr' command")
        
        key = args[0]
        
        # Get the current value
        current_value = self.storage.get(key)
        
        if current_value is None:
            # Key doesn't exist, start with 0
            new_value = 1
        else:
            # Key exists, try to parse as integer
            try:
                current_int = int(current_value)
                new_value = current_int + 1
            except ValueError:
                return self.formatter.error("ERR value is not an integer or out of range")
        
        # Set the new value
        self.storage.set(key, str(new_value))
        
        return self.formatter.integer(new_value)
    
    def get_name(self) -> str:
        return "INCR"


class MultiCommand(Command):
    """MULTI command implementation for Redis transactions."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 0:
            return self.formatter.error("wrong number of arguments for 'multi' command")
        
        # MULTI command starts a transaction
        # For now, we'll just return OK as the basic implementation
        # In a full implementation, this would set a transaction state
        return self.formatter.simple_string("OK")
    
    def get_name(self) -> str:
        return "MULTI"


class ExecCommand(Command):
    """EXEC command implementation for Redis transactions."""
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 0:
            return self.formatter.error("wrong number of arguments for 'exec' command")
        
        # EXEC command executes a transaction
        # For now, we'll return the error that EXEC without MULTI should return
        # In a full implementation, this would execute queued commands
        return self.formatter.error("ERR EXEC without MULTI")
    
    def get_name(self) -> str:
        return "EXEC"


class InfoCommand(Command):
    """INFO command implementation for Redis replication."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 1:
            return self.formatter.error("wrong number of arguments for 'info' command")
        
        section = args[0].lower()
        
        if section == "replication":
            # Return replication information based on server role
            if self.server and self.server.is_replica:
                info = "role:slave\r\n"
            else:
                # Master replication info
                info = f"role:master\r\nmaster_replid:{self.server.master_replid}\r\nmaster_repl_offset:{self.server.master_repl_offset}\r\n"
            return self.formatter.bulk_string(info)
        else:
            # For other sections, return empty info for now
            return self.formatter.bulk_string("")
    
    def get_name(self) -> str:
        return "INFO"


class ReplconfCommand(Command):
    """REPLCONF command implementation for Redis replication handshake."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 1:
            return self.formatter.error("wrong number of arguments for 'replconf' command")
        
        subcommand = args[0].lower()
        
        if subcommand == "listening-port":
            if len(args) != 2:
                return self.formatter.error("wrong number of arguments for 'replconf listening-port' command")
            # Acknowledge the replica's listening port
            return self.formatter.simple_string("OK")
        
        elif subcommand == "capa":
            if len(args) != 2:
                return self.formatter.error("wrong number of arguments for 'replconf capa' command")
            # Acknowledge the replica's capabilities
            return self.formatter.simple_string("OK")
        
        elif subcommand == "ack":
            if len(args) != 2:
                return self.formatter.error("wrong number of arguments for 'replconf ack' command")
            
            try:
                replica_offset = int(args[1])
                # Update replica's replication offset
                if self.server:
                    # Note: In a real implementation, we would need to identify which replica
                    # is sending this ACK. For now, we'll just acknowledge the ACK.
                    # The server would need to track which connection sent this command.
                    pass
                return self.formatter.simple_string("OK")
            except ValueError:
                return self.formatter.error("invalid offset value for 'replconf ack' command")
        
        elif subcommand == "getack":
            if len(args) != 2:
                return self.formatter.error("wrong number of arguments for 'replconf getack' command")
            
            # Handle REPLCONF GETACK * command
            if args[1] == "*":
                if self.server:
                    # Return ACK information for all replicas
                    # For now, return a simple ACK with current master offset
                    master_offset = self.server.master_repl_offset
                    return self.formatter.array([
                        self.formatter.bulk_string("REPLCONF"),
                        self.formatter.bulk_string("ACK"),
                        self.formatter.bulk_string(str(master_offset))
                    ])
                else:
                    return self.formatter.array([
                        self.formatter.bulk_string("REPLCONF"),
                        self.formatter.bulk_string("ACK"),
                        self.formatter.bulk_string("0")
                    ])
            else:
                return self.formatter.error("invalid argument for 'replconf getack' command")
        
        else:
            return self.formatter.error(f"unknown subcommand '{subcommand}' for 'replconf' command")
    
    def get_name(self) -> str:
        return "REPLCONF"


class PsyncCommand(Command):
    """PSYNC command implementation for Redis replication synchronization."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 2:
            return self.formatter.error("wrong number of arguments for 'psync' command")
        
        replication_id = args[0]
        offset = args[1]
        
        if self.server:
            master_replid = self.server.master_replid
            master_repl_offset = self.server.master_repl_offset
            
            # Check if we can do partial resynchronization
            if replication_id == master_replid and offset != "-1":
                try:
                    replica_offset = int(offset)
                    # Check if the replica's offset is in our backlog
                    if (self.server.replication_backlog and 
                        replica_offset >= self.server.replication_backlog[0][0]):
                        # Partial resynchronization possible
                        return self.formatter.simple_string(f"CONTINUE {master_replid}")
                except ValueError:
                    pass
        else:
            master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            master_repl_offset = 0
        
        # Full resynchronization needed
        fullresync_response = f"FULLRESYNC {master_replid} {master_repl_offset}"
        
        # Create empty RDB file (Redis Database file format)
        # Minimal valid empty RDB file
        empty_rdb = b"REDIS0009\xfa\x09redis-ver\x054.0.0\xfa\x0aredis-bits\xc0@\xfa\x05ctime\xc2m\x8b\x9f\xc2\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-preamble\xc0\x00\xfe\x00\xfb\x02\x00\x00\xff\x8a\x82\xf8\xe4\x89\x96\x8e"
        
        # Combine FULLRESYNC response and RDB file
        fullresync_bytes = self.formatter.simple_string(fullresync_response)
        
        # Send RDB file in special format: $<length>\r\n<contents> (no trailing \r\n)
        rdb_length = len(empty_rdb)
        rdb_response = f"${rdb_length}\r\n".encode() + empty_rdb
        
        return fullresync_bytes + rdb_response
    
    def get_name(self) -> str:
        return "PSYNC"


class WaitCommand(Command):
    """WAIT command implementation for waiting for replica acknowledgments."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 2:
            return self.formatter.error("wrong number of arguments for 'wait' command")
        
        try:
            numreplicas = int(args[0])
            timeout = int(args[1])
        except ValueError:
            return self.formatter.error("value is not an integer or out of range")
        
        if not self.server:
            # If no server context, return 0 (no replicas)
            return self.formatter.integer(0)
        
        # If no replicas connected, return 0 immediately
        if not self.server.connected_replicas:
            return self.formatter.integer(0)
        
        # If numreplicas is 0, return 0 immediately
        if numreplicas == 0:
            return self.formatter.integer(0)
        
        # Check if the previous command was SET - if not, return number of connected replicas
        if self.server.prev_command != "SET":
            return self.formatter.integer(len(self.server.connected_replicas))
        
        # Use the server's async wait method
        import asyncio
        try:
            # Get the current event loop or create a new one
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, we need to handle this differently
                # For now, we'll use a synchronous approach with proper waiting
                return self._execute_sync_wait(numreplicas, timeout)
            else:
                # Run the async wait method
                ack_count = loop.run_until_complete(self.server.wait_for_replica_acks(numreplicas, timeout))
                return self.formatter.integer(ack_count)
        except RuntimeError:
            # No event loop running, use sync approach
            return self._execute_sync_wait(numreplicas, timeout)
    
    def _execute_sync_wait(self, numreplicas: int, timeout: int) -> bytes:
        """Synchronous implementation of WAIT command."""
        import time
        
        # Check if the previous command was SET - if not, return number of connected replicas
        if self.server.prev_command != "SET":
            return self.formatter.integer(len(self.server.connected_replicas))
        
        # Set up for waiting
        self.server.waiting_for_acks = True
        self.server.replica_ack_counter = 0
        self.server.ack_received_from.clear()
        
        # Send REPLCONF GETACK to all replicas
        for reader, writer, offset in self.server.connected_replicas:
            try:
                getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                writer.write(getack_command.encode())
                # Note: We can't use await here since this is not an async method
                # The flush will happen when the connection is processed
            except Exception as e:
                print(f"Failed to send GETACK to replica: {e}")
        
        # Wait for ACKs with timeout
        start_time = time.time()
        timeout_seconds = timeout / 1000.0  # Convert milliseconds to seconds
        
        try:
            while True:
                # Check if we have enough ACKs
                if self.server.replica_ack_counter >= numreplicas:
                    print(f"WAIT: Got {self.server.replica_ack_counter} ACKs, requested {numreplicas}")
                    return self.formatter.integer(self.server.replica_ack_counter)
                
                # Check if timeout has expired
                if time.time() - start_time >= timeout_seconds:
                    print(f"WAIT: Timeout reached, got {self.server.replica_ack_counter} ACKs, requested {numreplicas}")
                    return self.formatter.integer(self.server.replica_ack_counter)
                
                # Small delay to prevent busy waiting
                time.sleep(0.001)  # 1ms delay
        finally:
            # Clean up waiting state
            self.server.waiting_for_acks = False
            self.server.ack_received_from.clear()
    
    def get_name(self) -> str:
        return "WAIT"


class ConfigCommand(Command):
    """CONFIG command implementation for Redis configuration."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) < 2:
            return self.formatter.error("wrong number of arguments for 'config' command")
        
        subcommand = args[0].upper()
        
        if subcommand == "GET":
            if len(args) != 2:
                return self.formatter.error("wrong number of arguments for 'config get' command")
            
            key = args[1]
            
            # Handle RDB-related config
            if key == "dir":
                if self.server and 'dir' in self.server.config:
                    return self.formatter.array([b"dir", self.server.config['dir'].encode()])
                else:
                    return self.formatter.array([b"dir", b"."])
            elif key == "dbfilename":
                if self.server and 'dbfilename' in self.server.config:
                    return self.formatter.array([b"dbfilename", self.server.config['dbfilename'].encode()])
                else:
                    return self.formatter.array([b"dbfilename", b"dump.rdb"])
            elif key == "save":
                return self.formatter.array([b"save", b"900 1 300 10 60 10000"])
            else:
                return self.formatter.array([])
        
        elif subcommand == "SET":
            if len(args) != 3:
                return self.formatter.error("wrong number of arguments for 'config set' command")
            
            key = args[1]
            value = args[2]
            
            # For simplicity, just return OK for any config set
            return self.formatter.simple_string("OK")
        
        else:
            return self.formatter.error(f"Unknown subcommand or wrong number of arguments for 'config' command")
    
    def get_name(self) -> str:
        return "CONFIG"


class SaveCommand(Command):
    """SAVE command implementation for creating RDB snapshots."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 0:
            return self.formatter.error("wrong number of arguments for 'save' command")
        
        # For simplicity, just return OK
        # In a real implementation, this would create an RDB file
        return self.formatter.simple_string("OK")
    
    def get_name(self) -> str:
        return "SAVE"


class BgsaveCommand(Command):
    """BGSAVE command implementation for background RDB snapshots."""
    
    def __init__(self, storage: StorageBackend, server=None):
        super().__init__(storage)
        self.server = server
    
    def execute(self, args: List[str]) -> bytes:
        if len(args) != 0:
            return self.formatter.error("wrong number of arguments for 'bgsave' command")
        
        # For simplicity, just return OK
        # In a real implementation, this would start a background save
        return self.formatter.simple_string("Background saving started")
    
    def get_name(self) -> str:
        return "BGSAVE"