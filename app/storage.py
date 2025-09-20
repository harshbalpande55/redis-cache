"""
Storage abstraction layer for Redis data.
Supports different storage backends and expiration.
"""
import time
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, Union


class StorageBackend(ABC):
    """Abstract base class for storage backends."""
    
    @abstractmethod
    def set(self, key: str, value: str, expires_at: Optional[float] = None) -> None:
        """Set a key-value pair with optional expiration."""
        pass
    
    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Get a value by key. Returns None if key doesn't exist or has expired."""
        pass
    
    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if key existed, False otherwise."""
        pass
    
    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if a key exists and hasn't expired."""
        pass
    
    @abstractmethod
    def rpush(self, key: str, *values: str) -> int:
        """Push values to the right of a list. Returns the new length of the list."""
        pass
    
    @abstractmethod
    def get_list(self, key: str) -> Optional[List[str]]:
        """Get a list by key. Returns None if key doesn't exist or is not a list."""
        pass
    
    @abstractmethod
    def lrange(self, key: str, start: int, stop: int) -> Optional[List[str]]:
        """Get a range of elements from a list. Returns None if key doesn't exist or is not a list."""
        pass
    
    @abstractmethod
    def lpush(self, key: str, *values: str) -> int:
        """Push values to the left of a list. Returns the new length of the list."""
        pass
    
    @abstractmethod
    def llen(self, key: str) -> int:
        """Get the length of a list. Returns 0 if key doesn't exist or is not a list."""
        pass

    @abstractmethod
    def lpop(self, key: str, count: int = 1) -> Optional[List[str]]:
        """Pop values from the left of a list. Returns None if key doesn't exist or is not a list."""
        pass
    
    @abstractmethod
    def blpop(self, key: str) -> Optional[str]:
        """Pop a value from the left of a list. Returns None if key doesn't exist or is not a list."""
        pass

    @abstractmethod
    def xadd(self, key: str, entry_id: str, fields: Dict[str, str]) -> str:
        """Add an entry to a stream. Returns the entry ID."""
        pass
    
    @abstractmethod
    def get_stream(self, key: str) -> Optional[Dict[str, Dict[str, str]]]:
        """Get a stream by key. Returns None if key doesn't exist or is not a stream."""
        pass

    @abstractmethod
    def xrange(self, key: str, start: str, end: str, count: Optional[int] = None) -> Optional[List[tuple]]:
        """Get a range of entries from a stream. Returns list of (id, fields) tuples."""
        pass

    @abstractmethod
    def xread(self, streams: List[tuple]) -> List[tuple]:
        """Read from multiple streams. Returns list of (stream_key, entries) tuples."""
        pass

    @abstractmethod
    def type(self, key: str) -> Optional[str]:
        """Get the type of a key. Returns None if key doesn't exist."""
        pass
    
    # Sorted Set methods
    @abstractmethod
    def zadd(self, key: str, *score_member_pairs: Union[str, float]) -> int:
        """Add members to a sorted set. Returns the number of new members added."""
        pass
    
    @abstractmethod
    def zrank(self, key: str, member: str) -> Optional[int]:
        """Get the rank of a member in a sorted set. Returns None if key doesn't exist or member not found."""
        pass
    
    @abstractmethod
    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> Optional[List]:
        """Get a range of members from a sorted set. Returns None if key doesn't exist."""
        pass
    
    @abstractmethod
    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set. Returns 0 if key doesn't exist."""
        pass
    
    @abstractmethod
    def zscore(self, key: str, member: str) -> Optional[float]:
        """Get the score of a member in a sorted set. Returns None if key doesn't exist or member not found."""
        pass
    
    @abstractmethod
    def zrem(self, key: str, *members: str) -> int:
        """Remove members from a sorted set. Returns the number of members removed."""
        pass
    
    # Geospatial methods
    @abstractmethod
    def geoadd(self, key: str, *longitude_latitude_member_pairs: Union[str, float]) -> int:
        """Add geospatial locations to a sorted set. Returns the number of new locations added."""
        pass
    
    @abstractmethod
    def geopos(self, key: str, *members: str) -> List[Optional[tuple]]:
        """Get coordinates of members. Returns list of (longitude, latitude) tuples or None."""
        pass
    
    @abstractmethod
    def geodist(self, key: str, member1: str, member2: str, unit: str = "m") -> Optional[float]:
        """Calculate distance between two members. Returns distance in specified unit."""
        pass
    
    @abstractmethod
    def georadius(self, key: str, longitude: float, latitude: float, radius: float, unit: str = "m", withcoord: bool = False, withdist: bool = False, count: Optional[int] = None) -> List:
        """Find members within radius of a point. Returns list of members with optional coordinates and distances."""
        pass

class InMemoryStorage(StorageBackend):
    """In-memory storage implementation with expiration support."""
    
    def __init__(self):
        # Format: {key: {"value": value, "type": "string"|"list", "expires_at": timestamp}}
        self._data: Dict[str, Dict[str, Any]] = {}
    
    def set(self, key: str, value: str, expires_at: Optional[float] = None) -> None:
        """Set a key-value pair with optional expiration."""
        self._data[key] = {
            "value": value,
            "type": "string",
            "expires_at": expires_at
        }
    
    def get(self, key: str) -> Optional[str]:
        """Get a string value by key. Returns None if key doesn't exist, has expired, or is not a string."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, remove it
            del self._data[key]
            return None
        
        # Only return string values
        if entry["type"] == "string":
            return entry["value"]
        return None
    
    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if key existed, False otherwise."""
        if key in self._data:
            del self._data[key]
            return True
        return False
    
    def exists(self, key: str) -> bool:
        """Check if a key exists and hasn't expired."""
        return self.get(key) is not None
    
    def clear(self) -> None:
        """Clear all data (useful for testing)."""
        self._data.clear()
    
    def size(self) -> int:
        """Get the number of keys in storage."""
        # Clean up expired keys first
        current_time = time.time()
        expired_keys = [
            key for key, entry in self._data.items()
            if entry["expires_at"] is not None and current_time > entry["expires_at"]
        ]
        for key in expired_keys:
            del self._data[key]
        
        return len(self._data)
    
    def rpush(self, key: str, *values: str) -> int:
        """Push values to the right of a list. Returns the new length of the list."""
        if key not in self._data:
            # Create new list
            self._data[key] = {
                "value": list(values),
                "type": "list",
                "expires_at": None
            }
            return len(values)
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, create new list
            self._data[key] = {
                "value": list(values),
                "type": "list",
                "expires_at": None
            }
            return len(values)
        
        # If key exists but is not a list, convert it to a list
        if entry["type"] != "list":
            # Convert existing value to list and append new values
            existing_list = [str(entry["value"])] + list(values)
            self._data[key] = {
                "value": existing_list,
                "type": "list",
                "expires_at": entry["expires_at"]
            }
            return len(existing_list)
        
        # Key exists and is a list, append values
        entry["value"].extend(values)
        return len(entry["value"])
    
    def get_list(self, key: str) -> Optional[List[str]]:
        """Get a list by key. Returns None if key doesn't exist, has expired, or is not a list."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, remove it
            del self._data[key]
            return None
        
        # Only return list values
        if entry["type"] == "list":
            return entry["value"]
        return None

    def lrange(self, key: str, start: int, stop: int) -> Optional[List[str]]:
        """Get a range of elements from a list. Returns None if key doesn't exist or is not a list."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return None
        
        # Only return list values
        if entry["type"] != "list":
            return None
        
        list_data = entry["value"]
        list_length = len(list_data)
        
        # Handle negative indexes (count from the end)
        if start < 0:
            start = max(0, list_length + start)
        if stop < 0:
            stop = max(0, list_length + stop)
        
        # Early returns for invalid ranges
        if start >= list_length or start > stop:
            return []
        
        # Clamp stop to valid range
        stop = min(stop, list_length - 1)
        
        # Return the range (stop is inclusive)
        return list_data[start:stop + 1]
    
    def lpush(self, key: str, *values: str) -> int:
        """Push values to the left of a list. Returns the new length of the list."""
        if key not in self._data:
            # Create new list (reverse values to maintain correct order)
            self._data[key] = {
                "value": list(values),
                "type": "list",
                "expires_at": None
            }
            return len(values)
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, create new list
            self._data[key] = {
                "value": list(values),
                "type": "list",
                "expires_at": None
            }
            return len(values)
        
        # If key exists but is not a list, convert it to a list
        if entry["type"] != "list":
            # Convert existing value to list and prepend new values
            # Reverse values so they appear in the correct order when prepended
            existing_list = [str(entry["value"])] + list(reversed(values))
            self._data[key] = {
                "value": existing_list,
                "type": "list",
                "expires_at": entry["expires_at"]
            }
            return len(existing_list)
        
        # Key exists and is a list, prepend values
        # Reverse values so they appear in the correct order when prepended
        entry["value"] = list(reversed(values)) + entry["value"]
        return len(entry["value"])

    def llen(self, key: str) -> int:
        """Get the length of a list. Returns 0 if key doesn't exist or is not a list."""
        if key not in self._data:
            return 0
        
        entry = self._data[key]
        if entry["type"] != "list":
            return 0
        return len(entry["value"])
    
    def lpop(self, key: str, count: int = 1) -> Optional[List[str]]:
        """Pop values from the left of a list. Returns None if key doesn't exist or is not a list."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, remove it
            del self._data[key]
            return None
        
        # Only pop from list values
        if entry["type"] != "list":
            return None
        
        # Check if list is empty
        if not entry["value"]:
            return None
        
        # Pop the requested number of elements (or all if count > list length)
        popped_elements = []
        actual_count = min(count, len(entry["value"]))
        
        for _ in range(actual_count):
            popped_elements.append(entry["value"].pop(0))
        
        return popped_elements
    
    def blpop(self, key: str) -> Optional[str]:
        """Pop a value from the left of a list. Returns None if key doesn't exist or is not a list."""
        # BLPOP is essentially the same as LPOP with count=1, but returns a single string
        result = self.lpop(key, 1)
        if result is None or not result:
            return None
        return result[0]
    
    def xadd(self, key: str, entry_id: str, fields: Dict[str, str]) -> str:
        """Add an entry to a stream. Returns the entry ID."""
        if key not in self._data:
            # Create new stream
            self._data[key] = {
                "value": {},
                "type": "stream",
                "expires_at": None
            }
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, create new stream
            self._data[key] = {
                "value": {},
                "type": "stream",
                "expires_at": None
            }
            entry = self._data[key]
        
        # If key exists but is not a stream, convert it to a stream
        if entry["type"] != "stream":
            # Convert existing value to stream
            self._data[key] = {
                "value": {},
                "type": "stream",
                "expires_at": entry["expires_at"]
            }
            entry = self._data[key]
        
        # Add entry to stream
        entry["value"][entry_id] = fields
        return entry_id
    
    def get_stream(self, key: str) -> Optional[Dict[str, Dict[str, str]]]:
        """Get a stream by key. Returns None if key doesn't exist or is not a stream."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, remove it
            del self._data[key]
            return None
        
        # Only return stream values
        if entry["type"] == "stream":
            return entry["value"]
        return None

    def xrange(self, key: str, start: str, end: str, count: Optional[int] = None) -> Optional[List[tuple]]:
        """Get a range of entries from a stream. Returns list of (id, fields) tuples."""
        stream = self.get_stream(key)
        if stream is None:
            return None
        
        if not stream:
            return []
        
        # Parse start and end IDs
        def parse_id(entry_id: str) -> tuple:
            """Parse entry ID into (time_ms, seq_num) tuple."""
            if entry_id == "-":
                return (0, 0)  # From beginning
            elif entry_id == "+":
                return (float('inf'), float('inf'))  # To end
            elif '-' not in entry_id:
                raise ValueError("Invalid entry ID format")
            time_part, seq_part = entry_id.split('-', 1)
            return (int(time_part), int(seq_part))
        
        try:
            start_parsed = parse_id(start)
            end_parsed = parse_id(end)
        except ValueError:
            return None
        
        # Filter entries within the range
        result = []
        for entry_id, fields in stream.items():
            try:
                entry_parsed = parse_id(entry_id)
                
                # Check if entry is within range (inclusive)
                if (entry_parsed >= start_parsed and entry_parsed <= end_parsed):
                    result.append((entry_id, fields))
            except ValueError:
                continue  # Skip invalid entry IDs
        
        # Sort by entry ID (chronological order)
        result.sort(key=lambda x: parse_id(x[0]))
        
        # Apply COUNT limit if specified
        if count is not None:
            if count == 0:
                result = []  # COUNT 0 returns empty array
            elif count > 0:
                result = result[:count]
        
        return result

    def xread(self, streams: List[tuple]) -> List[tuple]:
        """Read from multiple streams. Returns list of (stream_key, entries) tuples."""
        result = []
        
        for stream_key, start_id in streams:
            # For XREAD, we need to get entries with ID > start_id (exclusive)
            # So we need to find the next ID after start_id
            stream = self.get_stream(stream_key)
            if stream is None or not stream:
                continue
            
            # Handle $ ID - means "only new entries" (equivalent to max ID in stream)
            if start_id == "$":
                # Find the maximum ID in the stream
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
                        
                        # Check if this is the maximum ID so far
                        if (time_ms > max_time or 
                            (time_ms == max_time and seq_num > max_seq)):
                            max_time = time_ms
                            max_seq = seq_num
                            max_id = entry_id
                            
                    except ValueError:
                        continue
                
                # If no valid entries found, $ means no new entries
                if max_id is None:
                    continue
                
                # Use the max ID as the start_id for filtering
                start_id = max_id
            
            # Parse start_id to find the next ID
            def parse_id(entry_id: str) -> tuple:
                """Parse entry ID into (time_ms, seq_num) tuple."""
                if entry_id == "-":
                    return (0, 0)  # From beginning
                elif entry_id == "+":
                    return (float('inf'), float('inf'))  # To end
                elif '-' not in entry_id:
                    raise ValueError("Invalid entry ID format")
                time_part, seq_part = entry_id.split('-', 1)
                return (int(time_part), int(seq_part))
            
            try:
                start_parsed = parse_id(start_id)
            except ValueError:
                continue
            
            # Find entries with ID > start_id
            stream_entries = []
            for entry_id, fields in stream.items():
                try:
                    entry_parsed = parse_id(entry_id)
                    # Only include entries with ID > start_id
                    if entry_parsed > start_parsed:
                        stream_entries.append((entry_id, fields))
                except ValueError:
                    continue  # Skip invalid entry IDs
            
            # Sort by entry ID (chronological order)
            stream_entries.sort(key=lambda x: parse_id(x[0]))
            
            if stream_entries:
                result.append((stream_key, stream_entries))
        
        return result

    def type(self, key: str) -> Optional[str]:
        """Get the type of a key. Returns None if key doesn't exist."""
        if key not in self._data:
            return None
        return self._data[key]["type"]
    
    def zadd(self, key: str, *score_member_pairs: Union[str, float]) -> int:
        """Add members to a sorted set. Returns the number of new members added."""
        if len(score_member_pairs) % 2 != 0:
            raise ValueError("Score-member pairs must be even in number")
        
        if key not in self._data:
            # Create new sorted set
            self._data[key] = {
                "value": {},  # member -> score mapping
                "type": "zset",
                "expires_at": None
            }
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, create new sorted set
            self._data[key] = {
                "value": {},
                "type": "zset",
                "expires_at": None
            }
            entry = self._data[key]
        
        # If key exists but is not a sorted set, convert it
        if entry["type"] != "zset":
            self._data[key] = {
                "value": {},
                "type": "zset",
                "expires_at": entry["expires_at"]
            }
            entry = self._data[key]
        
        # Add members to sorted set
        new_members = 0
        for i in range(0, len(score_member_pairs), 2):
            score = float(score_member_pairs[i])
            member = str(score_member_pairs[i + 1])
            
            if member not in entry["value"]:
                new_members += 1
            
            entry["value"][member] = score
        
        return new_members
    
    def zrank(self, key: str, member: str) -> Optional[int]:
        """Get the rank of a member in a sorted set. Returns None if key doesn't exist or member not found."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return None
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return None
        
        if member not in entry["value"]:
            return None
        
        # Sort members by score, then by member name for ties
        sorted_members = sorted(entry["value"].items(), key=lambda x: (x[1], x[0]))
        
        # Find the rank (0-based index)
        for rank, (sorted_member, _) in enumerate(sorted_members):
            if sorted_member == member:
                return rank
        
        return None
    
    def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> Optional[List]:
        """Get a range of members from a sorted set. Returns None if key doesn't exist."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return None
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return None
        
        if not entry["value"]:
            return []
        
        # Sort members by score, then by member name for ties
        sorted_members = sorted(entry["value"].items(), key=lambda x: (x[1], x[0]))
        
        # Handle negative indices
        total_members = len(sorted_members)
        if start < 0:
            start = max(0, total_members + start)
        if stop < 0:
            stop = max(0, total_members + stop)
        
        # Clamp indices to valid range
        start = max(0, min(start, total_members - 1))
        stop = max(0, min(stop, total_members - 1))
        
        # Early return for invalid ranges
        if start > stop:
            return []
        
        # Get the range
        result = []
        for i in range(start, stop + 1):
            if i < len(sorted_members):
                member, score = sorted_members[i]
                if withscores:
                    result.extend([member, str(score)])
                else:
                    result.append(member)
        
        return result
    
    def zcard(self, key: str) -> int:
        """Get the number of members in a sorted set. Returns 0 if key doesn't exist."""
        if key not in self._data:
            return 0
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return 0
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return 0
        
        return len(entry["value"])
    
    def zscore(self, key: str, member: str) -> Optional[float]:
        """Get the score of a member in a sorted set. Returns None if key doesn't exist or member not found."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return None
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return None
        
        return entry["value"].get(member)
    
    def zrem(self, key: str, *members: str) -> int:
        """Remove members from a sorted set. Returns the number of members removed."""
        if key not in self._data:
            return 0
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return 0
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return 0
        
        removed_count = 0
        for member in members:
            if member in entry["value"]:
                del entry["value"][member]
                removed_count += 1
        
        # If sorted set is empty, remove the key
        if not entry["value"]:
            del self._data[key]
        
        return removed_count
    
    def _encode_geohash(self, longitude: float, latitude: float) -> int:
        """Encode longitude and latitude into a geohash score for sorted set storage."""
        # Redis geohash algorithm - exact implementation from the provided code
        
        # Constants
        MIN_LATITUDE = -85.05112878
        MAX_LATITUDE = 85.05112878
        MIN_LONGITUDE = -180
        MAX_LONGITUDE = 180
        
        LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
        LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
        
        # Clamp coordinates to valid ranges
        longitude = max(MIN_LONGITUDE, min(MAX_LONGITUDE, longitude))
        latitude = max(MIN_LATITUDE, min(MAX_LATITUDE, latitude))
        
        # Normalize to the range 0-2^26
        normalized_latitude = 2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
        normalized_longitude = 2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE
        
        # Truncate to integers
        normalized_latitude = int(normalized_latitude)
        normalized_longitude = int(normalized_longitude)
        
        return self._interleave(normalized_latitude, normalized_longitude)
    
    def _interleave(self, x: int, y: int) -> int:
        """Interleave two 32-bit integers into a 64-bit geohash."""
        x = self._spread_int32_to_int64(x)
        y = self._spread_int32_to_int64(y)
        
        y_shifted = y << 1
        return x | y_shifted
    
    def _spread_int32_to_int64(self, v: int) -> int:
        """Spread a 32-bit integer to 64-bit with interleaved bits."""
        v = v & 0xFFFFFFFF
        
        v = (v | (v << 16)) & 0x0000FFFF0000FFFF
        v = (v | (v << 8)) & 0x00FF00FF00FF00FF
        v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
        v = (v | (v << 2)) & 0x3333333333333333
        v = (v | (v << 1)) & 0x5555555555555555
        
        return v
    
    def _decode_geohash(self, geohash: int) -> tuple:
        """Decode geohash score back to longitude and latitude."""
        # Redis geohash decode algorithm - based on the provided decode function
        
        # Constants
        MIN_LATITUDE = -85.05112878
        MAX_LATITUDE = 85.05112878
        MIN_LONGITUDE = -180
        MAX_LONGITUDE = 180
        
        LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
        LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
        
        # Align bits of both latitude and longitude to take even-numbered position
        y = geohash >> 1
        x = geohash
        
        # Compact bits back to 32-bit ints
        grid_latitude_number = self._compact_int64_to_int32(x)
        grid_longitude_number = self._compact_int64_to_int32(y)
        
        return self._convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number)
    
    def _compact_int64_to_int32(self, v: int) -> int:
        """Compact a 64-bit integer with interleaved bits back to a 32-bit integer."""
        v = v & 0x5555555555555555
        v = (v | (v >> 1)) & 0x3333333333333333
        v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
        v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
        v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
        v = (v | (v >> 16)) & 0x00000000FFFFFFFF
        return v
    
    def _convert_grid_numbers_to_coordinates(self, grid_latitude_number: int, grid_longitude_number: int) -> tuple:
        """Convert grid numbers back to coordinates."""
        # Constants
        MIN_LATITUDE = -85.05112878
        MAX_LATITUDE = 85.05112878
        MIN_LONGITUDE = -180
        MAX_LONGITUDE = 180
        
        LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
        LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
        
        # Calculate the grid boundaries
        grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / (2**26))
        grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / (2**26))
        grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / (2**26))
        grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / (2**26))
        
        # Calculate the center point of the grid cell
        latitude = (grid_latitude_min + grid_latitude_max) / 2
        longitude = (grid_longitude_min + grid_longitude_max) / 2
        return (latitude, longitude)
    
    def _haversine_distance(self, lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """Calculate the great circle distance between two points on Earth in meters."""
        import math
        
        # Convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
        
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        
        # Radius of earth in meters (exact value used by Redis)
        r = 6372797.560856
        return c * r
    
    def geoadd(self, key: str, *longitude_latitude_member_pairs: Union[str, float]) -> int:
        """Add geospatial locations to a sorted set. Returns the number of new locations added."""
        if len(longitude_latitude_member_pairs) % 3 != 0:
            raise ValueError("Longitude-latitude-member pairs must be multiples of 3")
        
        if key not in self._data:
            # Create new sorted set
            self._data[key] = {
                "value": {},  # member -> (geohash, longitude, latitude)
                "type": "zset",
                "expires_at": None
            }
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, create new sorted set
            self._data[key] = {
                "value": {},
                "type": "zset",
                "expires_at": None
            }
            entry = self._data[key]
        
        # If key exists but is not a sorted set, convert it
        if entry["type"] != "zset":
            self._data[key] = {
                "value": {},
                "type": "zset",
                "expires_at": entry["expires_at"]
            }
            entry = self._data[key]
        
        # Add geospatial locations
        new_locations = 0
        for i in range(0, len(longitude_latitude_member_pairs), 3):
            longitude = float(longitude_latitude_member_pairs[i])
            latitude = float(longitude_latitude_member_pairs[i + 1])
            member = str(longitude_latitude_member_pairs[i + 2])
            
            # Coordinates are already validated in the command layer
            
            if member not in entry["value"]:
                new_locations += 1
            
            # Store geohash as score and coordinates for later retrieval
            geohash = self._encode_geohash(longitude, latitude)
            entry["value"][member] = geohash
        
        return new_locations
    
    def geopos(self, key: str, *members: str) -> List[Optional[tuple]]:
        """Get coordinates of members. Returns list of (longitude, latitude) tuples or None."""
        if key not in self._data:
            return [None] * len(members)
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return [None] * len(members)
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return [None] * len(members)
        
        result = []
        for member in members:
            if member in entry["value"]:
                geohash = entry["value"][member]
                # Convert geohash to integer if it's a float
                if isinstance(geohash, float):
                    geohash = int(geohash)
                latitude, longitude = self._decode_geohash(geohash)
                result.append((longitude, latitude))
            else:
                result.append(None)
        
        return result
    
    def geodist(self, key: str, member1: str, member2: str, unit: str = "m") -> Optional[float]:
        """Calculate distance between two members. Returns distance in specified unit."""
        if key not in self._data:
            return None
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return None
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return None
        
        if member1 not in entry["value"] or member2 not in entry["value"]:
            return None
        
        geohash1 = entry["value"][member1]
        geohash2 = entry["value"][member2]
        
        # Convert geohash to integer if it's a float
        if isinstance(geohash1, float):
            geohash1 = int(geohash1)
        if isinstance(geohash2, float):
            geohash2 = int(geohash2)
        
        lat1, lon1 = self._decode_geohash(geohash1)
        lat2, lon2 = self._decode_geohash(geohash2)
        
        # Calculate distance in meters
        distance_m = self._haversine_distance(lon1, lat1, lon2, lat2)
        
        # Convert to requested unit
        if unit.lower() == "m":
            return distance_m
        elif unit.lower() == "km":
            return distance_m / 1000.0
        elif unit.lower() == "mi":
            return distance_m / 1609.344
        elif unit.lower() == "ft":
            return distance_m * 3.28084
        else:
            raise ValueError(f"Invalid unit: {unit}")
    
    def georadius(self, key: str, longitude: float, latitude: float, radius: float, unit: str = "m", withcoord: bool = False, withdist: bool = False, count: Optional[int] = None) -> List:
        """Find members within radius of a point. Returns list of members with optional coordinates and distances."""
        if key not in self._data:
            return []
        
        entry = self._data[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            del self._data[key]
            return []
        
        # Only work with sorted sets
        if entry["type"] != "zset":
            return []
        
        # Convert radius to meters
        if unit.lower() == "km":
            radius_m = radius * 1000.0
        elif unit.lower() == "mi":
            radius_m = radius * 1609.344
        elif unit.lower() == "ft":
            radius_m = radius / 3.28084
        else:  # meters
            radius_m = radius
        
        # Find members within radius
        results = []
        for member, geohash in entry["value"].items():
            # Convert geohash to integer if it's a float
            if isinstance(geohash, float):
                geohash = int(geohash)
            lat, lon = self._decode_geohash(geohash)
            distance = self._haversine_distance(longitude, latitude, lon, lat)
            if distance <= radius_m:
                result = [member]
                if withdist:
                    # Convert distance back to requested unit
                    if unit.lower() == "km":
                        dist_value = distance / 1000.0
                    elif unit.lower() == "mi":
                        dist_value = distance / 1609.344
                    elif unit.lower() == "ft":
                        dist_value = distance * 3.28084
                    else:  # meters
                        dist_value = distance
                    result.append(dist_value)
                if withcoord:
                    result.extend([lon, lat])
                results.append(result)
        
        # Sort by distance if withdist is True
        if withdist:
            results.sort(key=lambda x: x[1])
        
        # Apply count limit
        if count is not None and count > 0:
            results = results[:count]
        
        return results