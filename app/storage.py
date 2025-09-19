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
    def type(self, key: str) -> Optional[str]:
        """Get the type of a key. Returns None if key doesn't exist."""
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
    
    def type(self, key: str) -> Optional[str]:
        """Get the type of a key. Returns None if key doesn't exist."""
        if key not in self._data:
            return None
        return self._data[key]["type"]