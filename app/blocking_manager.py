"""
Blocking client manager for BLPOP and other blocking commands.
Handles multiple clients waiting for elements to become available.
"""
import asyncio
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class BlockingClient:
    """Represents a client waiting for a blocking operation."""
    writer: asyncio.StreamWriter
    keys: List[str]  # List of keys the client is waiting for
    timestamp: float  # When the client started waiting
    command: str  # The blocking command (e.g., "BLPOP")


class BlockingManager:
    """Manages blocking clients for list operations."""
    
    def __init__(self):
        # Map of key -> list of waiting clients
        self._waiting_clients: Dict[str, List[BlockingClient]] = defaultdict(list)
        # Map of client writer -> blocking client info
        self._client_info: Dict[asyncio.StreamWriter, BlockingClient] = {}
    
    def add_blocking_client(self, writer: asyncio.StreamWriter, keys: List[str], command: str) -> None:
        """Add a client to the waiting list for the specified keys."""
        import time
        
        blocking_client = BlockingClient(
            writer=writer,
            keys=keys,
            timestamp=time.time(),
            command=command
        )
        
        # Add to waiting lists for each key
        for key in keys:
            self._waiting_clients[key].append(blocking_client)
        
        # Track client info
        self._client_info[writer] = blocking_client
    
    def remove_blocking_client(self, writer: asyncio.StreamWriter) -> None:
        """Remove a client from all waiting lists."""
        if writer not in self._client_info:
            return
        
        blocking_client = self._client_info[writer]
        
        # Remove from all key waiting lists
        for key in blocking_client.keys:
            if key in self._waiting_clients:
                try:
                    self._waiting_clients[key].remove(blocking_client)
                except ValueError:
                    pass  # Client might have been removed already
        
        # Remove client info
        del self._client_info[writer]
    
    def get_oldest_waiting_client(self, key: str) -> Optional[BlockingClient]:
        """Get the client that has been waiting the longest for the specified key."""
        if key not in self._waiting_clients or not self._waiting_clients[key]:
            return None
        
        # Sort by timestamp (oldest first)
        waiting_clients = sorted(self._waiting_clients[key], key=lambda c: c.timestamp)
        return waiting_clients[0]
    
    def remove_waiting_client(self, key: str, client: BlockingClient) -> None:
        """Remove a specific client from the waiting list for a key."""
        if key in self._waiting_clients:
            try:
                self._waiting_clients[key].remove(client)
            except ValueError:
                pass  # Client might have been removed already
        
        # Remove client info
        if client.writer in self._client_info:
            del self._client_info[client.writer]
    
    def has_waiting_clients(self, key: str) -> bool:
        """Check if there are any clients waiting for the specified key."""
        return key in self._waiting_clients and len(self._waiting_clients[key]) > 0
    
    def get_waiting_count(self, key: str) -> int:
        """Get the number of clients waiting for the specified key."""
        return len(self._waiting_clients.get(key, []))
    
    def cleanup_disconnected_clients(self) -> None:
        """Remove clients that have disconnected."""
        disconnected_writers = []
        
        for writer, blocking_client in self._client_info.items():
            if writer.is_closing():
                disconnected_writers.append(writer)
        
        for writer in disconnected_writers:
            self.remove_blocking_client(writer)
