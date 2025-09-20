"""
Main Redis server implementation.
Orchestrates all components using proper separation of concerns.
"""
import asyncio
import time
from typing import Optional, List

from app.storage import StorageBackend, InMemoryStorage
from app.protocol import RedisProtocolParser, RedisResponseFormatter
from app.command_registry import CommandRegistry
from app.blocking_manager import BlockingManager
from app.rdb_parser import RDBParser
from app.commands import (
    PingCommand, EchoCommand, SetCommand, GetCommand, 
    RpushCommand, DelCommand, ExistsCommand, LrangeCommand, LpushCommand,
    LlenCommand, LpopCommand, BlpopCommand, XaddCommand, XrangeCommand, XreadCommand, TypeCommand, IncrCommand, InfoCommand, ReplconfCommand, PsyncCommand, WaitCommand, ConfigCommand, SaveCommand, BgsaveCommand, KeysCommand, SubscribeCommand
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
        
        # Replica offset tracking - track per client
        self.replica_offsets = {}  # client_id -> offset
        
        # WAIT command tracking
        self.prev_command = None  # Track the previous command
        self.replica_ack_counter = 0  # Global ACK counter for WAIT
        self.replica_ack_lock = asyncio.Lock()  # Lock for ACK counter
        self.waiting_for_acks = False  # Flag to indicate if we're waiting for ACKs
        self.ack_received_from = set()  # Track which replicas have sent ACKs
        
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
        
        # Configuration parameters
        self.config = {
            'dir': '.',
            'dbfilename': 'dump.rdb'
        }
        
        # RDB parser
        self.rdb_parser = RDBParser()
        
        # Pub/Sub subscription management
        self.subscriptions = {}  # channel -> set of client writers
        self.client_subscriptions = {}  # client_id -> set of channels
        self.subscribed_clients = set()  # client_id -> bool (track clients in subscribed mode)
    
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
            WaitCommand(self.storage, self),
            ConfigCommand(self.storage, self),
            SaveCommand(self.storage, self),
            BgsaveCommand(self.storage, self),
            KeysCommand(self.storage),
            SubscribeCommand(self.storage, self),
            # MULTI and EXEC are now handled directly in handle_client
        ]
        
        for command in commands:
            self.command_registry.register(command)
    
    def set_replica_config(self, master_host: str, master_port: int) -> None:
        """Set this server as a replica of the specified master."""
        self.is_replica = True
        self.master_host = master_host
        self.master_port = master_port
    
    def set_config(self, dir_path: str, dbfilename: str) -> None:
        """Set configuration parameters for RDB persistence."""
        self.config['dir'] = dir_path
        self.config['dbfilename'] = dbfilename
    
    def subscribe_client_to_channel(self, client_id: int, writer: asyncio.StreamWriter, channel: str) -> int:
        """Subscribe a client to a channel. Returns the number of channels the client is subscribed to."""
        # Add client to channel's subscription list
        if channel not in self.subscriptions:
            self.subscriptions[channel] = set()
        self.subscriptions[channel].add(writer)
        
        # Add channel to client's subscription list
        if client_id not in self.client_subscriptions:
            self.client_subscriptions[client_id] = set()
        self.client_subscriptions[client_id].add(channel)
        
        # Mark client as being in subscribed mode
        self.subscribed_clients.add(client_id)
        
        # Return the number of channels this client is subscribed to
        return len(self.client_subscriptions[client_id])
    
    def unsubscribe_client_from_channel(self, client_id: int, writer: asyncio.StreamWriter, channel: str) -> int:
        """Unsubscribe a client from a channel. Returns the number of channels the client is subscribed to."""
        # Remove client from channel's subscription list
        if channel in self.subscriptions:
            self.subscriptions[channel].discard(writer)
            if not self.subscriptions[channel]:
                del self.subscriptions[channel]
        
        # Remove channel from client's subscription list
        if client_id in self.client_subscriptions:
            self.client_subscriptions[client_id].discard(channel)
            if not self.client_subscriptions[client_id]:
                del self.client_subscriptions[client_id]
                # Remove client from subscribed mode if no more subscriptions
                self.subscribed_clients.discard(client_id)
                return 0
            return len(self.client_subscriptions[client_id])
        
        return 0
    
    def cleanup_client_subscriptions(self, client_id: int, writer: asyncio.StreamWriter) -> None:
        """Clean up all subscriptions for a client when they disconnect."""
        if client_id in self.client_subscriptions:
            channels = list(self.client_subscriptions[client_id])
            for channel in channels:
                self.unsubscribe_client_from_channel(client_id, writer, channel)
        
        # Remove client from subscribed mode
        self.subscribed_clients.discard(client_id)
    
    def is_client_subscribed(self, client_id: int) -> bool:
        """Check if a client is in subscribed mode."""
        return client_id in self.subscribed_clients
    
    def is_command_allowed_in_subscribed_mode(self, command: str) -> bool:
        """Check if a command is allowed in subscribed mode."""
        allowed_commands = {
            'SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE', 
            'PING', 'QUIT', 'RESET'
        }
        return command.upper() in allowed_commands
    
    def load_rdb_data(self) -> None:
        """Load data from RDB file if it exists."""
        import os
        import time
        
        rdb_path = os.path.join(self.config['dir'], self.config['dbfilename'])
        
        if os.path.exists(rdb_path):
            try:
                with open(rdb_path, 'rb') as f:
                    rdb_data = f.read()
                
                if rdb_data:
                    print(f"Loading RDB file: {rdb_path}")
                    parsed_data = self.rdb_parser.parse_rdb_file(rdb_data)
                    
                    # Load data into storage
                    current_time = time.time()
                    loaded_count = 0
                    
                    for key, data in parsed_data.items():
                        value = data['value']
                        expires_at = data.get('expires_at')
                        
                        # Load all keys, including expired ones
                        # The GET command will handle expiry checking
                        
                        if isinstance(value, str):
                            # String value
                            self.storage.set(key, value, expires_at)
                            loaded_count += 1
                        elif isinstance(value, list):
                            # List value
                            if value:  # Only if list is not empty
                                self.storage.rpush(key, *value)
                                # Set expiry time for the list if it exists
                                if expires_at is not None:
                                    # We need to manually set the expiry time for the list
                                    # since rpush doesn't support expiry parameter
                                    if key in self.storage._data:
                                        self.storage._data[key]["expires_at"] = expires_at
                                loaded_count += 1
                        elif isinstance(value, dict):
                            # Hash value - for now, store as string representation
                            # In a full implementation, we'd need hash support in storage
                            self.storage.set(key, str(value), expires_at)
                            loaded_count += 1
                        elif isinstance(value, set):
                            # Set value - for now, store as string representation
                            # In a full implementation, we'd need set support in storage
                            self.storage.set(key, str(value), expires_at)
                            loaded_count += 1
                    
                    print(f"Loaded {loaded_count} keys from RDB file")
                else:
                    print("RDB file is empty")
            except Exception as e:
                print(f"Error loading RDB file: {e}")
        else:
            print(f"RDB file not found: {rdb_path}")
    
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
    
    def calculate_command_bytes(self, command: str, args: List[str]) -> int:
        """Calculate the number of bytes for a command in RESP format."""
        command_parts = [command] + args
        command_bytes = f"*{len(command_parts)}\r\n"
        for part in command_parts:
            command_bytes += f"${len(part)}\r\n{part}\r\n"
        return len(command_bytes.encode())
    
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
        
        # Initialize replica offset tracking
        if client_id not in self.replica_offsets:
            self.replica_offsets[client_id] = 0
        
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
                                    
                                    # Update replica offset for EXEC command
                                    command_bytes = self.calculate_command_bytes(command, args)
                                    self.replica_offsets[client_id] += command_bytes
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
                                    
                                    # Update replica offset for DISCARD command
                                    command_bytes = self.calculate_command_bytes(command, args)
                                    self.replica_offsets[client_id] += command_bytes
                            else:
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    response = self.response_formatter.error("ERR DISCARD without MULTI")
                                else:
                                    # Replica DISCARD commands don't send responses
                                    response = None
                                    
                                    # Update replica offset for DISCARD command
                                    command_bytes = self.calculate_command_bytes(command, args)
                                    self.replica_offsets[client_id] += command_bytes
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
                            
                            # Continue processing commands in replica mode
                            print(f"PSYNC completed for client {client_id}, continuing in replica mode")
                            # Set response to None to avoid sending it again
                            response = None
                            
                            # Start a separate task to handle replica commands
                            asyncio.create_task(self._handle_replica_connection(reader, writer, client_id))
                            # Exit the main loop for this connection
                            break
                        elif command == "REPLCONF" and len(args) > 0 and args[0].lower() == "ack":
                            # Handle REPLCONF ACK command and update replica offset
                            response = self.command_registry.execute_command(command, args)
                            # Update the replica's offset
                            if len(args) == 2:
                                try:
                                    replica_offset = int(args[1])
                                    self.update_replica_offset(writer, replica_offset)
                                    
                                    # Only increment ACK counter if we're waiting for ACKs and this replica hasn't ACKed yet
                                    if self.waiting_for_acks and writer not in self.ack_received_from:
                                        self.replica_ack_counter += 1
                                        self.ack_received_from.add(writer)
                                        print(f"ACK received: replica_offset={replica_offset}, ack_counter={self.replica_ack_counter}")
                                except ValueError:
                                    pass  # Invalid offset, ignore
                        elif command == "REPLCONF" and len(args) > 0 and args[0].lower() == "getack":
                            # Handle REPLCONF GETACK command - return current offset before processing
                            current_offset = self.replica_offsets[client_id]
                            ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(current_offset))}\r\n{current_offset}\r\n"
                            writer.write(ack_response.encode())
                            await writer.drain()
                            print(f"Replica {client_id} sent ACK response with offset {current_offset}")
                            
                            # Now update offset to include this REPLCONF command
                            command_bytes = self.calculate_command_bytes(command, args)
                            self.replica_offsets[client_id] += command_bytes
                            response = None  # No response needed for GETACK
                        else:
                            # Regular command execution
                            if self.client_transactions[client_id]['in_transaction']:
                                # Check if client is in subscribed mode and command is not allowed
                                if self.is_client_subscribed(client_id) and not self.is_command_allowed_in_subscribed_mode(command):
                                    response = self.response_formatter.error(f"ERR Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
                                else:
                                    # Handle PING command specially for subscribed mode in transactions
                                    if command == "PING" and self.is_client_subscribed(client_id):
                                        # In subscribed mode, PING returns ["pong", ""]
                                        response = self.response_formatter.array([
                                            self.response_formatter.bulk_string("pong"),
                                            self.response_formatter.bulk_string("")
                                        ])
                                    else:
                                        # Queue command for later execution
                                        self.client_transactions[client_id]['commands'].append((command, args))
                                    
                                    # Check if this is a replica connection
                                    is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                    
                                    if not is_replica:
                                        response = self.response_formatter.simple_string("QUEUED")
                                    else:
                                        # Replica commands in transactions don't send responses
                                        response = None
                                        
                                        # Update replica offset for queued commands
                                        command_bytes = self.calculate_command_bytes(command, args)
                                        self.replica_offsets[client_id] += command_bytes
                            else:
                                # Check if client is in subscribed mode and command is not allowed
                                if self.is_client_subscribed(client_id) and not self.is_command_allowed_in_subscribed_mode(command):
                                    response = self.response_formatter.error(f"ERR Can't execute '{command.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context")
                                else:
                                    # Handle PING command specially for subscribed mode
                                    if command == "PING" and self.is_client_subscribed(client_id):
                                        # In subscribed mode, PING returns ["pong", ""]
                                        response = self.response_formatter.array([
                                            self.response_formatter.bulk_string("pong"),
                                            self.response_formatter.bulk_string("")
                                        ])
                                    # Handle WAIT command specially since it needs async execution
                                    elif command == "WAIT":
                                        # Execute WAIT command with async support
                                        try:
                                            numreplicas = int(args[0])
                                            timeout = int(args[1])
                                            ack_count = await self.wait_for_replica_acks(numreplicas, timeout)
                                            response = self.response_formatter.integer(ack_count)
                                        except (ValueError, IndexError):
                                            response = self.response_formatter.error("wrong number of arguments for 'wait' command")
                                    else:
                                        # Execute other commands normally
                                        response = self.command_registry.execute_command(command, args)
                                
                                # Check if this is a replica connection
                                is_replica = is_replica_connection or self.client_transactions[client_id].get('is_replica', False)
                                
                                if not is_replica:
                                    # This is a regular client, propagate commands to replicas
                                    # Propagate only data-modifying commands to replicas
                                    if command not in ["PING", "REPLCONF", "PSYNC", "INFO", "GET", "LRANGE", "LLEN", "TYPE", "EXISTS", "ECHO", "WAIT"]:
                                        await self.propagate_command_to_replicas(command, args)
                                    
                                    # Increment replication offset only for data-modifying commands
                                    if command not in ["PING", "REPLCONF", "PSYNC", "INFO", "GET", "LRANGE", "LLEN", "TYPE", "EXISTS", "WAIT"]:
                                        command_bytes = len(data)
                                        self.increment_replication_offset(command_bytes)
                                    
                                    # Track the previous command for WAIT
                                    self.prev_command = command
                                else:
                                    # This is a replica receiving commands from master
                                    # Don't send response back to master, just process the command silently
                                    response = None
                                    
                                    # Update replica offset for all commands (including PING and REPLCONF)
                                    command_bytes = self.calculate_command_bytes(command, args)
                                    self.replica_offsets[client_id] += command_bytes
                        
                        # Check if this is a blocking command that needs special handling
                        if response and response.startswith(b"BLOCKING_REQUIRED"):
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
                        elif response and response.startswith(b"SUBSCRIBE_REQUIRED"):
                            # Handle SUBSCRIBE command
                            channel = response.decode('utf-8').split(':')[1]
                            subscription_count = self.subscribe_client_to_channel(client_id, writer, channel)
                            
                            # Send the SUBSCRIBE response: ["subscribe", channel, count]
                            subscribe_response = self.response_formatter.array([
                                self.response_formatter.bulk_string("subscribe"),
                                self.response_formatter.bulk_string(channel),
                                self.response_formatter.integer(subscription_count)
                            ])
                            writer.write(subscribe_response)
                            await writer.drain()
                            
                            # After SUBSCRIBE, the client enters pub/sub mode
                            # For now, we'll just keep the connection alive
                            # In a full implementation, we would handle PUBLISH commands here
                            # Multiple subscriptions are supported per client
                            print(f"Client {client_id} subscribed to channel '{channel}' (total subscriptions: {subscription_count})")
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
            
            # Clean up subscriptions for this client
            self.cleanup_client_subscriptions(client_id, writer)
            
            # Only clean up if this is not a replica connection
            # Replica connections are handled by the separate _handle_replica_connection task
            if not is_replica_connection:
                # Clean up transaction state for this client
                if client_id in self.client_transactions:
                    del self.client_transactions[client_id]
                
                # Clean up replica offset tracking
                if client_id in self.replica_offsets:
                    del self.replica_offsets[client_id]
                
                writer.close()
                await writer.wait_closed()
    
    
    async def _handle_replica_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, client_id: int) -> None:
        """Handle a replica connection by continuously reading and processing commands from the master."""
        print(f"Replica connection task started for client {client_id}, writer: {id(writer)}")
        try:
            buffer = b""
            last_ack_time = time.time()
            ack_interval = 1.0  # Send ACK every 1 second
            replica_offset = 0  # Track our replication offset
            
            while True:
                # Read data from master without timeout - this will block until data is available
                print(f"Replica {client_id} waiting for data from master...")
                data = await reader.read(1024)
                if not data:
                    print(f"Replica {client_id}: Master disconnected")
                    break
                
                print(f"Replica {client_id} received {len(data)} bytes from master: {data[:100]}...")
                buffer += data
                
                # Process complete commands from buffer
                while buffer:
                    try:
                        # Try to parse a command from the buffer
                        command, args = self.protocol_parser.parse_command(buffer)
                        if command:
                            print(f"Replica {client_id} received command from master: {command} {' '.join(args)}")
                            print(f"Replica {client_id} buffer length: {len(buffer)}, command length: {len(f'*{len([command] + args)}\\r\\n' + ''.join(f'${len(part)}\\r\\n{part}\\r\\n' for part in [command] + args))}")
                            
                            # Calculate the length of the processed command for offset tracking
                            command_bytes = f"*{len([command] + args)}\r\n"
                            for part in [command] + args:
                                command_bytes += f"${len(part)}\r\n{part}\r\n"
                            
                            # Handle REPLCONF GETACK command specially - it needs a response
                            if command == "REPLCONF" and args and args[0] == "GETACK":
                                print(f"Replica {client_id} received GETACK command, current offset: {replica_offset}")
                                # Send ACK response to master with current offset (before processing this command)
                                ack_response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n"
                                writer.write(ack_response.encode())
                                await writer.drain()
                                print(f"Replica {client_id} sent ACK response with offset {replica_offset}")
                                
                                # Now update offset to include this REPLCONF command
                                replica_offset += len(command_bytes.encode())
                            elif command == "REPLCONF" and args and args[0] == "ACK":
                                # Handle ACK response from replica to master
                                print(f"Replica {client_id} received ACK command: {command} {' '.join(args)}")
                                
                                # Update the master's ACK counter if we're waiting for ACKs
                                if self.waiting_for_acks and writer not in self.ack_received_from:
                                    self.replica_ack_counter += 1
                                    self.ack_received_from.add(writer)
                                    print(f"ACK received from replica {client_id}: ack_counter={self.replica_ack_counter}")
                                
                                # Update replica offset with the command length
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
                                print(f"Replica {client_id} sent ACK to master with offset {replica_offset}")
                        else:
                            # Incomplete command, wait for more data
                            break
                    except Exception as e:
                        print(f"Error parsing command in replica {client_id}: {e}")
                        break
        except Exception as e:
            print(f"Replica {client_id} connection error: {e}")
        finally:
            # Remove replica connection when it disconnects
            self.remove_replica_connection(writer)
            # Clean up subscriptions for this client
            self.cleanup_client_subscriptions(client_id, writer)
            # Clean up transaction state for this client
            if client_id in self.client_transactions:
                del self.client_transactions[client_id]
            # Clean up replica offset tracking
            if client_id in self.replica_offsets:
                del self.replica_offsets[client_id]
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
        
        # Load RDB data on startup
        self.load_rdb_data()
        
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
    
    async def wait_for_replica_acks(self, numreplicas: int, timeout: int) -> int:
        """Async method to wait for replica acknowledgments."""
        import time
        
        # Check if the previous command was SET - if not, return number of connected replicas
        if self.prev_command != "SET":
            return len(self.connected_replicas)
        
        # Set up for waiting
        self.waiting_for_acks = True
        self.replica_ack_counter = 0
        self.ack_received_from.clear()
        
        # Send REPLCONF GETACK to all replicas
        print(f"WAIT: Sending GETACK to {len(self.connected_replicas)} replicas")
        for i, (reader, writer, offset) in enumerate(self.connected_replicas):
            try:
                getack_command = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                writer.write(getack_command.encode())
                await writer.drain()
                print(f"WAIT: Sent GETACK to replica {i} with offset {offset}, writer: {id(writer)}")
                
                # Give the replica a moment to process the GETACK command
                await asyncio.sleep(0.01)
            except Exception as e:
                print(f"Failed to send GETACK to replica {i}: {e}")
        
        # Wait for ACKs with timeout
        start_time = time.time()
        timeout_seconds = timeout / 1000.0  # Convert milliseconds to seconds
        
        try:
            while True:
                # Check if we have enough ACKs
                if self.replica_ack_counter >= numreplicas:
                    print(f"WAIT: Got {self.replica_ack_counter} ACKs, requested {numreplicas}")
                    return self.replica_ack_counter
                
                # Check if timeout has expired
                if time.time() - start_time >= timeout_seconds:
                    print(f"WAIT: Timeout reached, got {self.replica_ack_counter} ACKs, requested {numreplicas}")
                    return self.replica_ack_counter
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.001)  # 1ms delay
        finally:
            # Clean up waiting state
            self.waiting_for_acks = False
            self.ack_received_from.clear()
    
    def get_stats(self) -> dict:
        """Get server statistics."""
        return {
            "registered_commands": self.command_registry.list_commands(),
            "storage_size": self.storage.size(),
            "storage_type": type(self.storage).__name__
        }
