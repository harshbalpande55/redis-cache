"""
RDB (Redis Database) file parser for reading and writing RDB files.
Handles the Redis RDB file format for data persistence.
"""
import struct
import time
from typing import Dict, Any, Optional, Tuple, List
from io import BytesIO


class RDBParser:
    """Parser for Redis RDB files."""
    
    def __init__(self):
        self.version = None
        self.redis_version = None
        self.redis_bits = None
        self.ctime = None
        self.used_mem = None
        self.aof_preamble = None
    
    def parse_rdb_file(self, rdb_data: bytes) -> Dict[str, Any]:
        """
        Parse an RDB file and return the data as a dictionary.
        
        Args:
            rdb_data: Raw RDB file bytes
            
        Returns:
            Dictionary containing the parsed data
        """
        if not rdb_data:
            return {}
        
        data = {}
        stream = BytesIO(rdb_data)
        
        try:
            # Parse RDB header
            self._parse_header(stream)
            
            # Parse database data
            while True:
                opcode = stream.read(1)
                if not opcode:
                    break
                
                if opcode == b'\xfe':  # SELECTDB
                    db_number = self._read_length(stream)
                    print(f"Selecting database {db_number}")
                elif opcode == b'\xfb':  # RESIZEDB
                    # Read hash table size and expiry hash table size
                    hash_table_size = self._read_length(stream)
                    expiry_hash_table_size = self._read_length(stream)
                    print(f"ResizeDB: hash_table_size={hash_table_size}, expiry_hash_table_size={expiry_hash_table_size}")
                elif opcode == b'\xff':  # EOF
                    break
                elif opcode == b'\xfd':  # EXPIRETIME_MS
                    expire_time = struct.unpack('<Q', stream.read(8))[0]
                    # Read the next opcode for the actual key
                    key_opcode = stream.read(1)
                    if key_opcode:
                        key, value = self._parse_key_value(stream, key_opcode)
                        if key:
                            data[key] = {
                                'value': value,
                                'expires_at': expire_time / 1000.0  # Convert to seconds
                            }
                elif opcode == b'\xfc':  # EXPIRETIME
                    # Read the timestamp as a 4-byte Unix timestamp
                    expire_time = struct.unpack('<I', stream.read(4))[0]
                    
                    # Read the next opcode for the actual key
                    key_opcode = stream.read(1)
                    if key_opcode:
                        key, value = self._parse_key_value(stream, key_opcode)
                        if key:
                            data[key] = {
                                'value': value,
                                'expires_at': float(expire_time)  # Unix timestamp in seconds
                            }
                elif opcode in [b'\x00', b'\x01', b'\x02', b'\x03', b'\x04', b'\x05', b'\x06', b'\x07', b'\x08', b'\x09', b'\x0a', b'\x0b', b'\x0c', b'\x0d', b'\x0e', b'\x0f']:
                    # Key-value pair
                    key, value = self._parse_key_value(stream, opcode)
                    if key:
                        data[key] = {
                            'value': value,
                            'expires_at': None
                        }
                else:
                    # Unknown opcode, skip
                    print(f"Unknown opcode: {opcode.hex()}")
                    break
        
        except Exception as e:
            print(f"Error parsing RDB file: {e}")
            return {}
        
        return data
    
    def _parse_header(self, stream: BytesIO) -> None:
        """Parse the RDB file header."""
        # Read magic string "REDIS"
        magic = stream.read(5)
        if magic != b'REDIS':
            raise ValueError("Invalid RDB file: missing REDIS magic string")
        
        # Read version
        version_bytes = stream.read(4)
        self.version = version_bytes.decode('ascii')
        
        # Parse auxiliary fields
        while True:
            aux_opcode = stream.read(1)
            if aux_opcode == b'\xfa':  # AUX field
                aux_key = self._read_string(stream)
                aux_value = self._read_string(stream)
                
                if aux_key == 'redis-ver':
                    self.redis_version = aux_value
                elif aux_key == 'redis-bits':
                    self.redis_bits = aux_value
                elif aux_key == 'ctime':
                    self.ctime = aux_value
                elif aux_key == 'used-mem':
                    self.used_mem = aux_value
                elif aux_key == 'aof-preamble':
                    self.aof_preamble = aux_value
            else:
                # Put back the byte and break
                stream.seek(-1, 1)
                break
    
    def _parse_key_value(self, stream: BytesIO, opcode: bytes) -> Tuple[Optional[str], Any]:
        """Parse a key-value pair based on the opcode."""
        try:
            # Read key
            key = self._read_string(stream)
            if not key:
                return None, None
            
            # Parse value based on opcode
            if opcode == b'\x00':  # String
                value = self._read_string(stream)
                return key, value
            elif opcode == b'\x01':  # List
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, values
            elif opcode == b'\x02':  # Set
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, set(values)
            elif opcode == b'\x03':  # Sorted Set
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    member = self._read_string(stream)
                    score_bytes = stream.read(8)
                    score = struct.unpack('<d', score_bytes)[0]
                    values[member] = score
                return key, values
            elif opcode == b'\x04':  # Hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    field = self._read_string(stream)
                    value = self._read_string(stream)
                    values[field] = value
                return key, values
            elif opcode == b'\x05':  # ZSet (ZIPLIST)
                # For simplicity, treat as regular list
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, values
            elif opcode == b'\x06':  # Hash (ZIPLIST)
                # For simplicity, treat as regular hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    field = self._read_string(stream)
                    value = self._read_string(stream)
                    values[field] = value
                return key, values
            elif opcode == b'\x07':  # List (ZIPLIST)
                # For simplicity, treat as regular list
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, values
            elif opcode == b'\x08':  # Intset
                # For simplicity, treat as regular set
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, set(values)
            elif opcode == b'\x09':  # Sorted Set (ZIPLIST)
                # For simplicity, treat as regular hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    member = self._read_string(stream)
                    value = self._read_string(stream)
                    values[member] = value
                return key, values
            elif opcode == b'\x0a':  # Hash (ZIPLIST)
                # For simplicity, treat as regular hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    field = self._read_string(stream)
                    value = self._read_string(stream)
                    values[field] = value
                return key, values
            elif opcode == b'\x0b':  # List (ZIPLIST)
                # For simplicity, treat as regular list
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, values
            elif opcode == b'\x0c':  # Intset
                # For simplicity, treat as regular set
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, set(values)
            elif opcode == b'\x0d':  # Sorted Set (ZIPLIST)
                # For simplicity, treat as regular hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    member = self._read_string(stream)
                    value = self._read_string(stream)
                    values[member] = value
                return key, values
            elif opcode == b'\x0e':  # Hash (ZIPLIST)
                # For simplicity, treat as regular hash
                length = self._read_length(stream)
                values = {}
                for _ in range(length):
                    field = self._read_string(stream)
                    value = self._read_string(stream)
                    values[field] = value
                return key, values
            elif opcode == b'\x0f':  # List (ZIPLIST)
                # For simplicity, treat as regular list
                length = self._read_length(stream)
                values = []
                for _ in range(length):
                    values.append(self._read_string(stream))
                return key, values
            else:
                print(f"Unknown value type opcode: {opcode.hex()}")
                return None, None
        
        except Exception as e:
            print(f"Error parsing key-value pair: {e}")
            return None, None
    
    def _read_string(self, stream: BytesIO) -> str:
        """Read a string from the RDB stream."""
        first_byte = stream.read(1)
        if not first_byte:
            return ""
        
        first_byte_val = first_byte[0]
        
        # Check if this is an encoded string
        if first_byte_val >= 0xc0:
            # Handle encoded integers as strings
            if first_byte_val == 0xc0:  # 8-bit integer
                value = struct.unpack('b', stream.read(1))[0]
                return str(value)
            elif first_byte_val == 0xc1:  # 16-bit integer
                value = struct.unpack('<h', stream.read(2))[0]
                return str(value)
            elif first_byte_val == 0xc2:  # 32-bit integer
                value = struct.unpack('<i', stream.read(4))[0]
                return str(value)
            else:
                # Other encoded values - treat as length
                stream.seek(-1, 1)  # Put back the byte
                length = self._read_length(stream)
        else:
            # Put back the byte and read length normally
            stream.seek(-1, 1)
            length = self._read_length(stream)
        
        if length == 0:
            return ""
        
        data = stream.read(length)
        return data.decode('utf-8')
    
    def _read_length(self, stream: BytesIO) -> int:
        """Read a length field from the RDB stream."""
        first_byte = stream.read(1)[0]
        
        if first_byte < 0x40:  # 6-bit length
            return first_byte
        elif first_byte < 0x80:  # 14-bit length
            second_byte = stream.read(1)[0]
            return ((first_byte & 0x3f) << 8) | second_byte
        elif first_byte == 0x80:  # 32-bit length
            length_bytes = stream.read(4)
            return struct.unpack('<I', length_bytes)[0]
        elif first_byte == 0x81:  # 64-bit length
            length_bytes = stream.read(8)
            return struct.unpack('<Q', length_bytes)[0]
        elif first_byte >= 0xc0:  # Special encoded values
            # Handle encoded integers
            if first_byte == 0xc0:  # 8-bit integer
                return struct.unpack('b', stream.read(1))[0]
            elif first_byte == 0xc1:  # 16-bit integer
                return struct.unpack('<h', stream.read(2))[0]
            elif first_byte == 0xc2:  # 32-bit integer
                return struct.unpack('<i', stream.read(4))[0]
            else:
                # Other encoded values
                return first_byte & 0x3f
        else:
            # Encoded length - this is the issue!
            # The length is encoded in the lower 6 bits
            return first_byte & 0x3f
    
    def create_empty_rdb(self) -> bytes:
        """Create an empty RDB file."""
        # RDB header
        rdb = b'REDIS'  # Magic string
        rdb += b'0009'  # Version
        
        # Auxiliary fields
        rdb += b'\xfa'  # AUX field
        rdb += b'\x09redis-ver\x054.0.0'  # Redis version
        rdb += b'\xfa\x0aredis-bits\xc0@'  # Redis bits (64-bit)
        rdb += b'\xfa\x05ctime\xc2m\x8b\x9f\xc2'  # Creation time
        rdb += b'\xfa\x08used-mem\xc2\xb0\xc4\x10\x00'  # Used memory
        rdb += b'\xfa\x08aof-preamble\xc0\x00'  # AOF preamble
        
        # Database selector
        rdb += b'\xfe\x00'  # SELECTDB 0
        
        # EOF marker
        rdb += b'\xff'  # EOF
        
        # CRC64 checksum (simplified)
        rdb += b'\x8a\x82\xf8\xe4\x89\x96\x8e'
        
        return rdb
