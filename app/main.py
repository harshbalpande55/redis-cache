import asyncio
import time

# Global storage for key-value pairs with expiration
# Format: {key: {"value": value, "expires_at": timestamp}}
storage = {}


def parse_redis_command(data):
    """Parse Redis protocol command from bytes"""
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


def handle_echo_command(args):
    """Handle ECHO command - return the message"""
    if len(args) != 1:
        return b"-ERR wrong number of arguments for 'echo' command\r\n"
    
    message = args[0]
    return f"${len(message)}\r\n{message}\r\n".encode('utf-8')


def handle_ping_command(args):
    """Handle PING command"""
    if len(args) == 0:
        return b"+PONG\r\n"
    elif len(args) == 1:
        message = args[0]
        return f"${len(message)}\r\n{message}\r\n".encode('utf-8')
    else:
        return b"-ERR wrong number of arguments for 'ping' command\r\n"

def handle_set_command(args):
    """Handle SET command"""
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'set' command\r\n"
    
    key = args[0]
    value = args[1]
    
    # Check for PX option (expiration in milliseconds)
    expires_at = None
    if len(args) == 4 and args[2].upper() == "PX":
        try:
            px_milliseconds = int(args[3])
            expires_at = time.time() + (px_milliseconds / 1000.0)  # Convert ms to seconds
        except ValueError:
            return b"-ERR value is not an integer or out of range\r\n"
    
    # Store the key-value pair with optional expiration
    storage[key] = {
        "value": value,
        "expires_at": expires_at
    }
    
    return b"+OK\r\n"

def handle_get_command(args):
    """Handle GET command"""
    if len(args) != 1:
        return b"-ERR wrong number of arguments for 'get' command\r\n"
    
    key = args[0]
    if key in storage:
        entry = storage[key]
        
        # Check if the key has expired
        if entry["expires_at"] is not None and time.time() > entry["expires_at"]:
            # Key has expired, remove it and return null
            del storage[key]
            return b"$-1\r\n"
        
        # Key exists and hasn't expired
        value = entry["value"]
        return f"${len(value)}\r\n{value}\r\n".encode('utf-8')
    else:
        return b"$-1\r\n"  # Redis null response for non-existent keys

async def handle_client(reader, writer):
    """Handle a single client connection using async I/O"""
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
                command, args = parse_redis_command(data)
                
                if command is None:
                    # Invalid command format
                    response = b"-ERR invalid command format\r\n"
                elif command == "PING":
                    response = handle_ping_command(args)
                elif command == "ECHO":
                    response = handle_echo_command(args)
                elif command == "SET":
                    response = handle_set_command(args)
                elif command == "GET":
                    response = handle_get_command(args)
                else:
                    # Unknown command
                    response = f"-ERR unknown command '{command}'\r\n".encode('utf-8')
                
                # Send response to client
                writer.write(response)
                await writer.drain()  # Ensure data is sent
                
            except ConnectionResetError:
                # Client disconnected unexpectedly
                break
    finally:
        writer.close()
        await writer.wait_closed()


async def main():
    # # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # # Uncomment this to pass the first stage
    # #
    # Create async server that handles multiple clients concurrently
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    
    # Start serving clients
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
