import asyncio


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
                
                # For this stage, respond with +PONG\r\n to any command
                writer.write(b"+PONG\r\n")
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
