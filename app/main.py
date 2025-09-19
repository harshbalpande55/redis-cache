"""
Main entry point for the Redis server.
Uses the new modular architecture with proper design patterns.
"""
import asyncio

from .redis_server import RedisServer


async def main():
    """Main entry point for the Redis server."""
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # Create and start the Redis server
    redis_server = RedisServer()
    await redis_server.start_server("localhost", 6379)


if __name__ == "__main__":
    asyncio.run(main())
