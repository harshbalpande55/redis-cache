"""
Main entry point for the Redis server.
Uses the new modular architecture with proper design patterns.
"""
import asyncio
import argparse
import sys

from .redis_server import RedisServer


async def main():
    """Main entry point for the Redis server."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Redis server')
    parser.add_argument('--port', type=int, default=6379, help='Port to listen on (default: 6379)')
    
    args = parser.parse_args()
    
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # Create and start the Redis server
    redis_server = RedisServer()
    await redis_server.start_server("localhost", args.port)


if __name__ == "__main__":
    asyncio.run(main())
