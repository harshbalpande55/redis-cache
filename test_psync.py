#!/usr/bin/env python3
import socket
import time

# Wait for server to start
time.sleep(1)

# Connect to server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 6380))

# Send PSYNC command
psync_cmd = b'*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n'
s.send(psync_cmd)

# Receive response
response = s.recv(1024)
print('PSYNC Response:', response.decode())

s.close()
