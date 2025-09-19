import socket


def main():
    # # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    # # Uncomment this to pass the first stage
    # #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    connection, _ = server_socket.accept() # wait for client
    
    # Loop to handle multiple commands on the same connection
    while True:
        try:
            # Read data from the client
            data = connection.recv(1024)
            if not data:
                # Client disconnected
                break
            
            # For this stage, respond with +PONG\r\n to any command
            connection.send(b"+PONG\r\n")
        except ConnectionResetError:
            # Client disconnected unexpectedly
            break
    
    connection.close()


if __name__ == "__main__":
    main()
