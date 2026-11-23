import socket
from communication.protocol import send_message, receive_message
import threading

HOST = "127.0.0.1"
PORT = 5001

# Processes an incoming request
def handle_client(conn):
    with conn:
        try:
            request = receive_message(conn)

            if request["type"] == "PING":
                response = {
                    "status": "success",
                    "message": "PONG"
                }

            else:
                response = {
                    "status": "error",
                    "message": "Unknown request type"
                }

            send_message(conn, response)

        except (ConnectionError, OSError, ValueError, KeyError) as error:
            print(f"Client error: {error}")


# Starts the storage node TCP server
def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen()

        print(f"Storage node listening on {HOST}:{PORT}")

        while True:
            conn, address = server.accept()
            print(f"Connection received from {address}")

            client_thread = threading.Thread(
                target=handle_client,
                args=(conn,),
                daemon=True
            )

            client_thread.start()

if __name__ == "__main__":
    start_server()
