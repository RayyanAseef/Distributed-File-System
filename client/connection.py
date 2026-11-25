
import socket

from communication.protocol import (
    send_message,
    receive_message,
    receive_exact
)


COORDINATOR_HOST = "127.0.0.1"
COORDINATOR_PORT = 5000
SOCKET_TIMEOUT = 30


class ClientConnection:

    def __init__(self, host=COORDINATOR_HOST, port=COORDINATOR_PORT):
        self.host = host
        self.port = port

    # Sends a request to the coordinator
    def request(self, message):

        with socket.create_connection(
            (self.host, self.port),
            timeout=SOCKET_TIMEOUT
        ) as sock:

            sock.settimeout(SOCKET_TIMEOUT)

            send_message(sock, message)
            return receive_message(sock)

    # Sends a chunk to a storage node
    def store_chunk(self, host, port, chunk_id, chunk_data):

        with socket.create_connection(
            (host, port),
            timeout=SOCKET_TIMEOUT
        ) as sock:

            sock.settimeout(SOCKET_TIMEOUT)

            # Send the chunk information
            send_message(sock, {
                "type": "STORE_CHUNK",
                "chunk_id": chunk_id,
                "size": len(chunk_data)
            })

            # Send the actual binary data
            sock.sendall(chunk_data)

            # Receive the storage confirmation
            return receive_message(sock)

    # Retrieves a chunk from a storage node
    def get_chunk(self, host, port, chunk_id):

        with socket.create_connection(
            (host, port),
            timeout=SOCKET_TIMEOUT
        ) as sock:

            sock.settimeout(SOCKET_TIMEOUT)

            send_message(sock, {
                "type": "GET_CHUNK",
                "chunk_id": chunk_id
            })

            response = receive_message(sock)

            if response.get("status") != "success":
                raise RuntimeError(
                    response.get("message", "Failed to retrieve chunk")
                )

            if response.get("chunk_id") != chunk_id:
                raise ValueError("Received unexpected chunk ID")

            chunk_size = response["size"]

            if type(chunk_size) is not int or not 0 <= chunk_size <= 4 * 1024 * 1024:
                raise ValueError("Invalid chunk size received")

            # Receive the exact number of binary bytes
            return receive_exact(sock, chunk_size)
