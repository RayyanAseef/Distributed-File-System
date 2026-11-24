from communication.protocol import send_message, receive_message, receive_exact
from pathlib import Path

import socket
import threading
import argparse
import os
import re
import uuid

HOST = "127.0.0.1"
MAX_CHUNK_SIZE = 4 * 1024 * 1024

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"

# Receives a chunk and saves it to disk
def store_chunk(conn, request, storage_dir):
    chunk_id = request["chunk_id"]
    chunk_size = request["size"]

    # Validate chunk information
    if not isinstance(chunk_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", chunk_id):
        raise ValueError("Invalid chunk ID")

    if type(chunk_size) is not int or not 0 <= chunk_size <= MAX_CHUNK_SIZE:
        raise ValueError("Invalid chunk size")

    # Receive exactly the expected number of bytes
    chunk_data = receive_exact(conn, chunk_size)

    # Determine where to save the chunk
    chunk_path = storage_dir / chunk_id
    temp_path = storage_dir / f".{uuid.uuid4().hex}.tmp"

    # Save the chunk using a temporary file
    try:
        with open(temp_path, "wb") as file:
            file.write(chunk_data)

        os.replace(temp_path, chunk_path)

    finally:
        temp_path.unlink(missing_ok=True)

    # Confirm successful storage
    send_message(conn, {"status": "success", "message": "Chunk stored successfully"})


# Retrieves a stored chunk and sends it to the client
def get_chunk(conn, request, storage_dir):
    chunk_id = request["chunk_id"]

    # Validate the chunk identifier
    if not isinstance(chunk_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", chunk_id):
        raise ValueError("Invalid chunk ID")

    chunk_path = storage_dir / chunk_id

    # Check whether the chunk exists
    if not chunk_path.is_file():
        send_message(conn, {"status": "error", "message": "Chunk not found"})
        return

    # Read the stored chunk
    with open(chunk_path, "rb") as file:
        chunk_data = file.read()

    # Send the chunk size before transmitting the bytes
    send_message(conn, {"status": "success", "chunk_id": chunk_id, "size": len(chunk_data)})

    # Send the actual chunk bytes
    conn.sendall(chunk_data)

# Processes an incoming client request
def handle_client(conn, storage_dir):
    with conn:
        conn.settimeout(30)

        try:
            request = receive_message(conn)

            if request["type"] == "PING":
                send_message(conn, {"status": "success", "message": "PONG"})

            elif request["type"] == "STORE_CHUNK":
                store_chunk(conn, request, storage_dir)

            elif request["type"] == "STORE_CHUNK":
                store_chunk(conn, request, storage_dir)

            else:
                send_message(conn, {"status": "error", "message": "Unknown request type"})

        except (ConnectionError, OSError, ValueError, KeyError, TypeError) as error:
            try:
                send_message(conn, {"status": "error", "message": str(error)})
            except OSError:
                pass


# Starts a storage node server
def start_server(node_id, port):
    storage_dir = DATA_DIR / f"node{node_id}"
    storage_dir.mkdir(parents=True, exist_ok=True)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, port))
        server.listen()

        print(f"Storage Node {node_id} listening on {HOST}:{port}")

        while True:
            conn, address = server.accept()

            print(f"Connection received from {address}")

            thread = threading.Thread(
                target=handle_client,
                args=(conn, storage_dir),
                daemon=True
            )

            thread.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)

    args = parser.parse_args()

    if args.id < 1 or not 1 <= args.port <= 65535:
        parser.error("Invalid node ID or port")

    start_server(args.id, args.port)
