import math
import socket
import threading
import uuid

from pathlib import Path

from communication.protocol import send_message, receive_message
from coordinator.metadata import MetadataManager


HOST = "127.0.0.1"
PORT = 5000

CHUNK_SIZE = 1024 * 1024
MAX_FILE_SIZE = 100 * 1024 * 1024

# Available storage nodes
STORAGE_NODES = {
    1: {"host": "127.0.0.1", "port": 5001},
    2: {"host": "127.0.0.1", "port": 5002},
    3: {"host": "127.0.0.1", "port": 5003}
}

# Shared coordinator state
metadata = MetadataManager()

pending_uploads = {}
upload_lock = threading.Lock()


# Determines where each chunk should be stored
def assign_chunks(upload_id, file_size):

    chunk_count = math.ceil(file_size / CHUNK_SIZE)
    node_ids = list(STORAGE_NODES.keys())

    if len(node_ids) < 2:
        raise ValueError("At least two storage nodes are required")

    chunks = []

    for index in range(chunk_count):

        # Select two different nodes using round-robin
        first_node = node_ids[index % len(node_ids)]
        second_node = node_ids[(index + 1) % len(node_ids)]

        chunk_size = min(
            CHUNK_SIZE,
            file_size - index * CHUNK_SIZE
        )

        chunks.append({
            "id": f"{upload_id}_{index}",
            "size": chunk_size,
            "nodes": [first_node, second_node]
        })

    return chunks


# Prepares an upload and reserves its filename
def initialize_upload(request):

    filename = request["filename"]
    file_size = request["size"]

    # Validate filename
    if (
        not isinstance(filename, str)
        or not filename
        or filename in (".", "..")
        or "/" in filename
        or "\\" in filename
        or len(filename) > 255
    ):
        raise ValueError("Invalid filename")

    # Validate file size
    if (
        type(file_size) is not int
        or not 0 <= file_size <= MAX_FILE_SIZE
    ):
        raise ValueError("Invalid file size")

    with upload_lock:

        if metadata.get_file(filename) is not None:
            raise ValueError("File already exists")

        for upload in pending_uploads.values():
            if upload["filename"] == filename:
                raise ValueError("File upload already in progress")

        upload_id = uuid.uuid4().hex

        chunks = assign_chunks(upload_id, file_size)

        pending_uploads[upload_id] = {
            "filename": filename,
            "size": file_size,
            "chunks": chunks
        }

    return {
        "status": "success",
        "upload_id": upload_id,
        "chunk_size": CHUNK_SIZE,
        "chunks": chunks,
        "storage_nodes": STORAGE_NODES
    }


# Registers a completed upload
def complete_upload(request):

    upload_id = request["upload_id"]

    with upload_lock:

        upload = pending_uploads.get(upload_id)

        if upload is None:
            raise ValueError("Unknown upload ID")

        # Store only the metadata needed for downloads
        chunks = [
            {
                "id": chunk["id"],
                "nodes": chunk["nodes"]
            }
            for chunk in upload["chunks"]
        ]

        metadata.add_file(
            upload["filename"],
            upload["size"],
            chunks
        )

        del pending_uploads[upload_id]

    return {
        "status": "success",
        "message": "Upload registered successfully"
    }


# Returns the metadata for a requested file
def get_file(request):

    filename = request["filename"]

    file_data = metadata.get_file(filename)

    if file_data is None:
        return {
            "status": "error",
            "message": "File not found"
        }

    return {
        "status": "success",
        "filename": filename,
        "size": file_data["size"],
        "chunks": file_data["chunks"],
        "storage_nodes": STORAGE_NODES
    }


# Processes a client request
def handle_client(conn):

    with conn:
        conn.settimeout(30)

        try:
            request = receive_message(conn)

            if not isinstance(request, dict):
                raise ValueError("Request must be a JSON object")

            request_type = request["type"]

            if request_type == "PING":
                response = {
                    "status": "success",
                    "message": "PONG"
                }

            elif request_type == "UPLOAD_INIT":
                response = initialize_upload(request)

            elif request_type == "UPLOAD_COMPLETE":
                response = complete_upload(request)

            elif request_type == "GET_FILE":
                response = get_file(request)

            elif request_type == "LIST_FILES":
                response = {
                    "status": "success",
                    "files": metadata.list_files()
                }

            else:
                response = {
                    "status": "error",
                    "message": "Unknown request type"
                }

            send_message(conn, response)

        except (
            ConnectionError,
            OSError,
            ValueError,
            KeyError,
            TypeError
        ) as error:

            print(f"Client error: {error}")

            try:
                send_message(conn, {
                    "status": "error",
                    "message": str(error)
                })
            except OSError:
                pass


# Starts the coordinator TCP server
def start_server():

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:

        server.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        server.bind((HOST, PORT))
        server.listen()

        print(f"Coordinator listening on {HOST}:{PORT}")

        while True:

            conn, address = server.accept()

            print(f"Connection received from {address}")

            thread = threading.Thread(
                target=handle_client,
                args=(conn,),
                daemon=True
            )

            thread.start()

if __name__ == "__main__":
    start_server()