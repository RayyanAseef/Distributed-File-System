
import uuid

from pathlib import Path

from client.connection import ClientConnection


ROOT_DIR = Path(__file__).resolve().parent.parent
DOWNLOAD_DIR = ROOT_DIR / "downloads"


class FileService:

    def __init__(self):
        self.connection = ClientConnection()

    # Retrieves the list of stored files
    def list_files(self):

        response = self.connection.request({
            "type": "LIST_FILES"
        })

        if response.get("status") != "success":
            raise RuntimeError(
                response.get("message", "Failed to list files")
            )

        return response["files"]

    # Uploads a file to the distributed storage system
    def upload(self, file_path):

        file_path = Path(file_path).expanduser()

        if not file_path.is_file():
            raise FileNotFoundError("File does not exist")

        filename = file_path.name
        file_size = file_path.stat().st_size

        # Request chunk assignments from the coordinator
        response = self.connection.request({
            "type": "UPLOAD_INIT",
            "filename": filename,
            "size": file_size
        })

        if response.get("status") != "success":
            raise RuntimeError(
                response.get("message", "Upload initialization failed")
            )

        upload_id = response["upload_id"]
        chunks = response["chunks"]
        nodes = response["storage_nodes"]

        # Read the file one chunk at a time
        with open(file_path, "rb") as file:

            for chunk in chunks:

                chunk_id = chunk["id"]
                chunk_size = chunk["size"]

                chunk_data = file.read(chunk_size)

                if len(chunk_data) != chunk_size:
                    raise IOError("Could not read complete file chunk")

                # Store two copies on different nodes
                for node_id in chunk["nodes"]:

                    node = nodes[str(node_id)]

                    result = self.connection.store_chunk(
                        node["host"],
                        node["port"],
                        chunk_id,
                        chunk_data
                    )

                    if result.get("status") != "success":
                        raise RuntimeError(
                            result.get("message", "Chunk storage failed")
                        )

        # Register the completed upload
        result = self.connection.request({
            "type": "UPLOAD_COMPLETE",
            "upload_id": upload_id
        })

        if result.get("status") != "success":
            raise RuntimeError(
                result.get("message", "Upload completion failed")
            )

        return {
            "filename": filename,
            "size": file_size,
            "chunks": len(chunks),
            "message": "Upload successful"
        }

    # Downloads and reconstructs a stored file
    def download(self, filename, destination=None):

        if (
            not isinstance(filename, str)
            or filename in ("", ".", "..")
            or Path(filename).name != filename
            or "\\" in filename
        ):
            raise ValueError("Invalid filename")

        # Request file metadata from the coordinator
        response = self.connection.request({
            "type": "GET_FILE",
            "filename": filename
        })

        if response.get("status") != "success":
            raise FileNotFoundError(
                response.get("message", "File not found")
            )

        chunks = response["chunks"]
        nodes = response["storage_nodes"]

        # Determine where the downloaded file will be saved
        if destination is None:
            DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
            output_path = DOWNLOAD_DIR / filename
        else:
            output_path = Path(destination).expanduser()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Use a temporary file until reconstruction finishes
        temp_path = output_path.with_name(
            f".{uuid.uuid4().hex}.tmp"
        )

        try:
            with open(temp_path, "wb") as file:

                for chunk in chunks:

                    chunk_id = chunk["id"]
                    chunk_data = None

                    # Try each available replica
                    for node_id in chunk["nodes"]:

                        node = nodes[str(node_id)]

                        try:
                            chunk_data = self.connection.get_chunk(
                                node["host"],
                                node["port"],
                                chunk_id
                            )
                            break

                        except (OSError, RuntimeError, ValueError) as error:
                            print(
                                f"Node {node_id} unavailable "
                                f"for {chunk_id}: {error}"
                            )

                    # Both replicas failed
                    if chunk_data is None:
                        raise RuntimeError(
                            f"Unable to retrieve chunk {chunk_id}"
                        )

                    # Reconstruct the file in chunk order
                    file.write(chunk_data)

            # Verify reconstructed file size
            if temp_path.stat().st_size != response["size"]:
                raise IOError("Downloaded file size does not match")

            # Make the completed file available
            temp_path.replace(output_path)

        finally:
            temp_path.unlink(missing_ok=True)

        return {
            "filename": filename,
            "path": str(output_path),
            "message": "Download successful"
        }
