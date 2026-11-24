
import json
import os
import threading
import uuid

from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
METADATA_PATH = ROOT_DIR / "data" / "metadata.json"

class MetadataManager:
    def __init__(self, path=METADATA_PATH):
        self.path = Path(path)

        # Protect metadata from concurrent modifications
        self.lock = threading.Lock()

        # Ensure the storage directory exists
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Load existing metadata
        self.metadata = self.load_metadata()

    # Loads metadata from the JSON file
    def load_metadata(self):
        if not self.path.exists():
            return {"files": {}}

        with open(self.path, "r", encoding="utf-8") as file:
            return json.load(file)

    # Saves metadata to the JSON file
    def save_metadata(self):
        temp_path = self.path.with_name(f".{uuid.uuid4().hex}.tmp")

        try:
            with open(temp_path, "w", encoding="utf-8") as file:
                json.dump(self.metadata, file, indent=4)

            os.replace(temp_path, self.path)

        finally:
            temp_path.unlink(missing_ok=True)

    # Registers a successfully uploaded file
    def add_file(self, filename, size, chunks):
        with self.lock:
            if filename in self.metadata["files"]:
                raise ValueError("File already exists")

            self.metadata["files"][filename] = {
                "size": size,
                "chunks": chunks
            }

            self.save_metadata()

    # Retrieves the metadata for a specific file
    def get_file(self, filename):
        with self.lock:
            file_data = self.metadata["files"].get(filename)

            if file_data is None:
                return None

            # Return a copy to avoid accidental modifications
            return {
                "size": file_data["size"],
                "chunks": [
                    {
                        "id": chunk["id"],
                        "nodes": chunk["nodes"].copy()
                    }
                    for chunk in file_data["chunks"]
                ]
            }

    # Returns the names of all stored files
    def list_files(self):
        with self.lock:
            return list(self.metadata["files"].keys())