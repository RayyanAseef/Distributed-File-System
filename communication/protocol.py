import json
import struct

# Number of bytes used to store the message length
HEADER_SIZE = 4

# Maximum allowed message size (16 MB)
MAX_MESSAGE_SIZE = 16 * 1024 * 1024

# Sends a JSON message through an existing TCP socket
def send_message(sock, message):
    # Convert the dictionary into JSON bytes
    data = json.dumps(message).encode("utf-8")

    if len(data) > MAX_MESSAGE_SIZE:
        raise ValueError("Message exceeds maximum allowed size")

    # Encode the length of the message as a 4-byte header
    header = struct.pack("!I", len(data))

    # Send the header followed by the message
    sock.sendall(header + data)

# Receives a JSON message through an existing TCP socket
def receive_message(sock):
    # Read the 4-byte message header
    header = receive_exact(sock, HEADER_SIZE)

    # Decode the message length
    message_size = struct.unpack("!I", header)[0]

    if message_size > MAX_MESSAGE_SIZE:
        raise ValueError("Incoming message exceeds maximum allowed size")

    # Read the complete message
    data = receive_exact(sock, message_size)

    # Convert JSON bytes back into a Python dictionary
    return json.loads(data.decode("utf-8"))

# Receives an exact number of bytes from a TCP socket
def receive_exact(sock, size):
    data = bytearray()

    while len(data) < size:
        # Read the remaining bytes
        chunk = sock.recv(size - len(data))

        # Detect a closed connection
        if not chunk:
            raise ConnectionError("Connection closed unexpectedly")

        data.extend(chunk)

    return bytes(data)