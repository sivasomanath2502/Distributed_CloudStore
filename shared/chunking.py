import hashlib
import uuid

CHUNK_SIZE = 1024 * 1024  # 1 MiB


def calculate_checksum(content: bytes) -> str:
    """
    Calculate SHA-256 checksum of binary content.
    """
    return hashlib.sha256(content).hexdigest()


def split_object(content: bytes) -> list:
    """
    Split an object into 1 MiB chunks.

    Each chunk contains:
    - chunk_id
    - sequence_number
    - content
    - size
    - checksum
    """

    chunks = []

    for offset in range(0, len(content), CHUNK_SIZE):

        chunk_content = content[
            offset:offset + CHUNK_SIZE
        ]

        chunk = {
            "chunk_id": str(uuid.uuid4()),
            "sequence_number": len(chunks) + 1,
            "content": chunk_content,
            "size": len(chunk_content),
            "checksum": calculate_checksum(chunk_content)
        }

        chunks.append(chunk)

    return chunks


def reconstruct_object(chunks: list) -> bytes:
    """
    Reconstruct the original object from its chunks.

    Verifies chunk checksums and preserves original ordering.
    """

    ordered_chunks = sorted(
        chunks,
        key=lambda chunk: chunk["sequence_number"]
    )

    reconstructed_content = bytearray()

    for chunk in ordered_chunks:

        content = chunk["content"]

        actual_checksum = calculate_checksum(content)

        if actual_checksum != chunk["checksum"]:
            raise ValueError(
                f"Checksum mismatch for chunk "
                f"{chunk['chunk_id']}"
            )

        reconstructed_content.extend(content)

    return bytes(reconstructed_content)