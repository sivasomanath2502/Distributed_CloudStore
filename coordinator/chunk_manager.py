import httpx

from coordinator.models import Chunk, ChunkReplica
from coordinator.placement import select_workers
from shared.chunking import calculate_checksum


REPLICATION_FACTOR = 2


async def store_chunk(db, object_id: str, chunk: dict, workers: list):
    """
    Store a chunk on two distinct healthy workers
    and record its locations in the database.
    """

    selected_workers = select_workers(
        chunk["chunk_id"],
        workers,
        REPLICATION_FACTOR
    )

    chunk_record = Chunk(
        chunk_id=chunk["chunk_id"],
        object_id=object_id,
        sequence_number=chunk["sequence_number"],
        size=chunk["size"],
        checksum=chunk["checksum"]
    )

    db.add(chunk_record)
    db.flush()

    successful_replicas = []

    async with httpx.AsyncClient(timeout=30.0) as client:

        for worker in selected_workers:
            try:
                response = await client.put(
                    f"{worker.base_url}/internal/chunks/{chunk['chunk_id']}",
                    content=chunk["content"],
                    headers={
                        "Content-Type": "application/octet-stream"
                    }
                )

                response.raise_for_status()
                result = response.json()

                if result["checksum"] != chunk["checksum"]:
                    raise RuntimeError("Worker checksum mismatch")

                replica = ChunkReplica(
                    chunk_id=chunk["chunk_id"],
                    worker_id=worker.worker_id,
                    status="ACTIVE",
                    checksum=chunk["checksum"]
                )

                db.add(replica)
                successful_replicas.append(worker.worker_id)

            except (httpx.HTTPError, RuntimeError, KeyError) as error:
                print(
                    f"Failed to store chunk {chunk['chunk_id']} "
                    f"on {worker.worker_id}: {error}"
                )

    db.flush()

    return {
        "chunk_id": chunk["chunk_id"],
        "sequence_number": chunk["sequence_number"],
        "size": chunk["size"],
        "checksum": chunk["checksum"],
        "replicas": successful_replicas,
        "replica_count": len(successful_replicas),
        "replication_complete": (
            len(successful_replicas) == REPLICATION_FACTOR
        )
    }