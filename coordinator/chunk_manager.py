import httpx

from coordinator.models import Chunk, ChunkReplica
from coordinator.placement import select_workers
from shared.chunking import calculate_checksum


REPLICATION_FACTOR = 2
WORKER_TIMEOUT = 30.0


async def store_chunk(
    db,
    object_id: str,
    chunk: dict,
    workers: list,
    load: dict = None
):
    """
    Store a chunk on the required number of distinct healthy workers.

    Initially selected workers are tried first. If a selected worker
    fails, other healthy workers are attempted as replacements.

    A copy counts only after the worker confirms the expected checksum.
    This function performs NO database writes: the caller records the
    chunk and replica metadata in one short transaction after every chunk
    is stored, so no long-lived write lock is held during network I/O.
    """

    chunk_id = chunk["chunk_id"]
    chunk_content = chunk["content"]
    expected_checksum = chunk["checksum"]

    # Step 1: Validate chunk integrity before distribution.

    actual_checksum = calculate_checksum(chunk_content)

    if actual_checksum != expected_checksum:
        raise ValueError(
            f"Chunk checksum mismatch before storage: {chunk_id}"
        )

    if len(chunk_content) != chunk["size"]:
        raise ValueError(
            f"Chunk size mismatch before storage: {chunk_id}"
        )

    # Step 2: Select initial workers using deterministic placement.

    selected_workers = select_workers(
        chunk_id,
        workers,
        REPLICATION_FACTOR,
        load
    )

    selected_ids = {
        worker.worker_id
        for worker in selected_workers
    }

    # Step 3: Add remaining healthy workers as fallback candidates.

    fallback_workers = sorted(
        [
            worker
            for worker in workers
            if (
                worker.status == "HEALTHY"
                and worker.worker_id not in selected_ids
            )
        ],
        key=lambda worker: (
            (load or {}).get(worker.worker_id, 0),
            worker.worker_id
        )
    )

    candidate_workers = (
        selected_workers + fallback_workers
    )

    successful_replicas = []

    # Step 5: Store the chunk until RF copies are confirmed.

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for worker in candidate_workers:

            if len(successful_replicas) >= REPLICATION_FACTOR:
                break

            try:

                response = await client.put(
                    f"{worker.base_url}/internal/chunks/{chunk_id}",
                    content=chunk_content,
                    headers={
                        "Content-Type": "application/octet-stream"
                    }
                )

                response.raise_for_status()

                result = response.json()

                worker_checksum = result.get("checksum")

                if worker_checksum != expected_checksum:

                    print(
                        f"[Controller] Checksum mismatch for "
                        f"{chunk_id} on {worker.worker_id}"
                    )

                    # Remove the unverified physical copy.

                    try:
                        await client.delete(
                            f"{worker.base_url}/internal/chunks/{chunk_id}"
                        )

                    except httpx.HTTPError:
                        print(
                            f"[Controller] Could not clean up "
                            f"{chunk_id} on {worker.worker_id}"
                        )

                    continue

                # Step 6: Track verified replicas (the caller writes
                # the database records).

                successful_replicas.append(
                    worker.worker_id
                )

                print(
                    f"[Controller] Stored chunk {chunk_id} "
                    f"on {worker.worker_id}"
                )

            except (
                httpx.HTTPError,
                ValueError,
                KeyError
            ) as error:

                print(
                    f"[Controller] Failed to store chunk "
                    f"{chunk_id} on {worker.worker_id}: {error}"
                )

                continue

    # Step 7: Return storage result.

    replica_count = len(successful_replicas)

    replication_complete = (
        replica_count == REPLICATION_FACTOR
    )

    if not replication_complete:

        print(
            f"[Controller] Replication incomplete for "
            f"{chunk_id}: {replica_count}/{REPLICATION_FACTOR}"
        )

    return {
        "chunk_id": chunk_id,
        "sequence_number": chunk["sequence_number"],
        "size": chunk["size"],
        "checksum": expected_checksum,
        "replicas": successful_replicas,
        "replica_count": replica_count,
        "replication_complete": replication_complete
    }