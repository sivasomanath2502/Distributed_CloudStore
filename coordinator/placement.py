import hashlib


def select_workers(
    chunk_id: str,
    workers: list,
    replication_factor: int = 2
):
    """
    Select distinct healthy workers for a chunk.

    Args:
        chunk_id: Unique identifier of the chunk.
        workers: List of Worker database objects.
        replication_factor: Number of worker copies required.

    Returns:
        List of selected healthy Worker objects.

    Raises:
        ValueError: If the input or replication factor is invalid.
        RuntimeError: If insufficient healthy workers are available.
    """

    if not chunk_id:
        raise ValueError("Chunk ID cannot be empty")

    if replication_factor < 1:
        raise ValueError("Replication factor must be at least 1")

    # Keep only healthy workers and remove duplicate worker IDs.
    healthy_workers_by_id = {
        worker.worker_id: worker
        for worker in workers
        if worker.status == "HEALTHY"
    }

    healthy_workers = sorted(
        healthy_workers_by_id.values(),
        key=lambda worker: worker.worker_id
    )

    if len(healthy_workers) < replication_factor:
        raise RuntimeError(
            f"Not enough healthy workers. "
            f"Required: {replication_factor}, "
            f"Available: {len(healthy_workers)}"
        )

    # Generate a deterministic starting position.
    hash_value = hashlib.sha256(
        chunk_id.encode("utf-8")
    ).hexdigest()

    start_index = int(hash_value, 16) % len(healthy_workers)

    # Select distinct workers using circular placement.
    selected_workers = []

    for offset in range(replication_factor):
        index = (start_index + offset) % len(healthy_workers)
        selected_workers.append(healthy_workers[index])

    return selected_workers