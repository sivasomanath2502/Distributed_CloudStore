import hashlib


def select_workers(
    chunk_id: str,
    workers: list,
    replication_factor: int = 2,
    load: dict = None
):
    """
    Select distinct healthy workers for a chunk.

    Args:
        chunk_id: Unique identifier of the chunk.
        workers: List of Worker database objects.
        replication_factor: Number of worker copies required.
        load: Optional {worker_id: stored_chunk_count}. When given, the
            least-loaded healthy workers are preferred (ties are broken
            by the deterministic hash order), which keeps chunk counts
            balanced across workers.

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

    # Load-aware selection: least-loaded workers first, ties broken by
    # the deterministic circular order starting at start_index.
    if load is not None:

        count = len(healthy_workers)

        ordered = sorted(
            range(count),
            key=lambda i: (
                load.get(healthy_workers[i].worker_id, 0),
                (i - start_index) % count
            )
        )

        return [
            healthy_workers[i]
            for i in ordered[:replication_factor]
        ]

    # Select distinct workers using circular placement.
    selected_workers = []

    for offset in range(replication_factor):
        index = (start_index + offset) % len(healthy_workers)
        selected_workers.append(healthy_workers[index])

    return selected_workers