import hashlib


def select_workers(chunk_id: str, workers: list, replication_factor: int = 2):
    """
    Select distinct healthy workers for a chunk.

    workers: list of Worker database objects
    """

    healthy_workers = sorted(
        [worker for worker in workers if worker.status == "HEALTHY"],
        key=lambda worker: worker.worker_id
    )

    if len(healthy_workers) < replication_factor:
        raise RuntimeError(
            f"Not enough healthy workers. "
            f"Required: {replication_factor}, Available: {len(healthy_workers)}"
        )

    # Deterministically choose a starting worker.
    hash_value = hashlib.sha256(chunk_id.encode()).hexdigest()
    start_index = int(hash_value, 16) % len(healthy_workers)

    # Select consecutive workers, wrapping around the list.
    selected = []

    for offset in range(replication_factor):
        index = (start_index + offset) % len(healthy_workers)
        selected.append(healthy_workers[index])

    return selected