import hashlib

import httpx

from coordinator.models import Chunk, ChunkReplica, Worker
from coordinator.repair import repair_chunk_replication


WORKER_TIMEOUT = 30.0


async def reconcile_worker(db, worker):
    """
    Reconcile controller replica records with a worker's
    actual physical chunk inventory.

    Validates chunk size and SHA-256 checksum.
    Discovers known chunks missing from replica records.
    Marks missing and corrupted replicas.
    Attempts to restore the replication factor.

    The caller is responsible for committing the transaction.
    """

    worker_id = worker.worker_id
    base_url = worker.base_url

    summary = {
        "worker_id": worker_id,
        "inventory_count": 0,
        "verified": 0,
        "discovered": 0,
        "missing": 0,
        "corrupted": 0,
        "repaired_chunks": [],
        "errors": []
    }

    async with httpx.AsyncClient(timeout=WORKER_TIMEOUT) as client:

        # STEP 1: Fetch worker inventory

        try:
            response = await client.get(
                f"{base_url}/internal/workers/{worker_id}/chunks"
            )
            response.raise_for_status()
            inventory_data = response.json()

        except (httpx.HTTPError, ValueError) as error:
            summary["errors"].append(
                f"Could not retrieve worker inventory: {error}"
            )
            return summary

        if not isinstance(inventory_data, dict):
            summary["errors"].append(
                "Invalid worker inventory response"
            )
            return summary

        if inventory_data.get("worker_id") != worker_id:
            summary["errors"].append(
                "Worker inventory ID mismatch"
            )
            return summary

        inventory = inventory_data.get("chunks", [])

        if not isinstance(inventory, list):
            summary["errors"].append(
                "Invalid worker inventory format"
            )
            return summary

        inventory_by_id = {}

        for item in inventory:
            if not isinstance(item, dict):
                continue

            file_id = item.get("file_id")
            size = item.get("size")

            if (
                isinstance(file_id, str)
                and isinstance(size, int)
                and not isinstance(size, bool)
                and size >= 0
            ):
                inventory_by_id[file_id] = size

        summary["inventory_count"] = len(inventory_by_id)

        # STEP 2: Load controller metadata

        known_chunks = db.query(Chunk).all()

        chunks_by_id = {
            chunk.chunk_id: chunk
            for chunk in known_chunks
        }

        existing_replicas = db.query(ChunkReplica).filter(
            ChunkReplica.worker_id == worker_id
        ).all()

        replicas_by_chunk = {
            replica.chunk_id: replica
            for replica in existing_replicas
        }

        candidate_chunk_ids = (
            set(replicas_by_chunk.keys())
            | (
                set(inventory_by_id.keys())
                & set(chunks_by_id.keys())
            )
        )

        # STEP 3: Verify physical chunk copies

        for chunk_id in sorted(candidate_chunk_ids):

            chunk = chunks_by_id.get(chunk_id)

            if chunk is None:
                continue

            replica = replicas_by_chunk.get(chunk_id)

            # Physical file is missing.

            if chunk_id not in inventory_by_id:

                if replica:
                    replica.status = "FAILED"
                    summary["missing"] += 1

                    print(
                        f"[Controller] Missing chunk "
                        f"{chunk_id} on {worker_id}"
                    )

                continue

            # Check reported size.

            reported_size = inventory_by_id[chunk_id]

            if reported_size != chunk.size:

                if replica:
                    replica.status = "CORRUPT"

                summary["corrupted"] += 1

                print(
                    f"[Controller] Size mismatch for "
                    f"{chunk_id} on {worker_id}"
                )

                continue

            # Retrieve and verify actual content.

            try:
                response = await client.get(
                    f"{base_url}/internal/chunks/{chunk_id}"
                )

                if response.status_code == 404:

                    if replica:
                        replica.status = "FAILED"

                    summary["missing"] += 1
                    continue

                response.raise_for_status()

                content = response.content

                actual_checksum = hashlib.sha256(
                    content
                ).hexdigest()

                if (
                    len(content) != chunk.size
                    or actual_checksum != chunk.checksum
                ):

                    if replica:
                        replica.status = "CORRUPT"

                    summary["corrupted"] += 1

                    print(
                        f"[Controller] Corrupt chunk "
                        f"{chunk_id} on {worker_id}"
                    )

                    continue

                # STEP 4: Mark verified replica ACTIVE

                if replica:

                    replica.status = "ACTIVE"
                    replica.checksum = chunk.checksum

                else:

                    replica = ChunkReplica(
                        chunk_id=chunk_id,
                        worker_id=worker_id,
                        status="ACTIVE",
                        checksum=chunk.checksum
                    )

                    db.add(replica)
                    replicas_by_chunk[chunk_id] = replica

                    summary["discovered"] += 1

                db.flush()

                summary["verified"] += 1

                print(
                    f"[Controller] Verified chunk "
                    f"{chunk_id} on {worker_id}"
                )

            except httpx.HTTPError as error:

                if replica:
                    replica.status = "FAILED"

                summary["errors"].append(
                    f"Could not verify {chunk_id}: {error}"
                )

        # STEP 5: Restore replication factor

        for chunk_id in sorted(candidate_chunk_ids):

            chunk = chunks_by_id.get(chunk_id)

            if chunk is None:
                continue

            try:

                content = await get_verified_chunk_content(
                    db,
                    chunk,
                    client
                )

                repair_result = await repair_chunk_replication(
                    db,
                    chunk,
                    content
                )

                if repair_result.get("repaired"):

                    summary["repaired_chunks"].append({
                        "chunk_id": chunk_id,
                        "result": repair_result
                    })

            except ValueError as error:

                summary["errors"].append(
                    f"Could not repair {chunk_id}: {error}"
                )

            except Exception as error:

                summary["errors"].append(
                    f"Repair error for {chunk_id}: {error}"
                )

    return summary


async def get_verified_chunk_content(db, chunk, client):
    """
    Retrieve a valid copy of a chunk from any healthy worker.

    Content must match the expected size and SHA-256 checksum.
    """

    workers = db.query(Worker).filter(
        Worker.status == "HEALTHY"
    ).all()

    for worker in workers:

        try:

            response = await client.get(
                f"{worker.base_url}/internal/chunks/{chunk.chunk_id}"
            )

            if response.status_code == 404:
                continue

            response.raise_for_status()

            content = response.content

            checksum = hashlib.sha256(content).hexdigest()

            if (
                len(content) == chunk.size
                and checksum == chunk.checksum
            ):
                return content

        except httpx.HTTPError:
            continue

    raise ValueError(
        f"No verified source copy available for chunk {chunk.chunk_id}"
    )