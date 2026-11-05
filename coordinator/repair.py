import httpx

from coordinator.models import Worker, ChunkReplica
from shared.chunking import calculate_checksum


REPLICATION_FACTOR = 2
WORKER_TIMEOUT = 30.0


async def repair_chunk_replication(db, chunk, content):
    """
    Restore a chunk's replication and repair corrupted replicas.

    Behavior:
    1. Verify the supplied source content.
    2. Verify existing physical copies on healthy workers.
    3. Mark missing copies as FAILED and corrupted copies as CORRUPT.
    4. If a corrupted replica exists and a valid source is available,
       restore the corrupted replica even when the replication factor
       is already satisfied.
    5. If replication is below the required factor, create replacement
       copies on eligible healthy workers.
    6. Reuse existing replica records where possible.

    The caller is responsible for committing the transaction.
    """

    chunk_id = chunk.chunk_id

    # ==========================================
    # STEP 1: VERIFY SOURCE CONTENT
    # ==========================================

    actual_checksum = calculate_checksum(content)

    if actual_checksum != chunk.checksum:
        return {
            "repaired": False,
            "reason": "Source chunk checksum mismatch"
        }

    if len(content) != chunk.size:
        return {
            "repaired": False,
            "reason": "Source chunk size mismatch"
        }

    # ==========================================
    # STEP 2: LOAD WORKERS AND REPLICA RECORDS
    # ==========================================

    workers = db.query(Worker).all()

    workers_by_id = {
        worker.worker_id: worker
        for worker in workers
    }

    existing_replicas = db.query(ChunkReplica).filter(
        ChunkReplica.chunk_id == chunk_id
    ).all()

    replicas_by_worker = {
        replica.worker_id: replica
        for replica in existing_replicas
    }

    valid_worker_ids = set()
    corrupted_worker_ids = set()

    # ==========================================
    # STEP 3: VERIFY EXISTING PHYSICAL COPIES
    # ==========================================

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for replica in existing_replicas:

            worker = workers_by_id.get(replica.worker_id)

            if not worker:
                replica.status = "FAILED"
                continue

            # Only healthy workers can currently provide
            # a verified physical copy.

            if worker.status != "HEALTHY":
                continue

            try:

                response = await client.get(
                    f"{worker.base_url}/internal/chunks/{chunk_id}"
                )

                if response.status_code == 404:

                    replica.status = "FAILED"

                    print(
                        f"[Controller] Chunk {chunk_id} missing "
                        f"on {worker.worker_id}"
                    )

                    continue

                response.raise_for_status()

                stored_content = response.content

                stored_checksum = calculate_checksum(
                    stored_content
                )

                # ------------------------------------------
                # CORRUPTION DETECTED
                # ------------------------------------------

                if (
                    stored_checksum != chunk.checksum
                    or len(stored_content) != chunk.size
                ):

                    replica.status = "CORRUPT"

                    corrupted_worker_ids.add(
                        worker.worker_id
                    )

                    print(
                        f"[Controller] Corrupt replica detected: "
                        f"{chunk_id} on {worker.worker_id}"
                    )

                    continue

                # ------------------------------------------
                # VALID COPY
                # ------------------------------------------

                replica.status = "ACTIVE"
                replica.checksum = chunk.checksum

                valid_worker_ids.add(
                    worker.worker_id
                )

            except httpx.HTTPError as error:

                replica.status = "FAILED"

                print(
                    f"[Controller] Could not verify chunk "
                    f"{chunk_id} on {worker.worker_id}: {error}"
                )

    # ==========================================
    # STEP 4: DETERMINE REPAIR REQUIREMENTS
    # ==========================================

    active_count = len(valid_worker_ids)

    repaired_workers = []

    # A corrupted replica must be repaired even when the
    # replication factor is already satisfied.
    workers_needing_corruption_repair = [
        worker
        for worker in workers
        if (
            worker.status == "HEALTHY"
            and worker.worker_id in corrupted_worker_ids
        )
    ]

    # Missing/failed replicas only need replacement when
    # the replication factor is below the required level.

    replication_repair_needed = (
        active_count < REPLICATION_FACTOR
    )

    # ==========================================
    # STEP 5: SELECT REPAIR TARGETS
    # ==========================================

    repair_targets = []

    # First priority:
    # restore known corrupted replicas.

    repair_targets.extend(
        sorted(
            workers_needing_corruption_repair,
            key=lambda worker: worker.worker_id
        )
    )

    # Second priority:
    # if RF is below target, use additional healthy workers
    # that do not already contain a valid copy.

    if replication_repair_needed:

        additional_candidates = sorted(
            [
                worker
                for worker in workers
                if (
                    worker.status == "HEALTHY"
                    and worker.worker_id not in valid_worker_ids
                    and worker.worker_id not in corrupted_worker_ids
                )
            ],
            key=lambda worker: worker.worker_id
        )

        repair_targets.extend(additional_candidates)

    # Remove duplicate worker IDs while preserving order.

    unique_targets = []
    seen_worker_ids = set()

    for worker in repair_targets:

        if worker.worker_id not in seen_worker_ids:

            unique_targets.append(worker)
            seen_worker_ids.add(worker.worker_id)

    repair_targets = unique_targets

    # ==========================================
    # STEP 6: NOTHING TO REPAIR
    # ==========================================

    if not repair_targets:

        replication_complete = (
            active_count >= REPLICATION_FACTOR
        )

        return {
            "repaired": False,
            "repaired_workers": [],
            "active_replica_count": active_count,
            "replication_factor": REPLICATION_FACTOR,
            "replication_complete": replication_complete,
            "reason": (
                "Replication and integrity already satisfied"
                if replication_complete
                else "No eligible healthy replacement worker"
            )
        }

    # ==========================================
    # STEP 7: WRITE VERIFIED COPIES
    # ==========================================

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for worker in repair_targets:

            # If this is an additional replacement rather than
            # corruption repair, stop once RF is satisfied.

            if (
                worker.worker_id not in corrupted_worker_ids
                and active_count >= REPLICATION_FACTOR
            ):
                break

            try:

                response = await client.put(
                    f"{worker.base_url}/internal/chunks/{chunk_id}",
                    content=content,
                    headers={
                        "Content-Type": "application/octet-stream"
                    }
                )

                response.raise_for_status()

                result = response.json()

                returned_checksum = result.get("checksum")

                returned_size = result.get("size")

                # ==========================================
                # VERIFY WORKER WRITE
                # ==========================================

                if (
                    returned_checksum != chunk.checksum
                    or returned_size != chunk.size
                ):

                    print(
                        f"[Controller] Replacement checksum/size "
                        f"mismatch on {worker.worker_id}"
                    )

                    try:

                        await client.delete(
                            f"{worker.base_url}/internal/chunks/{chunk_id}"
                        )

                    except httpx.HTTPError:

                        print(
                            f"[Controller] Cleanup failed on "
                            f"{worker.worker_id}"
                        )

                    continue

                # ==========================================
                # UPDATE OR CREATE REPLICA RECORD
                # ==========================================

                existing_replica = replicas_by_worker.get(
                    worker.worker_id
                )

                if existing_replica:

                    # Reuse the existing CORRUPT or FAILED record.

                    existing_replica.status = "ACTIVE"
                    existing_replica.checksum = chunk.checksum

                else:

                    new_replica = ChunkReplica(
                        chunk_id=chunk_id,
                        worker_id=worker.worker_id,
                        status="ACTIVE",
                        checksum=chunk.checksum
                    )

                    db.add(new_replica)

                    replicas_by_worker[worker.worker_id] = (
                        new_replica
                    )

                db.flush()

                # A repaired corrupted copy does not increase
                # the number of distinct valid replicas if it
                # was already counted as invalid.

                if worker.worker_id not in valid_worker_ids:

                    valid_worker_ids.add(
                        worker.worker_id
                    )

                    active_count += 1

                repaired_workers.append(
                    worker.worker_id
                )

                print(
                    f"[Controller] Repaired chunk {chunk_id} "
                    f"on {worker.worker_id}"
                )

            except (
                httpx.HTTPError,
                ValueError,
                KeyError
            ) as error:

                print(
                    f"[Controller] Repair failed on "
                    f"{worker.worker_id}: {error}"
                )

                continue

    # ==========================================
    # STEP 8: FINAL RESULT
    # ==========================================

    replication_complete = (
        active_count >= REPLICATION_FACTOR
    )

    return {
        "repaired": len(repaired_workers) > 0,
        "repaired_workers": repaired_workers,
        "active_replica_count": active_count,
        "replication_factor": REPLICATION_FACTOR,
        "replication_complete": replication_complete,
        "reason": (
            "Corrupted replica repaired and replication restored"
            if repaired_workers
            else "Replication could not be fully restored"
        )
    }