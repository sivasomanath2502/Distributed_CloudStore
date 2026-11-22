import httpx

from coordinator.models import Worker, ChunkReplica
from shared.chunking import calculate_checksum


REPLICATION_FACTOR = 2
WORKER_TIMEOUT = 30.0


async def repair_chunk_replication(db, chunk, content):
    """
    Restore a chunk's replication and repair corrupted replicas.

    Rules:

    1. Verify the supplied source content.
    2. Verify existing physical copies on healthy workers.
    3. Mark missing copies as FAILED.
    4. Mark corrupted copies as CORRUPT.
    5. Repair corrupted replicas when a valid source exists.
    6. If fewer than REPLICATION_FACTOR valid ACTIVE replicas exist,
       create only the required number of replacement copies.
    7. Reuse existing replica records where possible.
    8. Never create an additional replacement when the replication
       factor is already satisfied.

    The caller is responsible for committing the transaction.
    """

    chunk_id = chunk.chunk_id

    # ==========================================================
    # STEP 1: VERIFY SOURCE CONTENT
    # ==========================================================

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

    # ==========================================================
    # STEP 2: LOAD WORKERS AND REPLICA RECORDS
    # ==========================================================

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

    # ==========================================================
    # STEP 3: VERIFY EXISTING ACTIVE REPLICAS
    # ==========================================================

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for replica in existing_replicas:

            worker = workers_by_id.get(
                replica.worker_id
            )

            if not worker:

                replica.status = "FAILED"

                continue

            # Only healthy workers can currently provide
            # an active verified copy.

            if worker.status != "HEALTHY":

                if replica.status == "ACTIVE":
                    replica.status = "FAILED"

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

                # --------------------------------------------------
                # CORRUPTED COPY
                # --------------------------------------------------

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

                # --------------------------------------------------
                # VALID COPY
                # --------------------------------------------------

                if replica.status == "ACTIVE":

                    replica.checksum = chunk.checksum

                    valid_worker_ids.add(
                        worker.worker_id
                    )

                elif replica.status in (
                    "FAILED",
                    "STANDBY"
                ):

                    # A previously inactive/failed record may be
                    # promoted only if RF still needs it.
                    #
                    # We deliberately do not immediately promote it.
                    # RF calculation below determines whether it
                    # is actually needed.

                    pass

            except httpx.HTTPError as error:

                replica.status = "FAILED"

                print(
                    f"[Controller] Could not verify chunk "
                    f"{chunk_id} on {worker.worker_id}: {error}"
                )

    # ==========================================================
    # STEP 4: DETERMINE CURRENT ACTIVE REPLICATION
    # ==========================================================

    active_count = len(valid_worker_ids)

    repaired_workers = []

    # Healthy corrupted replicas should be repaired even if
    # RF is already satisfied elsewhere.

    workers_needing_corruption_repair = sorted(
        [
            worker
            for worker in workers
            if (
                worker.status == "HEALTHY"
                and worker.worker_id in corrupted_worker_ids
            )
        ],
        key=lambda worker: worker.worker_id
    )

    # ==========================================================
    # STEP 5: DETERMINE REPLACEMENT NEED
    # ==========================================================

    replacements_needed = max(
        0,
        REPLICATION_FACTOR - active_count
    )

    repair_targets = []

    # First repair corrupted replicas.
    repair_targets.extend(
        workers_needing_corruption_repair
    )

    # Then find healthy workers that do not currently
    # have an ACTIVE verified copy.

    if replacements_needed > 0:

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

        repair_targets.extend(
            additional_candidates
        )

    # Remove duplicate worker IDs.

    unique_targets = []
    seen_worker_ids = set()

    for worker in repair_targets:

        if worker.worker_id not in seen_worker_ids:

            unique_targets.append(worker)
            seen_worker_ids.add(worker.worker_id)

    repair_targets = unique_targets

    # ==========================================================
    # STEP 6: NOTHING TO REPAIR
    # ==========================================================

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

    # ==========================================================
    # STEP 7: WRITE VERIFIED COPIES
    # ==========================================================

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for worker in repair_targets:

            # If this is a normal replacement and RF has already
            # been restored, stop.
            #
            # Corruption repairs are still allowed because they
            # restore a known bad replica.

            is_corruption_repair = (
                worker.worker_id in corrupted_worker_ids
            )

            if (
                not is_corruption_repair
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

                returned_checksum = result.get(
                    "checksum"
                )

                returned_size = result.get(
                    "size"
                )

                # --------------------------------------------------
                # VERIFY WORKER WRITE
                # --------------------------------------------------

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

                # --------------------------------------------------
                # UPDATE OR CREATE REPLICA RECORD
                # --------------------------------------------------

                existing_replica = replicas_by_worker.get(
                    worker.worker_id
                )

                if existing_replica:

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

                    replicas_by_worker[
                        worker.worker_id
                    ] = new_replica

                db.flush()

                # Count this worker only once.

                if worker.worker_id not in valid_worker_ids:

                    valid_worker_ids.add(
                        worker.worker_id
                    )

                    active_count += 1

                repaired_workers.append(
                    worker.worker_id
                )

                print(
                    f"[Controller] Repaired chunk "
                    f"{chunk_id} on {worker.worker_id}"
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

    # ==========================================================
    # STEP 8: FINAL RESULT
    # ==========================================================

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