import hashlib

import httpx

from coordinator.models import Chunk, ChunkReplica, Worker
from coordinator.repair import repair_chunk_replication


REPLICATION_FACTOR = 2
WORKER_TIMEOUT = 30.0


async def reconcile_worker(db, worker):
    """
    Reconcile a worker's physical storage with controller metadata.

    The reconciliation process:

    1. Fetch the worker's physical chunk inventory.
    2. Compare physical chunks with controller metadata.
    3. Verify every known physical copy using checksum and size.
    4. Keep a recovered copy only when it is needed to satisfy
       the replication factor.
    5. Remove an extra recovered copy when enough valid copies
       already exist on other workers.
    6. Mark missing or corrupted replicas appropriately.
    7. Restore replication when the replication factor is below
       the required level.
    """

    worker_id = worker.worker_id

    result = {
        "worker_id": worker_id,
        "inventory_count": 0,
        "verified": 0,
        "discovered": 0,
        "missing": 0,
        "corrupted": 0,
        "repaired_chunks": [],
        "removed_excess_chunks": [],
        "errors": []
    }

    # ==========================================
    # STEP 1: FETCH WORKER INVENTORY
    # ==========================================

    try:
        async with httpx.AsyncClient(
            timeout=WORKER_TIMEOUT
        ) as client:

            response = await client.get(
                f"{worker.base_url}/internal/workers/"
                f"{worker_id}/chunks"
            )

            response.raise_for_status()

            inventory_data = response.json()

    except (
        httpx.HTTPError,
        ValueError,
        KeyError
    ) as error:

        result["errors"].append(
            f"Failed to fetch worker inventory: {error}"
        )

        return result

    inventory = inventory_data.get("chunks", [])

    result["inventory_count"] = len(inventory)

    inventory_chunk_ids = set()

    for item in inventory:

        if isinstance(item, dict):
            chunk_id = item.get("chunk_id")

        else:
            chunk_id = item

        if chunk_id:
            inventory_chunk_ids.add(chunk_id)

    # ==========================================
    # STEP 2: LOAD CONTROLLER METADATA
    # ==========================================

    chunks = db.query(Chunk).all()

    chunks_by_id = {
        chunk.chunk_id: chunk
        for chunk in chunks
    }

    existing_replicas = db.query(
        ChunkReplica
    ).filter(
        ChunkReplica.worker_id == worker_id
    ).all()

    replicas_by_chunk_id = {
        replica.chunk_id: replica
        for replica in existing_replicas
    }

    # ==========================================
    # STEP 3: VERIFY PHYSICAL COPIES
    # ==========================================

    candidate_chunks_for_repair = []

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        # ------------------------------------------
        # VERIFY CHUNKS PRESENT ON THIS WORKER
        # ------------------------------------------

        for chunk_id in sorted(inventory_chunk_ids):

            chunk = chunks_by_id.get(chunk_id)

            # --------------------------------------
            # UNKNOWN PHYSICAL CHUNK
            # --------------------------------------

            if not chunk:

                result["discovered"] += 1

                try:
                    response = await client.delete(
                        f"{worker.base_url}/internal/chunks/"
                        f"{chunk_id}"
                    )

                    if response.status_code not in (
                        200,
                        204,
                        404
                    ):
                        result["errors"].append(
                            f"Failed to remove unknown chunk "
                            f"{chunk_id} from {worker_id}"
                        )

                except httpx.HTTPError as error:

                    result["errors"].append(
                        f"Failed to remove unknown chunk "
                        f"{chunk_id} from {worker_id}: {error}"
                    )

                continue

            replica = replicas_by_chunk_id.get(chunk_id)

            # --------------------------------------
            # VERIFY PHYSICAL CONTENT
            # --------------------------------------

            try:

                response = await client.get(
                    f"{worker.base_url}/internal/chunks/"
                    f"{chunk_id}"
                )

                if response.status_code == 404:

                    if replica:
                        replica.status = "FAILED"

                    result["missing"] += 1

                    continue

                response.raise_for_status()

                content = response.content

                actual_checksum = hashlib.sha256(
                    content
                ).hexdigest()

                actual_size = len(content)

                # ----------------------------------
                # CORRUPTED COPY
                # ----------------------------------

                if (
                    actual_checksum != chunk.checksum
                    or actual_size != chunk.size
                ):

                    if replica:
                        replica.status = "CORRUPT"

                    result["corrupted"] += 1

                    print(
                        f"[Controller] Corrupt chunk "
                        f"{chunk_id} on {worker_id}"
                    )

                    candidate_chunks_for_repair.append(
                        chunk
                    )

                    continue

                # ----------------------------------
                # VALID COPY
                # ----------------------------------

                result["verified"] += 1

                print(
                    f"[Controller] Verified chunk "
                    f"{chunk_id} on {worker_id}"
                )

                # Find all currently ACTIVE replicas for
                # this chunk on other workers.

                active_replicas = db.query(
                    ChunkReplica
                ).filter(
                    ChunkReplica.chunk_id == chunk_id,
                    ChunkReplica.status == "ACTIVE"
                ).all()

                other_active_worker_ids = {
                    replica_item.worker_id
                    for replica_item in active_replicas
                    if replica_item.worker_id != worker_id
                }

                # ==================================
                # RECOVERED COPY IS EXCESS
                # ==================================

                if (
                    len(other_active_worker_ids)
                    >= REPLICATION_FACTOR
                ):

                    print(
                        f"[Controller] Removing excess recovered "
                        f"copy of chunk {chunk_id} from "
                        f"{worker_id}"
                    )

                    try:

                        delete_response = await client.delete(
                            f"{worker.base_url}/internal/chunks/"
                            f"{chunk_id}"
                        )

                        if delete_response.status_code not in (
                            200,
                            204,
                            404
                        ):

                            result["errors"].append(
                                f"Failed to remove excess chunk "
                                f"{chunk_id} from {worker_id}"
                            )

                            continue

                    except httpx.HTTPError as error:

                        result["errors"].append(
                            f"Failed to remove excess chunk "
                            f"{chunk_id} from {worker_id}: "
                            f"{error}"
                        )

                        continue

                    # Remove the controller replica record
                    # because this worker is no longer storing
                    # an active copy.

                    if replica:
                        db.delete(replica)
                        db.flush()

                    result["removed_excess_chunks"].append(
                        chunk_id
                    )

                    continue

                # ==================================
                # VALID COPY IS NEEDED
                # ==================================

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

                    replicas_by_chunk_id[chunk_id] = replica

                db.flush()

                # Check whether the chunk still needs
                # replication after accepting this copy.

                current_active_count = db.query(
                    ChunkReplica
                ).filter(
                    ChunkReplica.chunk_id == chunk_id,
                    ChunkReplica.status == "ACTIVE"
                ).count()

                if current_active_count < REPLICATION_FACTOR:

                    candidate_chunks_for_repair.append(
                        chunk
                    )

            except httpx.HTTPError as error:

                if replica:
                    replica.status = "FAILED"

                result["errors"].append(
                    f"Could not verify chunk {chunk_id} "
                    f"on {worker_id}: {error}"
                )

        # ==========================================
        # STEP 4: DETECT MISSING COPIES
        # ==========================================

        for replica in existing_replicas:

            chunk_id = replica.chunk_id

            if chunk_id not in inventory_chunk_ids:

                if replica.status == "ACTIVE":

                    replica.status = "FAILED"

                    result["missing"] += 1

                    chunk = chunks_by_id.get(chunk_id)

                    if chunk:
                        candidate_chunks_for_repair.append(
                            chunk
                        )

    # ==========================================
    # STEP 5: REPAIR REPLICATION SHORTFALLS
    # ==========================================

    unique_chunks = {}

    for chunk in candidate_chunks_for_repair:

        unique_chunks[chunk.chunk_id] = chunk

    for chunk in unique_chunks.values():

        try:

            # Get a verified source copy from any healthy
            # active replica.

            source_content = await get_verified_chunk_content(
                db,
                chunk
            )

            if source_content is None:

                result["errors"].append(
                    f"No verified source available for "
                    f"chunk {chunk.chunk_id}"
                )

                continue

            repair_result = await repair_chunk_replication(
                db,
                chunk,
                source_content
            )

            if repair_result.get("repaired"):

                repaired_workers = repair_result.get(
                    "repaired_workers",
                    []
                )

                result["repaired_chunks"].append({
                    "chunk_id": chunk.chunk_id,
                    "workers": repaired_workers
                })

        except Exception as error:

            result["errors"].append(
                f"Replication repair failed for "
                f"chunk {chunk.chunk_id}: {error}"
            )

    # ==========================================
    # STEP 6: FINAL RESULT
    # ==========================================

    db.flush()

    return result


async def get_verified_chunk_content(db, chunk):
    """
    Retrieve a verified copy of a chunk from any healthy
    ACTIVE replica.

    Returns:
        bytes: verified chunk content
        None: if no valid source copy is available
    """

    replicas = db.query(
        ChunkReplica
    ).filter(
        ChunkReplica.chunk_id == chunk.chunk_id,
        ChunkReplica.status == "ACTIVE"
    ).all()

    workers = db.query(Worker).all()

    workers_by_id = {
        worker.worker_id: worker
        for worker in workers
    }

    # Prefer healthy workers.

    replicas.sort(
        key=lambda replica: (
            workers_by_id.get(
                replica.worker_id
            ) is None
            or workers_by_id[
                replica.worker_id
            ].status != "HEALTHY"
        )
    )

    async with httpx.AsyncClient(
        timeout=WORKER_TIMEOUT
    ) as client:

        for replica in replicas:

            worker = workers_by_id.get(
                replica.worker_id
            )

            if not worker:
                replica.status = "FAILED"
                continue

            if worker.status != "HEALTHY":
                continue

            try:

                response = await client.get(
                    f"{worker.base_url}/internal/chunks/"
                    f"{chunk.chunk_id}"
                )

                if response.status_code == 404:

                    replica.status = "FAILED"
                    continue

                response.raise_for_status()

                content = response.content

                checksum = hashlib.sha256(
                    content
                ).hexdigest()

                if (
                    checksum != chunk.checksum
                    or len(content) != chunk.size
                ):

                    replica.status = "CORRUPT"

                    print(
                        f"[Controller] Invalid source replica "
                        f"{chunk.chunk_id} on "
                        f"{worker.worker_id}"
                    )

                    continue

                # Make sure the controller metadata reflects
                # the verified physical copy.

                replica.status = "ACTIVE"
                replica.checksum = chunk.checksum

                return content

            except httpx.HTTPError as error:

                replica.status = "FAILED"

                print(
                    f"[Controller] Could not retrieve source "
                    f"chunk {chunk.chunk_id} from "
                    f"{worker.worker_id}: {error}"
                )

    return None