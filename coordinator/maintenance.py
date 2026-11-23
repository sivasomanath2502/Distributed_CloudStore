"""
Background maintenance for the controller.

Responsibilities
----------------
1. Serialize reconciliation. Reconciliation used to run inside every
   register/heartbeat request, so after a controller restart four workers
   reconciled concurrently, SQLite reported "database is locked" and the
   failed reconciliation was never retried. Reconciliation now runs as a
   background task behind a single lock, and a failed run is retried.

2. Restore the replication factor proactively. Chunks that are
   under-replicated (for example because a worker is down) are copied to
   another healthy worker without waiting for a client read.

3. Re-examine chunks that have no ACTIVE copy but whose replica records
   point at healthy workers (state left behind by an interrupted
   reconciliation); reconciliation re-verifies the physical copies.

4. Scrub: periodically verify every stored chunk by checksum so silent
   corruption is found and repaired without a client read.

5. Retry physical deletes that could not be completed at DELETE time
   because a worker was unavailable.
"""

import asyncio
import os
import time

import httpx

from coordinator.database import SessionLocal
from coordinator.models import (
    Chunk,
    ChunkReplica,
    PendingDelete,
    Worker
)
from coordinator.reconciliation import (
    reconcile_worker,
    get_verified_chunk_content
)
from coordinator.repair import repair_chunk_replication


REPLICATION_FACTOR = 2

MAINTENANCE_INTERVAL = float(
    os.getenv("MAINTENANCE_INTERVAL_SECONDS", "15")
)

SCRUB_INTERVAL = float(
    os.getenv("SCRUB_INTERVAL_SECONDS", "120")
)

# After a controller (re)start, workers get this long to send their first
# heartbeat before the monitor may declare them unhealthy.
STARTUP_GRACE = float(
    os.getenv("STARTUP_GRACE_SECONDS", "35")
)

# Do not re-verify the same "no active copy" chunk more often than this.
DEEP_CHECK_BACKOFF = 120.0

# A worker whose reconciliation reported errors (for example because a
# chunk has no valid copy anywhere) is retried no more often than this.
RETRY_BACKOFF = 60.0


RECONCILE_LOCK = asyncio.Lock()

PENDING_RECONCILE = set()

_started_at = time.monotonic()
_inflight_uploads = 0
_last_deep_check = {}
_last_reconcile = {}
_tasks = set()


# ==========================================
# SMALL STATE HELPERS
# ==========================================

def mark_started():
    global _started_at
    _started_at = time.monotonic()


def in_startup_grace() -> bool:
    return (time.monotonic() - _started_at) < STARTUP_GRACE


def upload_started():
    global _inflight_uploads
    _inflight_uploads += 1


def upload_finished():
    global _inflight_uploads
    _inflight_uploads = max(0, _inflight_uploads - 1)


# ==========================================
# SERIALIZED RECONCILIATION
# ==========================================

async def reconcile_by_id(worker_id: str):
    """
    Reconcile one worker behind the global lock using its own session.
    A worker that fails reconciliation stays in PENDING_RECONCILE and is
    retried by the maintenance loop.
    """

    async with RECONCILE_LOCK:

        db = SessionLocal()

        try:

            worker = db.query(Worker).filter(
                Worker.worker_id == worker_id
            ).first()

            if worker is None or worker.status != "HEALTHY":
                # An unhealthy worker is reconciled when it recovers.
                PENDING_RECONCILE.discard(worker_id)
                return None

            _last_reconcile[worker_id] = time.monotonic()

            print(f"[Controller] Starting reconciliation for {worker_id}")

            result = await reconcile_worker(
                db,
                worker,
                purge_unknown=(_inflight_uploads == 0)
            )

            db.commit()

            if result.get("errors"):
                PENDING_RECONCILE.add(worker_id)
            else:
                PENDING_RECONCILE.discard(worker_id)

            print(
                f"[Controller] Reconciliation completed for "
                f"{worker_id}: {result}"
            )

            return result

        except Exception as error:

            db.rollback()

            PENDING_RECONCILE.add(worker_id)

            print(
                f"[Controller] Reconciliation failed for "
                f"{worker_id}: {error}"
            )

            return None

        finally:
            db.close()


def schedule_reconciliation(worker_id: str):
    """
    Queue a reconciliation without blocking the register/heartbeat
    request that triggered it.
    """

    PENDING_RECONCILE.add(worker_id)

    task = asyncio.create_task(reconcile_by_id(worker_id))

    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


# ==========================================
# PENDING PHYSICAL DELETES
# ==========================================

async def process_pending_deletes():

    db = SessionLocal()

    try:

        rows = db.query(PendingDelete).all()

        if not rows:
            return

        workers = {
            worker.worker_id: worker
            for worker in db.query(Worker).all()
        }

        async with httpx.AsyncClient(timeout=10.0) as client:

            for row in rows:

                worker = workers.get(row.worker_id)

                if worker is None:
                    db.delete(row)
                    continue

                if worker.status != "HEALTHY":
                    continue

                try:

                    response = await client.delete(
                        f"{worker.base_url}/internal/chunks/"
                        f"{row.chunk_id}"
                    )

                    if response.status_code in (200, 204, 404):
                        db.delete(row)

                        print(
                            f"[Controller] Completed pending delete of "
                            f"{row.chunk_id} on {row.worker_id}"
                        )

                except httpx.HTTPError:
                    pass

        db.commit()

    except Exception as error:

        db.rollback()

        print(f"[Controller] Pending delete processing error: {error}")

    finally:
        db.close()


# ==========================================
# REPLICATION STATE
# ==========================================

def _replica_maps(db):
    """
    Return (healthy worker ids, ACTIVE worker ids per chunk,
    non-ACTIVE worker ids per chunk).
    """

    healthy = {
        worker.worker_id
        for worker in db.query(Worker).filter(
            Worker.status == "HEALTHY"
        )
    }

    active = {}
    inactive = {}

    rows = db.query(
        ChunkReplica.chunk_id,
        ChunkReplica.worker_id,
        ChunkReplica.status
    ).all()

    for chunk_id, worker_id, status in rows:

        target = active if status == "ACTIVE" else inactive

        target.setdefault(chunk_id, set()).add(worker_id)

    return healthy, active, inactive


async def recheck_lost_chunks():
    """
    A chunk with no ACTIVE replica may still have intact physical copies
    on healthy workers (replica records are marked FAILED/CORRUPT by
    transient errors or an interrupted reconciliation). Reconcile those
    workers so the copies are re-verified and re-activated.
    """

    db = SessionLocal()

    workers_to_check = set()

    try:

        healthy, active, inactive = _replica_maps(db)

        now = time.monotonic()

        for chunk in db.query(Chunk).all():

            if active.get(chunk.chunk_id):
                continue

            candidates = inactive.get(chunk.chunk_id, set()) & healthy

            if not candidates:
                continue

            last = _last_deep_check.get(chunk.chunk_id, 0.0)

            if now - last < DEEP_CHECK_BACKOFF:
                continue

            _last_deep_check[chunk.chunk_id] = now

            workers_to_check |= candidates

    finally:
        db.close()

    for worker_id in sorted(workers_to_check):

        print(
            f"[Controller] Chunks without an active copy found; "
            f"re-verifying {worker_id}"
        )

        await reconcile_by_id(worker_id)


async def rereplicate():
    """
    Copy under-replicated chunks to additional healthy workers.
    """

    async with RECONCILE_LOCK:

        db = SessionLocal()

        try:

            healthy, active, _ = _replica_maps(db)

            for chunk in db.query(Chunk).all():

                holders = active.get(chunk.chunk_id, set())

                if len(holders) >= REPLICATION_FACTOR:
                    continue

                # Nothing to copy from.
                if not holders:
                    continue

                # No healthy worker left that could take another copy.
                if not (healthy - holders):
                    continue

                source = await get_verified_chunk_content(db, chunk)

                if source is None:
                    db.commit()
                    continue

                result = await repair_chunk_replication(
                    db,
                    chunk,
                    source
                )

                db.commit()

                if result.get("repaired"):

                    print(
                        f"[Controller] Re-replicated chunk "
                        f"{chunk.chunk_id} to "
                        f"{result.get('repaired_workers')} "
                        f"(active copies now "
                        f"{result.get('active_replica_count')})"
                    )

        except Exception as error:

            db.rollback()

            print(f"[Controller] Re-replication error: {error}")

        finally:
            db.close()


async def scrub_all_workers():
    """
    Verify every stored chunk on every healthy worker. Reconciliation
    marks missing/corrupt copies and repairs them from a valid replica.
    """

    db = SessionLocal()

    try:

        worker_ids = sorted(
            worker.worker_id
            for worker in db.query(Worker).filter(
                Worker.status == "HEALTHY"
            )
        )

    finally:
        db.close()

    for worker_id in worker_ids:
        await reconcile_by_id(worker_id)


# ==========================================
# MAINTENANCE LOOP
# ==========================================

async def maintenance_loop():

    last_scrub = time.monotonic()

    while True:

        await asyncio.sleep(MAINTENANCE_INTERVAL)

        if in_startup_grace():
            continue

        try:

            for worker_id in sorted(PENDING_RECONCILE):

                since = time.monotonic() - _last_reconcile.get(worker_id, 0.0)

                if since >= RETRY_BACKOFF:
                    await reconcile_by_id(worker_id)

            await process_pending_deletes()

            await recheck_lost_chunks()

            await rereplicate()

            if time.monotonic() - last_scrub >= SCRUB_INTERVAL:

                last_scrub = time.monotonic()

                await scrub_all_workers()

                await rereplicate()

        except asyncio.CancelledError:
            raise

        except Exception as error:

            print(f"[Controller] Maintenance error: {error}")
