import uuid
import json
import asyncio
import httpx

from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from urllib.parse import quote

from fastapi import (
    FastAPI,
    File,
    Form,
    UploadFile,
    HTTPException,
    Depends
)
from fastapi.responses import Response, JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError

from sqlalchemy import func

from sqlalchemy.orm import Session

from coordinator.database import (
    Base,
    engine,
    get_db,
    SessionLocal
)

from coordinator.models import (
    ObjectMetadata,
    Chunk,
    Worker,
    ChunkReplica,
    PendingDelete
)

from coordinator import maintenance
from coordinator.chunk_manager import store_chunk
from coordinator.repair import repair_chunk_replication
from shared.chunking import (
    split_object,
    calculate_checksum,
    reconstruct_object
)


# ==========================================
# CONFIGURATION
# ==========================================

REPLICATION_FACTOR = 2

HEARTBEAT_TIMEOUT = 30
HEARTBEAT_CHECK_INTERVAL = 5

HTTP_TIMEOUT = 30.0


# ==========================================
# DATABASE INITIALIZATION
# ==========================================

Base.metadata.create_all(bind=engine)


# ==========================================
# WORKER FAILURE DETECTION
# ==========================================
async def monitor_worker_heartbeats():
    """
    Monitor worker heartbeats and mark workers unhealthy
    when their heartbeat timeout is exceeded.

    Whenever a worker is unhealthy, all ACTIVE replica records
    belonging to that worker are marked FAILED so that controller
    metadata accurately reflects worker availability.
    """

    while True:

        # After a controller restart the stored heartbeat timestamps are
        # stale. Give workers time to heartbeat before judging them,
        # otherwise every worker and replica is wrongly marked failed.
        if maintenance.in_startup_grace():
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)
            continue

        db = SessionLocal()

        try:
            workers = db.query(Worker).all()

            current_time = datetime.now(timezone.utc).replace(
                tzinfo=None
            )

            for worker in workers:

                if worker.last_heartbeat is None:
                    continue

                last_heartbeat = worker.last_heartbeat

                # SQLite may return naive datetime objects.
                if last_heartbeat.tzinfo is not None:
                    last_heartbeat = last_heartbeat.replace(
                        tzinfo=None
                    )

                elapsed = (
                    current_time - last_heartbeat
                ).total_seconds()

                if elapsed > HEARTBEAT_TIMEOUT:

                    if worker.status != "UNHEALTHY":
                        print(
                            f"[Controller] Worker "
                            f"{worker.worker_id} is UNHEALTHY"
                        )

                    worker.status = "UNHEALTHY"

                    # IMPORTANT:
                    # Do this every monitoring cycle while the worker
                    # remains unhealthy. This also handles the case
                    # where the controller itself was restarted while
                    # the worker was already unhealthy.
                    replicas = db.query(ChunkReplica).filter(
                        ChunkReplica.worker_id == worker.worker_id,
                        ChunkReplica.status == "ACTIVE"
                    ).all()

                    for replica in replicas:
                        replica.status = "FAILED"

                    if replicas:
                        print(
                            f"[Controller] Marked "
                            f"{len(replicas)} replica(s) as FAILED "
                            f"for unhealthy worker "
                            f"{worker.worker_id}"
                        )

            db.commit()

        except Exception as error:
            db.rollback()

            print(
                f"[Controller] Heartbeat monitoring error: {error}"
            )

        finally:
            db.close()

        await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)


# ==========================================
# APPLICATION LIFESPAN
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    maintenance.mark_started()

    monitor_task = asyncio.create_task(
        monitor_worker_heartbeats()
    )

    maintenance_task = asyncio.create_task(
        maintenance.maintenance_loop()
    )

    print("[Controller] Worker monitoring started")

    try:
        yield

    finally:
        monitor_task.cancel()
        maintenance_task.cancel()

        for task in (monitor_task, maintenance_task):

            try:
                await task

            except asyncio.CancelledError:
                pass

        print("[Controller] Worker monitoring stopped")


# ==========================================
# FASTAPI APPLICATION
# ==========================================

app = FastAPI(
    title="Distributed Cloud Store - Coordinator",
    description=(
        "A distributed cloud object storage system "
        "with chunking, replication and fault tolerance."
    ),
    version="1.0.0",
    lifespan=lifespan
)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request, exc):
    """
    The API specification uses 400 (not FastAPI's default 422) for an
    invalid request.
    """

    errors = [
        {
            "field": ".".join(str(part) for part in error["loc"]),
            "message": error["msg"]
        }
        for error in jsonable_encoder(exc.errors())
    ]

    return JSONResponse(
        status_code=400,
        content={
            "detail": "Invalid request",
            "errors": errors
        }
    )


# ==========================================
# HEALTH CHECK
# ==========================================

@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "service": "coordinator"
    }


# ==========================================
# WORKER REGISTRATION
# ==========================================

@app.post("/internal/workers/register")
async def register_worker(
    worker_data: dict,
    db: Session = Depends(get_db)
):

    worker_id = worker_data.get("worker_id")
    base_url = worker_data.get("base_url")

    if not worker_id or not base_url:
        raise HTTPException(
            status_code=400,
            detail="worker_id and base_url are required"
        )

    worker = db.query(Worker).filter(
        Worker.worker_id == worker_id
    ).first()

    if worker:

        worker.base_url = base_url
        worker.status = "HEALTHY"
        worker.last_heartbeat = datetime.now(timezone.utc)

    else:

        worker = Worker(
            worker_id=worker_id,
            base_url=base_url,
            status="HEALTHY",
            last_heartbeat=datetime.now(timezone.utc)
        )

        db.add(worker)

    try:

        db.commit()
        db.refresh(worker)

    except Exception:

        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Worker registration failed"
        )

    print(
        f"[Controller] Worker registered: {worker.worker_id}"
    )

    # Reconcile the worker's storage after registration (in the
    # background, serialized, so requests never block or collide).
    maintenance.schedule_reconciliation(worker.worker_id)

    reconciliation_result = "scheduled"

    return {
        "message": "Worker registered successfully",
        "worker_id": worker.worker_id,
        "status": worker.status,
        "reconciliation": reconciliation_result
    }


# ==========================================
# WORKER HEARTBEAT
# ==========================================

@app.post("/internal/workers/heartbeat")
async def worker_heartbeat(
    heartbeat_data: dict,
    db: Session = Depends(get_db)
):

    worker_id = heartbeat_data.get("worker_id")

    if not worker_id:
        raise HTTPException(
            status_code=400,
            detail="worker_id is required"
        )

    worker = db.query(Worker).filter(
        Worker.worker_id == worker_id
    ).first()

    if not worker:
        raise HTTPException(
            status_code=404,
            detail="Worker is not registered"
        )

    # Detect recovery from an unhealthy state.
    was_unhealthy = worker.status != "HEALTHY"

    worker.status = "HEALTHY"
    worker.last_heartbeat = datetime.now(timezone.utc)

    try:

        db.commit()
        db.refresh(worker)

    except Exception:

        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Heartbeat update failed"
        )

    reconciliation_result = None

    # Reconcile only when the worker transitions back to HEALTHY.
    if was_unhealthy:

        print(
            f"[Controller] Worker recovery detected: {worker_id}"
        )

        maintenance.schedule_reconciliation(worker_id)

        reconciliation_result = "scheduled"

    return {
        "message": "Heartbeat received",
        "worker_id": worker.worker_id,
        "status": worker.status,
        "reconciliation": reconciliation_result
    }


# ==========================================
# LIST REGISTERED WORKERS
# ==========================================

@app.get("/internal/workers")
def list_workers(
    db: Session = Depends(get_db)
):

    workers = db.query(Worker).order_by(
        Worker.worker_id
    ).all()

    return {
        "total_workers": len(workers),

        "workers": [
            {
                "worker_id": worker.worker_id,
                "base_url": worker.base_url,
                "status": worker.status,
                "last_heartbeat": worker.last_heartbeat
            }
            for worker in workers
        ]
    }


# ==========================================
# UPLOAD DISTRIBUTED OBJECT
# ==========================================

@app.post("/api/v1/objects", status_code=201)
async def upload_object(
    file: UploadFile = File(...),
    metadata: str = Form(default="{}"),
    db: Session = Depends(get_db)
):
    # Track in-flight uploads: chunks are on the workers before their
    # metadata is committed, and reconciliation must not mistake them
    # for unknown chunks.
    maintenance.upload_started()

    try:
        return await _upload_object_impl(file, metadata, db)

    finally:
        maintenance.upload_finished()


async def _upload_object_impl(
    file: UploadFile,
    metadata: str,
    db: Session
):

    # Step 1: Validate custom metadata.

    try:
        custom_metadata = json.loads(metadata)

        if not isinstance(custom_metadata, dict):
            raise ValueError("Metadata must be a JSON object")

    except (json.JSONDecodeError, ValueError):

        raise HTTPException(
            status_code=400,
            detail="Invalid metadata. Provide a valid JSON object."
        )

    # Step 2: Find healthy workers.

    workers = db.query(Worker).filter(
        Worker.status == "HEALTHY"
    ).all()

    if len(workers) < REPLICATION_FACTOR:

        raise HTTPException(
            status_code=503,
            detail="At least two healthy workers are required"
        )

    # Step 3: Read uploaded object.

    content = await file.read()

    object_id = str(uuid.uuid4())

    object_checksum = calculate_checksum(content)

    # Step 4: Split object into chunks.

    chunks = split_object(content)

    # Step 5: Create object metadata.

    object_record = ObjectMetadata(
        object_id=object_id,
        object_name=file.filename or "unnamed",
        size=len(content),
        content_type=file.content_type or "application/octet-stream",
        checksum=object_checksum,
        chunk_count=len(chunks),
        custom_metadata=custom_metadata
    )

    stored_locations = []
    chunk_results = []

    try:

        # Current ACTIVE chunk count per worker, so that placement can
        # keep the cluster balanced.
        load = {
            worker_id: count
            for worker_id, count in db.query(
                ChunkReplica.worker_id,
                func.count(ChunkReplica.id)
            ).filter(
                ChunkReplica.status == "ACTIVE"
            ).group_by(ChunkReplica.worker_id).all()
        }

        # Step 6: Store chunks with replication (no database writes
        # happen during network I/O, so no write lock is held).

        for chunk in chunks:

            result = await store_chunk(
                db,
                object_id,
                chunk,
                workers,
                load
            )

            chunk_results.append(result)

            # Track successful physical copies for rollback cleanup.

            for worker_id in result["replicas"]:

                stored_locations.append(
                    (worker_id, chunk["chunk_id"])
                )

                load[worker_id] = load.get(worker_id, 0) + 1

            if not result["replication_complete"]:

                raise RuntimeError(
                    f"Replication failed for chunk {chunk['chunk_id']}"
                )

        # Step 7: Record object, chunk and replica metadata and commit
        # in one short transaction.

        db.add(object_record)
        db.flush()

        for chunk, result in zip(chunks, chunk_results):

            db.add(
                Chunk(
                    chunk_id=chunk["chunk_id"],
                    object_id=object_id,
                    sequence_number=chunk["sequence_number"],
                    size=chunk["size"],
                    checksum=chunk["checksum"]
                )
            )

        db.flush()

        for chunk, result in zip(chunks, chunk_results):

            for worker_id in result["replicas"]:

                db.add(
                    ChunkReplica(
                        chunk_id=chunk["chunk_id"],
                        worker_id=worker_id,
                        status="ACTIVE",
                        checksum=chunk["checksum"]
                    )
                )

        db.commit()

    except Exception as error:

        db.rollback()

        # Remove physical chunks created during this failed upload.

        workers_by_id = {
            worker.worker_id: worker
            for worker in workers
        }

        async with httpx.AsyncClient(timeout=10.0) as client:

            for worker_id, chunk_id in stored_locations:

                worker = workers_by_id.get(worker_id)

                if not worker:
                    continue

                try:

                    await client.delete(
                        f"{worker.base_url}/internal/chunks/{chunk_id}"
                    )

                except httpx.HTTPError:

                    print(
                        f"[Controller] Cleanup failed for "
                        f"{chunk_id} on {worker_id}"
                    )

        print(
            f"[Controller] Upload failed: {error}"
        )

        raise HTTPException(
            status_code=503,
            detail=f"Object upload failed: {str(error)}"
        )

    return {
        "message": "Object uploaded and replicated successfully",
        "object_id": object_id,
        "object_name": object_record.object_name,
        "size": len(content),
        "content_type": object_record.content_type,
        "checksum": object_checksum,
        "chunk_count": len(chunks),
        "replication_factor": REPLICATION_FACTOR,
        "chunks": chunk_results
    }


# ==========================================
# LIST OBJECTS
# ==========================================

@app.get("/api/v1/objects")
def list_objects(
    db: Session = Depends(get_db)
):

    objects = db.query(ObjectMetadata).order_by(
        ObjectMetadata.created_at.desc()
    ).all()

    return {
        "total_objects": len(objects),

        "objects": [
            {
                "object_id": obj.object_id,
                "object_name": obj.object_name,
                "size": obj.size,
                "content_type": obj.content_type,
                "checksum": obj.checksum,
                "chunk_count": obj.chunk_count,
                "created_at": obj.created_at,
                "updated_at": obj.updated_at
            }
            for obj in objects
        ]
    }


# ==========================================
# GET OBJECT METADATA
# ==========================================

@app.get("/api/v1/objects/{object_id}/metadata")
def get_object_metadata(
    object_id: str,
    db: Session = Depends(get_db)
):

    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:

        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    chunks = db.query(Chunk).filter(
        Chunk.object_id == object_id
    ).order_by(
        Chunk.sequence_number
    ).all()

    chunk_details = []

    for chunk in chunks:

        replicas = db.query(ChunkReplica).filter(
            ChunkReplica.chunk_id == chunk.chunk_id
        ).all()

        chunk_details.append(
            {
                "chunk_id": chunk.chunk_id,
                "sequence_number": chunk.sequence_number,
                "size": chunk.size,
                "checksum": chunk.checksum,

                "replicas": [
                    {
                        "worker_id": replica.worker_id,
                        "status": replica.status,
                        "checksum": replica.checksum
                    }
                    for replica in replicas
                ],

                "active_replica_count": sum(
                    1
                    for replica in replicas
                    if replica.status == "ACTIVE"
                )
            }
        )

    return {
        "object_id": obj.object_id,
        "object_name": obj.object_name,
        "size": obj.size,
        "content_type": obj.content_type,
        "checksum": obj.checksum,
        "chunk_count": obj.chunk_count,
        "custom_metadata": obj.custom_metadata,
        "created_at": obj.created_at,
        "updated_at": obj.updated_at,
        "chunks": chunk_details
    }


# ==========================================
# UPDATE OBJECT CUSTOM METADATA
# ==========================================

@app.patch("/api/v1/objects/{object_id}/metadata")
def update_object_metadata(
    object_id: str,
    updates: dict,
    db: Session = Depends(get_db)
):

    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:

        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    if not updates:

        raise HTTPException(
            status_code=400,
            detail="Metadata update cannot be empty"
        )

    # Merge metadata instead of replacing existing values.

    current_metadata = dict(obj.custom_metadata or {})

    updated_metadata = {
        **current_metadata,
        **updates
    }

    obj.custom_metadata = updated_metadata

    try:

        db.commit()
        db.refresh(obj)

    except Exception:

        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Failed to update object metadata"
        )

    return {
        "message": "Object metadata updated successfully",
        "object_id": obj.object_id,
        "custom_metadata": obj.custom_metadata,
        "updated_at": obj.updated_at
    }


# ==========================================
# DOWNLOAD AND RECONSTRUCT OBJECT
# ==========================================

@app.get("/api/v1/objects/{object_id}")
async def download_object(
    object_id: str,
    db: Session = Depends(get_db)
):

    # Step 1: Find object metadata.

    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:

        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    # Step 2: Retrieve chunks in sequence order.

    chunks = db.query(Chunk).filter(
        Chunk.object_id == object_id
    ).order_by(
        Chunk.sequence_number
    ).all()

    if len(chunks) != obj.chunk_count:

        raise HTTPException(
            status_code=500,
            detail="Object chunk metadata is incomplete"
        )

    reconstructed_chunks = []

    # Step 3: Retrieve and verify each chunk.

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:

        for chunk in chunks:

            replicas = db.query(ChunkReplica).filter(
                ChunkReplica.chunk_id == chunk.chunk_id,
                ChunkReplica.status == "ACTIVE"
            ).all()

            # Prefer replicas on workers currently marked healthy.
            replicas.sort(
                key=lambda replica: (
                    db.query(Worker.status).filter(
                        Worker.worker_id == replica.worker_id
                    ).scalar() != "HEALTHY"
                )
            )

            chunk_retrieved = False

            for replica in replicas:

                worker = db.query(Worker).filter(
                    Worker.worker_id == replica.worker_id
                ).first()

                if not worker:
                    replica.status = "FAILED"
                    continue

                # Do not attempt a request to a worker that the
                # heartbeat monitor already knows is unhealthy.
                if worker.status != "HEALTHY":
                    replica.status = "FAILED"
                    continue

                try:

                    response = await client.get(
                        f"{worker.base_url}/internal/chunks/{chunk.chunk_id}"
                    )

                    if response.status_code == 404:
                        replica.status = "FAILED"
                        continue

                    response.raise_for_status()

                    chunk_content = response.content

                    # Verify chunk checksum.

                    actual_checksum = calculate_checksum(
                        chunk_content
                    )

                    if (
                        len(chunk_content) != chunk.size
                        or actual_checksum != chunk.checksum
                    ):

                        replica.status = "CORRUPT"

                        print(
                            f"[Controller] Corrupt chunk "
                            f"{chunk.chunk_id} on {worker.worker_id}"
                        )

                        continue

                    # Repair missing replicas using verified content.
                    # Repair failure should not prevent a valid download.

                    try:

                        # Serialize with the background maintenance loop so
                        # that two repairers never create the same replica
                        # row, and re-read rows it may have just committed.
                        async with maintenance.RECONCILE_LOCK:

                            db.expire_all()

                            repair_result = await repair_chunk_replication(
                                db,
                                chunk,
                                chunk_content
                            )

                            db.commit()

                        print(
                            f"[Controller] Repair result for "
                            f"{chunk.chunk_id}: {repair_result}"
                        )

                    except Exception as error:

                        # A failed repair must neither fail the download
                        # nor leave the session in a broken state.
                        db.rollback()

                        print(
                            f"[Controller] Repair failed for "
                            f"{chunk.chunk_id}: {error}"
                        )

                    reconstructed_chunks.append(
                        {
                            "chunk_id": chunk.chunk_id,
                            "sequence_number": chunk.sequence_number,
                            "content": chunk_content,
                            "checksum": chunk.checksum
                        }
                    )

                    chunk_retrieved = True

                    print(
                        f"[Controller] Retrieved chunk "
                        f"{chunk.chunk_id} from {worker.worker_id}"
                    )

                    break

                except httpx.HTTPError as error:

                    replica.status = "FAILED"

                    print(
                        f"[Controller] Worker {worker.worker_id} "
                        f"unavailable: {error}"
                    )

            if not chunk_retrieved:

                db.commit()

                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"No valid replica available for "
                        f"chunk {chunk.chunk_id}"
                    )
                )

    # Step 4: Reconstruct original object.

    try:

        content = reconstruct_object(
            reconstructed_chunks
        )

    except ValueError as error:

        db.commit()

        raise HTTPException(
            status_code=500,
            detail=f"Object reconstruction failed: {str(error)}"
        )

    # Step 5: Verify complete object checksum.

    actual_checksum = calculate_checksum(content)

    if actual_checksum != obj.checksum:

        db.commit()

        raise HTTPException(
            status_code=500,
            detail="Object checksum mismatch after reconstruction"
        )

    db.commit()

    print(
        f"[Controller] Object {object_id} reconstructed successfully"
    )

    # Step 6: Return reconstructed object.

    return Response(
        content=content,
        media_type=obj.content_type,
        headers={
            "Content-Disposition": (
                f"attachment; filename*=UTF-8''{quote(obj.object_name)}"
            ),
            "X-Object-Checksum": actual_checksum,
            "X-Object-ID": object_id
        }
    )


# ==========================================
# DELETE OBJECT
# ==========================================

@app.delete("/api/v1/objects/{object_id}", status_code=204)
async def delete_object(
    object_id: str,
    db: Session = Depends(get_db)
):

    # Step 1: Find object.

    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:

        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    # Step 2: Find chunks.

    chunks = db.query(Chunk).filter(
        Chunk.object_id == object_id
    ).all()

    chunk_ids = [
        chunk.chunk_id
        for chunk in chunks
    ]

    # Step 3: Find all replica records.

    replicas = []

    if chunk_ids:

        replicas = db.query(ChunkReplica).filter(
            ChunkReplica.chunk_id.in_(chunk_ids)
        ).all()

    # Step 4: Delete physical chunk copies. A copy that cannot be removed
    # now (worker down) is queued and removed when the worker is healthy
    # again, so DELETE succeeds without leaving orphaned data behind.

    queued_workers = set()

    async with httpx.AsyncClient(timeout=10.0) as client:

        for replica in replicas:

            worker = db.query(Worker).filter(
                Worker.worker_id == replica.worker_id
            ).first()

            if not worker:
                continue

            deleted = False

            if worker.status == "HEALTHY":

                try:

                    response = await client.delete(
                        f"{worker.base_url}/internal/chunks/{replica.chunk_id}"
                    )

                    # 404 means the physical copy is already absent.

                    deleted = response.status_code in (200, 204, 404)

                except httpx.HTTPError:

                    deleted = False

            if not deleted:

                already_queued = db.query(PendingDelete).filter(
                    PendingDelete.worker_id == replica.worker_id,
                    PendingDelete.chunk_id == replica.chunk_id
                ).first()

                if not already_queued:

                    db.add(
                        PendingDelete(
                            worker_id=replica.worker_id,
                            chunk_id=replica.chunk_id
                        )
                    )

                queued_workers.add(replica.worker_id)

    if queued_workers:

        print(
            f"[Controller] Deferred physical delete on "
            f"{sorted(queued_workers)} for object {object_id}"
        )

    # Step 5: Remove metadata.

    # Step 6: Delete database records in dependency order.

    try:

        if chunk_ids:

            db.query(ChunkReplica).filter(
                ChunkReplica.chunk_id.in_(chunk_ids)
            ).delete(
                synchronize_session=False
            )

            db.query(Chunk).filter(
                Chunk.object_id == object_id
            ).delete(
                synchronize_session=False
            )

        db.delete(obj)

        db.commit()

    except Exception:

        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Failed to delete object metadata"
        )

    return Response(status_code=204)