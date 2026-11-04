import uuid
import asyncio
import httpx
import hashlib
import json

from fastapi import File, Form
from coordinator.models import ObjectMetadata, Chunk, ChunkReplica
from coordinator.chunk_manager import store_chunk
from shared.chunking import split_object, calculate_checksum
from fastapi import FastAPI, UploadFile, HTTPException, Depends
from fastapi.responses import Response

from sqlalchemy.orm import Session

from datetime import datetime, timezone, timedelta
from coordinator.models import Worker
from coordinator.models import ObjectMetadata, Chunk, ChunkReplica
from coordinator.database import Base, engine, get_db
from coordinator.models import FileMetadata
from coordinator.replication import (
    REPLICATION_FACTOR,
    get_healthy_nodes,
    replicate_file
)
from coordinator.models import FileMetadata, FileReplica
from coordinator.database import SessionLocal
from contextlib import asynccontextmanager
from urllib.parse import quote
from shared.chunking import reconstruct_object

@asynccontextmanager
async def lifespan(app: FastAPI):

    monitor_task = asyncio.create_task(
        monitor_worker_heartbeats()
    )

    print("[Controller] Worker monitoring started")

    yield

    monitor_task.cancel()

    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    print("[Controller] Worker monitoring stopped")


app = FastAPI(
    title="Distributed Cloud Store - Coordinator",
    lifespan=lifespan
)

Base.metadata.create_all(bind=engine)

STORAGE_NODES = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003"
}


@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "service": "coordinator"
    }


@app.post("/api/files/upload")
async def upload_file(
    file: UploadFile,
    db: Session = Depends(get_db)
):

    file_id = str(uuid.uuid4())

    content = await file.read()

    # Step 1: Find healthy storage nodes
    healthy_nodes = await get_healthy_nodes()

    if len(healthy_nodes) < REPLICATION_FACTOR:

        raise HTTPException(
            status_code=503,
            detail="Insufficient healthy storage nodes"
        )

    # Step 2: Select nodes for replication
    selected_nodes = healthy_nodes[:REPLICATION_FACTOR]

    # Step 3: Replicate file
    try:

        replicas = await replicate_file(
            file_id,
            file.filename,
            content,
            selected_nodes
        )

    except RuntimeError as error:

        raise HTTPException(
            status_code=503,
            detail=str(error)
        )

    # Step 4: Store file metadata
    metadata = FileMetadata(
        file_id=file_id,
        filename=file.filename,
        size=len(content),
        storage_node=replicas[0]["node_id"]
    )

    db.add(metadata)

    # Step 5: Store replica information
    for replica in replicas:

        db.add(
            FileReplica(
                file_id=file_id,
                node_id=replica["node_id"],
                status="ACTIVE",
                checksum=replica["checksum"]
            )
        )

    # Step 6: Commit metadata
    try:
        db.commit()

    except Exception:

        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Failed to save file metadata"
        )

    return {
        "message": "File uploaded and replicated successfully",
        "file_id": file_id,
        "filename": file.filename,
        "size": len(content),
        "replication_factor": len(replicas),
        "replicas": [
            replica["node_id"]
            for replica in replicas
        ],
        "checksum": replicas[0]["checksum"]
    }

@app.get("/api/files/{file_id}/replicas")
def get_file_replicas(
    file_id: str,
    db: Session = Depends(get_db)
):

    metadata = db.query(FileMetadata).filter(
        FileMetadata.file_id == file_id
    ).first()

    if not metadata:

        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    replicas = db.query(FileReplica).filter(
        FileReplica.file_id == file_id
    ).all()

    return {
        "file_id": file_id,
        "filename": metadata.filename,
        "replication_factor": sum(
            1 for replica in replicas
            if replica.status == "ACTIVE"
        ),
        "replicas": [
            {
                "node_id": replica.node_id,
                "status": replica.status,
                "checksum": replica.checksum
            }
            for replica in replicas
        ]
    }

@app.get("/api/files")
def list_files(db: Session = Depends(get_db)):

    files = db.query(FileMetadata).all()

    return files


@app.get("/api/files/{file_id}/download")
async def download_file(
    file_id: str,
    db: Session = Depends(get_db)
):
    metadata = db.query(FileMetadata).filter(
        FileMetadata.file_id == file_id
    ).first()

    if not metadata:
        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    replicas = db.query(FileReplica).filter(
        FileReplica.file_id == file_id,
        FileReplica.status == "ACTIVE"
    ).all()

    if not replicas:
        raise HTTPException(
            status_code=503,
            detail="No active replicas found"
        )

    async with httpx.AsyncClient(timeout=30.0) as client:

        for replica in replicas:
            node_url = STORAGE_NODES.get(replica.node_id)

            if not node_url:
                replica.status = "FAILED"
                db.commit()
                continue

            try:
                response = await client.get(
                    f"{node_url}/files/{file_id}"
                )
                response.raise_for_status()

            except httpx.HTTPError:
                print(f"{replica.node_id} is unavailable")

                replica.status = "FAILED"
                db.commit()
                continue

            # Verify file integrity before serving or replicating.
            actual_checksum = hashlib.sha256(
                response.content
            ).hexdigest()

            if actual_checksum != replica.checksum:
                print(f"Checksum mismatch on {replica.node_id}")

                replica.status = "CORRUPT"
                db.commit()
                continue

            # Find healthy nodes that do not already have
            # an active replica of this file.
            active_replicas = db.query(FileReplica).filter(
                FileReplica.file_id == file_id,
                FileReplica.status == "ACTIVE"
            ).all()

            active_node_ids = {
                item.node_id for item in active_replicas
            }

            if len(active_replicas) < REPLICATION_FACTOR:
                healthy_nodes = await get_healthy_nodes()

                replacement_nodes = [
                    node for node in healthy_nodes
                    if node[0] not in active_node_ids
                ]

                if replacement_nodes:
                    replacement = replacement_nodes[0]

                    try:
                        new_replicas = await replicate_file(
                            file_id,
                            metadata.filename,
                            response.content,
                            [replacement]
                        )

                        new_replica = new_replicas[0]

                        if new_replica["checksum"] == actual_checksum:
                            db.add(FileReplica(
                                file_id=file_id,
                                node_id=new_replica["node_id"],
                                status="ACTIVE",
                                checksum=new_replica["checksum"]
                            ))
                            db.commit()

                            print(
                                f"Replica recovered on "
                                f"{new_replica['node_id']}"
                            )
                        else:
                            await client.delete(
                                f"{replacement[1]}/files/{file_id}"
                            )
                            print("Recovery checksum mismatch")

                    except Exception as error:
                        db.rollback()
                        print(f"Re-replication failed: {error}")

            return Response(
                content=response.content,
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition":
                        f'attachment; filename="{metadata.filename}"',
                    "X-Served-By": replica.node_id
                }
            )

    raise HTTPException(
        status_code=503,
        detail="All storage replicas are unavailable"
    )

# ==========================================
# WORKER REGISTRATION
# ==========================================

@app.post("/internal/workers/register")
def register_worker(
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

    return {
        "message": "Worker registered successfully",
        "worker_id": worker.worker_id,
        "status": worker.status
    }


# ==========================================
# WORKER HEARTBEAT
# ==========================================

@app.post("/internal/workers/heartbeat")
def worker_heartbeat(
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

    worker.status = "HEALTHY"
    worker.last_heartbeat = datetime.now(timezone.utc)

    db.commit()

    return {
        "message": "Heartbeat received",
        "worker_id": worker.worker_id,
        "status": worker.status
    }


# ==========================================
# LIST REGISTERED WORKERS
# ==========================================

@app.get("/internal/workers")
def list_workers(db: Session = Depends(get_db)):
    workers = db.query(Worker).all()

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
# WORKER FAILURE DETECTION
# ==========================================

HEARTBEAT_TIMEOUT = 30
HEARTBEAT_CHECK_INTERVAL = 5


async def monitor_worker_heartbeats():

    while True:

        db = SessionLocal()

        try:
            workers = db.query(Worker).all()

            # SQLite commonly returns naive datetime values.
            current_time = datetime.now(timezone.utc).replace(
                tzinfo=None
            )

            for worker in workers:

                if worker.last_heartbeat is None:
                    continue

                last_heartbeat = worker.last_heartbeat

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
# DISTRIBUTED OBJECT UPLOAD
# ==========================================

@app.post("/api/v1/objects", status_code=201)
async def upload_object(
    file: UploadFile = File(...),
    metadata: str = Form(default="{}"),
    db: Session = Depends(get_db)
):
    # Step 1: Validate custom metadata
    try:
        custom_metadata = json.loads(metadata)

        if not isinstance(custom_metadata, dict):
            raise ValueError("Metadata must be a JSON object")

    except (json.JSONDecodeError, ValueError):
        raise HTTPException(
            status_code=400,
            detail="Invalid metadata. Provide a valid JSON object."
        )

    # Step 2: Find healthy workers
    workers = db.query(Worker).filter(
        Worker.status == "HEALTHY"
    ).all()

    if len(workers) < 2:
        raise HTTPException(
            status_code=503,
            detail="At least two healthy workers are required"
        )

    # Step 3: Read uploaded object
    content = await file.read()

    object_id = str(uuid.uuid4())
    object_checksum = calculate_checksum(content)

    # Step 4: Split object into chunks
    chunks = split_object(content)

    # Step 5: Save object metadata
    object_record = ObjectMetadata(
        object_id=object_id,
        object_name=file.filename or "unnamed",
        size=len(content),
        content_type=file.content_type or "application/octet-stream",
        checksum=object_checksum,
        chunk_count=len(chunks),
        custom_metadata=custom_metadata
    )

    db.add(object_record)

    stored_locations = []
    chunk_results = []

    try:
        # Ensure the object exists before inserting its chunks.
        db.flush()

        # Step 6: Store every chunk on two workers
        for chunk in chunks:

            result = await store_chunk(
                db,
                object_id,
                chunk,
                workers
            )

            chunk_results.append(result)

            # Remember successful storage locations for cleanup.
            for worker_id in result["replicas"]:
                stored_locations.append(
                    (worker_id, chunk["chunk_id"])
                )

            if not result["replication_complete"]:
                raise RuntimeError(
                    f"Replication failed for chunk {chunk['chunk_id']}"
                )

        # Step 7: Commit metadata and replica information
        db.commit()

    except Exception as error:
        db.rollback()

        # Clean up chunks already stored on workers.
        workers_by_id = {
            worker.worker_id: worker
            for worker in workers
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            for worker_id, chunk_id in stored_locations:
                worker = workers_by_id.get(worker_id)

                if worker:
                    try:
                        await client.delete(
                            f"{worker.base_url}/internal/chunks/{chunk_id}"
                        )
                    except httpx.HTTPError:
                        print(
                            f"Cleanup failed for chunk {chunk_id} "
                            f"on {worker_id}"
                        )

        raise HTTPException(
            status_code=503,
            detail=f"Object upload failed: {str(error)}"
        )

    # Step 8: Return upload result
    return {
        "message": "Object uploaded and replicated successfully",
        "object_id": object_id,
        "object_name": object_record.object_name,
        "size": len(content),
        "content_type": object_record.content_type,
        "checksum": object_checksum,
        "chunk_count": len(chunks),
        "replication_factor": 2,
        "chunks": chunk_results
    }

# ==========================================
# LIST OBJECTS
# ==========================================

@app.get("/api/v1/objects")
def list_objects(db: Session = Depends(get_db)):

    objects = db.query(ObjectMetadata).all()

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
    ).order_by(Chunk.sequence_number).all()

    chunk_details = []

    for chunk in chunks:

        replicas = db.query(ChunkReplica).filter(
            ChunkReplica.chunk_id == chunk.chunk_id
        ).all()

        chunk_details.append({
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
                1 for replica in replicas
                if replica.status == "ACTIVE"
            )
        })

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

    if not isinstance(updates, dict):
        raise HTTPException(
            status_code=400,
            detail="Metadata must be a JSON object"
        )

    if not updates:
        raise HTTPException(
            status_code=400,
            detail="Metadata update cannot be empty"
        )

    current_metadata = dict(obj.custom_metadata or {})
    updated_metadata = {**current_metadata, **updates}
    
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
    # Step 1: Find object metadata
    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    # Step 2: Retrieve chunks in sequence order
    chunks = db.query(Chunk).filter(
        Chunk.object_id == object_id
    ).order_by(Chunk.sequence_number).all()

    if len(chunks) != obj.chunk_count:
        raise HTTPException(
            status_code=500,
            detail="Object chunk metadata is incomplete"
        )

    reconstructed_chunks = []

    # Step 3: Retrieve and verify each chunk
    async with httpx.AsyncClient(timeout=30.0) as client:

        for chunk in chunks:

            replicas = db.query(ChunkReplica).filter(
                ChunkReplica.chunk_id == chunk.chunk_id,
                ChunkReplica.status == "ACTIVE"
            ).all()

            chunk_retrieved = False

            for replica in replicas:

                worker = db.query(Worker).filter(
                    Worker.worker_id == replica.worker_id
                ).first()

                if not worker:
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

                    # Verify chunk integrity
                    actual_checksum = calculate_checksum(chunk_content)

                    if actual_checksum != chunk.checksum:
                        replica.status = "CORRUPT"
                        print(
                            f"[Controller] Corrupt chunk "
                            f"{chunk.chunk_id} on {worker.worker_id}"
                        )
                        continue

                    reconstructed_chunks.append({
                        "chunk_id": chunk.chunk_id,
                        "sequence_number": chunk.sequence_number,
                        "content": chunk_content,
                        "checksum": chunk.checksum
                    })

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
                    detail=f"No valid replica available for chunk {chunk.chunk_id}"
                )

    # Step 4: Reconstruct the original object
    try:
        content = reconstruct_object(reconstructed_chunks)

    except ValueError as error:
        db.commit()

        raise HTTPException(
            status_code=500,
            detail=f"Object reconstruction failed: {str(error)}"
        )

    # Step 5: Verify complete object integrity
    actual_checksum = calculate_checksum(content)

    if actual_checksum != obj.checksum:
        raise HTTPException(
            status_code=500,
            detail="Object checksum mismatch after reconstruction"
        )

    db.commit()

    print(
        f"[Controller] Object {object_id} reconstructed successfully"
    )

    # Step 6: Return reconstructed object
    return Response(
        content=content,
        media_type=obj.content_type,
        headers={
            "Content-Disposition":
                f"attachment; filename*=UTF-8''{quote(obj.object_name)}",
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
    # Step 1: Find object
    obj = db.query(ObjectMetadata).filter(
        ObjectMetadata.object_id == object_id
    ).first()

    if not obj:
        raise HTTPException(
            status_code=404,
            detail="Object not found"
        )

    # Step 2: Find all chunks
    chunks = db.query(Chunk).filter(
        Chunk.object_id == object_id
    ).all()

    chunk_ids = [chunk.chunk_id for chunk in chunks]

    # Step 3: Find all replicas
    replicas = []

    if chunk_ids:
        replicas = db.query(ChunkReplica).filter(
            ChunkReplica.chunk_id.in_(chunk_ids)
        ).all()

    # Step 4: Delete physical chunks from workers
    failures = []

    async with httpx.AsyncClient(timeout=10.0) as client:

        for replica in replicas:

            worker = db.query(Worker).filter(
                Worker.worker_id == replica.worker_id
            ).first()

            if not worker:
                failures.append(replica.worker_id)
                continue

            try:
                response = await client.delete(
                    f"{worker.base_url}/internal/chunks/{replica.chunk_id}"
                )

                # 404 is acceptable: the chunk is already absent.
                if response.status_code not in (200, 204, 404):
                    failures.append(replica.worker_id)

            except httpx.HTTPError:
                failures.append(replica.worker_id)

    # Step 5: Do not delete metadata if physical cleanup is incomplete.
    if failures:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Could not delete all physical replicas. Retry after workers recover.",
                "unavailable_workers": sorted(set(failures))
            }
        )

    # Step 6: Delete database records in dependency order
    try:
        if chunk_ids:
            db.query(ChunkReplica).filter(
                ChunkReplica.chunk_id.in_(chunk_ids)
            ).delete(synchronize_session=False)

            db.query(Chunk).filter(
                Chunk.object_id == object_id
            ).delete(synchronize_session=False)

        db.delete(obj)
        db.commit()

    except Exception:
        db.rollback()

        raise HTTPException(
            status_code=500,
            detail="Failed to delete object metadata"
        )

    return Response(status_code=204)