import uuid

import httpx
import hashlib
from fastapi import FastAPI, UploadFile, HTTPException, Depends
from fastapi.responses import Response

from sqlalchemy.orm import Session

from coordinator.database import Base, engine, get_db
from coordinator.models import FileMetadata
from coordinator.replication import (
    REPLICATION_FACTOR,
    get_healthy_nodes,
    replicate_file
)
from coordinator.models import FileMetadata, FileReplica

app = FastAPI(
    title="Distributed Cloud Store - Coordinator",
    version="1.0.0"
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