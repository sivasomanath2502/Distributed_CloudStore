import os
import asyncio
import hashlib
import httpx

from fastapi import Request
from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import Response

from storage_node.storage import (
    save_file,
    get_file,
    delete_file,
    list_files
)


# ==========================================
# WORKER CONFIGURATION
# ==========================================

NODE_ID = os.getenv("NODE_ID", "node1")

NODE_PORT = os.getenv("NODE_PORT", "8001")

WORKER_URL = os.getenv(
    "WORKER_URL",
    f"http://localhost:{NODE_PORT}"
)

CONTROLLER_URL = os.getenv(
    "CONTROLLER_URL",
    "http://localhost:8000"
)

HEARTBEAT_INTERVAL = 10


# ==========================================
# WORKER REGISTRATION
# ==========================================

async def register_worker(client: httpx.AsyncClient):

    response = await client.post(
        f"{CONTROLLER_URL}/internal/workers/register",
        json={
            "worker_id": NODE_ID,
            "base_url": WORKER_URL
        }
    )

    response.raise_for_status()

    print(f"[{NODE_ID}] Registered with controller")


# ==========================================
# PERIODIC HEARTBEAT
# ==========================================

async def heartbeat_loop():

    async with httpx.AsyncClient(timeout=5.0) as client:

        while True:

            try:
                try:
                    response = await client.post(
                        f"{CONTROLLER_URL}/internal/workers/heartbeat",
                        json={
                            "worker_id": NODE_ID
                        }
                    )

                    if response.status_code == 404:
                        await register_worker(client)
                    else:
                        response.raise_for_status()
                        print(f"[{NODE_ID}] Heartbeat sent")

                except httpx.HTTPError:
                    print(
                        f"[{NODE_ID}] Controller unavailable. "
                        "Attempting registration..."
                    )

                    await register_worker(client)

            except Exception as error:
                print(f"[{NODE_ID}] Heartbeat error: {error}")

            await asyncio.sleep(HEARTBEAT_INTERVAL)


# ==========================================
# APPLICATION LIFECYCLE
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    heartbeat_task = asyncio.create_task(heartbeat_loop())

    yield

    heartbeat_task.cancel()

    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="Distributed Cloud Store - Storage Node",
    description="Independent distributed storage worker",
    version="2.0.0",
    lifespan=lifespan
)


# ==========================================
# HEALTH CHECK
# ==========================================

@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "service": "storage-node",
        "node_id": NODE_ID
    }


# ==========================================
# FILE UPLOAD
# ==========================================

@app.post("/files/{file_id}")
async def upload_file(file_id: str, file: UploadFile):

    content = await file.read()

    save_file(file_id, content)

    checksum = hashlib.sha256(content).hexdigest()

    return {
        "message": "File stored successfully",
        "file_id": file_id,
        "filename": file.filename,
        "size": len(content),
        "checksum": checksum
    }


# ==========================================
# FILE DOWNLOAD
# ==========================================

@app.get("/files/{file_id}")
def download_file(file_id: str):

    content = get_file(file_id)

    if content is None:
        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    return Response(
        content=content,
        media_type="application/octet-stream"
    )


# ==========================================
# FILE DELETE
# ==========================================

@app.delete("/files/{file_id}")
def remove_file(file_id: str):

    deleted = delete_file(file_id)

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail="File not found"
        )

    return {
        "message": "File deleted successfully",
        "file_id": file_id
    }

# ==========================================
# INTERNAL CHUNK STORAGE APIs
# ==========================================

@app.put("/internal/chunks/{chunk_id}")
async def store_chunk(
    chunk_id: str,
    request: Request
):
    try:
        content = await request.body()

        if not content:
            raise HTTPException(
                status_code=400,
                detail="Chunk cannot be empty"
            )

        save_file(chunk_id, content)

        checksum = hashlib.sha256(content).hexdigest()

        return {
            "message": "Chunk stored successfully",
            "chunk_id": chunk_id,
            "worker_id": NODE_ID,
            "size": len(content),
            "checksum": checksum
        }

    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail=str(error)
        )


@app.get("/internal/chunks/{chunk_id}")
def retrieve_chunk(chunk_id: str):

    try:
        content = get_file(chunk_id)

        if content is None:
            raise HTTPException(
                status_code=404,
                detail="Chunk not found"
            )

        return Response(
            content=content,
            media_type="application/octet-stream"
        )

    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail=str(error)
        )


@app.delete("/internal/chunks/{chunk_id}")
def remove_chunk(chunk_id: str):

    try:
        deleted = delete_file(chunk_id)

        if not deleted:
            raise HTTPException(
                status_code=404,
                detail="Chunk not found"
            )

        return Response(status_code=204)

    except ValueError as error:
        raise HTTPException(
            status_code=400,
            detail=str(error)
        )


@app.get("/internal/workers/{worker_id}/chunks")
def get_worker_chunks(worker_id: str):

    if worker_id != NODE_ID:
        raise HTTPException(
            status_code=404,
            detail="Worker not found"
        )

    chunks = list_files()

    return {
        "worker_id": NODE_ID,
        "total_chunks": len(chunks),
        "chunks": chunks
    }