import os
import asyncio
import hashlib
import httpx

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response

from storage_node.storage import (
    save_file,
    get_file,
    delete_file,
    list_files
)


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
CONTROLLER_TIMEOUT = 5.0


async def register_worker(client):
    response = await client.post(
        f"{CONTROLLER_URL}/internal/workers/register",
        json={
            "worker_id": NODE_ID,
            "base_url": WORKER_URL
        }
    )

    response.raise_for_status()

    print(
        f"[{NODE_ID}] Registered with controller "
        f"using {WORKER_URL}"
    )


async def heartbeat_loop():

    async with httpx.AsyncClient(
        timeout=CONTROLLER_TIMEOUT
    ) as client:

        # Initial registration.
        # If the controller is unavailable, keep retrying,
        # but do not start a registration/reconciliation storm.
        registered = False

        while not registered:

            try:
                await register_worker(client)
                registered = True

            except httpx.HTTPError as error:

                print(
                    f"[{NODE_ID}] Controller unavailable during "
                    f"startup registration: {error}"
                )

                await asyncio.sleep(5)

        # Normal heartbeat loop.
        while True:

            try:

                response = await client.post(
                    f"{CONTROLLER_URL}/internal/workers/heartbeat",
                    json={
                        "worker_id": NODE_ID
                    }
                )

                if response.status_code == 404:

                    print(
                        f"[{NODE_ID}] Worker is not registered. "
                        f"Attempting registration..."
                    )

                    try:
                        await register_worker(client)

                    except httpx.HTTPError as error:

                        print(
                            f"[{NODE_ID}] Registration failed: {error}"
                        )

                else:

                    response.raise_for_status()

                    print(
                        f"[{NODE_ID}] Heartbeat sent"
                    )

            except httpx.HTTPError as error:

                # IMPORTANT:
                # A network timeout or controller outage is NOT
                # treated as a registration event.
                print(
                    f"[{NODE_ID}] Controller unavailable: {error}"
                )

            except Exception as error:

                print(
                    f"[{NODE_ID}] Heartbeat error: {error}"
                )

            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app):

    heartbeat_task = asyncio.create_task(
        heartbeat_loop()
    )

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


@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "service": "storage-node",
        "node_id": NODE_ID
    }


@app.put("/internal/chunks/{chunk_id}")
async def store_chunk(
    chunk_id,
    request: Request
):

    try:

        content = await request.body()

        if not content:
            raise HTTPException(
                status_code=400,
                detail="Chunk cannot be empty"
            )

        save_file(
            chunk_id,
            content
        )

        checksum = hashlib.sha256(
            content
        ).hexdigest()

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
def retrieve_chunk(chunk_id):

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
def remove_chunk(chunk_id):

    try:

        deleted = delete_file(chunk_id)

        if not deleted:

            raise HTTPException(
                status_code=404,
                detail="Chunk not found"
            )

        return Response(
            status_code=204
        )

    except ValueError as error:

        raise HTTPException(
            status_code=400,
            detail=str(error)
        )


@app.get("/internal/workers/{worker_id}/chunks")
def get_worker_chunks(worker_id):

    if worker_id != NODE_ID:

        raise HTTPException(
            status_code=404,
            detail="Worker not found"
        )

    stored_files = list_files()

    # Normalize the storage-layer response to the controller's
    # chunk API contract.
    chunks = [
        {
            "chunk_id": item["file_id"],
            "size": item["size"]
        }
        for item in stored_files
    ]

    return {
        "worker_id": NODE_ID,
        "total_chunks": len(chunks),
        "chunks": chunks
    }