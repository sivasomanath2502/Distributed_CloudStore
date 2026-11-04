from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import Response
import os
import hashlib
from storage_node.storage import (
    save_file,
    get_file,
    delete_file
)

app = FastAPI(
    title="Distributed Cloud Store - Storage Node",
    description="Independent file storage service",
    version="1.0.0"
)


@app.get("/health")
def health_check():

    return {
        "status": "healthy",
        "service": "storage-node",
        "node_id": os.getenv("NODE_ID", "node1")
    }


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