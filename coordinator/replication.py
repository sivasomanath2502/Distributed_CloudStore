import httpx


REPLICATION_FACTOR = 2

STORAGE_NODES = {
    "node1": "http://localhost:8001",
    "node2": "http://localhost:8002",
    "node3": "http://localhost:8003"
}


async def get_healthy_nodes():

    healthy_nodes = []

    async with httpx.AsyncClient(timeout=3.0) as client:

        for node_id, node_url in STORAGE_NODES.items():

            try:

                response = await client.get(
                    f"{node_url}/health"
                )

                response.raise_for_status()

                healthy_nodes.append(
                    (node_id, node_url)
                )

            except httpx.HTTPError:

                print(f"{node_id} is unavailable")

    return healthy_nodes


async def replicate_file(
    file_id: str,
    filename: str,
    content: bytes,
    nodes: list
):

    successful_replicas = []

    async with httpx.AsyncClient(timeout=60.0) as client:

        try:

            for node_id, node_url in nodes:

                response = await client.post(
                    f"{node_url}/files/{file_id}",
                    files={
                        "file": (
                            filename,
                            content,
                            "application/octet-stream"
                        )
                    }
                )

                response.raise_for_status()

                result = response.json()

                if result["size"] != len(content):
                    raise RuntimeError("File size mismatch")

                successful_replicas.append({
                    "node_id": node_id,
                    "checksum": result["checksum"],
                    "size": result["size"]
                })

            # Verify all replicas
            checksums = {
                replica["checksum"]
                for replica in successful_replicas
            }

            if len(checksums) != 1:
                raise RuntimeError("Checksum mismatch")

            return successful_replicas

        except Exception as error:

            # Remove copies created during this operation
            for node_id, node_url in nodes:

                try:
                    await client.delete(
                        f"{node_url}/files/{file_id}"
                    )
                except httpx.HTTPError:
                    pass

            raise RuntimeError(
                f"Replication failed: {error}"
            ) from error