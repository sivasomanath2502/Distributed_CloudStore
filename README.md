# Distributed Cloud Object Store

A mini distributed cloud object storage system that stores large binary objects as fixed-size chunks and maintains replicated copies across multiple storage workers.

## Project

The system consists of:

- **Controller** — manages objects, metadata, chunk placement, replication, worker health, and recovery.
- **Storage Workers** — store chunk data persistently on local storage.
- **Client** — communicates with the Controller through REST APIs.

The system uses:

- 1 MiB chunks
- Replication factor of 2
- SHA-256 checksums for data integrity
- Persistent storage for each worker
- Docker Compose for deployment

## Project Structure

```text
Project/
├── coordinator/
│   ├── main.py
│   ├── database.py
│   ├── models.py
│   ├── placement.py
│   ├── chunk_manager.py
│   ├── repair.py
│   └── reconciliation.py
│
├── storage_node/
│   ├── main.py
│   └── storage.py
│
├── shared/
├── data/
│   ├── node1/
│   ├── node2/
│   ├── node3/
│   └── node4/
│
├── tests/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## How to Run

Start the controller and four storage workers:

```bash
docker-compose up -d --build
```

Check running containers:

```bash
docker-compose ps
```

The services are available at:

```text
Controller : http://localhost:8000
Worker 1   : http://localhost:8001
Worker 2   : http://localhost:8002
Worker 3   : http://localhost:8003
Worker 4   : http://localhost:8004
```

Swagger UI:

```text
http://localhost:8000/docs
```

## Upload an Object

```bash
curl -X POST \
  -F "file=@sample.bin" \
  http://localhost:8000/api/v1/objects
```

The response contains the generated object ID and metadata.

## Download an Object

Replace `<object_id>` with the returned object ID:

```bash
curl -o downloaded.bin \
  http://localhost:8000/api/v1/objects/<object_id>
```

Verify the downloaded file:

```bash
sha256sum downloaded.bin
```

## Object Metadata

Get metadata:

```bash
curl \
  http://localhost:8000/api/v1/objects/<object_id>/metadata
```

Update custom metadata:

```bash
curl -X PATCH \
  -H "Content-Type: application/json" \
  -d '{"project":"cloud-demo","owner":"siva","version":"1.0"}' \
  http://localhost:8000/api/v1/objects/<object_id>/metadata
```

List objects:

```bash
curl http://localhost:8000/api/v1/objects
```

Delete an object:

```bash
curl -X DELETE \
  http://localhost:8000/api/v1/objects/<object_id>
```

## Worker Monitoring

Check worker status:

```bash
curl http://localhost:8000/internal/workers
```

The Controller monitors worker heartbeats and marks workers as `UNHEALTHY` when heartbeats are not received within the configured timeout.

## Failure Recovery

The system maintains two copies of every chunk.

For example, to simulate worker failure:

```bash
docker-compose stop worker1
```

Wait for failure detection:

```bash
sleep 40
```

Check worker status:

```bash
curl http://localhost:8000/internal/workers
```

The affected replicas are marked as failed while the remaining healthy copies continue to provide the object.

Downloading the object triggers verification and recovery of missing replicas when required.

Restart the worker:

```bash
docker-compose start worker1
```

Verify recovery:

```bash
curl http://localhost:8000/internal/workers
```

## Data Integrity

Each chunk has a SHA-256 checksum.

The Controller verifies:

- Chunk size
- Chunk checksum
- Replica validity

If a replica is corrupted, a valid copy can be used to reconstruct the corrupted replica.

A corruption test can be performed by modifying a stored chunk in a worker's persistent storage and then downloading the object.

## Testing

Run the test suite with:

```bash
pytest -q
```

## Stopping the System

```bash
docker-compose down
```

Worker data and controller database are stored using persistent volumes/directories and are not removed by a normal `docker-compose down`.

## Main APIs

### Client APIs

```text
POST   /api/v1/objects
GET    /api/v1/objects/{object_id}
GET    /api/v1/objects/{object_id}/metadata
PATCH  /api/v1/objects/{object_id}/metadata
DELETE /api/v1/objects/{object_id}
GET    /api/v1/objects
```

### Internal Worker APIs

```text
POST   /internal/workers/register
POST   /internal/workers/heartbeat
PUT    /internal/chunks/{chunk_id}
GET    /internal/chunks/{chunk_id}
DELETE /internal/chunks/{chunk_id}
GET    /internal/workers/{worker_id}/chunks
```