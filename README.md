# Mini Distributed Cloud Object Store

A small distributed object store: one **controller** and four **storage
workers**, deployed with Docker Compose. Objects of any type are split into
1 MiB chunks, every chunk is stored on two different workers, and the system
detects and repairs worker failures and data corruption.

| Parameter | Value |
|---|---|
| Controller nodes | 1 (`controller`, port 8000) |
| Storage workers | 4 (`worker1` … `worker4`) |
| Chunk size | 1 MiB (last chunk may be smaller) |
| Replication factor | 2 (copies on different workers) |
| Worker storage | persistent host directories `./data/node1` … `./data/node4` |
| Orchestration | Docker Compose (no Kubernetes) |

## Architecture

```
                 clients (curl / CLI / demo script)
                              │  REST  /api/v1/...
                              ▼
                  ┌───────────────────────┐
                  │      controller       │  FastAPI + SQLite
                  │  metadata, placement, │  (./controller_data)
                  │  failure detection,   │
                  │  repair, scrubbing    │
                  └──┬─────┬─────┬─────┬──┘
        /internal/...│     │     │     │  register, heartbeat, chunk PUT/GET/DELETE
              ┌──────┘  ┌──┘  ┌──┘  └──────┐
              ▼         ▼     ▼            ▼
          worker1   worker2  worker3    worker4      FastAPI, chunks as files
         ./data/node1  …                ./data/node4
```

* Clients talk only to the controller and never need to know where chunks live.
* The controller stores object, chunk and replica metadata in SQLite and
  verifies every chunk by SHA-256.
* Workers are simple chunk stores; they register on start and send a heartbeat
  every 10 s.

## Prerequisites

Docker with the Compose plugin. For the demo script and the optional local
client: `bash`, `curl`, `python3`.

## Build and start

```bash
git clone https://github.com/sivasomanath2502/Distributed_CloudStore.git
cd Distributed_CloudStore

docker compose up -d --build        # builds the image, starts controller + 4 workers
docker compose ps                   # controller should be "healthy"
curl http://localhost:8000/health
curl http://localhost:8000/internal/workers   # all four workers HEALTHY
```

Stop / reset:

```bash
docker compose down                 # stop; data in ./data and ./controller_data is kept
rm -rf data/node* controller_data/coordinator.db*   # wipe all stored data (full reset)
```

## Using the system

### REST API with curl

```bash
API=http://localhost:8000/api/v1/objects

# Upload (multipart/form-data; metadata is optional JSON)
curl -F "file=@photo.jpg" -F 'metadata={"owner":"siva","project":"demo"}' $API

# List, download, metadata
curl $API
curl -o out.jpg $API/<object_id>
curl $API/<object_id>/metadata

# Update custom metadata (no re-upload)
curl -X PATCH -H 'Content-Type: application/json' -d '{"stage":"final"}' $API/<object_id>/metadata

# Delete
curl -X DELETE $API/<object_id>
```

| Method | Endpoint | Success |
|---|---|---|
| POST | `/api/v1/objects` | 201 |
| GET | `/api/v1/objects/{id}` | 200 (binary, `Content-Type` preserved) |
| GET | `/api/v1/objects/{id}/metadata` | 200 |
| PATCH | `/api/v1/objects/{id}/metadata` | 200 |
| DELETE | `/api/v1/objects/{id}` | 204 |
| GET | `/api/v1/objects` | 200 |

Errors are JSON (`{"detail": ...}`): 400 invalid request, 404 object not
found, 503 required workers/chunks unavailable, 500 internal error.

Internal APIs: `POST /internal/workers/register` and
`POST /internal/workers/heartbeat` (controller);
`PUT|GET|DELETE /internal/chunks/{chunk_id}` and
`GET /internal/workers/{worker_id}/chunks` (workers).
`GET /internal/workers` on the controller lists worker status.

The object metadata response contains the system fields (name, size,
content type, created/modified time, SHA-256 checksum, chunk count), the
custom key-value metadata, and for every chunk its sequence number, size,
checksum, and replicas (`worker_id`, `status`).

### Command-line client

```bash
pip install httpx
python -m client.cloudstore_cli upload ./photo.jpg --meta owner=siva stage=draft
python -m client.cloudstore_cli list
python -m client.cloudstore_cli info <object_id>
python -m client.cloudstore_cli patch <object_id> stage=final
python -m client.cloudstore_cli download <object_id> ./photo_copy.jpg   # verifies checksum
python -m client.cloudstore_cli delete <object_id>
```

or as a container (files in `./client_files` appear as `/files`):

```bash
docker compose run --rm client upload /files/photo.jpg
```

## Demonstration

`scripts/demo.sh` runs the six required demonstrations against the running
cluster (object storage, chunking and distribution, replication, metadata,
worker failure with `docker stop worker2` / `docker start worker2`, and data
integrity with intentional corruption). It prints the chunk → worker table
before, during and after the failure.

```bash
./scripts/demo.sh                 # about 2–3 minutes
PAUSE=1 ./scripts/demo.sh         # wait for ENTER between sections (for a live demo)
```

Manual equivalents:

```bash
# where are the chunks?
curl -s $API/<id>/metadata | python3 -m json.tool

# worker failure
docker stop worker2        # downloads keep working; ~30 s later worker2 is UNHEALTHY
curl http://localhost:8000/internal/workers
docker start worker2

# corruption (worker volumes are plain host directories)
printf 'X' | dd of=data/node1/<chunk_id> bs=1 seek=100 conv=notrunc
curl -o out.bin $API/<id>  # corrupt copy is detected and not returned; it is repaired
```

## How it works

**Upload.** The controller reads the object, splits it into 1 MiB chunks
(UUID per chunk, ordering kept by sequence number), computes per-chunk and
whole-object SHA-256, and stores each chunk on two distinct workers. A copy
only counts once the worker confirms the expected checksum. Placement chooses
the two least-loaded healthy workers (ties broken by a hash of the chunk id),
which keeps chunk counts balanced. Metadata is committed in one short
transaction only after every chunk is stored; if replication cannot be
achieved, the partial upload is rolled back and removed from the workers.

**Download.** For each chunk the controller reads a replica, verifies its
SHA-256, and moves on to the next replica if the copy is missing, unreachable
or corrupt. The whole object is verified against its stored checksum before
it is returned. If no valid copy of some chunk exists the request fails with
503 rather than returning bad data.

**Failure detection.** Workers heartbeat every 10 s. A worker silent for 30 s
is marked `UNHEALTHY` and its replicas `FAILED`. Objects remain readable from
the surviving copies. The metadata endpoint shows the number of `ACTIVE`
copies per chunk, so under-replication is visible.

**Restoring replication.** A background maintenance loop (every 15 s) copies
under-replicated chunks from a verified replica to another healthy worker, so
the replication factor is restored while the worker is still down, without
waiting for a client read. When a failed worker returns, it is reconciled: its
chunks are inventoried and verified, valid copies are re-activated if needed,
surplus copies (replication already restored elsewhere) are removed, and
unknown chunks are purged. Consequently a returning worker may come back
holding fewer chunks than before; the replication factor is what is
guaranteed, not the original placement.

**Integrity.** Every chunk has a SHA-256 stored in the metadata. Corruption is
detected on read (a corrupt copy is never returned when a valid one exists and
is rewritten from a valid replica) and by a periodic scrub (every 120 s) that
verifies all stored chunks and repairs missing or corrupt copies.

**Deletion.** Physical copies are removed from the workers. A copy that
cannot be removed (worker down) is queued and removed when the worker is
healthy again; the DELETE still returns 204.

**Controller restarts.** Metadata persists in `./controller_data`. After a
controller restart, workers get a 35 s grace period to heartbeat before
anything is declared failed; reconciliation runs serialized in the background
and is retried on error.

## Configuration

| Variable | Service | Default | Meaning |
|---|---|---|---|
| `DB_PATH` | controller | `coordinator.db` (`/app/db/coordinator.db` in Compose) | SQLite file |
| `MAINTENANCE_INTERVAL_SECONDS` | controller | 15 | re-replication / retry loop period |
| `SCRUB_INTERVAL_SECONDS` | controller | 120 | full integrity scrub period |
| `STARTUP_GRACE_SECONDS` | controller | 35 | no failure declarations after a controller start |
| `NODE_ID`, `NODE_PORT`, `WORKER_URL` | worker | per service | worker identity and address |
| `CONTROLLER_URL` | worker | `http://localhost:8000` (`http://controller:8000` in Compose) | where to register |
| `STORAGE_DIR` | worker | `data/node1` (`/app/data/nodeN` in Compose) | chunk directory |

The chunk size (1 MiB), replication factor (2) and heartbeat timing (10 s /
30 s) are constants in the source.

## Tests

```bash
pip install -r requirements.txt pytest
python -m pytest tests -q          # chunking, placement, API error contract
```

`scripts/demo.sh` is the end-to-end test against a running cluster.

## Known limitations

* Single controller (single point of failure for new requests; stored data and
  metadata survive a restart).
* The controller buffers a whole object in memory during upload/download, so
  very large objects are bounded by controller RAM.
* SQLite metadata store; fine for this scale, not for many controllers.
* No authentication or TLS.
* No automatic rebalancing of existing chunks onto a worker that returns or is
  added (new uploads are balanced).
* List endpoint is not paginated.
