#!/usr/bin/env bash
#
# Walks through the six demonstrations required by the project brief:
#   1 Object storage      4 Metadata
#   2 Chunking/distribution 5 Worker failure (docker stop/start worker2)
#   3 Replication         6 Data integrity (intentional corruption)
#
# Requirements: bash, curl, python3, docker (for stop/start). Run from the
# repository root with the cluster up:   ./scripts/demo.sh
#
# Environment overrides:
#   URL        controller address              (default http://localhost:8000)
#   DATA_DIR   host path of worker volumes     (default ./data)
#   FAIL_WAIT  max seconds to wait for failure detection (default 90; heartbeat timeout is 30 s)
#   PAUSE=1    wait for ENTER between sections (default: no pause)

set -u

URL="${URL:-http://localhost:8000}"
API="$URL/api/v1/objects"
DATA_DIR="${DATA_DIR:-./data}"
FAIL_WAIT="${FAIL_WAIT:-90}"
PAUSE="${PAUSE:-0}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

section() {
  echo
  echo "=============================================================="
  echo "  $*"
  echo "=============================================================="
  if [ "$PAUSE" = "1" ]; then read -r -p "  [ENTER to run] " _; fi
}

sha() { sha256sum "$1" | cut -d' ' -f1; }

json() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

workers() {
  curl -s "$URL/internal/workers" | python3 -c "
import json,sys
print('  workers:', ', '.join(f\"{w['worker_id']}={w['status']}\" for w in json.load(sys.stdin)['workers']))"
}

# Print the chunk -> replica table for an object.
table() {
  curl -s "$API/$1/metadata" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('  seq  size      copies (worker:status)                 active')
for c in d['chunks']:
    reps=', '.join(f\"{r['worker_id']}:{r['status']}\" for r in c['replicas'])
    print(f\"  {c['sequence_number']:<4} {c['size']:<9} {reps:<38} {c['active_replica_count']}\")"
}

# Number of physical chunk files per worker volume.
per_worker() {
  for d in "$DATA_DIR"/node*; do
    printf '  %s: %s chunk file(s)\n' "$(basename "$d")" "$(find "$d" -type f ! -name '.*' | wc -l)"
  done
}

# Exit 0 once every chunk of the object has >= 2 ACTIVE copies.
fully_replicated() {
  curl -s "$API/$1/metadata" | python3 -c "
import json,sys
d=json.load(sys.stdin)
sys.exit(0 if all(c['active_replica_count']>=2 for c in d['chunks']) else 1)"
}

wait_replicated() {   # object_id, timeout
  local t=0
  until fully_replicated "$1"; do
    sleep 2; t=$((t+2))
    [ "$t" -ge "$2" ] && { echo "  (not fully replicated after ${2}s)"; return 1; }
  done
  echo "  replication factor restored after ~${t}s"
}

compare() {   # original, downloaded
  if cmp -s "$1" "$2"; then echo "  RESULT: downloaded bytes are IDENTICAL to the original"
  else echo "  RESULT: MISMATCH"; fi
}

head -c 5767168 /dev/urandom > "$WORK/big.bin"      # 5.5 MiB -> 6 chunks
printf 'hello cloud object store\n' > "$WORK/small.txt"

# ---------------------------------------------------------------- 1
section "1. OBJECT STORAGE - upload and retrieve; verify identical"
echo "  original sha256: $(sha "$WORK/small.txt")"
RESP=$(curl -s -w '\n%{http_code}' -F "file=@$WORK/small.txt;type=text/plain" "$API")
echo "  POST $API -> HTTP $(echo "$RESP" | tail -1)"
SMALL=$(echo "$RESP" | head -n -1 | json "d['object_id']")
echo "  object_id: $SMALL"
curl -s -D "$WORK/h.txt" -o "$WORK/small.out" "$API/$SMALL"
echo "  GET -> $(head -1 "$WORK/h.txt" | tr -d '\r'); $(grep -i '^content-type' "$WORK/h.txt" | tr -d '\r')"
echo "  downloaded sha256: $(sha "$WORK/small.out")"
compare "$WORK/small.txt" "$WORK/small.out"

# ---------------------------------------------------------------- 2
section "2. CHUNKING AND DISTRIBUTION - 5.5 MiB object (> 1 MiB)"
echo "  original sha256: $(sha "$WORK/big.bin")"
RESP=$(curl -s -F "file=@$WORK/big.bin" -F 'metadata={"owner":"demo","course":"cloud-computing"}' "$API")
BIG=$(echo "$RESP" | json "d['object_id']")
echo "  object_id: $BIG"
echo "$RESP" | json "'  size=%d bytes, chunk_count=%d (last chunk is smaller than 1 MiB)' % (d['size'], d['chunk_count'])"
table "$BIG"
echo "  physical chunk files per worker volume:"
per_worker
curl -s -o "$WORK/big.out" "$API/$BIG"
compare "$WORK/big.bin" "$WORK/big.out"

# ---------------------------------------------------------------- 3
section "3. REPLICATION - two copies per chunk on different workers"
table "$BIG"
curl -s "$API/$BIG/metadata" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ok=all(len({r['worker_id'] for r in c['replicas'] if r['status']=='ACTIVE'})==2 for c in d['chunks'])
print('  RESULT: every chunk has 2 ACTIVE copies on 2 distinct workers' if ok else '  RESULT: replication problem')"

# ---------------------------------------------------------------- 4
section "4. METADATA - custom metadata, retrieve, modify without re-upload"
echo "  GET .../metadata:"
curl -s "$API/$BIG/metadata" | json "'    name=%s size=%d type=%s checksum=%s...\n    created=%s modified=%s\n    custom=%s' % (d['object_name'],d['size'],d['content_type'],d['checksum'][:16],d['created_at'],d['updated_at'],d['custom_metadata'])"
sleep 2
echo "  PATCH {\"course\":\"CC-2026\",\"reviewed\":\"yes\"}:"
curl -s -w '    HTTP %{http_code}\n' -o /dev/null -X PATCH -H 'Content-Type: application/json' \
  -d '{"course":"CC-2026","reviewed":"yes"}' "$API/$BIG/metadata"
curl -s "$API/$BIG/metadata" | json "'    custom=%s\n    modified=%s (changed, object data was not re-uploaded)' % (d['custom_metadata'], d['updated_at'])"

# ---------------------------------------------------------------- 5
section "5. WORKER FAILURE - docker stop worker2 / docker start worker2"
echo "--- BEFORE"
workers; table "$BIG"

echo "--- docker stop worker2"
docker stop worker2 >/dev/null && echo "  worker2 stopped"

echo "--- waiting for heartbeat-based failure detection (timeout is 30 s)"
T=0
until curl -s "$URL/internal/workers" | python3 -c "
import json,sys
sys.exit(0 if any(w['worker_id']=='worker2' and w['status']!='HEALTHY' for w in json.load(sys.stdin)['workers']) else 1)"; do
  sleep 1; T=$((T+1)); [ "$T" -ge "$FAIL_WAIT" ] && break
done
echo "  failure detected after ~${T}s"
workers
echo "  replica state right after detection (worker2's copies are FAILED; the maintenance loop re-replicates within seconds):"
table "$BIG"

echo "--- DURING FAILURE: the object is still served from the surviving copies"
curl -s -o "$WORK/f1.out" -w '  GET -> HTTP %{http_code}\n' "$API/$BIG"
compare "$WORK/big.bin" "$WORK/f1.out"

echo "--- DURING FAILURE: replication factor is restored on the surviving workers"
wait_replicated "$BIG" 60
table "$BIG"
curl -s -o "$WORK/f2.out" -w '  GET -> HTTP %{http_code}\n' "$API/$BIG"
compare "$WORK/big.bin" "$WORK/f2.out"

echo "--- docker start worker2"
docker start worker2 >/dev/null && echo "  worker2 started"
for _ in $(seq 1 30); do
  curl -s "$URL/internal/workers" | python3 -c "
import json,sys
sys.exit(0 if any(w['worker_id']=='worker2' and w['status']=='HEALTHY' for w in json.load(sys.stdin)['workers']) else 1)" && break
  sleep 2
done
sleep 10   # allow reconciliation of the returning worker
echo "--- AFTER RECOVERY"
workers; table "$BIG"
echo "  physical chunk files per worker volume (returning worker's surplus copies are cleaned up):"
per_worker
curl -s -o "$WORK/f3.out" -w '  GET after recovery -> HTTP %{http_code}\n' "$API/$BIG"
compare "$WORK/big.bin" "$WORK/f3.out"
fully_replicated "$BIG" && echo "  RESULT: required replication factor (2) is maintained" \
                       || echo "  RESULT: replication factor NOT maintained"

# ---------------------------------------------------------------- 6
section "6. DATA INTEGRITY - intentional corruption of stored data"
read -r CW CC <<< "$(curl -s "$API/$BIG/metadata" | python3 -c "
import json,sys
c=json.load(sys.stdin)['chunks'][0]
r=[r for r in c['replicas'] if r['status']=='ACTIVE'][0]
print(r['worker_id'].replace('worker','node'), c['chunk_id'])")"
FILE="$DATA_DIR/$CW/$CC"
echo "  target: chunk 1 stored at $FILE"
echo "  sha256 before corruption: $(sha "$FILE")"
python3 - "$FILE" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read())
b[1000] ^= 0xFF; b[len(b) // 2] ^= 0xFF
open(p, 'wb').write(bytes(b))
PY
echo "  sha256 after  corruption: $(sha "$FILE")   <-- corrupted"
echo "--- download (the corrupt copy must be detected and NOT returned)"
curl -s -o "$WORK/c1.out" -w '  GET -> HTTP %{http_code}\n' "$API/$BIG"
compare "$WORK/big.bin" "$WORK/c1.out"
echo "  replica states after detection:"
table "$BIG"
sleep 3
echo "  controller log:"
docker logs controller 2>&1 | grep -iE "corrupt|repair" | tail -4 | sed 's/^/    /' || true
echo "  sha256 of the stored copy after repair: $(sha "$FILE") (restored from the valid replica)"

echo "--- both replicas of one chunk corrupted: corrupt data must never be served"
read -r W1 W2 C2 <<< "$(curl -s "$API/$BIG/metadata" | python3 -c "
import json,sys
c=json.load(sys.stdin)['chunks'][1]
a=[r['worker_id'].replace('worker','node') for r in c['replicas'] if r['status']=='ACTIVE']
print(a[0], a[1], c['chunk_id'])")"
for w in "$W1" "$W2"; do
python3 - "$DATA_DIR/$w/$C2" <<'PY'
import sys
p = sys.argv[1]
b = bytearray(open(p, 'rb').read()); b[10] ^= 0xFF
open(p, 'wb').write(bytes(b))
PY
done
CODE=$(curl -s -o "$WORK/c2.out" -w '%{http_code}' "$API/$BIG")
echo "  GET -> HTTP $CODE: $(head -c 160 "$WORK/c2.out")"
[ "$CODE" = "503" ] && echo "  RESULT: request refused (503) rather than returning corrupt data"

echo
echo "Demo finished."
