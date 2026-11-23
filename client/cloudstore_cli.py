"""
Command-line client for the Mini Distributed Cloud Object Store.

The client only talks to the controller's public REST API and never needs
to know which workers hold an object's chunks.

Usage:
  python -m client.cloudstore_cli [--url URL] <command> ...

Commands:
  upload <path> [--meta key=value ...]   Upload a file
  download <object_id> <output_path>     Download and verify an object
  info <object_id>                       Show metadata and chunk locations
  patch <object_id> key=value ...        Add/update custom metadata
  list                                   List stored objects
  delete <object_id>                     Delete an object
"""

import argparse
import hashlib
import json
import mimetypes
import os
import sys

import httpx


def _fail(response):
    try:
        detail = response.json()
    except ValueError:
        detail = response.text
    print(f"HTTP {response.status_code}: {detail}", file=sys.stderr)
    sys.exit(1)


def _kv(pairs):
    result = {}
    for pair in pairs or []:
        if "=" not in pair:
            print(f"Invalid metadata '{pair}', expected key=value", file=sys.stderr)
            sys.exit(2)
        key, value = pair.split("=", 1)
        result[key] = value
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--url", default=os.getenv("CLOUDSTORE_URL", "http://localhost:8000"))
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload")
    up.add_argument("path")
    up.add_argument("--meta", nargs="*", default=[])

    down = sub.add_parser("download")
    down.add_argument("object_id")
    down.add_argument("output")

    for name in ("info", "delete"):
        sub.add_parser(name).add_argument("object_id")

    pa = sub.add_parser("patch")
    pa.add_argument("object_id")
    pa.add_argument("pairs", nargs="+")

    sub.add_parser("list")

    args = parser.parse_args()
    base = args.url.rstrip("/") + "/api/v1/objects"

    with httpx.Client(timeout=120.0) as client:

        if args.command == "upload":
            content_type = mimetypes.guess_type(args.path)[0] or "application/octet-stream"
            with open(args.path, "rb") as handle:
                response = client.post(
                    base,
                    files={"file": (os.path.basename(args.path), handle, content_type)},
                    data={"metadata": json.dumps(_kv(args.meta))},
                )
            if response.status_code != 201:
                _fail(response)
            body = response.json()
            print(json.dumps({k: body[k] for k in (
                "object_id", "object_name", "size", "content_type", "checksum", "chunk_count")}, indent=2))

        elif args.command == "download":
            response = client.get(f"{base}/{args.object_id}")
            if response.status_code != 200:
                _fail(response)
            with open(args.output, "wb") as handle:
                handle.write(response.content)
            digest = hashlib.sha256(response.content).hexdigest()
            expected = response.headers.get("X-Object-Checksum")
            print(f"saved {len(response.content)} bytes to {args.output}")
            print(f"sha256 {digest} ({'matches' if digest == expected else 'DIFFERS FROM'} controller checksum)")

        elif args.command == "info":
            response = client.get(f"{base}/{args.object_id}/metadata")
            if response.status_code != 200:
                _fail(response)
            print(json.dumps(response.json(), indent=2))

        elif args.command == "patch":
            response = client.patch(f"{base}/{args.object_id}/metadata", json=_kv(args.pairs))
            if response.status_code != 200:
                _fail(response)
            print(json.dumps(response.json(), indent=2))

        elif args.command == "list":
            response = client.get(base)
            if response.status_code != 200:
                _fail(response)
            data = response.json()
            print(f"{data['total_objects']} object(s)")
            for obj in data["objects"]:
                print(f"{obj['object_id']}  {obj['size']:>12}  {obj['content_type']:<28} {obj['object_name']}")

        elif args.command == "delete":
            response = client.delete(f"{base}/{args.object_id}")
            if response.status_code != 204:
                _fail(response)
            print("deleted")


if __name__ == "__main__":
    try:
        main()
    except httpx.ConnectError:
        print(
            "Cannot reach the controller. Is the cluster running "
            "(docker compose up -d --build) and is --url / CLOUDSTORE_URL correct?",
            file=sys.stderr
        )
        sys.exit(3)
