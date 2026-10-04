#!/usr/bin/env python3
"""Hash bytes emitted by the Common Lisp runtime consumer.

ag-gRPC exposes decoded CLOS messages, so the generated consumer writes the
actual protobuf request and response bytes. This collector turns those files
into the same receipt shape used by the Elixir and Erlang lanes.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path


def digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def metadata(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    rpc = re.search(r":rpc\s+\"([^\"]+)\"", text)
    shape = re.search(r":shape\s+\"([^\"]+)\"", text)
    execution = re.search(r":execution\s+\"([^\"]+)\"", text)
    status = re.search(r":status\s+(-?\d+|NIL)", text, re.IGNORECASE)
    value: object = None if not status or status.group(1).upper() == "NIL" else int(status.group(1))
    return {"rpc": rpc.group(1) if rpc else None, "shape": shape.group(1) if shape else None, "execution": execution.group(1) if execution else None, "status": value}


def main() -> int:
    if len(sys.argv) != 5:
        print("usage: collect-runtime-observation-receipt.py PROJECT OUTPUT SOURCE_REVISION MANIFEST_SHA256", file=sys.stderr)
        return 2
    project = Path(sys.argv[1])
    output = Path(sys.argv[2])
    source_revision = sys.argv[3]
    manifest = sys.argv[4]
    observation_dir = project / "runtime-observations"
    requests = sorted(observation_dir.glob("*.request.bin"))
    observations = []
    for request in requests:
        match = re.match(r"(\d+)\.request\.bin$", request.name)
        if not match:
            continue
        prefix = match.group(1)
        responses = sorted(observation_dir.glob(f"{prefix}.response.*.bin"))
        item = {
            "request_file": request.name,
            "request_sha256": digest(request),
            "response_files": [response.name for response in responses],
            "response_sha256": [digest(response) for response in responses],
            "response_frames": len(responses),
        }
        meta = observation_dir / f"{prefix}.meta.sexp"
        if meta.exists():
            item.update(metadata(meta))
        observations.append(item)
    payload = {
        "schema": "acyclic.runtime-consumer-receipt.v1",
        "language": "common-lisp",
        "source_revision": source_revision,
        "rust_authority_manifest_sha256": manifest,
        "observations": observations,
        "rpc_count": len(observations),
        "byte_evidence": "actual-ag-proto-serialized-request-and-response-files",
    }
    Path(output).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
