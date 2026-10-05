#!/usr/bin/env python3
"""Emit the streaming request scenarios owned by the Rust manifest.

The generated language consumers use this small, line-oriented sidecar only as
an input to their generated protobuf decoders.  They record the bytes emitted
by those decoders after sending each message, so the receipt remains evidence
from the language runtime rather than a copy of Rust bytes.
"""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: write-rust-stream-request-scenarios.py MANIFEST OUTPUT", file=sys.stderr)
        return 2
    manifest_path = Path(sys.argv[1])
    output = Path(sys.argv[2])
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    records = document.get("records")
    if not isinstance(records, list):
        raise SystemExit("Rust request manifest records must be an array")
    entries: list[tuple[str, list[str]]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise SystemExit(f"records[{index}] must be an object")
        rpc = record.get("rpc")
        typed = record.get("typed_request")
        if not isinstance(rpc, str):
            continue
        frames = (typed.get("serialized_frames") if isinstance(typed, dict) else None) or record.get("request_frames")
        if frames is None:
            continue
        if not isinstance(frames, list) or not frames:
            raise SystemExit(f"{rpc}: typed_request.serialized_frames must be non-empty")
        encoded: list[str] = []
        for frame_index, frame in enumerate(frames):
            if not isinstance(frame, dict):
                raise SystemExit(f"{rpc}: serialized_frames[{frame_index}] must be an object")
            if isinstance(frame.get("serialized_hex"), str):
                try:
                    raw = bytes.fromhex(frame["serialized_hex"])
                except ValueError as error:
                    raise SystemExit(f"{rpc}: serialized_frames[{frame_index}] is not hex: {error}") from error
            elif isinstance(frame.get("bytes_base64"), str):
                try:
                    raw = base64.b64decode(frame["bytes_base64"], validate=True)
                except ValueError as error:
                    raise SystemExit(f"{rpc}: serialized_frames[{frame_index}] is not base64: {error}") from error
            else:
                raise SystemExit(f"{rpc}: serialized_frames[{frame_index}] is missing serialized_hex or bytes_base64")
            encoded.append(base64.b64encode(raw).decode("ascii"))
        entries.append((rpc, encoded))
    entries.sort(key=lambda entry: entry[0])
    if len({rpc for rpc, _ in entries}) != len(entries):
        raise SystemExit("Rust request manifest contains duplicate streaming RPC identities")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        "".join(f"{rpc}\t{','.join(frames)}\n" for rpc, frames in entries),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
