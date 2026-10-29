"""Derive live identity inputs from the preserved original protobuf corpus."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys

SOURCE = "28d5c495b63e6d7efd7556d164a49f33574ac80c"
ROOT = Path(__file__).resolve().parents[5]


def original(path):
    return subprocess.check_output(["git", "show", f"{SOURCE}:{path}"], cwd=ROOT)


def varint(data, pos):
    value = 0
    for shift in range(0, 70, 7):
        if pos >= len(data):
            raise ValueError("truncated varint")
        byte = data[pos]
        pos += 1
        value |= (byte & 127) << shift
        if byte < 128:
            return value, pos
    raise ValueError("invalid varint")


def encoded_varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def fields(data):
    pos = 0
    while pos < len(data):
        start = pos
        key, pos = varint(data, pos)
        number, kind = key >> 3, key & 7
        if not number:
            raise ValueError("zero field number")
        if kind == 0:
            _, pos = varint(data, pos)
        elif kind == 1:
            pos += 8
        elif kind == 2:
            length, pos = varint(data, pos)
            pos += length
        elif kind == 5:
            pos += 4
        else:
            raise ValueError("unsupported wire kind")
        if pos > len(data):
            raise ValueError("truncated field")
        yield number, kind, data[start:pos]


def frame(data, pos):
    size, start = varint(data, pos)
    end = start + size
    if end > len(data):
        raise ValueError("truncated frame")
    return data[start:end], end


def protocol(data):
    # Original CommandEnvelope, EventEnvelope and live ResumeRequest all use
    # field 1 for the identical ProtocolIdentity message. Keep those bytes.
    return b"".join(raw for number, kind, raw in fields(data) if number == 1)


def admission(data):
    command, end = frame(data, 0)
    _, actual_end = frame(data, end)
    request = bytearray()
    for number, kind, raw in fields(command):
        if number == 3 and kind == 2:
            _, payload = varint(raw, 0)
            # Preserve the original length and identity payload exactly; only
            # remap CommandEnvelope.operation(3) to Admission.operation(1).
            request.extend(encoded_varint((1 << 3) | 2))
            request.extend(raw[payload:])
    return encoded_varint(len(request)) + request + data[end:actual_end] + data[actual_end:]


def derive(crate, target, transform, retired):
    prefix = f"{crate}/corpus/{target}/"
    paths = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", SOURCE, "--", prefix],
        cwd=ROOT, text=True,
    ).splitlines()
    derived = []
    for path in paths:
        data = original(path)
        try:
            result = bytes(transform(data))
            method = transform.__name__
        except ValueError:
            result = data
            method = "malformed original retained verbatim"
        destination = ROOT / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(result)
        derived.append({
            "path": path,
            "original_sha256": hashlib.sha256(data).hexdigest(),
            "derived_sha256": hashlib.sha256(result).hexdigest(),
            "derivation": method,
        })
    (ROOT / retired / "derived.json").write_text(json.dumps({
        "original_commit": SOURCE,
        "scope": "Source-byte extraction, not an execution or equivalence result.",
        "files": derived,
    }, indent=2) + "\n", encoding="utf-8")


def main():
    derive("rust/crates/harness-fuzz", "protocol_identity", protocol,
           "rust/crates/harness-fuzz/retired/aggregate_protocol")
    derive("rust/fuzz", "harness_admission", admission,
           "rust/fuzz/retired/aggregate_admission")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"STEP_FAILED corpus derivation: {error}", file=sys.stderr)
        print("Recovery: python rust/crates/harness-fuzz/retired/aggregate_protocol/derive_corpus.py", file=sys.stderr)
        sys.exit(1)
