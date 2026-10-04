#!/usr/bin/env python3
"""Validate a generated Rustdoc artifact tree without invoking Cargo."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import blake3


SCHEMA = "acyclic.sdk.docs.rustdoc-blake3-validation.v1"


def digest(path: Path) -> str:
    hasher = blake3.blake3()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def load_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def validate(output_dir: Path) -> dict[str, object]:
    errors: list[str] = []
    generation_path = output_dir / "generation-receipt.json"
    generation = load_json(generation_path)
    if not isinstance(generation, dict):
        raise ValueError("generation receipt is not an object")
    artifacts = generation.get("artifacts")
    if not isinstance(artifacts, list):
        raise ValueError("generation receipt artifacts is not an array")
    observed: list[dict[str, object]] = []
    profiles: Counter[str] = Counter()
    source_revision = generation.get("source_revision")
    toolchain = generation.get("toolchain")
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, dict):
            errors.append(f"artifact[{index}] is not an object")
            continue
        profile = str(artifact.get("profile", ""))
        package = str(artifact.get("package_name", ""))
        profiles[profile] += 1
        json_rel = artifact.get("rustdoc_json")
        receipt_rel = artifact.get("receipt")
        if not isinstance(json_rel, str) or not isinstance(receipt_rel, str):
            errors.append(f"artifact[{index}] is missing paths")
            continue
        json_path = output_dir / json_rel
        receipt_path = output_dir / receipt_rel
        if not json_path.is_file():
            errors.append(f"missing rustdoc JSON: {json_rel}")
            continue
        if not receipt_path.is_file():
            errors.append(f"missing receipt: {receipt_rel}")
            continue
        actual_json_blake3 = digest(json_path)
        declared_json_blake3 = artifact.get("rustdoc_json_blake3")
        if actual_json_blake3 != declared_json_blake3:
            errors.append(f"rustdoc JSON BLAKE3 mismatch: {json_rel}")
        receipt = load_json(receipt_path)
        if not isinstance(receipt, dict):
            errors.append(f"receipt is not an object: {receipt_rel}")
            continue
        if receipt.get("source_revision") != source_revision:
            errors.append(f"source revision mismatch: {receipt_rel}")
        if receipt.get("toolchain") != toolchain:
            errors.append(f"toolchain mismatch: {receipt_rel}")
        if receipt.get("rustdoc_json_blake3") != actual_json_blake3:
            errors.append(f"receipt JSON BLAKE3 mismatch: {receipt_rel}")
        observed.append(
            {
                "profile": profile,
                "package": package,
                "target": artifact.get("target"),
                "features": artifact.get("features", []),
                "rustdoc_json": json_rel,
                "rustdoc_json_blake3": actual_json_blake3,
                "receipt": receipt_rel,
                "receipt_sha256": sha256(receipt_path),
            }
        )
    return {
        "schema": SCHEMA,
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "validator": {
            "script": "scripts/verify-rustdoc-artifacts.py",
            "script_sha256": sha256(Path(__file__)),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "blake3": importlib.metadata.version("blake3"),
        },
        "output_dir": str(output_dir),
        "source_revision": source_revision,
        "toolchain": toolchain,
        "generation_receipt_sha256": sha256(generation_path),
        "artifact_count": len(artifacts),
        "profile_counts": dict(sorted(profiles.items())),
        "artifacts": observed,
        "valid": not errors,
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("receipt", type=Path)
    args = parser.parse_args()
    result = validate(args.output_dir.resolve())
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"valid": result["valid"], "artifact_count": result["artifact_count"], "errors": result["errors"]}))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
