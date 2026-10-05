#!/usr/bin/env python3
"""Thin compatibility launcher for Rust-owned runtime consumers.

The qualification shell keeps its historical argument shape.  This launcher
only discovers the Rust authority manifest and invokes sdk-runtime-consumer;
it does not parse protobuf or invent target metadata.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def authority_for(paths: list[Path]) -> Path:
    configured = os.environ.get("ACYCLIC_RUST_AUTHORITY_MANIFEST")
    if configured:
        candidate = Path(configured)
        if candidate.is_file():
            return candidate
    for path in paths:
        current = path.resolve().parent
        for parent in (current, *current.parents):
            candidate = parent / "rust-authority.json"
            if candidate.is_file():
                return candidate
    raise SystemExit("Rust authority manifest was not found; pass ACYCLIC_RUST_AUTHORITY_MANIFEST")


def typed_manifest_for(paths: list[Path]) -> Path:
    configured = os.environ.get("ACYCLIC_RUST_TYPED_REQUEST_MANIFEST")
    if configured:
        candidate = Path(configured)
        if candidate.is_file():
            return candidate
    for path in paths:
        current = path.resolve().parent
        for parent in (current, *current.parents):
            for name in ("rust-typed-request-manifest-current.json", "rust-typed-request-manifest.json"):
                candidate = parent / name
                if candidate.is_file():
                    return candidate
    raise SystemExit("Rust typed request manifest was not found; pass ACYCLIC_RUST_TYPED_REQUEST_MANIFEST")


def main(language: str) -> int:
    if len(sys.argv) < 4:
        print("usage: write-*-runtime-consumer.py GENERATED_ROOT PROTO... OUTPUT", file=sys.stderr)
        return 2
    output = Path(sys.argv[-1]).resolve()
    proto_paths = [Path(value) for value in sys.argv[2:-1]]
    authority = authority_for(proto_paths)
    typed = typed_manifest_for(proto_paths)
    configured = os.environ.get("ACYCLIC_RUNTIME_CONSUMER_BIN")
    if configured:
        command = [configured]
    else:
        source_root = Path(__file__).resolve().parents[1]
        command = [
            os.environ.get("CARGO", "cargo"),
            "run",
            "--quiet",
            "--locked",
            "--manifest-path",
            str(source_root / "rust/crates/sdk-generation/Cargo.toml"),
            "--bin",
            "sdk-runtime-consumer",
            "--",
        ]
    command.extend(["--language", language, "--authority", str(authority), "--typed-request-manifest", str(typed), "--output", str(output)])
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    language = Path(sys.argv[0]).stem.removeprefix("write-").removesuffix("-runtime-consumer")
    raise SystemExit(main(language))
