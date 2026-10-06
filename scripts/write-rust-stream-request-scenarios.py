#!/usr/bin/env python3
"""Thin launcher for the Rust-owned streaming scenario projection."""
from __future__ import annotations

import sys
import os
import subprocess
from pathlib import Path


def authority_for(typed: Path) -> Path:
    configured = os.environ.get("ACYCLIC_RUST_AUTHORITY_MANIFEST")
    if configured and Path(configured).is_file():
        return Path(configured)
    for parent in (typed.resolve().parent, *typed.resolve().parents):
        candidate = parent / "rust-authority.json"
        if candidate.is_file():
            return candidate
    raise SystemExit("Rust authority manifest was not found; pass ACYCLIC_RUST_AUTHORITY_MANIFEST")


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: write-rust-stream-request-scenarios.py TYPED_MANIFEST OUTPUT", file=sys.stderr)
        return 2
    typed = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    authority = authority_for(typed)
    configured = os.environ.get("ACYCLIC_RUNTIME_CONSUMER_BIN")
    if configured:
        command = [configured]
    else:
        source_root = Path(__file__).resolve().parents[1]
        command = [os.environ.get("CARGO", "cargo"), "run", "--quiet", "--locked", "--manifest-path", str(source_root / "rust/crates/sdk-generation/Cargo.toml"), "--bin", "sdk-runtime-consumer", "--"]
    command.extend(["--authority", str(authority), "--typed-request-manifest", str(typed), "--stream-scenarios", str(output)])
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
