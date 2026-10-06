#!/usr/bin/env python3
"""Thin launcher for Rust-owned generated RPC coverage verification."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from _write_runtime_consumer import authority_for


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: verify-generated-rpc-coverage.py GENERATED_ROOT PROTO...", file=sys.stderr)
        return 2
    generated_root = Path(sys.argv[1]).resolve()
    authority = authority_for([Path(value) for value in sys.argv[2:]])
    configured = os.environ.get("ACYCLIC_RUNTIME_CONSUMER_BIN")
    if configured:
        command = [configured]
    else:
        source_root = Path(__file__).resolve().parents[1]
        command = [os.environ.get("CARGO", "cargo"), "run", "--quiet", "--locked", "--manifest-path", str(source_root / "rust/crates/sdk-generation/Cargo.toml"), "--bin", "sdk-runtime-consumer", "--"]
    command.extend(["--verify-coverage", str(generated_root), "--authority", str(authority)])
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
