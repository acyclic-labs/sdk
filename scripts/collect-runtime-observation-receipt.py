#!/usr/bin/env python3
"""Thin launcher for the Rust-owned runtime observation receipt collector."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 6:
        print("usage: collect-runtime-observation-receipt.py PROJECT OUTPUT SOURCE_REVISION MANIFEST_SHA256 AUTHORITY", file=sys.stderr)
        return 2
    project = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2]).resolve()
    configured = os.environ.get("ACYCLIC_RUNTIME_CONSUMER_BIN")
    if configured:
        command = [configured]
    else:
        source_root = Path(__file__).resolve().parents[1]
        command = [os.environ.get("CARGO", "cargo"), "run", "--quiet", "--locked", "--manifest-path", str(source_root / "rust/crates/sdk-generation/Cargo.toml"), "--bin", "sdk-runtime-consumer", "--"]
    command.extend(["--collect-receipt", str(project), "--output", str(output), "--source-revision", sys.argv[3], "--manifest-sha256", sys.argv[4], "--authority", str(Path(sys.argv[5]).resolve())])
    return subprocess.run(command, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
