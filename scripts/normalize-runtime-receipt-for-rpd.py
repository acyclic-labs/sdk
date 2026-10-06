#!/usr/bin/env python3
"""Thin compatibility launcher for the Rust-owned receipt normalizer.

Receipt identity, byte validation, ordered execution matching, and RPD
projection live in ``sdk-runtime-consumer``. This wrapper keeps the existing
command name for downstream qualification jobs without duplicating authority
or protobuf semantics in Python.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--language", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[1]
    command = [
        "cargo",
        "run",
        "--quiet",
        "--locked",
        "--manifest-path",
        str(repo / "rust/crates/sdk-generation/Cargo.toml"),
        "--bin",
        "sdk-runtime-consumer",
        "--",
        "--language",
        args.language,
        "--inventory",
        str(args.inventory),
        "--receipt",
        str(args.receipt),
        "--normalize-output",
        str(args.output),
    ]
    subprocess.run(command, cwd=repo, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
