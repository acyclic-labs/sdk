#!/usr/bin/env python3
"""Fail closed when a generated language surface drops a Rust-owned RPC."""
from __future__ import annotations

import re
import sys
from pathlib import Path


def variants(name: str) -> tuple[str, ...]:
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    kebab = snake.replace("_", "-")
    return name, snake, kebab


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: verify-generated-rpc-coverage.py GENERATED_ROOT PROTO...", file=sys.stderr)
        return 2
    generated_root = Path(sys.argv[1])
    proto_paths = [Path(path) for path in sys.argv[2:]]
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(proto_paths)
    )
    names = re.findall(r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", source)
    generated = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in generated_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".ex", ".erl", ".ml", ".lisp", ".lsp"}
    )
    missing = [
        name for name in names
        if not any(re.search(rf"(?<![A-Za-z0-9_-]){re.escape(candidate)}(?![A-Za-z0-9_-])", generated)
                   for candidate in variants(name))
    ]
    if missing:
        print("Generated client surface is missing Rust RPCs: " + ", ".join(missing), file=sys.stderr)
        return 1
    print(f"verified {len(names)} Rust RPCs in {generated_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
