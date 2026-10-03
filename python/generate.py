"""Invoke grpcio-tools for the Rust-owned Python transport target.

Orchestration, tool-version checks, authority validation, package import
normalization, and drift checking belong to the standalone Rust generator in
``rust/crates/sdk-python``. This file intentionally only invokes protoc.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


IMPORT_RE = re.compile(r'^\s*import\s+"([^"]+)"\s*;', re.MULTILINE)


def authority_sources(schema_root: Path) -> list[str]:
    """Return every Rust-emitted proto and its imported dependency closure."""

    marker = schema_root / "rust-authority.json"
    try:
        manifest = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read Rust authority manifest {marker}: {error}") from error
    if manifest.get("schema") != "acyclic.sdk.rust-authority.v1" or manifest.get("authority") != "rust":
        raise RuntimeError(f"{marker} is not a Rust authority export")
    families = manifest.get("families")
    if not isinstance(families, list) or not families:
        raise RuntimeError(f"{marker} contains no Rust authority families")

    sources: list[str] = []
    queued = [family.get("source") for family in families if isinstance(family, dict)]
    seen: set[str] = set()
    while queued:
        relative = queued.pop(0)
        if not isinstance(relative, str) or not relative or relative in seen:
            continue
        root = schema_root.resolve()
        path = (root / relative).resolve()
        if root not in path.parents:
            raise RuntimeError(f"Rust authority import escapes schema root: {relative}")
        if not path.is_file():
            raise RuntimeError(f"Rust authority proto is missing: {relative}")
        seen.add(relative)
        sources.append(relative)
        for dependency in IMPORT_RE.findall(path.read_text(encoding="utf-8")):
            dependency_path = (root / dependency).resolve()
            if root in dependency_path.parents and dependency_path.is_file():
                queued.append(dependency)
    if not sources:
        raise RuntimeError(f"{marker} contains no Rust authority proto sources")
    return sources


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()

    import grpc_tools

    grpc_include = Path(grpc_tools.__file__).parent / "_proto"
    args.output.mkdir(parents=True, exist_ok=True)
    sources = authority_sources(args.schema_root)
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(args.schema_root),
        "-I",
        str(grpc_include),
        "--python_out=" + str(args.output),
        "--grpc_python_out=" + str(args.output),
    ]
    command.extend(sources)
    subprocess.run(command, cwd=args.schema_root, check=True)


if __name__ == "__main__":
    main()
