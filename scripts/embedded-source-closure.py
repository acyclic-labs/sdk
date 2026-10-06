#!/usr/bin/env python3
"""Emit the canonical local Cargo source closure for an embedded artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True, type=pathlib.Path)
    parser.add_argument("--manifest", required=True, type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()

    repository = args.repository.resolve()
    manifest = args.manifest.resolve()
    metadata = json.loads(
        subprocess.check_output(
            [
                "cargo",
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--manifest-path",
                str(manifest),
            ],
            cwd=repository,
            text=True,
            encoding="utf-8",
        )
    )
    packages = {str(package["id"]): package for package in metadata["packages"]}
    root_id = str(metadata["resolve"]["root"])
    reachable: set[str] = set()
    pending = [root_id]
    nodes = {str(node["id"]): node for node in metadata["resolve"]["nodes"]}
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        for dependency in nodes[package_id].get("dependencies", []):
            dependency_id = str(dependency if isinstance(dependency, str) else dependency["pkg"])
            if dependency_id in packages:
                pending.append(dependency_id)

    paths: set[str] = set()
    for package_id in reachable:
        package = packages[package_id]
        if package.get("source"):
            continue
        package_root = pathlib.Path(package["manifest_path"]).resolve().parent
        package_root.relative_to(repository)
        for path in package_root.rglob("*"):
            if not path.is_file():
                continue
            relative = path.relative_to(repository)
            if any(part in {".git", "target", "node_modules"} for part in relative.parts):
                continue
            paths.add(relative.as_posix())

    for root_input in (
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "rust-toolchain",
        ".cargo/config.toml",
        ".cargo/config",
    ):
        if (repository / root_input).is_file():
            paths.add(root_input)

    ordered = sorted(paths)
    lines = [
        f"{relative}\t{hashlib.sha256((repository / relative).read_bytes()).hexdigest()}"
        for relative in ordered
    ]
    digest = hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest()
    result = {"paths": ordered, "digest": digest}
    encoded = json.dumps(result, separators=(",", ":"))
    if args.output:
        args.output.write_text(encoded + "\n", encoding="utf-8")
    else:
        sys.stdout.write(encoded + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
