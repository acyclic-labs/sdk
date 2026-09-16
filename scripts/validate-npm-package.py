#!/usr/bin/env python3
"""Validate one bounded npm tarball before release or publication."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import sys
import tarfile
import zlib


MAX_ARCHIVE_BYTES = 104_857_600
MAX_EXPANDED_BYTES = 536_870_912
REPOSITORY_URL = "git+https://github.com/acyclic-labs/sdk.git"


def validate_archive(archive: Path, name: str, version: str, directory: str) -> str:
    size = archive.stat().st_size
    if not 0 < size <= MAX_ARCHIVE_BYTES:
        raise RuntimeError("npm archive exceeds its size bound")
    with archive.open("rb") as source:
        compressed = source.read(MAX_ARCHIVE_BYTES + 1)
    if len(compressed) != size:
        raise RuntimeError("npm archive changed while it was being read")
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    expanded = decoder.decompress(compressed, MAX_EXPANDED_BYTES + 1)
    if len(expanded) > MAX_EXPANDED_BYTES:
        raise RuntimeError("npm archive expands beyond its size bound")
    expanded += decoder.flush()
    if len(expanded) > MAX_EXPANDED_BYTES:
        raise RuntimeError("npm archive expands beyond its size bound")
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise RuntimeError("npm archive must contain one complete gzip stream")

    seen: set[str] = set()
    manifest: bytes | None = None
    readme: bytes | None = None
    has_js = False
    has_types = False
    with tarfile.open(fileobj=io.BytesIO(expanded), mode="r:") as package:
        for member in package:
            path = PurePosixPath(member.name)
            if (
                member.name in seen
                or path.is_absolute()
                or ".." in path.parts
                or not path.parts
                or path.parts[0] != "package"
                or not (member.isfile() or member.isdir())
            ):
                raise RuntimeError("npm archive contains an unsafe path")
            seen.add(member.name)
            if member.isfile():
                extracted = package.extractfile(member)
                if extracted is None:
                    raise RuntimeError("npm archive member cannot be read")
                if member.name == "package/package.json":
                    manifest = extracted.read(1_048_577)
                    if len(manifest) > 1_048_576:
                        raise RuntimeError("npm manifest exceeds its size bound")
                elif member.name == "package/README.md":
                    readme = extracted.read(262_145)
                    if len(readme) > 262_144:
                        raise RuntimeError("npm README exceeds its size bound")
                elif member.name.startswith("package/dist/"):
                    has_js |= member.name.endswith((".js", ".mjs", ".cjs"))
                    has_types |= member.name.endswith(".d.ts")
    if manifest is None or not has_js or not has_types or readme is None or len(readme.strip()) == 0:
        raise RuntimeError("npm archive lacks its README, manifest, or compiled public output")
    metadata = json.loads(manifest)
    expected_repository = {
        "type": "git",
        "url": REPOSITORY_URL,
        "directory": directory,
    }
    if (
        metadata.get("name") != name
        or metadata.get("version") != version
        or metadata.get("private") is not False
        or metadata.get("license") != "Apache-2.0"
        or metadata.get("repository") != expected_repository
    ):
        raise RuntimeError("npm manifest does not match the qualified package")
    if readme.decode("utf-8").splitlines()[0].strip() != f"# {name}":
        raise RuntimeError("npm README does not match the qualified package")
    return hashlib.sha256(compressed).hexdigest()


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit("usage: validate-npm-package.py ARCHIVE NAME VERSION DIRECTORY")
    print(validate_archive(Path(sys.argv[1]), *sys.argv[2:]))


if __name__ == "__main__":
    main()
