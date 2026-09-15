#!/usr/bin/env python3
"""Verify that npm serves the exact qualified tarball bytes."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import urllib.parse
import urllib.request


def bounded(url: str, limit: int) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "acyclic-sdk-exact-npm-publisher"})
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read(limit + 1)
    if len(payload) > limit:
        raise RuntimeError("npm response exceeds its size bound")
    return payload


def main() -> None:
    if len(sys.argv) != 5:
        raise SystemExit("usage: verify-npm-publication.py PACKAGE VERSION SHA256 SIZE")
    package, version, expected_sha, size_text = sys.argv[1:]
    if not re.fullmatch(r"@acyclic-labs/[a-z0-9-]+", package):
        raise RuntimeError("invalid npm package")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[+-][0-9A-Za-z.-]+)?", version):
        raise RuntimeError("invalid npm version")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha):
        raise RuntimeError("invalid npm checksum")
    expected_size = int(size_text)
    if not 0 < expected_size <= 104_857_600:
        raise RuntimeError("invalid npm archive size")
    metadata_url = f"https://registry.npmjs.org/{urllib.parse.quote(package, safe='')}/{urllib.parse.quote(version, safe='')}"
    metadata = json.loads(bounded(metadata_url, 1_048_576))
    bare = package.split("/", 1)[1]
    expected_url = f"https://registry.npmjs.org/{package}/-/{bare}-{version}.tgz"
    if metadata.get("name") != package or metadata.get("version") != version or metadata.get("dist", {}).get("tarball") != expected_url:
        raise RuntimeError("npm metadata does not match the qualified package")
    archive = bounded(expected_url, expected_size)
    if len(archive) != expected_size or hashlib.sha256(archive).hexdigest() != expected_sha:
        raise RuntimeError("npm tarball bytes differ from the qualified asset")


if __name__ == "__main__":
    main()
