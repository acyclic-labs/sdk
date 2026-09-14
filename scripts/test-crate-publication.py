#!/usr/bin/env python3
"""Regression tests for exact-archive crates.io publication."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest


SOURCE_SHA = "1" * 40
PACKAGE = "acyclic-machines"
VERSION = "1.0.0-rc.5"
PREFIX = f"{PACKAGE}-{VERSION}"


def load_script(name: str):
    path = Path(__file__).with_name(name)
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


publisher = load_script("publish-crate.py")
fetcher = load_script("fetch-release-crate.py")


def crate_bytes(*, trailing: bytes = b"", duplicate_manifest: bool = False) -> bytes:
    manifest = (
        f'[package]\nname = "{PACKAGE}"\nversion = "{VERSION}"\n'
        'edition = "2021"\nlicense = "Apache-2.0"\n'
    ).encode()
    vcs = json.dumps(
        {"git": {"sha1": SOURCE_SHA}, "path_in_vcs": "rust/crates/machines"}
    ).encode()
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        for name, contents in (("Cargo.toml", manifest), (".cargo_vcs_info.json", vcs)):
            member = tarfile.TarInfo(f"{PREFIX}/{name}")
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
        if duplicate_manifest:
            member = tarfile.TarInfo(f"{PREFIX}/Cargo.toml")
            member.size = len(manifest)
            archive.addfile(member, io.BytesIO(manifest))
    return gzip.compress(payload.getvalue(), mtime=0) + trailing


class PublicationTests(unittest.TestCase):
    def validate(self, contents: bytes, source_sha: str = SOURCE_SHA) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            crate = Path(temporary) / f"{PREFIX}.crate"
            crate.write_bytes(contents)
            publisher.validate_archive(
                crate,
                PACKAGE,
                VERSION,
                hashlib.sha256(contents).hexdigest(),
                source_sha,
                "rust/crates/machines",
            )

    def test_accepts_one_commit_bound_gzip_stream(self) -> None:
        self.validate(crate_bytes())

    def test_rejects_wrong_source_commit(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "selected source commit"):
            self.validate(crate_bytes(), "0" * 40)

    def test_rejects_trailing_gzip_bytes(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "one complete gzip stream"):
            self.validate(crate_bytes(trailing=b"garbage"))

    def test_rejects_duplicate_tar_members(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unsafe path"):
            self.validate(crate_bytes(duplicate_manifest=True))

    def test_bounds_metadata_reads(self) -> None:
        self.assertEqual(fetcher.read_bounded(io.BytesIO(b"abc"), 3), b"abc")
        with self.assertRaisesRegex(RuntimeError, "size bound"):
            fetcher.read_bounded(io.BytesIO(b"abcd"), 3)


if __name__ == "__main__":
    unittest.main()
