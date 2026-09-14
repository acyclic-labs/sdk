#!/usr/bin/env python3
"""Regression tests for exact-archive crates.io publication."""

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock


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


def crate_bytes(
    *,
    package: str = PACKAGE,
    path_in_vcs: str = "rust/crates/machines",
    trailing: bytes = b"",
    duplicate_manifest: bool = False,
) -> bytes:
    prefix = f"{package}-{VERSION}"
    manifest = (
        f'[package]\nname = "{package}"\nversion = "{VERSION}"\n'
        'edition = "2021"\nlicense = "Apache-2.0"\n'
    ).encode()
    vcs = json.dumps(
        {"git": {"sha1": SOURCE_SHA}, "path_in_vcs": path_in_vcs}
    ).encode()
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        for name, contents in (("Cargo.toml", manifest), (".cargo_vcs_info.json", vcs)):
            member = tarfile.TarInfo(f"{prefix}/{name}")
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
        if duplicate_manifest:
            member = tarfile.TarInfo(f"{prefix}/Cargo.toml")
            member.size = len(manifest)
            archive.addfile(member, io.BytesIO(manifest))
    return gzip.compress(payload.getvalue(), mtime=0) + trailing


class PublicationTests(unittest.TestCase):
    def test_inference_uses_public_acyclic_name(self) -> None:
        package = "acyclic-inference"
        path_in_vcs = publisher.qualified_package_path(package)
        self.assertEqual(path_in_vcs, "rust/crates/inference")
        contents = crate_bytes(package=package, path_in_vcs=path_in_vcs)
        with tempfile.TemporaryDirectory() as temporary:
            crate = Path(temporary) / f"{package}-{VERSION}.crate"
            crate.write_bytes(contents)
            publisher.validate_archive(
                crate,
                package,
                VERSION,
                hashlib.sha256(contents).hexdigest(),
                SOURCE_SHA,
                path_in_vcs,
            )
        with self.assertRaisesRegex(RuntimeError, "no qualified release path"):
            publisher.qualified_package_path("inference-sdk")

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

    def test_rejects_oversize_archive_before_reading(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            crate = Path(temporary) / f"{PREFIX}.crate"
            with crate.open("wb") as archive:
                archive.truncate(publisher.MAX_CRATE_BYTES + 1)
            with mock.patch.object(Path, "read_bytes", side_effect=AssertionError):
                with self.assertRaisesRegex(RuntimeError, "size bound"):
                    publisher.validate_archive(
                        crate,
                        PACKAGE,
                        VERSION,
                        "0" * 64,
                        SOURCE_SHA,
                        "rust/crates/machines",
                    )

    def test_fetcher_preserves_preexisting_output(self) -> None:
        asset = f"{PREFIX}.crate"
        tag = f"machines-v{VERSION}"
        release = {
            "tag_name": tag,
            "draft": False,
            "assets": [
                {
                    "name": asset,
                    "size": 1,
                    "digest": f"sha256:{hashlib.sha256(b'x').hexdigest()}",
                    "state": "uploaded",
                    "browser_download_url": (
                        f"https://github.com/acyclic-labs/sdk/releases/download/"
                        f"{tag}/{asset}"
                    ),
                }
            ],
        }
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / asset
            output.write_bytes(b"preserve")
            response = io.BytesIO(json.dumps(release).encode())
            with (
                mock.patch.dict(
                    fetcher.os.environ,
                    {"GITHUB_REPOSITORY": "acyclic-labs/sdk", "GITHUB_TOKEN": "token"},
                ),
                mock.patch.object(fetcher.sys, "argv", ["fetch", tag, asset, str(output)]),
                mock.patch.object(fetcher.urllib.request, "urlopen", return_value=response),
                mock.patch.object(fetcher.urllib.request, "build_opener") as build_opener,
            ):
                with self.assertRaisesRegex(RuntimeError, "already exists"):
                    fetcher.main()
            self.assertEqual(output.read_bytes(), b"preserve")
            build_opener.assert_not_called()

    def test_bounds_metadata_reads(self) -> None:
        self.assertEqual(fetcher.read_bounded(io.BytesIO(b"abc"), 3), b"abc")
        with self.assertRaisesRegex(RuntimeError, "size bound"):
            fetcher.read_bounded(io.BytesIO(b"abcd"), 3)

    def test_release_tag_object_types(self) -> None:
        script_path = Path(__file__).with_name("prepare-crate-publication.sh").resolve()
        script = script_path.as_posix()
        bash = os.environ.get("BASH", "bash")
        for object_type in ("commit", "tag"):
            command = (
                f"source {shlex.quote(script)}; "
                f"validate_release_object_type {shlex.quote(object_type)}"
            )
            subprocess.run([bash, "-c", command], check=True)
        for object_type in ("blob", "tree", ""):
            command = (
                f"source {shlex.quote(script)}; "
                f"validate_release_object_type {shlex.quote(object_type)}"
            )
            rejected = subprocess.run(
                [bash, "-c", command],
                stderr=subprocess.PIPE,
                text=True,
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("tag or commit", rejected.stderr)

    def test_inference_release_family_uses_public_name(self) -> None:
        script_path = Path(__file__).with_name("prepare-crate-publication.sh").resolve()
        script = script_path.as_posix()
        bash = os.environ.get("BASH", "bash")
        command = (
            f"source {shlex.quote(script)}; "
            "qualified_release_family acyclic-inference"
        )
        resolved = subprocess.run(
            [bash, "-c", command],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
        self.assertEqual(resolved.stdout, "inference\n")

        legacy = subprocess.run(
            [
                bash,
                "-c",
                f"source {shlex.quote(script)}; qualified_release_family inference-sdk",
            ],
            stderr=subprocess.PIPE,
            text=True,
        )
        self.assertNotEqual(legacy.returncode, 0)
        self.assertIn("no qualified release family", legacy.stderr)


if __name__ == "__main__":
    unittest.main()
