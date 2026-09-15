#!/usr/bin/env python3
"""Regression tests for exact-archive npm publication."""

from __future__ import annotations

import gzip
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


def load_script(name: str):
    path = Path(__file__).with_name(name)
    spec = importlib.util.spec_from_file_location(name.removesuffix(".py"), path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {name}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


validator = load_script("validate-npm-package.py")
qualification = load_script("typescript-qualification.py")
NAME = "@acyclic-labs/objects"
VERSION = "1.0.0-rc.2"
DIRECTORY = "typescript/packages/objects"


def archive_bytes(
    *,
    repository_directory: str = DIRECTORY,
    trailing: bytes = b"",
    link: bool = False,
    dist_js: bytes = b"export {};",
) -> bytes:
    manifest = json.dumps({
        "name": NAME, "version": VERSION, "private": False, "license": "Apache-2.0",
        "repository": {"type": "git", "url": validator.REPOSITORY_URL, "directory": repository_directory},
    }).encode()
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        for name, contents in (("package/package.json", manifest), ("package/dist/index.js", dist_js), ("package/dist/index.d.ts", b"export {};")):
            member = tarfile.TarInfo(name)
            member.size = len(contents)
            archive.addfile(member, io.BytesIO(contents))
        if link:
            member = tarfile.TarInfo("package/dist/link.js")
            member.type = tarfile.SYMTYPE
            member.linkname = "../../outside"
            archive.addfile(member)
    return gzip.compress(payload.getvalue(), mtime=0) + trailing


class NpmPublicationTests(unittest.TestCase):
    def validate(self, contents: bytes) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "package.tgz"
            archive.write_bytes(contents)
            validator.validate_archive(archive, NAME, VERSION, DIRECTORY)

    def test_accepts_compiled_public_package(self) -> None:
        self.validate(archive_bytes())

    def test_rejects_wrong_repository_directory(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "manifest"):
            self.validate(archive_bytes(repository_directory="typescript/packages/sdk"))

    def test_rejects_trailing_gzip_bytes(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "one complete gzip stream"):
            self.validate(archive_bytes(trailing=b"garbage"))

    def test_rejects_links(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unsafe path"):
            self.validate(archive_bytes(link=True))

    def test_qualification_receipt_rejects_mutated_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(__file__).parent.parent
            archive = Path(temporary) / f"acyclic-labs-objects-{VERSION}.tgz"
            archive.write_bytes(archive_bytes())
            checksum, size = qualification.digest(archive)
            packages = qualification.expected_assets(root)
            for package in packages:
                package.update({"sha256": "0" * 64, "size": 1})
                if package["asset"] == archive.name:
                    package.update({"sha256": checksum, "size": size})
            receipt = Path(temporary) / "QUALIFICATION.json"
            receipt.write_text(json.dumps({
                "revision": 1,
                "source_commit": "a" * 40,
                "packages": packages,
            }), encoding="utf-8")
            qualification.verify(receipt, "a" * 40, archive.name, archive, root)
            archive.write_bytes(archive_bytes(dist_js=b"export const mutated = true;"))
            validator.validate_archive(archive, NAME, VERSION, DIRECTORY)
            with self.assertRaisesRegex(RuntimeError, "qualified artifact"):
                qualification.verify(receipt, "a" * 40, archive.name, archive, root)

    def test_publication_shells_parse_and_map_all_packages(self) -> None:
        root = Path(__file__).parent
        bash = os.environ.get("BASH", "bash")
        scripts = [root / "prepare-npm-publication.sh", root / "check-typescript-packages.sh"]
        subprocess.run([bash, "-n", *(str(path) for path in scripts)], check=True)
        prepare = shlex.quote(str(scripts[0]))
        for slug, expected in {
            "objects": "@acyclic-labs/objects\tobjects\tobjects",
            "stream": "@acyclic-labs/stream\tstream\tstream",
            "inference": "@acyclic-labs/inference\tinference\tinference",
            "machines": "@acyclic-labs/machines\tmachines\tmachines",
            "fs": "@acyclic-labs/fs\tfilesystem\tfs",
            "sdk": "@acyclic-labs/sdk\tsdk\tsdk",
        }.items():
            result = subprocess.run(
                [bash, "-c", f"source {prepare}; qualified_npm_package {shlex.quote(slug)}"],
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            )
            self.assertEqual(result.stdout.rstrip("\n"), expected)


if __name__ == "__main__":
    unittest.main()
