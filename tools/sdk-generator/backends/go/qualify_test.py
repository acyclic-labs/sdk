"""Archive admission regressions; no compiler or runtime downloads required."""

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import qualify


class ArchiveAdmission(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        authority = self.root / "authority"
        authority.mkdir()
        (authority / "root.proto").write_bytes(b"schema fixture")
        (authority / "root.bin").write_bytes(b"descriptor fixture")
        manifest = {
            "schema": "acyclic.sdk.rust-authority.v1", "authority": "rust",
            "source_revision": "test-only", "families": [{
                "source": "root.proto", "source_sha256": qualify.digest(b"schema fixture"),
                "descriptor": "root.bin", "descriptor_sha256": qualify.digest(b"descriptor fixture")}],
        }
        manifest_bytes = json.dumps(manifest).encode()
        (authority / "rust-authority.json").write_bytes(manifest_bytes)
        self.receipt = {
            "schema": "acyclic.sdk.go-producer-receipt.v1", "target": "go", "authority": "rust",
            "source_revision": "test-only", "authority_manifest_sha256": qualify.digest(manifest_bytes),
            "go_version": "go1.27.2",
        }
        self.args = argparse.Namespace(package=self.root / "package.zip", sha256="",
            authority=authority, receipt=self.root / "receipt.json", go=Path(sys.executable),
            output=self.root / "output")

    def package(self, entries):
        with zipfile.ZipFile(self.args.package, "w") as archive:
            for name, data, mode in entries:
                info = zipfile.ZipInfo(name)
                info.external_attr = mode << 16
                archive.writestr(info, data)
        self.receipt["outputs"] = [name for name, _, _ in entries]
        self.receipt["output_sha256"] = {name: qualify.digest(data) for name, data, _ in entries}
        self.args.receipt.write_text(json.dumps(self.receipt), encoding="utf-8")
        self.args.sha256 = qualify.digest(self.args.package.read_bytes())

    def rejected(self, reason):
        with patch("qualify.subprocess.check_output", return_value="go version go1.27.2 test/test"), \
                patch("qualify.subprocess.run") as command:
            with self.assertRaisesRegex(ValueError, reason):
                qualify.qualify(self.args)
            command.assert_not_called()
        self.assertFalse(self.args.output.exists())

    def test_wrong_archive_digest(self):
        self.package([("go.mod", b"module fixture", 0o100644)])
        self.args.sha256 = "0" * 64
        self.rejected("archive digest mismatch")

    def test_wrong_payload_digest(self):
        self.package([("go.mod", b"module fixture", 0o100644)])
        self.receipt["output_sha256"]["go.mod"] = "0" * 64
        self.args.receipt.write_text(json.dumps(self.receipt), encoding="utf-8")
        self.rejected("payload digest mismatch")

    def test_parent_escape(self):
        self.package([("../escaped.txt", b"escape", 0o100644)])
        self.rejected("unsafe package entry")
        self.assertFalse((self.root / "escaped.txt").exists())

    def test_symlink_entry(self):
        self.package([("go.mod", b"../outside", 0o120777)])
        self.rejected("unsafe package entry")

    def test_noncanonical_path(self):
        self.package([("nested//go.mod", b"module fixture", 0o100644)])
        self.rejected("unsafe package entry")

    @unittest.skipUnless(os.name == "nt", "Windows filesystem aliases")
    def test_windows_case_collision(self):
        self.package([("go.mod", b"first", 0o100644), ("GO.MOD", b"second", 0o100644)])
        self.rejected("collide")

    @unittest.skipUnless(os.name == "nt", "Windows device aliases")
    def test_windows_device(self):
        self.package([("NUL.txt", b"device", 0o100644)])
        self.rejected("unsafe Windows package entry")


if __name__ == "__main__":
    unittest.main()
