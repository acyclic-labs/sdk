#!/usr/bin/env python3
"""Negative integrity checks for source-bound Rustdoc artifact receipts."""
import copy
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("rustdoc_artifact_verifier", Path(__file__).with_name("verify-rustdoc-artifacts.py"))
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)

class RustdocReceiptIntegrity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="rustdoc-integrity-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.json = self.root / "crate.json"
        self.json.write_text('{"index":{}}')
        self.artifact = dict(profile="host-default", package_name="fixture", target="x86_64-unknown-linux-gnu", features=[], source_blake3="a" * 64, profile_blake3="b" * 64, rustdoc_json="crate.json", rustdoc_json_blake3=verifier.digest(self.json), receipt="crate.receipt.json")
        self.receipt = {**self.artifact, "schema_version": 1, "source_revision": "c" * 40, "toolchain": "1.98.1"}
        self.generation = dict(schema_version=1, source_revision="c" * 40, toolchain="1.98.1", artifacts=[self.artifact])

    def check(self, generation=None, receipt=None):
        (self.root / "generation-receipt.json").write_text(json.dumps(self.generation if generation is None else generation))
        (self.root / "crate.receipt.json").write_text(json.dumps(self.receipt if receipt is None else receipt))
        return verifier.validate(self.root)

    def test_exact_artifact_passes(self):
        self.assertTrue(self.check()["valid"])

    def test_empty_generation_rejected(self):
        self.assertFalse(self.check({**self.generation, "artifacts": []})["valid"])

    def test_missing_generation_identity_rejected(self):
        for field in ("source_revision", "toolchain", "schema_version"):
            with self.subTest(field=field):
                generation = {**self.generation, field: None}
                receipt = {**self.receipt, field: None}
                self.assertFalse(self.check(generation, receipt)["valid"])

    def test_duplicate_profile_package_rejected(self):
        self.assertFalse(self.check({**self.generation, "artifacts": [self.artifact, copy.deepcopy(self.artifact)]})["valid"])

    def test_swapped_receipt_identity_rejected(self):
        for field, wrong in (("package_name", "different"), ("profile", "wasm"), ("target", "wasm32-unknown-unknown"), ("features", ["different"]), ("source_blake3", "d" * 64), ("profile_blake3", "e" * 64), ("schema_version", 2)):
            with self.subTest(field=field):
                self.assertFalse(self.check(receipt={**self.receipt, field: wrong})["valid"])

    def test_artifact_identity_must_be_typed_and_present(self):
        for field, wrong in (("profile", 12), ("package_name", None), ("target", ""), ("features", "feature"), ("source_blake3", ""), ("profile_blake3", None)):
            with self.subTest(field=field):
                artifact = {**self.artifact, field: wrong}
                self.assertFalse(self.check({**self.generation, "artifacts": [artifact]})["valid"])

    def test_tampered_json_rejected(self):
        self.json.write_text('{"index":{"tampered":true}}')
        self.assertFalse(self.check()["valid"])

    def test_paths_cannot_escape_artifact_tree(self):
        for field in ("rustdoc_json", "receipt"):
            with self.subTest(field=field):
                artifact = {**self.artifact, field: "../outside.json"}
                result = self.check({**self.generation, "artifacts": [artifact]})
                self.assertFalse(result["valid"])
                self.assertTrue(any("escapes artifact tree" in error for error in result["errors"]))

if __name__ == "__main__":
    unittest.main()
