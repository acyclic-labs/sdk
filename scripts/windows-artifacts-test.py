#!/usr/bin/env python3
"""Real Windows export admission and interrupted-transfer regression controls."""

import errno
from contextlib import nullcontext
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch


path = Path(__file__).with_name("windows-artifacts.py")
if len(sys.argv) > 1 and sys.argv[1].endswith(".py"):
    path = Path(sys.argv.pop(1))
api = ModuleType("windows_artifacts")
api.__file__ = str(path)
exec(compile(path.read_bytes(), str(path), "exec"), api.__dict__)


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.records = self.root / "records"
        self.records.mkdir()

    def export(self, extra=(), missing=False, duplicate=False, bad_file=False, host=None):
        data = b"admitted-cross-built-fixture"
        expected = hashlib.sha256(data).hexdigest()
        manifest = dict(artifacts=[dict(name="probe.exe", sha256=expected)], dlls={}, source={})
        members = [("manifest.json", json.dumps(manifest).encode())]
        if not missing:
            members.append(("bin/probe.exe", b"corrupt" if bad_file else data))
        if duplicate:
            members.append(members[0])
        if host:
            source_name = "rust/crates/native-runtime/src/bin/acyclic-process-host.rs"
            source = b"source-qualified host fixture"
            manifest["source"][source_name] = hashlib.sha256(source).hexdigest()
            receipt = dict(exit=0, target="x86_64-pc-windows-msvc", executable_sha256=expected,
                           source={source_name: manifest["source"][source_name]})
            if host == "changed-source":
                receipt["source"][source_name] = "0" * 64
            raw_receipt = json.dumps(receipt).encode()
            manifest["process_host"] = dict(name="probe.exe" if host == "alias" else "host.exe",
                                            sha256=expected, receipt=receipt,
                                            receipt_sha256=hashlib.sha256(raw_receipt).hexdigest())
            members[0] = ("manifest.json", json.dumps(manifest).encode())
            members.extend([("bin/host.exe", data), ("source/" + source_name, source)])
            if host != "missing-receipt":
                members.append(("host-receipt.json", b"{}" if host == "changed-receipt" else raw_receipt))
        members.extend(extra)
        archive = self.root / "export.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            for name, contents in members:
                entry = tarfile.TarInfo(name)
                entry.size = len(contents)
                tar.addfile(entry, io.BytesIO(contents))
        return archive, api.digest(archive), data

    def assert_rejected_without_writes(self, **kwargs):
        archive, checksum, _ = self.export(**kwargs)
        with self.assertRaises(ValueError):
            api.admit_archive(self.records, archive, checksum)
        self.assertEqual(list(self.records.iterdir()), [])

    def test_unexpected_tail_has_no_side_effects(self):
        self.assert_rejected_without_writes(extra=[("unexpected.txt", b"reject")])

    def test_traversal_tail_has_no_side_effects(self):
        self.assert_rejected_without_writes(extra=[("../escaped.exe", b"reject")])

    def test_case_alias_has_no_side_effects(self):
        self.assert_rejected_without_writes(extra=[("BIN/PROBE.EXE", b"reject")])

    def test_missing_required_member(self):
        self.assert_rejected_without_writes(missing=True)

    def test_duplicate_manifest(self):
        self.assert_rejected_without_writes(duplicate=True)

    def test_bad_member_digest(self):
        self.assert_rejected_without_writes(bad_file=True)

    def test_bad_archive_digest(self):
        archive, _, _ = self.export()
        with self.assertRaises(ValueError):
            api.admit_archive(self.records, archive, "0" * 64)
        self.assertEqual(list(self.records.iterdir()), [])

    def test_owned_host_is_part_of_the_admitted_export(self):
        archive, checksum, data = self.export(host="valid")
        admitted = api.admit_archive(self.records, archive, checksum)
        self.assertEqual((admitted.directory / "bin/host.exe").read_bytes(), data)
        self.assertEqual(json.loads((admitted.directory / "host-receipt.json").read_text()),
                         admitted.manifest["process_host"]["receipt"])

    def test_host_source_must_match_the_exported_graph(self):
        self.assert_rejected_without_writes(host="changed-source")

    def test_host_receipt_must_be_present(self):
        self.assert_rejected_without_writes(host="missing-receipt")

    def test_host_receipt_must_match_the_manifest(self):
        self.assert_rejected_without_writes(host="changed-receipt")

    def test_host_cannot_alias_a_test_artifact(self):
        self.assert_rejected_without_writes(host="alias")

    def test_capacity_rejects_before_writes(self):
        archive, checksum, _ = self.export()
        with patch.object(api.shutil, "disk_usage", return_value=SimpleNamespace(free=0)):
            with self.assertRaises(OSError):
                api.admit_archive(self.records, archive, checksum)
        self.assertEqual(list(self.records.iterdir()), [])

    def test_resume_retains_partial_bytes(self):
        archive, checksum, data = self.export()
        partial = self.records / "bin/probe.exe"
        partial.parent.mkdir()
        partial.write_bytes(b"incomplete transfer")
        admitted = api.admit_archive(self.records, archive, checksum)
        self.assertEqual((admitted.directory / "bin/probe.exe").read_bytes(), data)
        self.assertEqual(partial.read_bytes(), b"incomplete transfer")
        self.assertNotEqual(admitted.directory, self.records)

    def test_complete_resume_preserves_existing_file(self):
        archive, checksum, _ = self.export()
        api.admit_archive(self.records, archive, checksum)
        paths = {p: p.stat().st_mtime_ns for p in self.records.rglob("*") if p.is_file()}
        api.admit_archive(self.records, archive, checksum)
        self.assertEqual({p: p.stat().st_mtime_ns for p in paths}, paths)

    def test_all_extents_are_reserved_before_any_payload_write(self):
        archive, checksum, _ = self.export()
        reserved = []
        reserve, write = api.materializer._reserve_file, api.materializer._write_member

        def allocation(path, size):
            reserve(path, size)
            reserved.append(path)

        def consume(*args):
            self.assertEqual(len(reserved), 2, "payload precedes complete physical admission")
            return write(*args)

        with (patch.object(api.materializer, "_reserve_file", allocation),
              patch.object(api.materializer, "_write_member", consume)):
            admitted = api.admit_archive(self.records, archive, checksum)
        self.assertEqual(len(reserved), 2)
        self.assertTrue(admitted.directory.is_dir())

    def test_physical_capacity_refusal_copies_no_payload(self):
        archive, checksum, _ = self.export()
        reserved = []
        reserve = api.materializer._reserve_file

        def exhausted(path, size):
            reserved.append(path)
            if len(reserved) == 2:
                raise OSError(errno.ENOSPC, "controlled physical admission refusal")
            return reserve(path, size)

        with (patch.object(api.materializer, "_reserve_file", exhausted),
              patch.object(api.materializer, "_write_member",
                           side_effect=AssertionError("payload copied before full reservation")),
              patch.object(api.shutil, "copyfileobj",
                           side_effect=AssertionError("legacy streaming copied before reservation"))):
            with self.assertRaises(OSError):
                api.admit_archive(self.records, archive, checksum)
        self.assertEqual(len(reserved), 2)
        self.assertEqual(list(self.records.iterdir()), [])

    def test_incomplete_closure_is_retained_and_never_adopted(self):
        archive, checksum, data = self.export()
        partial = self.records / ("closure-" + "a" * 32)
        (partial / "bin").mkdir(parents=True)
        (partial / "bin/probe.exe").write_bytes(data)
        (partial / "manifest.json").write_bytes(b"interrupted manifest")
        admitted = api.admit_archive(self.records, archive, checksum)
        self.assertNotEqual(admitted.directory, partial)
        self.assertEqual((partial / "manifest.json").read_bytes(), b"interrupted manifest")
        self.assertEqual((admitted.directory / "bin/probe.exe").read_bytes(), data)

    def test_existing_exact_archive_does_not_fetch(self):
        archive, checksum, _ = self.export()
        archive.rename(self.records / "artifacts.tar.gz")
        log = "WIN_TEST_ARCHIVE " + checksum + " .bb-win-test-0123abcd.tar.gz\n"
        with patch.object(api.subprocess, "check_output", side_effect=AssertionError("unexpected SSH")):
            actual, admitted = api.fetch_export(self.records, log)
        self.assertEqual((actual, admitted), (self.records / "artifacts.tar.gz", checksum))

    def test_fetch_capacity_rejects_before_scp(self):
        log = "; src wt-sdk/fixture-job target sdk/fixture;\nWIN_TEST_ARCHIVE " + "0" * 64 + " .bb-win-test-0123abcd.tar.gz\n"
        with (patch.object(api.subprocess, "check_output", return_value='{"bytes": 100, "expanded_bytes": 200}'),
              patch.object(api.subprocess, "run", side_effect=AssertionError("unexpected SCP")),
              patch.object(api.subprocess, "Popen", side_effect=AssertionError("unexpected SCP")),
              patch.object(api.shutil, "disk_usage", return_value=SimpleNamespace(free=0))):
            with self.assertRaises(OSError):
                api.fetch_export(self.records, log)
        self.assertEqual(list(self.records.iterdir()), [])

    def test_completed_fetch_is_reused_without_another_transfer(self):
        archive, checksum, _ = self.export()
        data = archive.read_bytes()
        log = "; src wt-sdk/fixture-job target sdk/fixture;\nWIN_TEST_ARCHIVE " + checksum + " .bb-win-test-0123abcd.tar.gz\n"
        partial = self.records / ".bb-win-test-0123abcd.tar.gz"
        partial.write_bytes(b"interrupted SCP")

        def transfer(argv, **kwargs):
            incoming = next(self.records.glob("incoming-*.tar.gz"))
            # A local pipe supplies our archive protocol bytes. This does not
            # simulate an SSH/SCP response; real transport has its own receipt.
            self.assertEqual(incoming.stat().st_size, len(data))
            self.assertEqual(argv[-1], "/dev/stdout")
            self.assertEqual(kwargs["stdout"], api.subprocess.PIPE)
            self.assertEqual(kwargs["stdin"], api.subprocess.DEVNULL)
            return nullcontext(SimpleNamespace(stdout=io.BytesIO(data), wait=lambda: 0))

        sizes = json.dumps(dict(bytes=len(data), expanded_bytes=100))
        with (patch.object(api.subprocess, "check_output", return_value=sizes),
              patch.object(api.subprocess, "Popen", side_effect=transfer) as scp,
              patch.object(api.subprocess, "run", side_effect=AssertionError("unreserved transfer"))):
            first = api.fetch_export(self.records, log)
            second = api.fetch_export(self.records, log)
        self.assertEqual(first, second)
        self.assertEqual(scp.call_count, 1)
        self.assertEqual(first, (partial, checksum))
        retained = list((self.records / "failed-transfers").glob("*/" + partial.name))
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0].read_bytes(), b"interrupted SCP")

    def test_transfer_physical_reservation_failure_starts_no_scp(self):
        log = "; src wt-sdk/fixture-job target sdk/fixture;\nWIN_TEST_ARCHIVE " + "0" * 64 + " .bb-win-test-0123abcd.tar.gz\n"
        with (patch.object(api.subprocess, "check_output", return_value='{"bytes": 100, "expanded_bytes": 200}'),
              patch.object(api.materializer, "reserved_file", create=True, side_effect=OSError(errno.ENOSPC, "physical admission refused")) as reserve,
              patch.object(api.subprocess, "Popen", side_effect=AssertionError("SCP before physical admission")),
              patch.object(api.subprocess, "run", side_effect=AssertionError("SCP before physical admission"))):
            with self.assertRaisesRegex(RuntimeError, "step=reserve-transfer.*recovery=bash scripts/windows-test.sh --consume"):
                api.fetch_export(self.records, log)
        self.assertEqual(reserve.call_count, 1)
        refusal, = self.records.glob("incoming-*.failure.json")
        self.assertEqual(json.loads(refusal.read_text())["received_bytes"], 0)

    def test_failed_transfer_keeps_reserved_backing_and_exact_received_count(self):
        archive, checksum, _ = self.export()
        data = archive.read_bytes()
        log = "; src wt-sdk/fixture-job target sdk/fixture;\nWIN_TEST_ARCHIVE " + checksum + " .bb-win-test-0123abcd.tar.gz\n"
        def interrupted(argv, **kwargs):
            return nullcontext(SimpleNamespace(stdout=io.BytesIO(data[:10]), wait=lambda: 1))
        with (patch.object(api.subprocess, "check_output", return_value=json.dumps(dict(bytes=len(data), expanded_bytes=100))),
              patch.object(api.subprocess, "Popen", side_effect=interrupted)):
            with self.assertRaisesRegex(RuntimeError, "step=copy-reserved-transfer.*truncated admitted payload"):
                api.fetch_export(self.records, log)
        incoming, = self.records.glob("incoming-*.tar.gz")
        self.assertEqual(incoming.stat().st_size, len(data))
        self.assertEqual(incoming.read_bytes()[:10], data[:10])
        refusal, = self.records.glob("incoming-*.failure.json")
        record = json.loads(refusal.read_text())
        self.assertEqual(record["received_bytes"], 10)
        self.assertEqual(record["expected_bytes"], len(data))


if __name__ == "__main__":
    unittest.main()
