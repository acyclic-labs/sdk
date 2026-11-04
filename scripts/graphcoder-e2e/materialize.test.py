import errno
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("materialize", Path(__file__).with_name("materialize.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class Materialization(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "qualification" / "artifacts"
        self.root.mkdir(parents=True)
        (self.root / "argument").mkdir()
        if os.name == "nt":
            scratch = patch.dict(os.environ, {"E2E_SCRATCH_ROOT": str(self.root)})
            scratch.start()
            self.addCleanup(scratch.stop)
        self.archive = self.root / "input.tar.gz"
        self.contents = {"bin/tool.exe": b"binary" * 1024, "source/fixture.txt": b"fixture\n" * 1024}
        fixture = ("tests/fixtures/proof-policy/canonical-exports-v1/bundle/.rustdoc/profiles/"
                   "profile-1003d772898959a081259696ca59636cf0fca2c91ce60b3a5c5edafb4d7b2358.json")
        self.contents["source/scripts/" + fixture] = (Path(__file__).resolve().parent.parent / fixture).read_bytes()
        self.inventory = {name: {"sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
                          for name, data in self.contents.items()}
        self.pack(self.contents.items())

    def pack(self, entries):
        with tarfile.open(self.archive, "w:gz") as bundle:
            for name, data in entries:
                member = tarfile.TarInfo(name)
                member.size = len(data)
                bundle.addfile(member, io.BytesIO(data))
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def prepare(self, **options):
        # Ordinary caller paths may contain parent traversal; the archive's
        # members still obey the separate strict relative-member contract.
        parent = self.root / "argument" / ".."
        return module.materialize(parent / self.archive.name, self.digest, self.inventory, parent, **options)

    def test_every_payload_write_follows_the_complete_physical_reservation(self):
        reserved = []
        reserve, write = module._reserve_file, module._write_member

        def allocation(path, size):
            reserve(path, size)
            self.assertEqual(path.stat().st_size, size)
            reserved.append(path)

        def consume(*args):
            self.assertEqual(len(reserved), len(self.inventory), "copy began before capacity admission")
            return write(*args)

        with patch.object(module, "_reserve_file", allocation), patch.object(module, "_write_member", consume):
            result = self.prepare()
        self.addCleanup(shutil.rmtree, result)
        for name, expected in self.contents.items():
            self.assertEqual((result / name).read_bytes(), expected)

    def test_reserved_stream_never_reopens_or_outgrows_admitted_backing(self):
        path = self.root / "transfer"
        data = self.archive.read_bytes()
        expected = hashlib.sha256(data).hexdigest()
        with module.reserved_file(path, len(data)) as output:
            identity = path.stat()
            self.assertEqual(identity.st_size, len(data))
            self.assertTrue(output.seekable())
            module.write_reserved(io.BytesIO(data), output, len(data), expected)
            self.assertEqual(output.tell(), len(data))
            self.assertEqual(path.stat().st_ino, identity.st_ino)
        self.assertEqual(path.read_bytes(), data)
        with self.assertRaises(FileExistsError):
            with module.reserved_file(path, len(data)):
                self.fail("existing transfer was reopened")
        oversized = self.root / "oversized"
        with module.reserved_file(oversized, len(data)) as output:
            with self.assertRaisesRegex(ValueError, "payload changed"):
                module.write_reserved(io.BytesIO(data + b"extra"), output, len(data), expected)
            self.assertEqual(output.tell(), len(data))
            self.assertEqual(oversized.stat().st_size, len(data))

    def test_capacity_rejection_copies_no_payload_and_publishes_no_fixture(self):
        reserved, copied = [], []
        reserve, write = module._reserve_file, module._write_member

        def exhausted(path, size):
            reserved.append(path)
            if len(reserved) == 2:
                raise OSError(errno.ENOSPC, "controlled extent admission failure")
            return reserve(path, size)

        def consume(*args):
            copied.append(args[1])
            return write(*args)

        with patch.object(module, "_reserve_file", exhausted), patch.object(module, "_write_member", consume):
            with self.assertRaises(OSError):
                self.prepare()
        self.assertEqual(len(reserved), 2)
        self.assertEqual(copied, [])
        self.assertEqual(list(self.root.glob("closure-*")), [])

    def test_existing_fixture_identity_is_rejected_without_mutation(self):
        identity = "a" * 32
        existing = self.root / ("closure-" + identity)
        existing.mkdir()
        (existing / "sentinel").write_bytes(b"retained")
        with self.assertRaises(FileExistsError):
            self.prepare(identity=identity)
        self.assertEqual((existing / "sentinel").read_bytes(), b"retained")
        self.assertEqual(list(existing.iterdir()), [existing / "sentinel"])

    def test_malformed_or_incomplete_archives_fail_before_reservation(self):
        for entries in [[("../escape", b"x")], [("bin/tool.exe", self.contents["bin/tool.exe"])],
                        list(self.contents.items()) + [next(iter(self.contents.items()))]]:
            self.pack(entries)
            with patch.object(module, "_reserve_file") as reserve:
                with self.assertRaises(ValueError):
                    self.prepare()
                reserve.assert_not_called()
        self.assertEqual(list(self.root.glob("closure-*")), [])


if __name__ == "__main__":
    unittest.main()
