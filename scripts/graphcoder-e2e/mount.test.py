"""Linux-only real FUSE resource proof; invoke explicitly on bb2, never skip."""
import ctypes
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from owned_mounts import MountLease

HERE = Path(__file__).resolve().parent


def host(root, mode="exit", leased=True, lease_root=None):
    command = [sys.executable, str(HERE / "owned-process.py")]
    if leased:
        command += ["--mount-root", str(lease_root or root), "--"]
    command += [sys.executable, str(HERE / "mount-fixture.py"), str(root / "mounted space\ttab"), mode]
    return subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)


def finish(process):
    try:
        code = process.wait()
        return code, process.stdout.read(), process.stderr.read()
    finally:
        process.stdin.close()
        process.stdout.close()
        process.stderr.close()


class MountOwnership(unittest.TestCase):
    def test_installed_pi_directories_remain_inside_acceptance_lease(self):
        module = HERE / "fixture-directory.mjs"
        for prefix in [".pi-native-recursive-", ".pi-native-cuts-"]:
            for mode in ["exit", "wait"]:
                with self.subTest(prefix=prefix, mode=mode), tempfile.TemporaryDirectory() as root:
                    script = (f"import {{hostFixtureDirectory}} from {json.dumps(module.as_uri())};"
                              f"console.log(hostFixtureDirectory({json.dumps(prefix)}));")
                    directory = Path(subprocess.check_output(
                        ["node", "--input-type=module", "-e", script], text=True,
                        env={**os.environ, "ACYCLIC_TEST_FIXTURE_ROOT": root}).strip())
                    self.assertTrue(directory.is_relative_to(Path(root).resolve()))
                    lease = MountLease(root)
                    process = host(directory, mode, lease_root=root)
                    try:
                        if mode == "wait":
                            self.assertIn("mounted", json.loads(process.stdout.readline()))
                            self.assertEqual(len(lease.mounts()), 1)
                            process.stdin.close()
                        code, _, error = finish(process)
                        self.assertEqual(code, 0 if mode == "exit" else 124, error)
                        self.assertEqual(lease.mounts(), [])
                    finally:
                        if process.poll() is None:
                            process.stdin.close()
                            process.wait()
                        lease.retire()
                        lease.close()

    def test_process_only_negative_control_leaves_a_real_disconnected_mount(self):
        with tempfile.TemporaryDirectory() as root:
            lease = MountLease(root)
            try:
                code, output, error = finish(host(Path(root), leased=False))
                self.assertEqual(code, 0, error)
                self.assertIn("mounted", json.loads(output))
                self.assertEqual(len(lease.mounts()), 1)
                with self.assertRaises(OSError) as failure:
                    os.stat(Path(root) / "mounted space\ttab")
                self.assertEqual(failure.exception.errno, errno.ENOTCONN)
            finally:
                lease.retire()
                lease.close()

    def test_exit_and_cancellation_retire_mounts_before_scope_completion(self):
        for mode in ["exit", "wait"]:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as root:
                lease = MountLease(root)
                process = host(Path(root), mode)
                try:
                    if mode == "wait":
                        self.assertIn("mounted", json.loads(process.stdout.readline()))
                        self.assertEqual(len(lease.mounts()), 1)
                        process.stdin.close()
                    code, _, error = finish(process)
                    self.assertEqual(code, 0 if mode == "exit" else 124, error)
                    self.assertEqual(lease.mounts(), [])
                    self.assertTrue((Path(root) / "mounted space\ttab").is_dir())
                finally:
                    if process.poll() is None:
                        process.stdin.close()
                        process.wait()
                    lease.retire()
                    lease.close()

    def test_fixture_lease_cannot_retire_a_prefix_siblings_mount(self):
        with tempfile.TemporaryDirectory() as parent:
            root, sibling = Path(parent) / "fixture", Path(parent) / "fixture-other"
            root.mkdir()
            sibling.mkdir()
            peer = MountLease(sibling)
            try:
                code, _, error = finish(host(sibling, leased=False))
                self.assertEqual(code, 0, error)
                code, _, error = finish(host(root))
                self.assertEqual(code, 0, error)
                self.assertEqual(len(peer.mounts()), 1, "unrelated mount was retired")
            finally:
                peer.retire()
                peer.close()

    def test_existing_mount_and_failed_retirement_cannot_be_reported_as_closed(self):
        with tempfile.TemporaryDirectory() as root:
            lease = MountLease(root)
            try:
                code, _, error = finish(host(Path(root), leased=False))
                self.assertEqual(code, 0, error)
                with self.assertRaisesRegex(RuntimeError, "fresh and unmounted"):
                    MountLease(root)
                class RefusedKernel:
                    def umount2(self, path, flags):
                        ctypes.set_errno(errno.EBUSY)
                        return -1
                with patch("owned_mounts.ctypes.CDLL", return_value=RefusedKernel()):
                    with self.assertRaises(OSError) as failure:
                        lease.retire()
                    self.assertEqual(failure.exception.errno, errno.EBUSY)
                self.assertEqual(len(lease.mounts()), 1)
            finally:
                lease.retire()
                lease.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
