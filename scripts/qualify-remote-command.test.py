#!/usr/bin/env python3
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).with_name("qualify-remote-command.py")
spec = importlib.util.spec_from_file_location("remote_command", SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class RemoteCommandTests(unittest.TestCase):
    def test_actual_command_failure_cannot_become_a_successful_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            for code in (0, 17):
                receipt = Path(directory) / f"{code}.json"
                command = [sys.executable, "-c", f"raise SystemExit({code})"]
                result = subprocess.run([sys.executable, str(SOURCE), "run",
                                         str(receipt), "owned-run", *command], check=False)
                self.assertEqual(result.returncode, code)
                record = json.loads(receipt.read_text())
                self.assertEqual(module.verify(record, "owned-run", command), code)
                verified = subprocess.run([sys.executable, str(SOURCE), "verify",
                                           str(receipt), "owned-run", *command], check=False)
                self.assertEqual(verified.returncode, code)
                if code:
                    # The observed former caller used SSH's false zero instead
                    # of this receipt. It fails exactly the outcome oracle.
                    with self.assertRaises(AssertionError):
                        self.assertEqual(0, module.verify(record, "owned-run", command))

    def test_only_the_exact_terminal_admission_is_accepted(self):
        command = ["owned-script"]
        record = {"schema": 1, "state": "completed", "nonce": "owned-run",
                  "command": command, "runner_sha256": module.runner_hash(), "exit_code": 0}
        self.assertEqual(module.verify(record, "owned-run", command), 0)
        for changed in ({"state": "running"}, {"nonce": "previous-run"},
                        {"command": ["another-script"]}, {"runner_sha256": "old-runner"},
                        {"exit_code": False}, {"schema": 2}):
            with self.assertRaises(ValueError):
                module.verify(record | changed, "owned-run", command)

    def test_existing_admission_cannot_execute_again(self):
        with tempfile.TemporaryDirectory() as directory:
            receipt = Path(directory) / "result.json"
            effect = Path(directory) / "effect"
            command = [sys.executable, "-c", f"open({str(effect)!r}, 'w').close()"]
            receipt.write_text('{"state":"running"}')
            with self.assertRaises(FileExistsError):
                module.run(receipt, "owned-run", command)
            self.assertFalse(effect.exists())


if __name__ == "__main__":
    unittest.main()
