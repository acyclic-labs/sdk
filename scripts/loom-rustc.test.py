#!/usr/bin/env python3
"""Exercise the POSIX compiler wrapper's real descriptor and process handoff."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest

SOURCE = Path(__file__).with_name("loom-rustc.sh")


class CompilerHandoffTests(unittest.TestCase):
    def test_inherited_descriptor_arguments_identity_and_terminal_status(self):
        for code, arguments in ((0, []), (23, ["--cfg", 'feature="model-checking"']),
                                (-signal.SIGTERM, [])):
            with self.subTest(code=code):
                reader, writer = os.pipe()
                try:
                    os.write(writer, b"compiler-credit")
                finally:
                    os.close(writer)
                try:
                    child = subprocess.Popen(
                        ["sh", str(SOURCE), sys.executable, str(Path(__file__).resolve()),
                         "--compiler", str(reader), str(code), *arguments],
                        pass_fds=(reader,), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True)
                    output, error = child.communicate()
                finally:
                    os.close(reader)
                self.assertEqual(child.returncode, code, error)
                observed = json.loads(output)
                self.assertEqual(observed["pid"], child.pid)
                self.assertEqual(observed["credit"], "compiler-credit")
                expected = [*arguments, "--cfg", "loom"] if arguments else []
                self.assertEqual(observed["arguments"], expected)

    def test_launch_failure_names_the_recovery(self):
        result = subprocess.run(["sh", str(SOURCE), str(SOURCE / "missing-rustc")],
                                capture_output=True, text=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STEP_FAILED loom-rustc:", result.stderr)
        self.assertIn("recovery: restore the Rust toolchain", result.stderr)
        self.assertIn("node scripts/qualify-mcp-loom.mjs --gate", result.stderr)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--compiler"]:
        print(json.dumps({"pid": os.getpid(), "credit": os.read(int(sys.argv[2]), 32).decode(),
                          "arguments": sys.argv[4:]}), flush=True)
        code = int(sys.argv[3])
        if code < 0:
            os.kill(os.getpid(), -code)
        sys.exit(code)
    unittest.main()
