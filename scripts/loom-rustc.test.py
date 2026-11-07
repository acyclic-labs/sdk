#!/usr/bin/env python3
"""Exercise the compiler wrapper's real arguments, descriptors and exit status."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest

SOURCE = Path(__file__).with_name("loom-rustc.py")


class CompilerHandoffTests(unittest.TestCase):
    def test_inherited_descriptor_arguments_identity_and_terminal_status(self):
        cases = [(0, ["argument with spaces", 'embedded"quote', "trailing\\"]),
                 (23, ["--cfg", 'feature="model-checking"'])]
        if os.name == "posix":
            cases.append((-signal.SIGTERM, []))
        for code, arguments in cases:
            with self.subTest(code=code):
                reader, writer = os.pipe()
                try:
                    os.write(writer, b"compiler-credit")
                finally:
                    os.close(writer)
                inherited = reader
                if os.name == "nt":
                    import msvcrt
                    inherited = msvcrt.get_osfhandle(reader)
                    os.set_handle_inheritable(inherited, True)
                try:
                    child = subprocess.Popen(
                        [sys.executable, str(SOURCE), sys.executable, str(Path(__file__).resolve()),
                         "--compiler", str(inherited), str(code), *arguments],
                        **({"pass_fds": (reader,)} if os.name == "posix" else {"close_fds": False}),
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True)
                    output, error = child.communicate()
                finally:
                    os.close(reader)
                self.assertEqual(child.returncode, code, error)
                observed = json.loads(output)
                if os.name == "posix":
                    self.assertEqual(observed["pid"], child.pid)
                self.assertEqual(observed["credit"], "compiler-credit")
                expected = [*arguments, "--cfg", "loom"] if "--cfg" in arguments else arguments
                self.assertEqual(observed["arguments"], expected)

    def test_launch_failure_names_the_recovery(self):
        result = subprocess.run([sys.executable, str(SOURCE), str(SOURCE / "missing-rustc")],
                                capture_output=True, text=True, check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("STEP_FAILED loom-rustc:", result.stderr)
        self.assertIn("recovery: restore the Rust toolchain", result.stderr)
        self.assertIn("node scripts/qualify-mcp-loom.mjs --gate", result.stderr)


if __name__ == "__main__":
    if sys.argv[1:2] == ["--compiler"]:
        reader = int(sys.argv[2])
        if os.name == "nt":
            import msvcrt
            reader = msvcrt.open_osfhandle(reader, os.O_RDONLY)
        print(json.dumps({"pid": os.getpid(),
                          "credit": os.read(reader, 32).decode(),
                          "arguments": sys.argv[4:]}), flush=True)
        code = int(sys.argv[3])
        if code < 0:
            os.kill(os.getpid(), -code)
        sys.exit(code)
    unittest.main()
