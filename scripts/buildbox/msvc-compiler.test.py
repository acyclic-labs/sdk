import importlib.util
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import subprocess
import threading
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location("msvc_compiler", Path(__file__).with_name("msvc-compiler.py"))
compiler = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compiler)


class CompilerOwnershipTests(unittest.TestCase):
    def test_concurrent_invocations_own_and_release_identically_named_intermediates(self):
        rendezvous = threading.Barrier(2)
        scratch = []

        def run(command):
            self.assertEqual(command[:2], ["clang", "--driver-mode=cl"])
            boundary = command.index("--")
            option = command[boundary - 1]
            self.assertTrue(option.startswith("/Fa"), command)
            directory = Path(option[3:])
            scratch.append(directory)
            source = command[-1]
            intermediate = directory / "same.asm"
            intermediate.write_text(source)
            rendezvous.wait(timeout=2)
            self.assertEqual(intermediate.read_text(), source)
            self.assertNotEqual(scratch[0], scratch[1])
            rendezvous.wait(timeout=2)
            return subprocess.CompletedProcess(command, 7)

        with patch.object(compiler.subprocess, "run", side_effect=run):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(compiler.compile_owned, ["-c", "--", source])
                           for source in ["first/same.S", "second/same.S"]]
                self.assertEqual([future.result() for future in futures], [7, 7])
        self.assertTrue(all(not path.exists() for path in scratch))


if __name__ == "__main__":
    unittest.main()
