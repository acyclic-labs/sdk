"""The Windows helper owns argv, never a transient worktree input pathname."""
import ast
import errno
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


def load_helper():
    source = (Path(__file__).parent / 'win-test.sh').read_text()
    source = source.split("<<'WIN_TEST_PY'\n", 1)[1].rsplit('\nWIN_TEST_PY', 1)[0]
    module = ast.parse(source)
    assert isinstance(module.body[-1], ast.Try)
    # Load the real factories without launching Windows main on the build box.
    module.body.pop()
    namespace = {'__name__': 'windows_input_fixture'}
    exec(compile(module, 'win-test.sh', 'exec'), namespace)
    return namespace


class ImmutableWindowsInputTests(unittest.TestCase):
    def test_payload_survives_producer_cleanup_and_owns_exact_config(self):
        helper = load_helper()
        helper['REMOTE'] = 'import json; print(json.dumps(CONFIG, ensure_ascii=False))'
        config = {'value': "quotes '\"; $() ` newline\n Unicode \u25cb", 'fixtures': ['a b']}
        expected = json.loads(json.dumps(config))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            helper['INPUT_ROOT'] = str(root)
            command, program = helper['remote_invocation'](config)
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            old_input = root / 'transient-job.py'
            old_input.write_bytes(program)
            old_command = [sys.executable, str(old_input)]
            old_input.unlink()
            # Former pathname ownership loses the program at producer cleanup.
            broken = subprocess.run(old_command, cwd=root, capture_output=True)
            self.assertNotEqual(broken.returncode, 0)
            self.assertIn(b'No such file or directory', broken.stderr)
            config['value'] = 'changed after dispatch'
            helper['REMOTE'] = 'raise RuntimeError("changed after dispatch")'
            result = subprocess.run(command, cwd=root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), expected)
            self.assertEqual(list(root.iterdir()), [])



    def test_program_size_never_enters_executor_argv_and_forged_input_is_refused(self):
        helper = load_helper()
        helper['REMOTE'] = '#' * (1024 * 1024) + '\nprint(CONFIG["value"])'
        with tempfile.TemporaryDirectory() as directory:
            helper['INPUT_ROOT'] = directory
            command, program = helper['remote_invocation']({'value': 'owned'})
            # Restore the former full-program argument: Linux refuses its size.
            with self.assertRaises(OSError) as failure:
                subprocess.run([sys.executable, '-c', program.decode()], capture_output=True)
            self.assertEqual(failure.exception.errno, errno.E2BIG)
            self.assertLess(sum(map(len, command)), 1024)
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), 'owned')
            self.assertEqual(list(Path(directory).iterdir()), [])
            staged = subprocess.run(helper['stage_invocation'](command), input=program, capture_output=True)
            self.assertEqual(staged.returncode, 0, staged.stderr)
            owned = Path(command[-2])
            owned.chmod(0o644)
            owned.write_text('raise RuntimeError("forged")')
            refused = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('Windows input digest mismatch', refused.stderr)

if __name__ == '__main__':
    unittest.main()
