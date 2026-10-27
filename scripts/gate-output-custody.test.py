import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
import unittest

helper = Path(sys.argv.pop(1)).resolve() if len(sys.argv) > 1 else Path(__file__).with_name('gate-target-cache.py')
spec = importlib.util.spec_from_file_location('gate_output_custody', helper)
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
fixture = Path(os.environ.get('SDK_BUILD_TOOLS_DIR', '/home/var/lib/sdk-build-tools')) / 'coordinator/fixtures/e2e1495-clippy.txt'
samples = fixture.read_bytes().splitlines(keepends=True)


class GateOutputCustody(unittest.TestCase):
    def test_staged_runner_is_owned_by_its_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)/'runner.sh'
            original = Path(__file__).with_name('gate-test-runner.sh').read_bytes()
            source.write_bytes(original); source.chmod(0o600)
            first = gate.executable_runner(source)
            source.write_bytes(original+b'\n# controlled successor identity\n')
            second = gate.executable_runner(source)
            self.assertNotEqual(first, second)
            self.assertEqual(first.read_bytes(), original)
            self.assertEqual(first.stat().st_mode & 0o777, 0o700)
            self.assertNotEqual(second.read_bytes(), original)

    def test_nested_real_helper_cannot_replace_outer_diagnostics(self):
        outer = gate.output_directory()
        witness = outer / '.gate-clippy.out'
        witness.write_bytes(samples[0])
        child = subprocess.run([sys.executable, '-B', str(helper), 'output'],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        nested = Path(child.stdout.decode().strip())
        (nested / '.gate-clippy.out').write_bytes(samples[1])
        self.assertNotEqual(outer, nested)
        self.assertEqual(witness.read_bytes(), samples[0])

    def test_same_cwd_concurrent_qualifiers_have_exclusive_output(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            outputs = list(pool.map(lambda _: gate.output_directory(), range(4)))
        self.assertEqual(len(set(outputs)), 4)
        for path in outputs:
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertTrue(path.is_relative_to(Path(os.environ['BB_JOB_HOME']).parent))
            self.assertFalse(path.is_relative_to(Path.cwd()))

    def test_non_job_output_also_has_exclusive_custody(self):
        env = {'TMPDIR': os.environ['TMPDIR']}
        first, second = gate.output_directory(env), gate.output_directory(env)
        self.assertNotEqual(first, second)
        self.assertNotEqual(first, Path.cwd())


if __name__ == '__main__':
    result = unittest.main(exit=False).result
    if not result.wasSuccessful():
        print('Gate output custody proof step failed: see assertion evidence; recovery command: '
              'python3 -B scripts/gate-output-custody.test.py', file=sys.stderr)
        sys.exit(1)
