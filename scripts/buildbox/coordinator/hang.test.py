import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('gate_hang', HERE/'gate-hang.py')
hang = importlib.util.module_from_spec(spec); spec.loader.exec_module(hang)
START = '     Running tests/identity.rs (/mnt/bb/targets/debug/deps/identity-abcd)\n'
SLOW = 'test identity_processes has been running for over 60 seconds\n'
KILLED = '[buildbox] hang reaper: the job used 394 ms of CPU in the last 10 min (minimum 5 s): presumed hung (deadlocked test or stuck wait); killed (rc 124), admission slot freed.\n'


class HangTests(unittest.TestCase):
    def test_idle_watchdog_names_the_last_unfinished_test(self):
        self.assertIn('hung test identity_processes in tests/identity.rs', hang.unfinished(START+SLOW+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+KILLED, 224))
        self.assertIsNone(hang.unfinished(START+'test done ... ok\n'+KILLED, 124))

    def test_finished_or_previous_binary_warnings_do_not_accuse_a_test(self):
        self.assertIsNone(hang.unfinished(START+SLOW+'test identity_processes ... ok\n'+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+'     Running tests/other.rs (/tmp/other)\n'+KILLED, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+'test identity_processes ... FAILED\n'+KILLED, 124))
        text = START+SLOW+'test second has been running for over 60 seconds\n'+KILLED
        self.assertIn('hung test second', hang.unfinished(text, 124))
        self.assertIn('second, identity_processes', hang.unfinished(text, 124))

    def test_late_completions_after_watchdog_are_read_before_any_verdict(self):
        source = (HERE/'gate-hang.py').read_text()
        old = {'__name__':'early_stop_control'}
        exec(compile(source.replace('watchdog = True', 'watchdog = True\n            break'), 'early-stop-control', 'exec'), old)
        for status in ['ok', 'FAILED', 'ignored']:
            with self.subTest(status=status):
                text = START+SLOW+KILLED+f'test identity_processes ... {status}\n'
                self.assertIsNone(hang.unfinished(text, 124))
                self.assertIn('hung test identity_processes', old['unfinished'](text, 124))
        self.assertIsNone(hang.unfinished(START+SLOW+KILLED+'     Running tests/next.rs (/tmp/next)\n', 124))

    def test_actual_coordinator_separates_source_hang_from_transport_loss(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('# An ended client stream'):source.index('# A successful payload')]
        block = block.replace('python Q:/lanes/gate-hang.py', f'"{sys.executable}" "{HERE / "gate-hang.py"}"')
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root/'.codex-fleet').mkdir()
            for text, rc, negative, expected in [
                (START+SLOW+KILLED, 124, False, 1),
                (START+SLOW, 124, False, 4),
                (START+SLOW+KILLED, 224, False, 4),
                (START+SLOW+'test identity_processes ... ok\n'+KILLED, 124, False, 4),
                (START+SLOW+KILLED+'test identity_processes ... ok\n', 124, False, 4),
                (START+SLOW+KILLED, 124, True, 4),
            ]:
                log = root/'gate.log'; log.write_text(text)
                actual = block
                if negative:
                    actual = actual.replace('if [ "$rc" = 124 ]; then', 'if false; then')
                run = subprocess.run(['bash', '-c', f'rc={rc};n=1;log="{log}"\n{actual}\nexit 0\n'],
                                     env={**os.environ, 'HOME':tmp}, capture_output=True, text=True)
                self.assertEqual(run.returncode, expected, run.stdout+run.stderr)
                self.assertIn('hung test identity_processes' if expected == 1 else 'INFRA', run.stdout)
                self.assertNotIn('GATE_DONE', log.read_text())


if __name__ == '__main__':
    unittest.main(verbosity=2)
