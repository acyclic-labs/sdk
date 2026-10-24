import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('gate_queue', HERE/'gate-queue.py')
queue = importlib.util.module_from_spec(spec); spec.loader.exec_module(queue)


class QueueTests(unittest.TestCase):
    def test_concurrent_producers_are_a_set_without_lost_peers(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            (state/'pending.txt').write_text('1\n1\n2\n')
            jobs = [subprocess.Popen([sys.executable, str(HERE/'gate-queue.py'), tmp, 'add', 'pending', str(p)])
                    for p in [*range(1, 17), *range(1, 9)]]
            self.assertTrue(all(job.wait() == 0 for job in jobs))
            self.assertEqual((state/'pending.txt').read_text().split(), list(map(str, range(1, 17))))
            # The former append mechanism represents duplicates even when
            # there are no concurrent writers; membership is not a set.
            with (state/'pending.txt').open('a') as old:
                old.write('1\n')
            self.assertNotEqual((state/'pending.txt').read_text().split(), list(map(str, range(1, 17))))

    def test_take_and_baseline_transfer_share_the_same_set_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            queue.add(state, 'pending', [2, 1, 2])
            queue.add(state, 'held-pending', [1, 3])
            queue.add(state, 'baseline-pending', [3, 4])
            def call(*args):
                return subprocess.run([sys.executable, str(HERE/'gate-queue.py'), tmp, *args], capture_output=True, text=True)
            self.assertEqual(call('move', 'baseline-pending', 'pending').returncode, 0)
            taken = call('take', 'pending', 'held-pending')
            self.assertEqual(taken.returncode, 0, taken.stderr)
            self.assertEqual(taken.stdout.split(), ['1', '2', '3', '4'])
            self.assertEqual(call('has', 'pending').returncode, 1)
            self.assertEqual((state/'held-pending.txt').read_text(), '')

    def test_bad_queue_is_infra_and_does_not_erase_other_members(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            queue.add(state, 'pending', [1, 2])
            (state/'held-pending.txt').write_text('not-a-pr\n')
            run = subprocess.run([sys.executable, str(HERE/'gate-queue.py'), tmp, 'take', 'pending', 'held-pending'], capture_output=True, text=True)
            self.assertEqual(run.returncode, 6)
            self.assertIn('preparation-INFRA', run.stderr)
            self.assertEqual(queue.members(state/'pending.txt'), [1, 2])

    def test_every_queue_writer_uses_the_shared_transaction(self):
        for name in ['gate-drain.sh', 'gate-merge.py', 'merge-bg.sh', 'main-gate.sh']:
            text = (HERE/name).read_text()
            self.assertIn('gate-queue.py', text, name)
            self.assertNotRegex(text, r'>>\s*["\w$:/.-]*pending\.txt', name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
