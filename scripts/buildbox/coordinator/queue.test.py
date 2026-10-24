import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

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
            self.assertEqual(call('has', 'active').returncode, 0)
            # A concurrent producer is independent of the acknowledged claim.
            queue.add(state, 'pending', [5])
            self.assertEqual(call('ack', 'active', '1', '2', '3', '4').returncode, 0)
            self.assertEqual(call('has', 'active').returncode, 1)
            self.assertEqual(queue.members(state/'pending.txt'), [5])

    def test_partial_clear_and_killed_claim_remain_recoverable(self):
        original = (HERE/'gate-queue.py').read_text()
        old = original.replace('    publish(claim, prs)\n', '')
        self.assertNotEqual(original, old)
        for killed in [False, True]:
            for negative in [False, True]:
                with self.subTest(killed=killed, negative=negative), tempfile.TemporaryDirectory() as tmp:
                    state = Path(tmp)
                    queue.add(state, 'pending', [1])
                    queue.add(state, 'held-pending', [2])
                    if killed:
                        driver = '''import os,signal,sys
from pathlib import Path
namespace={'__name__':'claim_fixture'}
source=Path(sys.argv[1]).read_text()
if sys.argv[3]=='old':source=source.replace('    publish(claim, prs)\\n','')
exec(compile(source,'claim_fixture','exec'),namespace)
publish=namespace['publish']
def interrupted(path,prs):
 if path.name=='held-pending.txt' and not prs:
  print('SECOND_CLEAR',flush=True);os.kill(os.getpid(),signal.SIGSTOP)
 publish(path,prs)
namespace['publish']=interrupted
sys.argv=['gate-queue',sys.argv[2],'take','pending','held-pending']
raise SystemExit(namespace['main']())
'''
                        child = subprocess.Popen([sys.executable, '-B', '-c', driver,
                                                  str(HERE/'gate-queue.py'), tmp, 'old' if negative else 'new'],
                                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                        try:
                            self.assertEqual(child.stdout.readline().strip(), 'SECOND_CLEAR')
                        finally:
                            child.kill(); child.communicate()
                        self.assertEqual(child.returncode, -9)
                    else:
                        namespace = {'__name__':'claim_fixture'}
                        exec(compile(old if negative else original, 'claim_fixture', 'exec'), namespace)
                        publish = namespace['publish']
                        def failed(path, prs):
                            if path.name == 'held-pending.txt' and not prs:
                                raise OSError('second replacement refused')
                            publish(path, prs)
                        with queue.locked(state), patch.dict(namespace, publish=failed):
                            with self.assertRaises(OSError):
                                namespace['take'](state, ['pending', 'held-pending'])
                    # The actual former shape loses PR1 after clearing pending.
                    recoverable = {p for name in ['pending', 'held-pending', 'active']
                                   for p in queue.members(state/(name+'.txt'))}
                    self.assertEqual(recoverable, {2} if negative else {1, 2})
                    recovered = subprocess.run([sys.executable, '-B', str(HERE/'gate-queue.py'), tmp,
                                                'take', 'pending', 'held-pending'], capture_output=True, text=True)
                    self.assertEqual(recovered.returncode, 0, recovered.stderr)
                    self.assertEqual(recovered.stdout.split(), ['2'] if negative else ['1', '2'])

    def test_failed_delivery_and_acknowledgment_keep_custody(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            queue.add(state, 'pending', [1, 2])
            with queue.locked(state):
                taken = queue.take(state, ['pending'])
            with patch.object(sys, 'argv', ['gate-queue', tmp, 'take', 'pending']), \
                 patch.object(sys, 'stdout') as output, patch.object(sys, 'stderr'):
                output.writelines.side_effect = BrokenPipeError('claim consumer died')
                self.assertEqual(queue.main(), 6)
            self.assertEqual(queue.members(state/'active.txt'), taken)
            # A coordinator can die after receiving the list, before dispatch.
            with queue.locked(state):
                self.assertEqual(queue.take(state, ['pending']), taken)
                with patch.object(queue, 'publish', side_effect=OSError('ack refused')):
                    with self.assertRaises(OSError):
                        queue.acknowledge(state, taken)
            self.assertEqual(queue.members(state/'active.txt'), taken)
            with queue.locked(state):
                queue.acknowledge(state, [1])
            self.assertEqual(queue.members(state/'active.txt'), [2])

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
