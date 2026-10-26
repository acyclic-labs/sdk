"""Real-kernel controls for per-job caps (scripts/buildbox/job_caps.py).

A bounded task loop in one admitted lease stops at the lease's root-owned
ceiling and the job fails alone with a typed refusal, while a concurrent lease
finishes. The pinned former adapter is the negative control: the same loop
passes the ceiling and stops only at the test's own small bound.
"""
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import job_caps
from admission_policy import CPU_LEASE_SHAPE

CPUS = CPU_LEASE_SHAPE[1]
BOUND = job_caps.TASKS_MAX+256


def lease_root():
    path = Path('/sys/fs/cgroup'+Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1])
    while not path.name.startswith('run-'): path = path.parent
    return path


def worker(mode, signals):
    signals = Path(signals)
    if mode == 'peer':
        for _ in range(32): subprocess.run(['true'], check=True)
        return 0
    run = lease_root()
    stop = threading.Event(); threads = []; refused = None; fork_errno = None
    while len(threads) < BOUND:
        thread = threading.Thread(target=stop.wait, daemon=True)
        try: thread.start()
        except RuntimeError: refused = 'thread'; break
        threads.append(thread)
    if refused:
        try:
            pid = os.fork()
            if pid == 0: os._exit(0)
            os.waitpid(pid, 0)
        except BlockingIOError as error: fork_errno = error.errno
    caps = {name: (run/name).read_text().strip() for name in ('pids.max', 'memory.max')}
    (signals/'saturated.json').write_text(json.dumps({'tasks': len(threads)+1, 'refused': refused, 'fork_errno': fork_errno, 'caps': caps}))
    deadline = time.monotonic()+8
    while not (signals/'release').exists() and time.monotonic() < deadline: time.sleep(0.02)
    stop.set()
    for thread in threads: thread.join()
    # Memory: a delegated child below the lease shares the same refusal path.
    probe = run/'work'/'memory-probe'; probe.mkdir()
    try:
        job_caps.write(probe, {'pids.max': 64, 'memory.max': 32 << 20})
        before = job_caps.events(probe)
        child = subprocess.run([sys.executable, '-c', 'x=b"x"*(128<<20)'],
                               preexec_fn=lambda: (probe/'cgroup.procs').write_text('0'))
        (signals/'memory.json').write_text(json.dumps({'rc': child.returncode, 'refusals': job_caps.exceeded(probe, before)}))
    finally: probe.rmdir()
    return 0


class JobCaps(unittest.TestCase):
    def test_limits_are_the_approved_interim_ceilings(self):
        for cpus in (1, 16):
            self.assertEqual(job_caps.limits(cpus), {'pids.max': 16384, 'memory.max': 160 << 30})
        with self.assertRaises(ValueError): job_caps.limits(0)

    def loop(self, source):
        from cpu_test_fixture import KernelFixture
        with tempfile.TemporaryDirectory() as signals, KernelFixture(source) as fixture:
            os.chmod(signals, 0o777)
            capped = fixture.bootstrap([__file__, 'worker', 'loop', signals], index=0, popen=True,
                                       stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic()+8
            while not Path(signals, 'saturated.json').exists():
                self.assertIsNone(capped.poll(), capped.stderr.read() if capped.poll() is not None else '')
                self.assertLess(time.monotonic(), deadline); time.sleep(0.02)
            peer = fixture.bootstrap([__file__, 'worker', 'peer', signals], index=1)
            Path(signals, 'release').touch()
            _, stderr = capped.communicate(timeout=8)
            return (json.loads(Path(signals, 'saturated.json').read_text()), peer, capped.returncode, stderr,
                    json.loads(Path(signals, 'memory.json').read_text()))

    def test_task_loop_is_capped_alone_and_former_adapter_is_not(self):
        caps = job_caps.limits(CPUS)
        loop, peer, rc, stderr, memory = self.loop(HERE)
        self.assertEqual(loop['caps'], {name: str(value) for name, value in caps.items()})
        self.assertEqual((loop['refused'], loop['fork_errno']), ('thread', errno.EAGAIN))
        self.assertLessEqual(loop['tasks'], caps['pids.max'])
        self.assertEqual(peer, 0, 'concurrent lease must finish while its neighbour is at the ceiling')
        self.assertNotEqual(rc, 0, stderr)
        self.assertIn('BB_CAP_EXCEEDED kind=TASKS_MAX cap=pids.max='+str(caps['pids.max']), stderr)
        self.assertIn('BB_CAP_EXCEEDED kind=MEMORY_MAX', stderr)
        self.assertIn('recovery=', stderr)
        self.assertEqual(memory['rc'], -9)
        self.assertTrue(memory['refusals'] and memory['refusals'][0].startswith('BB_CAP_EXCEEDED kind=MEMORY_MAX cap=memory.max=33554432'))
        # Negative control: the pinned uncapped adapter lets the same loop pass
        # the ceiling; it stops only at this test's bound.
        fixture = HERE/'fixtures/job-caps'
        origin = json.loads((fixture/'origin.json').read_text())
        former = fixture/'cpu_leases.former.py'
        self.assertEqual(hashlib.sha256(former.read_bytes()).hexdigest(), origin['sha256'])
        with tempfile.TemporaryDirectory() as bundle:
            for item in HERE.glob('*.py'): shutil.copyfile(item, Path(bundle)/item.name)
            shutil.copyfile(former, Path(bundle)/'cpu_leases.py'); os.chmod(bundle, 0o755)
            loop, peer, rc, stderr, _ = self.loop(Path(bundle))
        self.assertIsNone(loop['refused'])
        self.assertEqual(loop['tasks'], BOUND+1)
        self.assertGreater(loop['tasks'], caps['pids.max'])
        self.assertEqual((peer, rc), (0, 0), stderr)
        self.assertNotIn('BB_CAP_EXCEEDED kind=TASKS_MAX', stderr)


if __name__ == '__main__':
    if sys.argv[1:2] == ['worker']: raise SystemExit(worker(*sys.argv[2:4]))
    unittest.main()
