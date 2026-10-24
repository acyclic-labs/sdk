"""Exercise PID 1 adoption in real namespaces, without touching runtime fixtures."""
import importlib.util
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


def probe(source, mode, outcome, record):
    sys.path.insert(0, str(Path(source).parent))
    spec = importlib.util.spec_from_file_location('owner', source)
    owner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owner)
    assert os.getpid() == 1
    if outcome == 'signal-race':
        real_waitid = os.waitid
        child = []
        def racing_waitid(*args):
            try:event = real_waitid(*args)
            except ChildProcessError:event = None
            if event is None and not child:
                mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGCHLD})
                pid = os.fork()
                if pid == 0:os._exit(0)
                child.append(pid)
                # Force an exit after the reaper's last empty observation,
                # but before its draining guard is released.
                real_waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT)
                signal.sigwait({signal.SIGCHLD})
                signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                os.kill(os.getpid(), signal.SIGCHLD)
            return event
        owner.os.waitid = racing_waitid
        try:
            with owner.adopted_child_reaper(-1):
                remaining = int(Path(f'/proc/{child[0]}').exists())
            print(json.dumps({'remaining_during_wait': remaining}), flush=True)
        finally:
            owner.os.waitid = real_waitid
        return
    if outcome == 'ignored-handler':
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        try:
            with owner.adopted_child_reaper(-1):
                raise AssertionError('ignored child statuses accepted')
        except RuntimeError as error:
            assert 'already owned' in str(error)
            print(json.dumps({'refused_ignored_handler': True}), flush=True)
        finally:
            signal.signal(signal.SIGCHLD, signal.SIG_DFL)
        return
    if outcome == 'supervisor-killed':
        command = owner.OwnedCommand([sys.executable, '-B', '-c', 'import signal;signal.pause()'],
                                     dict(os.environ), lambda identity: None)
        try:
            os.kill(command.child.pid, signal.SIGKILL)
            try:
                command.wait()
                raise AssertionError('missing command outcome was accepted')
            except RuntimeError as error:
                assert 'without an outcome' in str(error)
            assert command.child.wait() == -signal.SIGKILL
            print(json.dumps({'status': -signal.SIGKILL, 'missing_outcome': True}), flush=True)
        finally:
            command.close()
        return
    # The command reaps its direct children. Its double-forked children are
    # deliberately adopted by namespace init, exactly as daemonizing tools are.
    payload = '''import os,select,signal,sys,json
from pathlib import Path
r,w=os.pipe()
for _ in range(32):
    pid=os.fork()
    if pid==0:
        os.close(r)
        if os.fork()==0:
            os.write(w,(str(os.getpid())+'\\n').encode());os._exit(0)
        os._exit(0)
    os.waitpid(pid,0)
os.close(w)
with os.fdopen(r) as stream:pids=[int(line) for line in stream]
for pid in pids:
    try:fd=os.pidfd_open(pid)
    except ProcessLookupError:continue
    try:select.select([fd],[],[])
    finally:os.close(fd)
Path(sys.argv[1]).write_text(json.dumps(pids))
if sys.argv[2]=='signal':os.kill(os.getpid(),signal.SIGTERM)
raise SystemExit(int(sys.argv[2]))
'''
    command = owner.OwnedCommand([sys.executable, '-B', '-c', payload, record, outcome],
                                 dict(os.environ), lambda identity: None)
    try:
        status = command.wait()
        pids = json.loads(Path(record).read_text())
        remaining = [pid for pid in pids if Path(f'/proc/{pid}').exists()]
        # The live custody supervisor must not be reaped or have its result
        # stolen by the adoption handler. Check before closing that custody.
        assert command.child.poll() is None
        print(json.dumps({'status': status, 'remaining': len(remaining),
                          'adopted': len(pids), 'mode': mode}), flush=True)
    finally:
        command.close()


class ReapingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='bb-reaping-', dir='/mnt/bb/tmp')
        self.directory = Path(self.temp.name)
        for name in ['bb-job.py', 'bb_target_store.py', 'admission_policy.py']:
            shutil.copyfile(HERE/name, self.directory/name)
        original = (self.directory/'bb-job.py').read_text()
        old = original.replace('        with adopted_child_reaper(self.child.pid):\n'
                               '            line = self.status.readline()',
                               '        line = self.status.readline()')
        self.assertNotEqual(original, old)
        (self.directory/'old.py').write_text(old)
        racy = original.replace('        old_mask=signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGCHLD})\n', '')
        racy = racy.replace('            signal.pthread_sigmask(signal.SIG_SETMASK,old_mask)\n', '')
        self.assertNotEqual(original, racy)
        (self.directory/'racy.py').write_text(racy)

    def tearDown(self):
        self.temp.cleanup()

    def run_probe(self, outcome, negative=False, racy=False):
        source = self.directory/('racy.py' if racy else 'old.py' if negative else 'bb-job.py')
        command = ['sudo', '-n', 'unshare', '--mount', '--pid', '--fork', '--kill-child',
                   '--mount-proc', 'setpriv', '--reuid=var', '--regid=var', '--init-groups',
                   sys.executable, '-B', str(Path(__file__).resolve()), '--probe', str(source),
                   'old' if negative else 'fixed', outcome, str(self.directory/'children.json')]
        result = subprocess.run(command, capture_output=True, text=True, timeout=6)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_adopted_children_are_reaped_while_command_custody_remains_live(self):
        for outcome, expected in [('0', 0), ('17', 17), ('signal', -signal.SIGTERM)]:
            with self.subTest(outcome=outcome):
                result = self.run_probe(outcome)
                self.assertEqual(result['adopted'], 32)
                self.assertEqual(result['remaining'], 0)
                self.assertEqual(result['status'], expected)
        negative = self.run_probe('17', negative=True)
        self.assertEqual(negative['remaining'], 32)
        self.assertEqual(negative['status'], 17)

    def test_unexpected_supervisor_death_retains_managed_status_and_missing_outcome(self):
        result = self.run_probe('supervisor-killed')
        self.assertEqual(result, {'status': -signal.SIGKILL, 'missing_outcome': True})

    def test_exit_during_last_empty_observation_cannot_lose_its_notification(self):
        self.assertEqual(self.run_probe('signal-race')['remaining_during_wait'], 0)
        self.assertEqual(self.run_probe('signal-race', racy=True)['remaining_during_wait'], 1)

    def test_ignored_sigchld_cannot_silently_destroy_managed_status(self):
        self.assertEqual(self.run_probe('ignored-handler'), {'refused_ignored_handler': True})


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--probe':
        probe(*sys.argv[2:])
    else:
        unittest.main()
