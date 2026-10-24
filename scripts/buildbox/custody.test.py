"""Real delegated-cgroup/daemon tests, executed only on bb2."""
import importlib.util
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import job_custody as custody

spec = importlib.util.spec_from_file_location('bb_job', HERE / 'bb-job.py')
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)


class CustodyTests(unittest.TestCase):
    def test_stop_job_refuses_foreign_or_missing_pid_namespace_before_signalling(self):
        with tempfile.TemporaryDirectory(dir=job.JOBS) as directory:
            record = Path(directory) / 'job.json'
            for namespace in [None, os.readlink('/proc/self/ns/pid') + '-foreign']:
                record.write_text(json.dumps({'pid': os.getpid(),
                                             'identity': job.identity(os.getpid()),
                                             'pid_namespace': namespace}))
                with patch.object(job.os, 'killpg') as signalled:
                    with self.assertRaisesRegex(RuntimeError, 'another or unknown PID namespace'):
                        job.stop_job(record)
                    signalled.assert_not_called()

    def test_real_cache_daemon_and_detached_writer_are_joined_before_both_deletions(self):
        for broken in [False, True]:
            record = custody.prepare()
            with tempfile.TemporaryDirectory(prefix='custody-', dir=job.JOBS) as directory:
                root = Path(directory)
                home = root.parent / ('job-' + root.name)
                home.mkdir()
                prefix = root.parent / ('custody-outer-' + root.name)
                for suffix in ['.targets', '.tmp']:
                    Path(str(prefix) + suffix).mkdir()
                # Real sccache daemonizes and closes inherited descriptors.
                endpoint = home / 'sccache.sock'
                env = {'PATH': os.environ['PATH'], 'HOME': str(home),
                       'SCCACHE_SERVER_UDS': str(endpoint),
                       'SCCACHE_DIR': str(home / 'cache')}
                started = subprocess.run(
                    [job.SCCACHE, '--start-server'], env=env, capture_output=True,
                    preexec_fn=lambda: custody.enter(record))
                self.assertEqual(started.returncode, 0, started.stderr)
                server = job.server_identity(endpoint)
                server_fd = os.pidfd_open(server['pid'])
                ready_r, ready_w = os.pipe()
                release_r, release_w = os.pipe()
                report_r, report_w = os.pipe()
                code = '''import os,sys
from pathlib import Path
if os.fork(): os._exit(0)
os.setsid()
if os.fork(): os._exit(0)
os.write(int(sys.argv[2]),str(os.getpid()).encode()+b'\\n')
os.read(int(sys.argv[3]),1)
try:
 Path(sys.argv[1]).joinpath('compiler-output').write_bytes(b'published')
 os.write(int(sys.argv[4]),b'published')
except FileNotFoundError: os.write(int(sys.argv[4]),b'deleted-while-alive')
'''
                writer = subprocess.Popen(
                    [sys.executable, '-c', code, str(home), str(ready_w), str(release_r), str(report_w)],
                    pass_fds=(ready_w, release_r, report_w),
                    preexec_fn=lambda: custody.enter(record))
                os.close(ready_w)
                os.close(release_r)
                os.close(report_w)
                writer.wait()
                pid = int(os.read(ready_r, 128))
                writer_fd = os.pidfd_open(pid)
                runtime = {'custody': record, 'hermetic_home': str(home), 'targets_policy': 'job-owned'}
                meta = root / 'runtime.json'
                meta.write_text(json.dumps(runtime))
                try:
                    if broken:
                        job.stop_server(server)
                        job.remove_home(home)
                        self.assertFalse(select.select([writer_fd], [], [], 0)[0])
                        os.write(release_w, b'x')
                        self.assertEqual(os.read(report_r, 128), b'deleted-while-alive')
                    else:
                        job.cleanup_runtime(meta, prefix)
                        self.assertEqual(json.loads(meta.read_text())['quiescence'], 'kernel-populated-zero')
                        self.assertTrue(select.select([writer_fd], [], [], 0)[0])
                        self.assertTrue(select.select([server_fd], [], [], 0)[0])
                        self.assertFalse(home.exists())
                        self.assertFalse(Path(str(prefix) + '.targets').exists())
                        self.assertFalse(Path(str(prefix) + '.tmp').exists())
                finally:
                    proof = custody.join(record)
                    job.cleanup_outer(prefix, proof, record)
                    if home.exists():
                        job.remove_home(home)
                    for fd in [ready_r, release_w, report_r, writer_fd, server_fd]:
                        os.close(fd)

    def test_cleanup_refuses_missing_or_changed_custody_before_deleting_any_files(self):
        record = custody.prepare()
        with self.assertRaises(ValueError):
            custody.join({**record, 'inode': record['inode'] + 1})
        with self.assertRaises(ValueError):
            custody.join({**record, 'path': '/sys/fs/cgroup'})
        with tempfile.TemporaryDirectory(prefix='job-custody-refusal-', dir=job.JOBS) as directory:
            root = Path(directory)
            meta = root / 'runtime.json'
            meta.write_text(json.dumps({'hermetic_home': str(root)}))
            for prefix in [None, str(root / 'outer')]:
                with self.assertRaises(KeyError):
                    job.cleanup_runtime(meta, prefix)
                self.assertTrue(root.exists())
        custody.join(record)


if __name__ == '__main__':
    unittest.main()
