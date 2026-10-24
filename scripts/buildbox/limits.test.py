"""Caller rlimit parity across the real scope, daemon and namespace boundaries."""
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import tempfile
import unittest

from admission_policy import capture_limits, parse_limits, LIMIT_ENV

HERE = Path(__file__).resolve().parent
PROBE = 'import json,resource;print(json.dumps({k:list(resource.getrlimit(v)) for k,v in vars(resource).items() if k.startswith("RLIMIT_") and k!="RLIMIT_OFILE"}))'


class LimitTests(unittest.TestCase):
    def scoped(self, limits, restore=True):
        child = [sys.executable, str(HERE/'admission_policy.py'), 'exec-user', 'var', '--'] if restore else ['runuser', '-u', 'var', '--']
        # Print the exact requested shell control too, rather than comparing
        # only Python's view of the nofile limits.
        probe = 'ulimit -Sn; ulimit -Hn; exec '+sys.executable+' -c '+__import__('shlex').quote(PROBE)
        return subprocess.run(['sudo', '-n', '--preserve-env='+LIMIT_ENV, 'systemd-run', '--scope', '-q',
                               '--slice=bbjobs.slice', '--', *child, 'bash', '-c', probe],
                              env={**os.environ, LIMIT_ENV:json.dumps(limits)}, capture_output=True,
                              preexec_fn=lambda: [resource.setrlimit(getattr(resource,name), pair) for name,pair in limits.items()])

    def assert_parity(self, result, limits):
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        soft, hard, rest = result.stdout.decode().split('\n', 2)
        shell = lambda n: 'unlimited' if n == resource.RLIM_INFINITY else str(n)
        self.assertEqual([soft, hard], [shell(x) for x in limits['RLIMIT_NOFILE']])
        self.assertEqual(json.loads(rest), limits)

    def test_real_systemd_scope_matches_caller_and_old_path_negative(self):
        limits = capture_limits()
        self.assert_parity(self.scoped(limits), limits)
        # Scope+PAM used to replace caller limits, independent of our helper.
        changed = dict(limits)
        changed['RLIMIT_NOFILE'] = [1536, 8192]
        changed['RLIMIT_NPROC'] = [min(10000, limits['RLIMIT_NPROC'][0]), limits['RLIMIT_NPROC'][1]]
        changed['RLIMIT_MEMLOCK'] = [4096, limits['RLIMIT_MEMLOCK'][1]]
        changed['RLIMIT_STACK'] = [4*1024*1024, limits['RLIMIT_STACK'][1]]
        changed['RLIMIT_CORE'] = [0, limits['RLIMIT_CORE'][1]]
        self.assert_parity(self.scoped(changed), changed)
        with self.assertRaises(AssertionError): self.assert_parity(self.scoped(changed, False), changed)

    def test_native_ticket_captures_each_caller_not_daemon(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root = Path(temporary)
            # Production entry/daemon/runtime, with only their fixture receipt
            # and namespace registry roots redirected outside shared JOBS.
            for name in ['admission_policy.py','bb_target_store.py','job_exports.py']:
                (root/name).write_bytes((HERE/name).read_bytes())
            (root/'jobs').mkdir()
            source = (HERE/'bb-job.py').read_text().replace("JOBS=Path('/mnt/bb/jobs')", f"JOBS=Path({str(root/'jobs')!r})")
            (root/'bb-job.py').write_text(source)
            (root/'bb-admit.py').write_bytes((HERE/'bb-admit.py').read_bytes())
            env = {**os.environ, 'BB_ADMIT_ROOT':str(root/'queue'), 'BB_CACHE_DISABLED':'1', 'BUILD_SLOT_EXEC_IN_PAYLOAD':'1'}
            for name in ['BB_ADMIT_WRAPPED_COMMAND','BB_ADMITTED','BB_JOB_ACTIVE',LIMIT_ENV]: env.pop(name, None)
            try:
                for soft in [2048, 4096]:
                    meta = root/('meta-'+str(soft)); meta.write_text('{}')
                    payload = ('import resource,os,subprocess,sys,json;'
                               f'resource.setrlimit(resource.RLIMIT_NOFILE,({soft},8192));'
                               'from admission_policy import capture_limits;expected=capture_limits();'
                               f'command=[sys.executable,{str(root/"bb-admit.py")!r},"lane","cloud","--",sys.executable,"-c",{PROBE!r}];'
                               'r=subprocess.run(command,capture_output=True);'
                               'assert r.returncode==0,r.stderr;assert json.loads(r.stdout)==expected,(r.stdout,expected);print(r.stdout.decode(),end="")')
                    result = subprocess.run([sys.executable,'-c',payload], cwd=root,
                                            env={**env,'BUILD_SLOT_RECEIPT_META':str(meta)},capture_output=True)
                    self.assertEqual(result.returncode,0,result.stderr.decode())
                    self.assertEqual(json.loads(result.stdout)['RLIMIT_NOFILE'], [soft,8192])
                # Remove only ticket capture. With one shared daemon, the
                # second caller now gets the first caller's limits. This is
                # the production inheritance bug, rather than a mocked API.
                original = (root/'bb-admit.py').read_text()
                (root/'bb-admit.py').write_text(original.replace(
                    'os.environ[LIMIT_ENV] = json.dumps(capture_limits())',
                    'pass  # negative control: inherit daemon limits'))
                negative_env = {**env, 'BB_ADMIT_ROOT':str(root/'negative-queue')}
                for soft in [2048,4096]:
                    meta = root/('negative-meta-'+str(soft)); meta.write_text('{}')
                    # Keep the same code/probe and change the requested soft
                    # limit in its first statement only.
                    changed = payload.replace('RLIMIT_NOFILE,(4096,8192)', f'RLIMIT_NOFILE,({soft},8192)')
                    result = subprocess.run([sys.executable,'-c',changed], cwd=root,
                                            env={**negative_env,'BUILD_SLOT_RECEIPT_META':str(meta)},capture_output=True)
                    self.assertEqual(result.returncode,0 if soft==2048 else 1,result.stderr.decode())
                    if soft==4096:self.assertIn(b'AssertionError',result.stderr)
            finally:
                for queue in ['queue','negative-queue']:
                    state = root/queue/'daemon.json'
                    if state.exists(): os.kill(json.loads(state.read_text())['pid'], signal.SIGTERM)

    def test_limits_schema_refuses_missing_extra_invalid_or_inverted(self):
        limits = capture_limits()
        self.assertEqual(parse_limits(json.dumps(limits)), limits)
        for bad in [{}, {**limits,'unknown':[0,0]}, {**limits,'RLIMIT_NOFILE':[True,8192]},
                    {**limits,'RLIMIT_NOFILE':[8192,4096]}, {**limits,'RLIMIT_NOFILE':[-2,-1]}]:
            with self.assertRaisesRegex(RuntimeError, 'RLIMIT_INVALID admission-INFRA'): parse_limits(json.dumps(bad))


if __name__ == '__main__': unittest.main()
