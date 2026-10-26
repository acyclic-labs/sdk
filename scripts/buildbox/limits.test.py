"""Caller rlimit parity across the real scope, daemon and namespace boundaries."""
import json
import hashlib
import os
from pathlib import Path
import resource
import signal
import select
import subprocess
import sys
import tempfile
import unittest
# Every payload in this suite is inline code or a fixture it writes itself;
# declare that empty closure explicitly instead of inheriting the enclosing job's.
import input_closure
input_closure.declare(os.environ)

from admission_policy import capture_limits, parse_limits, LIMIT_ENV

HERE = Path(__file__).resolve().parent
PROBE = 'import json,resource;print(json.dumps({k:list(resource.getrlimit(v)) for k,v in vars(resource).items() if k.startswith("RLIMIT_") and k!="RLIMIT_OFILE"}))'


class LimitTests(unittest.TestCase):
    def setUp(self):
        from cpu_test_fixture import KernelFixture, stage_worker_bundle
        self.fixture=KernelFixture()
        self.stage_worker_bundle=stage_worker_bundle

    def tearDown(self):self.fixture.close()
    def test_generated_watcher_joins_exact_owner_exit_and_actual_old_sleep_retains_the_lease(self):
        source=(HERE/'buildbox.mjs').read_text()
        current=source[source.index('p_birth=$(python3'):source.index('# Last use for bb-target-prune')]
        former=HERE/'fixtures/cpu-leases/former-watcher.txt'
        info=json.loads(former.with_name('former-watcher.origin.json').read_text())
        self.assertEqual(hashlib.sha256(former.read_bytes()).hexdigest(),info['sha256'])
        for negative in [False,True]:
            with self.subTest(negative=negative), tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
                root=Path(temporary);fifo=root/'release';os.mkfifo(fifo)
                release=os.open(fifo,os.O_RDWR)
                block=former.read_text() if negative else current
                block=block.replace("${idleOk ? 'false' : 'true'}",'false').replace('${maxSec}','10800').replace('${Math.round(maxSec / 60)}','180').replace('\\${','${')
                block=block.replace('/home/var/bin/admission_policy.py',str(HERE/'admission_policy.py'))
                if negative:block=block.replace('do sleep 30','do echo WATCH_READY; sleep 30')
                # Instrument only the sampling boundary. The production
                # helper still receives its actual 30-second sample interval.
                driver='''python3(){
 if [ "${2:-}" = watch-wait ] && [ "${5:-}" = 30 ]; then echo WATCH_READY; fi
 command python3 "$@"
}
'''+f'J={root}/job;touch "$J.hb"\n'+f'{sys.executable} -c '+__import__('shlex').quote('import sys;print("OWNER_READY",flush=True);sys.stdin.buffer.read(1)')+f' < {fifo} &\np=$!\n'+block+'\necho WATCHER_JOINED\nexit "$rc"\n'
                process=subprocess.Popen(['bash','-c',driver],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
                try:
                    while True:
                        self.assertTrue(select.select([process.stdout],[],[],3)[0],'watcher did not reach real sampling boundary')
                        line=process.stdout.readline()
                        self.assertTrue(line,'watcher exited before sampling')
                        if line==b'WATCH_READY\n':break
                    os.write(release,b'x')
                    if negative:
                        with self.assertRaises(subprocess.TimeoutExpired):process.communicate(timeout=.1)
                        os.killpg(process.pid,signal.SIGKILL)
                        process.communicate(timeout=3)
                    else:
                        output,error=process.communicate(timeout=3)
                        self.assertEqual(process.returncode,0,error.decode())
                        self.assertIn(b'WATCHER_JOINED',output)
                finally:
                    os.close(release)
                    if process.poll() is None:os.killpg(process.pid,signal.SIGKILL);process.communicate(timeout=3)
    def test_public_entries_capture_before_nodes_libuv_and_nested_entry_recaptures(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);(root/'jobs').mkdir()
            self.stage_worker_bundle(root)
            names=['build-slot','bb-admit','bb-admit.py','bb-job.py','admission_policy.py',
                   'bb_target_store.py','job_exports.py','buildbox-receipt.mjs']
            for name in names:
                data=(HERE/name).read_text().replace('/home/var/bin/',str(root)+'/')
                if name=='bb-job.py':data=data.replace("JOBS=Path('/mnt/bb/jobs')",f"JOBS=Path({str(root/'jobs')!r})")
                if name=='buildbox-receipt.mjs':data=data.replace('/mnt/bb/jobs',str(root/'jobs'))
                (root/name).write_text(data);(root/name).chmod(0o755)
            self.fixture.copy_adapter(root/'cpu_leases.py')
            env={**os.environ,'BB_ADMIT_ROOT':str(root/'queue'),'BB_CACHE_DISABLED':'1','BUILD_SLOT_EXEC_IN_PAYLOAD':'1','BUILD_SLOT_TENANT':'cloud'}
            for key in [LIMIT_ENV,'BB_CALLER_RLIMITS_READY','BB_ADMITTED','BB_JOB_ACTIVE',
                        'BB_ADMIT_WRAPPED_COMMAND','BUILD_SLOT_RECEIPT_META','BUILD_SLOT_RECEIPT_WRAPPED']:
                env.pop(key,None)
            limits=capture_limits();limits['RLIMIT_NOFILE']=[1536,8192]
            nested=('import resource,subprocess,json;resource.setrlimit(resource.RLIMIT_NOFILE,(2048,8192));'
                    f'r=subprocess.run([{str(root/"build-slot")!r},"python3","-c",{PROBE!r}],capture_output=True);'
                    'assert r.returncode==0,r.stderr;assert json.loads(r.stdout)["RLIMIT_NOFILE"]==[2048,8192],r.stdout;')
            code=PROBE+';'+nested
            def invoke(entry):
                argv=[str(root/entry)]+(['lane','cloud','--'] if entry=='bb-admit' else [])
                return subprocess.run([*argv,'python3','-c',code],cwd=root,env=env,capture_output=True,
                                      preexec_fn=lambda:resource.setrlimit(resource.RLIMIT_NOFILE,(1536,8192)))
            try:
                for entry in ['build-slot','bb-admit']:
                    good=invoke(entry)
                    self.assertEqual(good.returncode,0,good.stderr.decode())
                    self.assertEqual(json.loads(good.stdout),limits)
                    file=root/entry;original=file.read_text()
                    file.write_text(original.replace('export BB_CALLER_RLIMITS_JSON BB_CALLER_RLIMITS_READY=1',
                                                     ': # negative control: Node starts before captured limits are exported'))
                    bad=invoke(entry)
                    # libuv changes the soft limit even when no systemd scope
                    # is involved. Without the public-entry capture, parity
                    # must fail (or nested parity must reject the command).
                    self.assertTrue(bad.returncode!=0 or json.loads(bad.stdout)!=limits)
                    file.write_text(original)
            finally:
                state=root/'queue/daemon.json'
                if state.exists():os.kill(json.loads(state.read_text())['pid'],signal.SIGTERM)

    def scoped(self, limits, restore=True):
        # Print the exact requested shell control too, rather than comparing
        # only Python's view of the nofile limits.
        identity='import os,pwd;u=pwd.getpwuid(os.getuid());assert os.environ["USER"]==os.environ["LOGNAME"]==u.pw_name;'
        probe = 'ulimit -Sn; ulimit -Hn; exec '+sys.executable+' -c '+__import__('shlex').quote(identity+PROBE)
        # CPU admission replaces the systemd scope; exercise that one kernel
        # constructor. The actual former PAM transition is the negative.
        child=['bash','-c',probe]
        if not restore:child=['sudo','-n','/usr/sbin/runuser','-u','var','--',*child]
        return subprocess.run(['sudo','-n','--preserve-env','/usr/bin/python3','-B',str(self.fixture.module),
                               'exec','0',__import__('uuid').uuid4().hex,'graphcoder','--',*child],
                              env=self.fixture.environment({LIMIT_ENV:json.dumps(limits)}), capture_output=True,
                              preexec_fn=lambda: [resource.setrlimit(getattr(resource,name), pair) for name,pair in limits.items()])

    def assert_parity(self, result, limits):
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        soft, hard, rest = result.stdout.decode().split('\n', 2)
        shell = lambda n: 'unlimited' if n == resource.RLIM_INFINITY else str(n)
        self.assertEqual([soft, hard], [shell(x) for x in limits['RLIMIT_NOFILE']])
        self.assertEqual(json.loads(rest), limits)

    def test_real_kernel_scope_matches_caller_and_old_pam_negative(self):
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
            self.stage_worker_bundle(root)
            # Production entry/daemon/runtime, with only their fixture receipt
            # and namespace registry roots redirected outside shared JOBS.
            for name in ['admission_policy.py','bb_target_store.py','job_exports.py']:
                (root/name).write_bytes((HERE/name).read_bytes())
            self.fixture.copy_adapter(root/'cpu_leases.py')
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
                    self.assertNotEqual(result.returncode,0,result.stderr.decode())
                    self.assertIn(b'caller-rlimits',result.stderr)
            finally:
                for queue in ['queue','negative-queue']:
                    state = root/queue/'daemon.json'
                    if state.exists(): os.kill(json.loads(state.read_text())['pid'], signal.SIGTERM)

    def test_limits_schema_refuses_missing_extra_invalid_or_inverted(self):
        limits = capture_limits()
        self.assertEqual(parse_limits(json.dumps(limits)), limits)
        for bad in [{}, {**limits,'unknown':[0,0]}, {**limits,'RLIMIT_NOFILE':[True,8192]},
                    {**limits,'RLIMIT_NOFILE':[8192,4096]}, {**limits,'RLIMIT_NOFILE':[-2,-1]},
                    {**limits,'RLIMIT_NOFILE':[2**100,2**100]}]:
            with self.assertRaisesRegex(RuntimeError, 'RLIMIT_INVALID admission-INFRA'): parse_limits(json.dumps(bad))
        bad={**limits,'RLIMIT_NOFILE':[2**100,2**100]}
        rejected=subprocess.run([sys.executable,str(HERE/'admission_policy.py'),'exec-user','var','--',
                                 'echo','MUST_NOT_EXECUTE'],env={**os.environ,LIMIT_ENV:json.dumps(bad)},capture_output=True)
        self.assertEqual(rejected.returncode,224,rejected.stderr.decode());self.assertEqual(rejected.stdout,b'')
        self.assertIn(b'RLIMIT_INVALID admission-INFRA',rejected.stderr)
        # The old unchecked conversion escaped as OverflowError / exit 1.
        with self.assertRaises(OverflowError):resource.setrlimit(resource.RLIMIT_NOFILE,[2**100,2**100])


if __name__ == '__main__': unittest.main()
