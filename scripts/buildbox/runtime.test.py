import contextlib
import fcntl
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import socket
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent

def load(name):
    spec=importlib.util.spec_from_file_location(name,HERE/(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
admit=load('bb-admit');job=load('bb-job');pruner=load('prune-wt-sdk');exports=load('job_exports')

class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='r19-runtime-',dir='/mnt/bb/tmp')
        self.d=Path(self.temp.name)
    def tearDown(self):self.temp.cleanup()
    def worker(self,negative=False,cleanup_failure=False):
        shutil.copyfile(HERE/'job_exports.py',self.d/'job_exports.py')
        source=(HERE/'bb-job.py').read_text()
        source=source.replace("real_home=Path('/home/var')",f'real_home=Path({str(self.d/"real-home")!r})')
        source=source.replace("'disk_free':disk()", "'disk_free':[{'path':'fixture','free_percent':50}]")
        if negative:source=source.replace("file=sys.stderr,flush=True", "flush=True")
        if cleanup_failure:source=source.replace('try:remove_home(job)','try:raise OSError("cleanup control")')
        p=self.d/('negative.py' if negative else 'worker.py');p.write_text(source);return p
    def run_job(self,argv,negative=False,extra=None,cleanup_failure=False):
        meta=self.d/'fixture.receipt-meta.v2.json';meta.write_text('{}')
        env={**os.environ,'PATH':'/home/var/.cargo/bin:'+os.environ['PATH'], 'BUILD_SLOT_RECEIPT_META':str(meta),'BUILD_SLOT_EXEC_IN_PAYLOAD':'1'}
        env.pop('BB_JOB_ACTIVE',None);env.pop('BB_ADMITTED',None)
        env.update(extra or {})
        result=subprocess.run([sys.executable,str(self.worker(negative,cleanup_failure)),*argv],cwd=self.d,env=env,capture_output=True)
        return result,json.loads(Path(str(meta)+'.runtime').read_text())
    def test_cargo_metadata_stdout_through_build_slot_and_negative_control(self):
        (self.d/'Cargo.toml').write_text('[package]\nname="gate-stdout-fixture"\nversion="0.0.0"\nedition="2024"\n')
        (self.d/'src').mkdir();(self.d/'src/lib.rs').write_text('')
        # Actual entrypoint; only installed helper locations/receipt ownership
        # are redirected to this isolated fixture. Cargo is the real tool.
        script=self.d/'build-slot';script.write_text((HERE/'build-slot').read_text().replace('/home/var/bin/bb-job.py',str(self.worker())))
        meta=self.d/'fixture.receipt-meta.v2.json';meta.write_text('{}')
        env={**os.environ,'BUILD_SLOT_RECEIPT_WRAPPED':'1','BB_ADMITTED':'1','BUILD_SLOT_RECEIPT_META':str(meta),'BUILD_SLOT_EXEC_IN_PAYLOAD':'1'}
        env.pop('BB_JOB_ACTIVE',None)
        args=['bash',str(script),'cargo','metadata','--format-version','1','--no-deps']
        result=subprocess.run(args,cwd=self.d,env=env,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr.decode());self.assertEqual(json.loads(result.stdout)['packages'][0]['name'],'gate-stdout-fixture')
        self.assertIn(b'BB_DISK_FREE',result.stderr);self.assertIn(b'GATE_HOME_GUARD',result.stderr)
        script.write_text(script.read_text().replace(str(self.d/'worker.py'),str(self.worker(True))))
        bad=subprocess.run(args,cwd=self.d,env=env,capture_output=True)
        self.assertEqual(bad.returncode,0,bad.stderr.decode())
        with self.assertRaises(json.JSONDecodeError):json.loads(bad.stdout)
    def test_stdout_bytes_modes_targets_and_xwin_cache(self):
        code='''import os,stat,sys
from pathlib import Path
for name in ['HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_RUNTIME_DIR','TMPDIR']:
 assert stat.S_IMODE(Path(os.environ[name]).stat().st_mode)==0o700,name
for name in ['CARGO_TARGET_DIR','GATE_MAIN_TARGET_DIR','GATE_FEATURE_TARGET_DIR','GATE_WINDOWS_RUNTIME_TARGET_DIR']:
 path=Path(os.environ[name]);path.mkdir(parents=True,exist_ok=True);(path/'object').write_bytes(b'compiled')
assert os.environ['XWIN_CACHE_DIR']=='/mnt/bb/toolchains/cargo-xwin'
sys.stdout.buffer.write(bytes(range(256)))
'''
        result,runtime=self.run_job([sys.executable,'-c',code])
        self.assertEqual(result.returncode,0,result.stderr.decode());self.assertEqual(result.stdout,bytes(range(256)))
        self.assertTrue(runtime['targets_removed']);self.assertFalse(Path(runtime['hermetic_home']).exists())
        self.assertIn(b'[buildbox]',result.stderr)
    def test_only_shared_sccache_is_excluded_and_other_real_state_still_fails(self):
        home=self.d/'real-home';(home/'.cache/sccache').mkdir(parents=True)
        (home/'.local/share/acyclic').mkdir(parents=True)
        before=job.inventory(home);(home/'.cache/sccache/shared-object').write_bytes(b'x')
        self.assertEqual(job.growth(before,job.inventory(home)),[])
        for rel in ['.cache/cargo-xwin/toolchain.cmake','.cache/other/object','.local/share/acyclic/harness/state']:
            p=home/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'leak')
            self.assertIn(str(p),job.growth(before,job.inventory(home)))
        result,runtime=self.run_job([sys.executable,'-c',f'from pathlib import Path;Path({str(home/".cache/real-leak")!r}).write_text("bad")'])
        self.assertEqual(result.returncode,2);self.assertEqual(result.stdout,b'');self.assertIn(b'GATE_HOME_LEAK',result.stderr)
        self.assertIn(str(home/'.cache/real-leak'),runtime['home_leaks'])
    def test_unreadable_directories_cleanup_negative_control(self):
        p=self.d/'job';(p/'moved/deep').mkdir(parents=True);(p/'moved/deep/data').write_bytes(b'x')
        (p/'moved').chmod(0)
        try:
            with self.assertRaises(PermissionError):shutil.rmtree(p)
            job.remove_home(p);self.assertFalse(p.exists())
        finally:
            if p.exists():job.remove_home(p)
    def test_outputs_are_archived_before_target_removal(self):
        prefix=Path('/mnt/bb/jobs')/('r19-export-'+str(os.getpid()))
        Path(str(prefix)+'.t0').touch()
        target=self.d/'target';target.mkdir();(target/'test.exe').write_bytes(b'executable')
        os.utime(target/'test.exe',(1,1)) # named Cargo outputs survive fresh builds too
        try:
            exports.prepare({'prefix':str(prefix),'cwd':str(self.d),'paths':['/work/target/test.exe'],'build':True},str(target))
            shutil.rmtree(target)
            manifest=json.loads(Path(str(prefix)+'.exports/manifest.json').read_text())
            import tarfile
            with tarfile.open(manifest[0]['archive']) as tar:self.assertEqual(tar.extractfile('test.exe').read(),b'executable')
        finally:
            Path(str(prefix)+'.t0').unlink();shutil.rmtree(str(prefix)+'.exports',ignore_errors=True)
    def test_failed_export_is_infra_and_does_not_contaminate_stdout(self):
        result,runtime=self.run_job([sys.executable,'-c','print("child output",end="")'],extra={'BB_JOB_EXPORT_SPEC':json.dumps({'prefix':'/invalid','paths':[]})})
        self.assertEqual(result.returncode,224,result.stderr.decode())
        self.assertEqual(result.stdout,b'child output')
        self.assertIn(b'[buildbox] export INFRA:',result.stderr)
        self.assertIn('export_error',runtime);self.assertTrue(runtime['targets_removed'])

    def test_glibc_pinned_export_keeps_the_requested_local_path(self):
        prefix=Path('/mnt/bb/jobs')/('r19-glibc-'+str(os.getpid()));Path(str(prefix)+'.t0').touch()
        target=self.d/'target';p=target/'x86_64-unknown-linux-gnu/debug/tool';p.parent.mkdir(parents=True);p.write_bytes(b'linux-binary')
        spec={'prefix':str(prefix),'cwd':str(self.d),'paths':['/work/target/debug/tool'],'build':True}
        try:
            with self.assertRaises(FileNotFoundError):exports.prepare(spec,str(target)) # old mapping loses the binary
            exports.prepare({**spec,'targetMapping':{'triple':'x86_64-unknown-linux-gnu','profiles':['debug','release']}},str(target))
            import tarfile
            manifest=json.loads(Path(str(prefix)+'.exports/manifest.json').read_text())
            with tarfile.open(manifest[0]['archive']) as tar:
                self.assertEqual(tar.getnames(),['debug/tool']);self.assertEqual(tar.extractfile('debug/tool').read(),b'linux-binary')
        finally:Path(str(prefix)+'.t0').unlink();shutil.rmtree(str(prefix)+'.exports',ignore_errors=True)

    def test_cleanup_failure_has_infra_status_and_retains_child_output(self):
        result,runtime=self.run_job([sys.executable,'-c','print("child output",end="")'],cleanup_failure=True)
        try:
            self.assertEqual(result.returncode,224,result.stderr.decode());self.assertEqual(result.stdout,b'child output')
            self.assertIn('cleanup control',runtime['cleanup_error']);self.assertIn(b'cleanup INFRA',result.stderr)
        finally:job.remove_home(Path(runtime['hermetic_home']))

    def test_pruning_age_count_leases_active_process_and_metadata(self):
        root=self.d/'wt-sdk';root.mkdir();mirror=self.d/'sdk.git';locks=self.d/'locks';locks.mkdir()
        now=time.time()
        def add(name,age):
            p=root/name;p.mkdir();os.utime(p,(now-age,now-age));return p
        add('stale',50000);fresh=add('fresh',5);leased=add('leased',50001);busy=add('busy',50002);recent_git=add('recent-git',50003)
        metadata=mirror/'worktrees/recent-git';metadata.mkdir(parents=True);(metadata/'index').touch()
        with (locks/'leased.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            result=pruner.prune(root,mirror,locks,max_count=3,active={str(busy/'child')},now=now)
        self.assertIn('stale',result['removed']);self.assertTrue(leased.exists());self.assertTrue(busy.exists());self.assertTrue(recent_git.exists())
        self.assertFalse(fresh.exists()) # count cap also evicts fresh unused entries
        self.assertEqual(result['remaining'],3)
        result=pruner.prune(root,mirror,locks,max_count=1,active=set(),now=now)
        self.assertEqual(result['remaining'],1);self.assertTrue(recent_git.exists())
        result=pruner.prune(root,mirror,locks,max_count=0,active={str(recent_git)},now=now)
        self.assertTrue(result['over_cap']);self.assertEqual(result['held'],['recent-git'])
        self.assertTrue(recent_git.exists()) # count pressure never evicts active work
    def test_pruner_removes_unreadable_targets_without_following_symlinks(self):
        self.assertFalse(pruner.prune(self.d/'missing',self.d/'mirror',self.d/'locks',active=set())['over_cap'])
        root=self.d/'wt-sdk';root.mkdir();p=root/'old';(p/'target/deep').mkdir(parents=True)
        (p/'target/deep/object').write_bytes(b'x');(p/'target').chmod(0)
        outside=self.d/'keep';outside.mkdir();(outside/'object').write_bytes(b'keep')
        (p/'link').symlink_to(outside,target_is_directory=True)
        try:
            with self.assertRaises(PermissionError):shutil.rmtree(p)
            result=pruner.prune(root,self.d/'mirror',self.d/'locks',max_count=0,active=set())
            self.assertEqual(result['removed'],['old']);self.assertEqual((outside/'object').read_bytes(),b'keep')
        finally:
            if p.exists():job.remove_home(p)

    def test_admission_disk_refusal_is_stderr_with_stdout_negative(self):
        source=(HERE/'bb-admit.py').read_text().replace("if ROOT==Path('/mnt/bb/admit'):", 'if True:')
        source=source.replace('space=os.statvfs(path)', 'space=type("Space",(),{"f_bavail":9,"f_blocks":100,"f_frsize":4096})()')
        source=source.replace("Path('/mnt/bb/log/alerts.jsonl')",f'Path({str(self.d/"alerts.jsonl")!r})')
        for negative in [False,True]:
            worker=self.d/f'admit-{negative}.py';worker.write_text(source.replace("t['fds'][2]","t['fds'][1]") if negative else source)
            root=self.d/f'admit-{negative}';root.mkdir()
            env={**os.environ,'BB_ADMIT_ROOT':str(root),'BB_ADMIT_WRAPPED_COMMAND':'1'}
            result=subprocess.run([sys.executable,str(worker),'lane','cloud','--',sys.executable,'-c','print("must not run")'],env=env,capture_output=True)
            self.assertEqual(result.returncode,224)
            self.assertIn(b'BB_DISK_LOW',result.stdout if negative else result.stderr)
            if not negative:self.assertEqual(result.stdout,b'')
            daemon=json.loads((root/'daemon.json').read_text());os.kill(daemon['pid'],15)

    def test_upgrade_barrier_drains_existing_jobs_without_admitting_new_tickets(self):
        root=self.d/'admit';root.mkdir()
        (root/'lease-stale.json').write_text(json.dumps({'pid':999999999,'identity':None}))
        env={**os.environ,'BB_ADMIT_ROOT':str(root),'BB_ADMIT_WRAPPED_COMMAND':'1'}
        first=subprocess.Popen([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c','import time;time.sleep(.3)'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        until=time.monotonic()+2
        while not any(p.name!='lease-stale.json' for p in root.glob('lease-*')) and time.monotonic()<until:time.sleep(.01)
        self.assertTrue(any(p.name!='lease-stale.json' for p in root.glob('lease-*')))
        marker=self.d/'new-ticket'
        with (root/'upgrade.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            second=subprocess.Popen([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;Path({str(marker)!r}).touch()'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            first.communicate(timeout=2);self.assertEqual(first.returncode,0)
            self.assertFalse(marker.exists())
            # Without the upgrade flock, a new ticket incorrectly enters.
            negative=self.d/'no-barrier.py';negative.write_text((HERE/'bb-admit.py').read_text().replace('fcntl.flock(upgrade,fcntl.LOCK_SH)','pass'))
            bad=subprocess.run([sys.executable,str(negative),'lane','cloud','--','true'],env=env,capture_output=True,timeout=2)
            self.assertEqual(bad.returncode,0)
        second.communicate(timeout=2);self.assertEqual(second.returncode,0);self.assertTrue(marker.exists())
        self.assertFalse((root/'lease-stale.json').exists())
        daemon=json.loads((root/'daemon.json').read_text())
        import hashlib
        self.assertEqual(daemon['source_sha256'],hashlib.sha256((HERE/'bb-admit.py').read_bytes()).hexdigest())
        os.kill(daemon['pid'],15)

    def test_recovered_leases_need_identity_and_survive_pidfd_exit_race(self):
        with patch.object(admit.os,'pidfd_open') as opened:
            self.assertIsNone(admit.live_pidfd(999999999,None));opened.assert_not_called()
        with patch.object(admit,'identity',return_value='birth'),patch.object(admit.os,'pidfd_open',side_effect=ProcessLookupError):
            self.assertIsNone(admit.live_pidfd(999999999,'birth'))
        # The previous None == None recovery attempts pidfd_open and crashes.
        with self.assertRaises(ProcessLookupError):
            if admit.identity(999999999)==None:os.pidfd_open(999999999)

    def test_killed_job_receipt_cleans_ephemeral_targets(self):
        worker=self.worker();receipt=self.d/'receipt.mjs';receipts=self.d/'receipts'
        source=(HERE/'buildbox-receipt.mjs').read_text().replace('/home/var/bin/bb-job.py',str(worker)).replace("const dir = '/mnt/bb/jobs'",f'const dir = {str(receipts)!r}')
        receipt.write_text(source);(self.d/'buildbox-receipt.mjs').write_text(source)
        env={**os.environ,'BUILD_SLOT_DEFER_EXEC':'1','BUILD_SLOT_LABEL':'gate-speed-r19-kill-control'}
        env.pop('BB_JOB_ACTIVE',None);env.pop('BUILD_SLOT_EXEC_IN_PAYLOAD',None)
        marker=self.d/'target-path'
        code=f'from pathlib import Path;import os,time;p=Path(os.environ["CARGO_TARGET_DIR"]);(p/"object").write_text("compiled");Path({str(marker)!r}).write_text(str(p));time.sleep(60)'
        process=subprocess.Popen([job.NODE,str(receipt),'run-bb',sys.executable,str(worker),sys.executable,'-c',code],cwd=self.d,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            until=time.monotonic()+5
            while not marker.exists() and time.monotonic()<until:time.sleep(.01)
            self.assertTrue(marker.exists());target=Path(marker.read_text());self.assertTrue(target.exists())
            process.kill();process.wait(timeout=2)
            until=time.monotonic()+5;found=[]
            while time.monotonic()<until:
                found=list(receipts.glob('*.receipt.json'))
                if found:break
                time.sleep(.01)
            self.assertEqual(len(found),1)
            receipt_data=json.loads(found[0].read_text())
            self.assertEqual((receipt_data['exit_status'],receipt_data['termination']),(137,'interrupted'))
            self.assertTrue(receipt_data['targets_removed']);self.assertFalse(target.exists())
            self.assertTrue(receipt_data['home_removed'])
            self.assertIn(b'BUILD_SLOT_EXEC=',process.stderr.read())
            self.assertEqual(process.stdout.read(),b'')
        finally:
            if process.poll() is None:process.kill();process.wait()
            process.stdout.close();process.stderr.close()

    def ticket(self,seq,tenant,tier=2):return {'seq':seq,'tenant':tenant,'tier':tier,'cls':'compile'}

    def test_killed_generated_wrapper_cleans_outer_scratch_with_negative_control(self):
        worker=self.worker()
        generator=self.d/'generator.mjs';generator.write_text((HERE/'buildbox.mjs').read_text()+'\nexport {launchScript};\n')
        receipt_source=(HERE/'buildbox-receipt.mjs').read_text().replace('/home/var/bin/bb-job.py',str(worker)).replace('interval = 1000','interval = 20')
        (self.d/'buildbox-receipt.mjs').write_text(receipt_source)
        for negative in [False,True]:
            prefix=Path('/mnt/bb/jobs')/f'r19-wrapper-{os.getpid()}-{negative}'
            marker=self.d/f'wrapper-ready-{negative}'
            probe=self.d/'generate-wrapper.mjs'
            probe.write_text('''import {launchScript} from './generator.mjs';import fs from 'node:fs';
const prefix=process.argv[2];
const id={commit:'a'.repeat(40),tree:'b'.repeat(40),content:'c'.repeat(64),forced:[],autocrlf:'false',groups:'e30='};
const script=launchScript('/mnt/bb/wt-sdk/fixture.0','sdk/fixture','.', ['true'],prefix,'','sdk','',id,60,true,{repo:'sdk',argv:['true'],pr_number:1415});
const wrapped=[...script.matchAll(/echo ([A-Za-z0-9+/=]+) \\| base64 -d > \\$J\\.(sh|wrap|admitted)\\b/g)].find(m=>m[2]==='wrap');
let body=Buffer.from(wrapped[1],'base64').toString();
body=body.slice(0,body.indexOf('mkdir -p /mnt/bb/locks/wt-sdk')).replace('recover /mnt/bb/jobs','recover '+process.argv[3]);
fs.writeFileSync(prefix+'.wrap',body);
const meta=script.match(/echo ([A-Za-z0-9+/=]+) \\| base64 -d > \\$J\\.receipt-meta\\.v2\\.json/);
fs.writeFileSync(prefix+'.receipt-meta.v2.json',Buffer.from(meta[1],'base64'));
''')
            subprocess.run([job.NODE,str(probe),str(prefix),str(self.d)],check=True,capture_output=True)
            source=receipt_source.replace("if (m.side === 'bb2' && m.job_prefix) {",'if (false) {') if negative else receipt_source
            Path(str(prefix)+'.receipt-tool.mjs').write_text(source)
            code=f'from pathlib import Path;import time;Path({str(marker)!r}).touch();time.sleep(60)'
            import shlex
            with Path(str(prefix)+'.wrap').open('a') as wrapper:
                wrapper.write(f'\nmkdir -p "$J.targets" "$J.tmp"\nprintf scratch > "$J.targets/object"\nprintf scratch > "$J.tmp/object"\nexport BUILD_SLOT_RECEIPT_META="$J.receipt-meta.v2.json"\nsetsid {shlex.join([sys.executable,str(worker),sys.executable,"-c",code])} &\npayload=$!\nnode "$J.receipt-tool.mjs" job "$J.receipt-meta.v2.json" "$payload"\nwait "$payload"\n')
            process=subprocess.Popen(['bash',str(prefix)+'.wrap'],env={**os.environ,'PATH':str(Path(job.NODE).parent)+':'+os.environ['PATH']},stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
            try:
                until=time.monotonic()+4
                while not marker.exists() and time.monotonic()<until:time.sleep(.01)
                self.assertTrue(marker.exists())
                process.kill();process.wait(timeout=2)
                receipt=Path(str(prefix)+'.receipt.json');until=time.monotonic()+5
                while not receipt.exists() and time.monotonic()<until:time.sleep(.01)
                data=json.loads(receipt.read_text())
                self.assertEqual((data['exit_status'],data['termination']),(137,'interrupted'))
                self.assertTrue(data['home_removed']);self.assertTrue(data['targets_removed'])
                for suffix in ['.targets','.tmp']:self.assertEqual(Path(str(prefix)+suffix).exists(),negative)
                self.assertEqual(bool(data.get('outer_targets_removed')),not negative)
            finally:
                if process.poll() is None:process.kill();process.wait()
                process.stdout.close();process.stderr.close();job.cleanup_outer(prefix)
                for p in prefix.parent.glob(prefix.name+'.*'):
                    if p.is_file():p.unlink()

    def test_fair_share_and_fifo_negative(self):
        s=admit.Scheduler({'compile':1,'other':1,'light':10},{'graphcoder':3,'cloud':3})
        tickets=[self.ticket(i,t) for i,t in enumerate(['graphcoder']*3+['cloud']*3)]
        for t in tickets:s.add(t)
        result=[]
        for _ in tickets:
            t=s.take('compile'); result.append(t['tenant']);s.release(t)
        self.assertEqual(result,['graphcoder','cloud']*3)
        self.assertNotEqual([t['tenant'] for t in tickets],result) # FIFO negative

    def test_tenant_cap_and_tiers_negative(self):
        s=admit.Scheduler({'compile':3,'other':1,'light':10},{'graphcoder':1,'cloud':1})
        s.add(self.ticket(0,'graphcoder'));s.add(self.ticket(1,'graphcoder'));s.add(self.ticket(2,'cloud'))
        a=s.take('compile');b=s.take('compile');self.assertIsNone(s.take('compile'))
        self.assertNotEqual(s.running['compile'],s.limits['compile']) # global space does not bypass cap
        s.release(a);self.assertEqual(s.take('compile')['tenant'],'graphcoder')
        s.add(self.ticket(3,'cloud',0));self.assertEqual(s.take('compile')['tier'],0) # explicit critical exemption

    def test_daemon_killed_client_holds_until_command_exits_and_releases(self):
        root=self.d/'admit';root.mkdir();(root/'admit.conf').write_text('compile=1 tenant.graphcoder=1 tenant.cloud=1\n')
        env={**os.environ,'BB_ADMIT_ROOT':str(root),'BB_ADMIT_WRAPPED_COMMAND':'1'}
        args=[sys.executable,str(HERE/'bb-admit.py'),'lane','graphcoder','--',sys.executable,'-c',f'from pathlib import Path; import time,os;Path({str(self.d/"pid")!r}).write_text(str(os.getpid()));time.sleep(60)']
        first=subprocess.Popen(args,env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            until=time.monotonic()+5
            while not (self.d/'pid').exists() and time.monotonic()<until:time.sleep(.01)
            self.assertTrue((self.d/'pid').exists())
            pid=int((self.d/'pid').read_text()); self.assertIsNotNone(admit.identity(pid))
            marker=self.d/'second'
            second=subprocess.Popen([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;Path({str(marker)!r}).write_text("ran")'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            time.sleep(.1);self.assertFalse(marker.exists()) # queued negative control
            first.kill();first.wait(timeout=5)
            out,err=second.communicate(timeout=5);self.assertEqual(second.returncode,0,err)
            self.assertTrue(marker.exists());self.assertIsNone(admit.identity(pid))
        finally:
            if first.poll() is None:first.kill();first.wait()
            first.stdout.close(); first.stderr.close()
            info=json.loads((root/'daemon.json').read_text());os.kill(info['pid'],signal.SIGTERM)

    def test_two_sccache_servers_are_isolated(self):
        a=self.d/'a';b=self.d/'b';a.mkdir();b.mkdir()
        envA=dict(os.environ);envB=dict(os.environ)
        first,status=job.start_cache(a,envA);second,status2=job.start_cache(b,envB)
        try:
            self.assertEqual((status,status2),('ready','ready'))
            self.assertNotEqual(first['pid'],second['pid']);self.assertNotEqual(first['endpoint'],second['endpoint'])
            job.stop_server(first)
            r=subprocess.run([job.SCCACHE,'--show-stats'],env=envB,capture_output=True)
            self.assertEqual(r.returncode,0);self.assertEqual(job.server_identity(b/'sccache.sock')['pid'],second['pid'])
            stale={**second,'identity':'wrong birth'};job.stop_server(stale)
            self.assertEqual(job.server_identity(b/'sccache.sock')['pid'],second['pid'])
            self.assertEqual(envA['SCCACHE_DIR'],'/mnt/bb/sccache');self.assertEqual(envA['SCCACHE_IDLE_TIMEOUT'],'120')
        finally:job.stop_server(first);job.stop_server(second)

    def test_fixed_shared_port_negative_control(self):
        c=socket.socket();c.bind(('127.0.0.1',0));port=c.getsockname()[1];c.close()
        env={**os.environ,'SCCACHE_SERVER_PORT':str(port),'SCCACHE_DIR':'/mnt/bb/sccache','SCCACHE_IDLE_TIMEOUT':'30'}
        env.pop('SCCACHE_SERVER_UDS',None)
        r=subprocess.run([job.SCCACHE,'--start-server'],env=env,capture_output=True)
        self.assertEqual(r.returncode,0,r.stderr)
        try:
            again=subprocess.run([job.SCCACHE,'--start-server'],env=env,capture_output=True)
            self.assertNotEqual(again.returncode,0) # both jobs collide on one server
        finally:subprocess.run([job.SCCACHE,'--stop-server'],env=env,capture_output=True)

    def test_startup_failure_is_explicit_and_uncached(self):
        d=self.d/'failed';d.mkdir();env=dict(os.environ)
        with patch.object(job.subprocess,'run',return_value=subprocess.CompletedProcess([],2,'','forced bind failure')):
            server,status=job.start_cache(d,env)
        self.assertIsNone(server);self.assertEqual(status,'startup-failed:forced bind failure')

if __name__=='__main__':unittest.main(verbosity=2)
