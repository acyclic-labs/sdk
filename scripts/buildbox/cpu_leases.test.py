"""Real kernel controls. Source/layout provenance is retained in each result.

No global admission/cgroup/service is modified: root freezes the same adapter
with only its two owned fixture roots relocated. Children do no build work.
"""
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import input_closure
import uuid
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from admission_policy import cpu_layout, cpu_host_set, capture_limits, LIMIT_ENV

HERE=Path(__file__).resolve().parent


class CpuPolicy(unittest.TestCase):
    def test_fixture_custody_cannot_overwrite_enclosing_job_accounting(self):
        from cpu_test_fixture import KernelFixture
        with patch.dict(os.environ,{'BUILD_SLOT_RECEIPT_META':'enclosing-job'}):
            self.assertNotIn('BUILD_SLOT_RECEIPT_META',KernelFixture.environment())
            self.assertEqual(KernelFixture.environment({'BUILD_SLOT_RECEIPT_META':'owned-fixture'})['BUILD_SLOT_RECEIPT_META'],'owned-fixture')
            self.assertEqual(dict(os.environ)['BUILD_SLOT_RECEIPT_META'],'enclosing-job','former inherited environment aliases the real receipt')

    def test_one_pool_across_all_classes_and_priority_with_no_test_reserve(self):
        spec=importlib.util.spec_from_file_location('admit',HERE/'bb-admit.py')
        admit=importlib.util.module_from_spec(spec);spec.loader.exec_module(admit)
        scheduler=admit.Scheduler(caps={'graphcoder':8,'cloud':8})
        for index in range(9):
            scheduler.add({'id':str(index),'cls':['compile','other','light'][index%3],
                           'tier':2,'tenant':['graphcoder','cloud'][index%2],'seq':index})
        active=[scheduler.take() for _ in range(8)]
        self.assertEqual(len({ticket['cpu_index'] for ticket in active}),8)
        self.assertIsNone(scheduler.take())
        scheduler.release(active[0]);self.assertIsNotNone(scheduler.take())
        # Captured deployed code supplies the negative; no made-up Cargo or
        # external-tool outputs stand in for the old admission authority.
        fixture=HERE/'fixtures/cpu-leases'
        info=json.loads((fixture/'origin.json').read_text())
        old=fixture/'bb-admit.pinned.py'
        self.assertEqual(hashlib.sha256(old.read_bytes()).hexdigest(),info['sha256'])
        spec=importlib.util.spec_from_file_location('actual_old_admission',old)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        former=module.Scheduler(caps={'graphcoder':8,'cloud':8})
        for index in range(9):former.add({'cls':['compile','other','light'][index%3],
                         'tier':2,'tenant':['graphcoder','cloud'][index%2],'seq':index})
        admitted=[former.take(['compile','other','light'][index%3]) for index in range(9)]
        self.assertTrue(all(admitted),'actual count-only authority oversubscribes eight CPU leases')
        layouts=list(map(set,cpu_layout()))
        for a in range(8):
            self.assertEqual(len(layouts[a]),16)
            for b in range(a):self.assertFalse(layouts[a]&layouts[b])
            self.assertFalse(layouts[a]&set(cpu_host_set()))

    def test_missing_tenant_refuses_before_daemon_or_payload(self):
        from admission_policy import require_tenant
        for tenant in ['',None,'unknown']:
            with self.assertRaisesRegex(RuntimeError,'kind=CPU_LEASE step=tenant-label.*recovery='):
                require_tenant(tenant)
        self.assertEqual(require_tenant('cloud'),'cloud')
        old=(HERE/'build-slot').read_text().replace('tenant=${BUILD_SLOT_TENANT:-}',
                                                   'tenant=${BUILD_SLOT_TENANT:-graphcoder}')
        self.assertIn('tenant=${BUILD_SLOT_TENANT:-graphcoder}',old,'actual former default is the negative')
        for label in ['', 'unknown']:
            result=subprocess.run([sys.executable,str(HERE/'bb-admit.py'),'lane',label,'--','/usr/bin/printf','child'],
                                  capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertEqual(result.stdout,'')
            self.assertIn('step=tenant-label',result.stderr)
            self.assertIn('/home/var/bin/build-slot /usr/bin/printf child',result.stderr)

    def test_host_cannot_create_a_second_admission_authority(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            result=subprocess.run([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--','/usr/bin/printf','escaped'],
                env={**os.environ,'BB_ADMIT_ROOT':temporary},capture_output=True,text=True)
            self.assertEqual(result.returncode,224,result.stderr)
            self.assertEqual(result.stdout,'')
            self.assertIn('step=admission-root',result.stderr)
            self.assertNotIn('Traceback',result.stderr)
            self.assertEqual(list(Path(temporary).iterdir()),[])


class KernelLeases(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from cpu_test_fixture import KernelFixture
        cls.fixture=KernelFixture()
        cls.control=cls.fixture.control;cls.group=cls.fixture.group;cls.module=cls.fixture.module
        # Every payload here is inline interpreter code with no reference inputs.
        # The producer (this suite) declares that closure explicitly, as every
        # opaque job must; admission still validates and canonicalizes it.
        cls.env=input_closure.declare(cls.fixture.environment({LIMIT_ENV:json.dumps(capture_limits()),'BB_ADMIT_TIER':'2','BB_CACHE_DISABLED':'1'}))

    @classmethod
    def tearDownClass(cls):
        cls.fixture.close()

    def invoke(self,index,code,token=None,env=None):
        token=token or uuid.uuid4().hex
        process=subprocess.Popen(['sudo','-n','--preserve-env','/usr/bin/python3','-B',str(self.module),
                                  'exec',str(index),token,'cloud','--',sys.executable,'-c',code],
                                 env={**self.env,**(env or {})},stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        return process,token

    def test_durable_handoff_refuses_payload_when_admission_dies_before_acknowledgement(self):
        address='\0bb-cpu-handoff-control-'+uuid.uuid4().hex
        with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as listener:
            listener.bind(address);listener.listen(1)
            process,token=self.invoke(3,'print("payload-before-durable-ack")',
                                     env={'BB_CPU_ADMISSION_SOCKET':'@'+address[1:]})
            try:
                self.assertTrue(select.select([listener],[],[],3)[0])
                with listener.accept()[0] as peer:
                    self.assertEqual(peer.recv(64),token.encode())
                    self.assertFalse(select.select([process.stdout],[],[],.02)[0])
                output,error=process.communicate(timeout=3)
                self.assertEqual(process.returncode,224,error.decode())
                self.assertNotIn(b'payload-before-durable-ack',output)
                self.assertIn(b'step=admission-handoff',error)
                self.assertFalse((self.control/(token+'.json')).exists())
            finally:
                if process.poll() is None:process.kill();process.communicate(timeout=3)
        # Actual execution without the handoff is the negative: the payload
        # starts immediately, before any durable-dispatch acknowledgement.
        process,_=self.invoke(3,'print("payload-before-durable-ack")')
        output,error=process.communicate(timeout=3)
        self.assertEqual(process.returncode,0,error.decode())
        self.assertIn(b'payload-before-durable-ack',output)

    def test_empty_prepublication_constructor_is_reclaimed_once(self):
        old=uuid.uuid4().hex
        code='import sys;sys.path.insert(0,sys.argv[1]);import cpu_leases as c;(c.prepare(4)/("run-"+sys.argv[2])).mkdir()'
        subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',code,str(self.control),old],check=True)
        process,_=self.invoke(4,'print("empty-constructor-reclaimed")')
        output,error=process.communicate(timeout=3)
        self.assertEqual(process.returncode,0,error.decode())
        self.assertIn(b'empty-constructor-reclaimed',output)
        self.assertFalse((self.group/'slot-4'/('run-'+old)).exists())

    def ticket(self,stream):
        # Input-closure admission happens before dispatch, so the client's
        # protocol is the admission proof first and the queue ticket second.
        self.assertTrue(stream.readline().startswith(b'BB_INPUTS_ADMITTED '))
        line=stream.readline();self.assertIn(b'BB_ADMIT_TICKET',line)
        return line

    def ready(self,process):
        self.assertTrue(select.select([process.stdout],[],[],3)[0],'bounded fixture child did not become ready')
        line=process.stdout.readline()
        if not line:self.fail(process.stderr.read().decode())
        return json.loads(line)

    def finish(self,process):
        output,error=process.communicate(b'finish\n',timeout=3)
        self.assertEqual(process.returncode,0,error.decode())
        return output

    def test_concurrent_starts_disjoint_kernel_bounds_and_no_oversubscription(self):
        code='import os,json,sys;print(json.dumps(sorted(os.sched_getaffinity(0))),flush=True);sys.stdin.readline()'
        active=[]
        try:
            with ThreadPoolExecutor(max_workers=8) as workers:active=list(workers.map(lambda index:self.invoke(index,code),range(8)))
            sets=[set(self.ready(process)) for process,_ in active]
            for index,values in enumerate(sets):
                self.assertEqual(values,set(cpu_layout()[index]))
                for former in sets[:index]:self.assertFalse(values&former)
            ninth,_=self.invoke(0,'print("escaped")')
            output,error=ninth.communicate(timeout=3)
            self.assertEqual(ninth.returncode,224,error.decode());self.assertNotIn(b'escaped',output)
            # Actual pre-lease caller affinity spans more CPUs than one lease.
            self.assertGreater(len(os.sched_getaffinity(0)),len(sets[0]))
        finally:
            for process,_ in active:self.finish(process)

    def test_affinity_parent_ceiling_and_nested_scope_cannot_escape(self):
        code='''import os,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[0] if False else REPLACE_ROOT)
import cpu_leases as c
owner=c.current();outside=next(cpu for cpu in range(192) if cpu not in owner['cpus'])
try:os.sched_setaffinity(0,{outside});raise AssertionError('escaped cpuset')
except OSError:pass
try:(Path(owner['cgroup']).parent/'cpuset.cpus').write_text('0-191');raise AssertionError('changed root ceiling')
except PermissionError:pass
os.sched_setaffinity(0,set(range(192)))
assert os.sched_getaffinity(0)==set(owner['cpus'])
probe="import os;from pathlib import Path;assert len(os.sched_getaffinity(0))==16;p=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::',1)[1].lstrip('/');assert (p.parent/'memory.swap.max').read_text().strip()=='0';assert (p.parent/'memory.high').read_text().strip()=='536870912'"
assert c.nested_scope(['--scope','-p','MemoryMax=1G','-p','MemorySwapMax=0','-p','MemoryHigh=512M','--',sys.executable,'-c',probe])==0
print(json.dumps(owner))
'''.replace('REPLACE_ROOT',repr(str(self.control)))
        process,_=self.invoke(0,code);output,error=process.communicate(timeout=3)
        self.assertEqual(process.returncode,0,error.decode());self.assertEqual(len(json.loads(output)['cpus']),16)

    def test_peer_migration_requires_root_owned_common_ancestor(self):
        # cgroup v2 checks the common ancestor's cgroup.procs as well as the
        # destination. A same-UID writable peer leaf cannot bypass that check.
        common=self.group/'cgroup.procs'
        for negative in [False,True]:
            victim,token=self.invoke(1,'import sys;print("true",flush=True);sys.stdin.readline()')
            self.ready(victim)
            target=self.group/'slot-1'/('run-'+token)/'work/job/cgroup.procs'
            permission='import os,sys;os.chown(sys.argv[1],int(sys.argv[2]),-1)'
            try:
                if negative:subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',permission,str(common),str(os.getuid())],check=True)
                code='''import os,json
from pathlib import Path
before=sorted(os.sched_getaffinity(0));denied=False
try:Path(TARGET).write_text('0')
except PermissionError:denied=True
print(json.dumps({'denied':denied,'before':before,'after':sorted(os.sched_getaffinity(0))}))
'''.replace('TARGET',repr(str(target)))
                attacker,_=self.invoke(0,code);output,error=attacker.communicate(timeout=3)
                self.assertEqual(attacker.returncode,0,error.decode())
                result=json.loads(output)
                self.assertEqual(result['denied'],not negative)
                self.assertEqual(result['before']==result['after'],not negative)
            finally:
                if negative:subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',permission,str(common),'0'],check=True)
                self.finish(victim)

    def test_crashed_manager_keeps_lease_until_orphans_are_retired(self):
        code='import os,json,sys;print(json.dumps(os.getpid()),flush=True);sys.stdin.readline()'
        process,token=self.invoke(1,code)
        self.ready(process)
        record=json.loads((self.control/(token+'.json')).read_text())
        manager=record['manager']
        kill='''import sys,os,signal
sys.path.insert(0,sys.argv[1]);from admission_policy import process_identity
pid=int(sys.argv[2]);assert process_identity(pid)==sys.argv[3]
fd=os.pidfd_open(pid);assert process_identity(pid)==sys.argv[3];signal.pidfd_send_signal(fd,signal.SIGKILL)
'''
        subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',kill,str(self.control),str(manager['pid']),manager['identity']],check=True)
        process.wait(timeout=3)
        second,_=self.invoke(1,'print("premature")');out,err=second.communicate(timeout=3)
        self.assertEqual(second.returncode,224,err.decode());self.assertNotIn(b'premature',out)
        retired=subprocess.run(['sudo','-n','/usr/bin/python3','-B',str(self.module),'cancel',token],capture_output=True,check=True)
        self.assertTrue(json.loads(retired.stdout)['empty'])
        process.communicate(timeout=3)
        successor,_=self.invoke(1,'print("reclaimed")');out,err=successor.communicate(timeout=3)
        self.assertEqual(successor.returncode,0,err.decode());self.assertIn(b'reclaimed',out)

    def test_kill_during_retirement_recovers_final_counters_and_slot(self):
        former=HERE/'fixtures/cpu-leases/former-retire.py'
        provenance=json.loads(former.with_name('former-retire.origin.json').read_text())
        self.assertEqual(hashlib.sha256(former.read_bytes()).hexdigest(),provenance['sha256'])
        driver='''import os,sys,signal
from pathlib import Path
sys.path.insert(0,sys.argv[1]);import cpu_leases as c
token,boundary,former=sys.argv[2:]
if former!='new':
 exec(Path(former).read_text(),c.__dict__)
 old_retire=c.retire
 c.retire=lambda token,outcome=None:old_retire(token)
rmdir=Path.rmdir;atomic=c.atomic_json
def interrupted_rmdir(path,*args,**kwargs):
 result=rmdir(path,*args,**kwargs)
 if boundary=='rmdir' and path.name=='run-'+token:os.kill(os.getpid(),signal.SIGKILL)
 return result
def interrupted_atomic(path,*args,**kwargs):
 result=atomic(path,*args,**kwargs)
 if boundary=='terminal' and Path(path).name=='slot-5.terminal':os.kill(os.getpid(),signal.SIGKILL)
 return result
Path.rmdir=interrupted_rmdir;c.atomic_json=interrupted_atomic
c.execute(5,token,'cloud',[sys.executable,'-c','pass'])
'''
        for boundary in ['rmdir','terminal']:
            for negative in [True,False]:
                with self.subTest(boundary=boundary,negative=negative), tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
                    token=uuid.uuid4().hex
                    meta=str(Path(temporary)/'owned-fixture-meta')
                    killed=subprocess.run(['sudo','-n','--preserve-env','/usr/bin/python3','-B','-c',driver,
                        str(self.control),token,boundary,str(former) if negative else 'new'],
                        env={**self.env,'BUILD_SLOT_RECEIPT_META':meta},capture_output=True)
                    self.assertIn(killed.returncode,[-9,137],killed.stderr.decode())
                    recovered_result=None
                    if not negative and boundary=='rmdir':
                        # Startup must adopt a root record even when the old
                        # manager already removed its directory before dying.
                        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
                            queue=Path(temporary);(queue/'admit.conf').write_text('compile=1\n')
                            env={**self.env,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1'}
                            env.pop('RUSTC_WRAPPER',None)
                            try:
                                resumed=subprocess.run([sys.executable,str(self.control/'bb-admit.py'),'lane','cloud','--',
                                    sys.executable,'-c','print("retirement-adopted")'],env=env,capture_output=True,timeout=3)
                                self.assertEqual(resumed.returncode,0,resumed.stderr.decode())
                                self.assertIn(b'retirement-adopted',resumed.stdout)
                                recovered_result=json.loads((self.control/'slot-5.terminal').read_bytes())
                            finally:
                                info=queue/'daemon.json'
                                if info.exists():
                                    daemon=json.loads(info.read_text())
                                    from admission_policy import process_identity
                                    if process_identity(daemon['pid'])==daemon['identity']:os.kill(daemon['pid'],signal.SIGTERM)
                    else:
                        recovery=subprocess.run(['sudo','-n','/usr/bin/python3','-B',str(self.module),'cancel',token],capture_output=True)
                    if negative:
                        self.assertEqual(recovery.returncode,224,recovery.stderr.decode())
                        # The actual old retirement deleted the only counters.
                        # Remove only this isolated empty fixture's stale record.
                        cleanup='from pathlib import Path;import sys;p=Path(sys.argv[1]);assert not Path(sys.argv[2]).exists();p.unlink()'
                        subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',cleanup,
                            str(self.control/(token+'.json')),str(self.group/'slot-5'/('run-'+token))],check=True)
                    else:
                        if recovered_result is None:
                            self.assertEqual(recovery.returncode,0,recovery.stderr.decode())
                            recovered_result=json.loads(recovery.stdout)
                        result=recovered_result
                        self.assertTrue(result['empty']);self.assertEqual(result['id'],token)
                        self.assertIn('usage_usec',result['cpu_stat'])
                        self.assertFalse((self.control/(token+'.json')).exists())
                        receipt=json.loads(Path(meta+'.cpu-lease.terminal').read_text())
                        self.assertEqual(receipt['id'],token);self.assertTrue(receipt['empty'])
                        self.assertEqual(receipt['termination'],'command')
                        self.assertEqual(receipt['exit_status'],0,'completed payload outcome survives manager termination')
                    successor,_=self.invoke(5,'print("retirement-reclaimed")')
                    output,error=successor.communicate(timeout=3)
                    self.assertEqual(successor.returncode,0,error.decode())
                    self.assertIn(b'retirement-reclaimed',output)

    def test_kill_after_record_unlink_preserves_receipt_across_slot_reuse(self):
        former=HERE/'fixtures/cpu-leases/former-retire-sidecar.py'
        provenance=json.loads(former.with_name('former-retire-sidecar.origin.json').read_text())
        self.assertEqual(hashlib.sha256(former.read_bytes()).hexdigest(),provenance['sha256'])
        driver='''import os,sys,signal
from pathlib import Path
sys.path.insert(0,sys.argv[1]);import cpu_leases as c
token,former=sys.argv[2:]
if former!='new':
 exec(Path(former).read_text(),c.__dict__)
 old_retire=c.retire
 c.retire=lambda token,outcome=None:old_retire(token)
unlink=Path.unlink
def killed_unlink(path,*args,**kwargs):
 result=unlink(path,*args,**kwargs)
 if path==c.record_path(token):os.kill(os.getpid(),signal.SIGKILL)
 return result
Path.unlink=killed_unlink
c.execute(5,token,'cloud',[sys.executable,'-c','pass'])
'''
        for negative in [True,False]:
            with self.subTest(negative=negative), tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
                token=uuid.uuid4().hex;meta=str(Path(temporary)/'owned-fixture-meta')
                killed=subprocess.run(['sudo','-n','--preserve-env','/usr/bin/python3','-B','-c',driver,
                    str(self.control),token,str(former) if negative else 'new'],
                    env={**self.env,'BUILD_SLOT_RECEIPT_META':meta},capture_output=True)
                self.assertIn(killed.returncode,[-9,137],killed.stderr.decode())
                self.assertFalse((self.control/(token+'.json')).exists())
                successor,successor_token=self.invoke(5,'print("same-slot-reused")')
                output,error=successor.communicate(timeout=3)
                self.assertEqual(successor.returncode,0,error.decode())
                self.assertIn(b'same-slot-reused',output)
                self.assertEqual(json.loads((self.control/'slot-5.terminal').read_text())['id'],successor_token)
                sidecar=Path(meta+'.cpu-lease.terminal')
                if negative:self.assertFalse(sidecar.exists(),'captured former retirement loses the receipt')
                else:
                    receipt=json.loads(sidecar.read_text())
                    self.assertEqual(receipt['id'],token);self.assertTrue(receipt['empty'])
                    self.assertIn('usage_usec',receipt['cpu_stat'])
                    self.assertEqual(receipt['exit_status'],0);self.assertEqual(receipt['termination'],'command')

    def test_real_shared_admission_bounds_all_classes_queues_ninth_and_reclaims_killed_job(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);queue=root/'admit';queue.mkdir()
            (queue/'admit.conf').write_text('tenant.graphcoder=8 tenant.cloud=8\n')
            env={**self.env,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1'}
            env.pop('RUSTC_WRAPPER',None)
            command=[sys.executable,str(self.control/'bb-admit.py'),'lane']
            code='import os,json,sys;print(json.dumps(sorted(os.sched_getaffinity(0))),flush=True);sys.stdin.readline()'
            active=[];ninth=None
            try:
                for index in range(8):
                    process=subprocess.Popen([*command,['cloud','graphcoder'][index%2],'--',sys.executable,'-c',code],
                         env={**env,'BB_ADMIT_CLASS':['compile','other','light'][index%3]},
                         stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                    active.append(process)
                layouts=[set(self.ready(process)) for process in active]
                self.assertEqual(len(set.union(*layouts)),128)
                ninth=subprocess.Popen([*command,'cloud','--',sys.executable,'-c','print("ninth-admitted")'],
                         env=env,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                self.assertTrue(select.select([ninth.stderr],[],[],3)[0])
                self.ticket(ninth.stderr)
                self.assertFalse(select.select([ninth.stdout],[],[],.05)[0], 'ninth bypassed the full shared pool')
                active[0].kill();active[0].wait(timeout=3)
                output,error=ninth.communicate(timeout=3)
                self.assertEqual(ninth.returncode,0,error.decode());self.assertIn(b'ninth-admitted',output)
                active[0].communicate(timeout=3)
                # A read-only Cargo query is not a heavy phase and never takes
                # a lease; this actual JSON response retains clean stdout.
                (root/'Cargo.toml').write_text('[package]\nname="cpu-query-control"\nversion="0.0.0"\n')
                (root/'src').mkdir();(root/'src/lib.rs').write_text('')
                query=subprocess.run([*command,'graphcoder','--','/home/var/.cargo/bin/cargo','metadata',
                                      '--format-version','1','--no-deps'],cwd=root,env=env,capture_output=True,timeout=2)
                self.assertEqual(query.returncode,0,query.stderr.decode())
                self.assertEqual(json.loads(query.stdout)['packages'][0]['name'],'cpu-query-control')
            finally:
                if (queue/'daemon.log').exists():print('CPU_DAEMON_CONTROL '+(queue/'daemon.log').read_text(),file=sys.stderr)
                for process in active:
                    if process.poll() is None:
                        process.communicate(b'finish\n',timeout=3)
                    else:process.communicate(timeout=3)
                if ninth and ninth.poll() is None:ninth.kill();ninth.communicate(timeout=3)
                info=queue/'daemon.json'
                if info.exists():os.kill(json.loads(info.read_text())['pid'],signal.SIGTERM)

    def test_nested_client_reuses_kernel_lease_and_cannot_change_tenant(self):
        code='''import os,json,subprocess,sys
from pathlib import Path
sys.path.insert(0,ROOT);import cpu_leases as c
command=[sys.executable,str(Path(ROOT)/'bb-admit.py'),'lane']
env={**os.environ,'BB_ADMIT_WRAPPED_COMMAND':'1'}
for tenant,expected in [('cloud',0),('graphcoder',224)]:
 result=subprocess.run([*command,tenant,'--',sys.executable,'-c','import os,json;print(json.dumps(sorted(os.sched_getaffinity(0))))'],env=env,capture_output=True)
 if expected==0:
  assert result.returncode==0,result.stderr
  assert json.loads(result.stdout)==sorted(os.sched_getaffinity(0))
 else:assert result.returncode!=0 and b'nested-tenant' in result.stderr,result.stderr
print(json.dumps(c.current()['id']))
'''.replace('ROOT',repr(str(self.control)))
        process,token=self.invoke(2,code);output,error=process.communicate(timeout=3)
        self.assertEqual(process.returncode,0,error.decode());self.assertEqual(json.loads(output),token)

    def test_daemon_restart_recovers_live_lease_and_reaps_crashed_manager_before_reuse(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);queue=root/'admit';queue.mkdir()
            (queue/'admit.conf').write_text('compile=1 tenant.graphcoder=1 tenant.cloud=1\n')
            env={**self.env,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1'}
            env.pop('RUSTC_WRAPPER',None)
            entry=[sys.executable,str(self.control/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c']
            first=subprocess.Popen([*entry,'import os,json,sys;print(json.dumps(sorted(os.sched_getaffinity(0))),flush=True);sys.stdin.readline()'],
                                   env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            second=None
            try:
                original=set(self.ready(first))
                ticket=json.loads(self.ticket(first.stderr).decode().split(' ',1)[1])
                record=json.loads((self.control/(ticket['id']+'.json')).read_text())
                daemon=json.loads((queue/'daemon.json').read_text())
                os.kill(daemon['pid'],signal.SIGKILL)
                first.wait(timeout=3)
                second=subprocess.Popen([*entry,'print("recovered-successor")'],env=env,
                                        stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                self.assertTrue(select.select([second.stderr],[],[],3)[0])
                self.ticket(second.stderr)
                self.assertFalse(select.select([second.stdout],[],[],.05)[0], 'restart released a live predecessor')
                replacement=json.loads((queue/'daemon.json').read_text())
                self.assertNotEqual(daemon['pid'],replacement['pid'])
                self.assertEqual(set(json.loads((self.control/(ticket['id']+'.json')).read_text())['cpus']),original)
                code='''import os,sys,signal
sys.path.insert(0,sys.argv[1]);import cpu_leases as c
record,_=c.verified_record(sys.argv[2]);manager=record['manager'];pid=manager['pid']
assert c.process_identity(pid)==manager['identity'];fd=os.pidfd_open(pid)
assert c.process_identity(pid)==manager['identity'];signal.pidfd_send_signal(fd,signal.SIGKILL)
'''
                subprocess.run(['sudo','-n','/usr/bin/python3','-B','-c',code,str(self.control),ticket['id']],check=True)
                output,error=second.communicate(timeout=3)
                self.assertEqual(second.returncode,0,error.decode());self.assertIn(b'recovered-successor',output)
                first.communicate(timeout=3)
                self.assertFalse((self.control/(ticket['id']+'.json')).exists())
            finally:
                if (queue/'daemon.log').exists():print('CPU_RESTART_CONTROL '+(queue/'daemon.log').read_text(),file=sys.stderr)
                for process in [first,second]:
                    if process is not None and process.poll() is None:process.kill();process.wait(timeout=3)
                info=queue/'daemon.json'
                if info.exists():
                    daemon=json.loads(info.read_text())
                    from admission_policy import process_identity
                    if process_identity(daemon['pid'])==daemon['identity']:os.kill(daemon['pid'],signal.SIGTERM)


if __name__=='__main__':unittest.main()
