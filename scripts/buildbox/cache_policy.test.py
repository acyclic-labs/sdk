"""Real Unix peers and the pinned deployed launchers, not invented tool output."""
import ast
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import socket
import select
import signal
import pwd
import shutil
import concurrent.futures
import uuid
import threading
import time
import fcntl
import unittest
import input_closure
from unittest.mock import patch
import admission_policy as policy

HERE = Path(__file__).resolve().parent


def load(file, name):
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


cache = load(HERE/'bb-cache.py', 'cache_authority_tests')
job = load(HERE/'bb-job.py', 'cache_job_tests')


class CachePolicy(unittest.TestCase):
    def test_canonical_policy_and_disagreement_before_any_spawn(self):
        self.assertEqual(policy.cache_environment({})['SCCACHE_CACHE_SIZE'], '50G')
        self.assertEqual(policy.SCCACHE_CACHE_BYTES, 53687091200)
        for env in [{'SCCACHE_CACHE_SIZE': '400G'}, {'SCCACHE_DIR': '/tmp'},
                    {'SCCACHE_SERVER_PORT': '1234'}, {'SCCACHE_IDLE_TIMEOUT': '120'},
                    {'SCCACHE_CLIENT_SIDE':'0'}, {'SCCACHE_CONF':'/tmp/config'},
                    {'SCCACHE_ERROR_LOG':'/tmp/log'}, {'SCCACHE_DIST_SCHEDULER_URL':'http://localhost'}]:
            with self.subTest(env=env), patch.object(subprocess, 'run') as spawned:
                with self.assertRaisesRegex(policy.CachePolicyError, 'kind=CACHE_POLICY'):
                    cache.admitted(env)
                spawned.assert_not_called()

    def test_missing_service_is_typed_before_compiler_or_uncached_bypass(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as root:
            with patch.object(cache, 'CACHE_ROOT', Path(root)/'absent'), patch.object(os, 'execvpe') as compiler:
                with self.assertRaisesRegex(policy.CachePolicyError, 'recovery='):
                    cache.compiler(['/usr/bin/true', '-Cincremental=control'])
                compiler.assert_not_called()

    def test_host_owner_cannot_start_in_a_private_job_namespace(self):
        # Exercise the actual boundary also when bb-install qualifies directly
        # on the host: this private init is Python rather than host systemd.
        result=subprocess.run(['sudo','-n','unshare','--mount','--pid','--fork','--mount-proc',
                               '/usr/bin/python3','-B',str(HERE/'bb-cache.py'),'serve'],
                              capture_output=True,text=True)
        self.assertEqual(result.returncode,224,result.stderr)
        self.assertIn('host cache authority cannot run in a job PID namespace',result.stderr)

    def test_both_actual_entry_points_use_one_authority(self):
        marker = policy.CachePolicyError('unknown', detail='entry-point control')
        with patch.object(cache, 'admitted', side_effect=marker) as admitted:
            with self.assertRaises(policy.CachePolicyError): cache.compiler(['/usr/bin/true'])
            admitted.assert_called_once()
        with patch.object(job, 'cache_authority', return_value=cache), patch.object(cache, 'admitted', side_effect=marker):
            with self.assertRaises(policy.CachePolicyError): job.start_cache(Path('/unused'), {})
        wrapper = (HERE/'sccache-rustc').read_text()
        self.assertNotIn('SCCACHE_CACHE_SIZE', wrapper)
        self.assertNotIn('--start-server', wrapper)
        self.assertNotIn('400G', (HERE/'buildbox.mjs').read_text())

    def test_shared_owner_teardown_and_cpu_are_never_charged_per_client(self):
        owner = {'scope': 'host-shared', 'pid': os.getpid(), 'identity': job.identity(os.getpid())}
        with patch.object(job.os, 'pidfd_open') as signalled, patch.object(job, 'child_cpu', return_value=7):
            job.stop_server(owner); signalled.assert_not_called()
            result = job.cost_receipt({}, owner, 2, 100, 0)
        self.assertEqual(result['attributed_cpu_seconds'], 5)
        self.assertIsNone(result['owned_cache_cpu_seconds'])
        self.assertIn('unattributed', result['cpu_coverage'])

    def test_pressure_uses_bounded_existing_accounting_and_retains_floor(self):
        self.assertIsNone(policy.disk_pressure([{'free_percent':16}]))
        with patch.object(Path,'open',side_effect=OSError('accounting absent')):
            report=policy.disk_pressure([{'free_percent':9}])
        self.assertIn('BB_DISK_PRESSURE',report);self.assertIn('accounting_unavailable',report)
        self.assertTrue(policy.disk_low([{'free_percent':9}]))
        self.assertFalse(policy.disk_low([{'free_percent':10}]))
        for file in ['bb-job.py','bb-admit.py']:
            source=(HERE/file).read_text()
            self.assertIn('disk_pressure(',source);self.assertIn('disk_low(',source)

    def test_actual_pinned_old_launcher_would_spawn_400g(self):
        fixtures = HERE/'fixtures/cache-policy'
        provenance = json.loads((fixtures/'origin.json').read_text())
        for name, info in provenance['files'].items():
            self.assertEqual(hashlib.sha256((fixtures/name).read_bytes()).hexdigest(), info['sha256'])
        original = ast.parse((fixtures/'bb-job.pinned.py').read_text())
        selected = [node for node in original.body if isinstance(node, ast.FunctionDef) and node.name == 'start_cache']
        namespace = {'subprocess': subprocess, 'SCCACHE': cache.SCCACHE, 'sys': sys}
        exec(compile(ast.Module(body=selected, type_ignores=[]), 'actual-pinned-start-cache', 'exec'), namespace)
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as root:
            with patch.object(subprocess, 'run', side_effect=OSError('intercept before spawn')) as spawned:
                namespace['start_cache'](Path(root), {})
                self.assertEqual(spawned.call_args.kwargs['env']['SCCACHE_CACHE_SIZE'], '400G')
            # Actual old shell adapter's incremental bypass still exposes its
            # unconditional policy override, without starting another cache.
            old = subprocess.run(['bash', str(fixtures/'sccache-rustc.pinned'), '/usr/bin/python3', '-c',
                                  'import os;print(os.environ["SCCACHE_CACHE_SIZE"])', 'incremental=control'],
                                 capture_output=True, text=True, check=True)
            self.assertEqual(old.stdout.strip(), '400G')


class HostAuthority(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from cpu_test_fixture import KernelFixture
        cls.cpu_fixture=KernelFixture()
        cls.root=Path('/run/buildbox-cache-test-'+uuid.uuid4().hex);cls.root.mkdir(mode=0o755)
        cls.data=Path(tempfile.mkdtemp(prefix='cache-policy-',dir='/mnt/bb/tmp'))
        account=pwd.getpwnam('var');os.chown(cls.data,account.pw_uid,account.pw_gid)
        cls.stage=cls.root/'bin';cls.stage.mkdir()
        # Same production entry points; only the two policy storage roots are
        # relocated to this isolated real-server fixture, never shared data.
        for name in ['bb-cache.py','sccache-rustc','admission_policy.py']:
            source=(HERE/name).read_bytes()
            if name=='admission_policy.py':
                source=source.replace(b"Path('/run/buildbox-cache')",repr(cls.root).replace('PosixPath','Path').encode())
                source=source.replace(b"'/mnt/bb/sccache'",repr(str(cls.data)).encode())
            (cls.stage/name).write_bytes(source);(cls.stage/name).chmod(0o555)
        sys.path.insert(0,str(cls.stage))
        cls.original_policy=sys.modules['admission_policy']
        cls.policy=load(cls.stage/'admission_policy.py','staged_cache_policy')
        sys.modules['admission_policy']=cls.policy
        try:cls.service=load(cls.stage/'bb-cache.py','staged_cache_authority')
        finally:sys.modules['admission_policy']=cls.original_policy;sys.path.pop(0)
        cls.ready=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
        cls.ready.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
        cls.ready.bind(str(cls.root/'notify'));cls.ready.settimeout(5)
        cls.log=open(cls.data/'authority.log','wb')
        launch_env={key:value for key,value in os.environ.items() if not key.startswith('SCCACHE_')}
        launch_env['NOTIFY_SOCKET']=str(cls.root/'notify')
        starts=[subprocess.Popen(['/usr/bin/python3','-B',str(cls.stage/'bb-cache.py'),'serve'],
                     env=launch_env,stdout=cls.log,stderr=cls.log,
                     preexec_fn=lambda:__import__('resource').setrlimit(__import__('resource').RLIMIT_NOFILE,(32,32))) for _ in range(2)]
        try:
            packet,ancillary,_,_=cls.ready.recvmsg(128,socket.CMSG_SPACE(12))
            assert packet==b'READY=1'
            authority_pid=next(__import__('struct').unpack('3i',value)[0] for level,kind,value in ancillary
                               if level==socket.SOL_SOCKET and kind==socket.SCM_CREDENTIALS)
            cls.process=next(process for process in starts if process.pid==authority_pid)
            cls.loser=next(process for process in starts if process is not cls.process)
            assert cls.loser.wait(timeout=5)==224
        except BaseException:
            for process in starts:
                if process.poll() is None:process.terminate()
                process.wait()
            cls.log.close()
            print((cls.data/'authority.log').read_text(),file=sys.stderr);raise
        cls.owner=cls.service.validate_server()

    @classmethod
    def tearDownClass(cls):
        descriptor=os.pidfd_open(cls.owner['pid'])
        try:
            if cls.service.birth(cls.owner['pid'])==cls.owner['identity']:
                signal.pidfd_send_signal(descriptor,signal.SIGTERM)
        finally:os.close(descriptor)
        cls.process.wait(timeout=5);cls.log.close();cls.ready.close()
        shutil.rmtree(cls.root);shutil.rmtree(cls.data)
        cls.cpu_fixture.close()

    def client_env(self,extra=None):
        env={k:v for k,v in self.cpu_fixture.environment().items() if not k.startswith('SCCACHE_')}
        # These clients own isolated non-repository fixtures. sudo deliberately
        # does not forward the enclosing producer's declaration; each client
        # declares its own complete closure instead of inheriting unrelated
        # source/base/reference inputs from another receipt.
        input_closure.declare(env,root=self.data)
        env.update(extra or {});return env

    def test_concurrent_starts_leave_one_birth_verified_owner(self):
        self.assertEqual(self.loser.returncode,224)
        second=subprocess.run(['/usr/bin/python3','-B',str(self.stage/'bb-cache.py'),'serve'],
                              env=self.client_env(),capture_output=True,text=True)
        self.assertEqual(second.returncode,224,second.stderr)
        self.assertEqual(self.service.validate_server(),self.owner)
        self.assertEqual(len(list(self.service.cache_owners())),1)

    def test_cold_compiler_stays_in_lease_with_actual_server_side_negative(self):
        controls=load(HERE/'cpu_leases.test.py','cache_cpu_kernel_controls').KernelLeases
        controls.setUpClass()
        try:
            probe=self.stage/'compiler-probe'
            # Keep rustup's multicall basename; resolving the symlink would
            # execute `rustup --crate-name ...` rather than rustc.
            compiler=Path('/home/var/.cargo/bin/rustc')
            probe.write_text('#!/usr/bin/python3 -B\nimport os,json,sys\nfrom pathlib import Path\n'
                'with open(os.environ["CPU_COMPILER_TRACE"],"a") as stream:stream.write(json.dumps({"cpus":sorted(os.sched_getaffinity(0)),"cgroup":Path("/proc/self/cgroup").read_text(),"argv":sys.argv[1:]})+"\\n")\n'
                'os.execv('+repr(str(compiler))+',['+repr(str(compiler))+',*sys.argv[1:]])\n')
            probe.chmod(0o555)
            for negative in [False,True]:
                name='lease_control_'+uuid.uuid4().hex
                source=self.data/(name+'.rs');source.write_text('pub fn answer() -> u32 { 42 }\n')
                trace=self.data/(name+'.jsonl')
                env=controls.env.copy()
                for key in list(env):
                    if key.startswith('SCCACHE_'):env.pop(key)
                env.update(self.policy.cache_environment({}));env['CPU_COMPILER_TRACE']=str(trace)
                env['RUSTUP_HOME']='/home/var/.rustup'
                env['TMPDIR']=str(self.data)
                # Execute the actual default server-side path as the negative,
                # rather than asserting only a configuration constant.
                if negative:env.pop('SCCACHE_CLIENT_SIDE')
                entry=[self.service.SCCACHE] if negative else [str(self.stage/'sccache-rustc')]
                command=[*entry,str(probe),'--crate-name',name,'--crate-type','rlib',
                         '--emit=link','--out-dir',str(self.data),str(source)]
                result=subprocess.run(['sudo','-n','--preserve-env','/usr/bin/python3','-B',str(controls.module),
                    'exec','0',uuid.uuid4().hex,'cloud','--',*command],env=env,capture_output=True,text=True)
                self.assertEqual(result.returncode,0,result.stderr)
                rows=[json.loads(line) for line in trace.read_text().splitlines()]
                codegen=[row for row in rows if '--crate-name' in row['argv']]
                self.assertTrue(codegen,'a cold real compiler invocation must be observed')
                expected=set(policy.cpu_layout()[0])
                if negative:self.assertTrue(any(set(row['cpus'])!=expected for row in codegen))
                else:
                    for row in codegen:self.assertEqual(set(row['cpus']),expected)
                print('CPU_CACHE_COMPILER '+json.dumps({'negative_server_side':negative,'codegen':codegen}),file=sys.stderr)
        finally:controls.tearDownClass()

    def test_actual_job_reuses_owner_and_teardown_leaves_it_alive(self):
        with patch.object(job,'cache_authority',return_value=self.service):
            first,status=job.start_cache(self.data,{})
            second,nested_status=job.start_cache(self.data,{})
        self.assertEqual((status,nested_status),('ready','ready'))
        self.assertEqual(first,second);self.assertEqual(first,self.owner)
        job.stop_server(first)
        self.assertEqual(self.service.validate_server(),self.owner)
        stats=subprocess.run([self.service.SCCACHE,'--show-stats','--stats-format','json'],
                             env=self.policy.cache_environment({}),capture_output=True,text=True,check=True)
        self.assertEqual(json.loads(stats.stdout)['max_cache_size'],53687091200)

    def test_fresh_targets_reuse_compiler_outputs_with_incremental_negative_control(self):
        # One real tiny Rust producer, distinct from product qualification.
        # Fresh Cargo target directories alone do not measure compiler hits.
        name='reuse_control_'+uuid.uuid4().hex
        source=self.data/(name+'.rs');source.write_text('pub fn value()->u64 { 42 }\n')
        env=self.client_env({'RUSTUP_HOME':'/home/var/.rustup','TMPDIR':str(self.data)})
        def hits():
            result=subprocess.run([self.service.SCCACHE,'--show-stats','--stats-format','json'],
                env=self.policy.cache_environment(env),capture_output=True,text=True,check=True)
            stats=json.loads(result.stdout)
            def total(value):return sum(total(v) for v in value.values()) if isinstance(value,dict) else value
            return total(stats['stats']['cache_hits']['counts'])
        observations=[]
        for mode in ['cold','warm','incremental']:
            target=self.data/(name+'-'+mode);target.mkdir(mode=0o777);target.chmod(0o777)
            before=hits()
            command=['runuser','-u','var','--',str(self.stage/'sccache-rustc'),
                '/home/var/.cargo/bin/rustc','--crate-name',name,'--crate-type','rlib',
                '--emit=link','--out-dir',str(target),str(source)]
            if mode=='incremental':command+=['-C','incremental='+str(target/'incremental')]
            result=subprocess.run(command,env=env,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            observations.append(hits()-before)
        self.assertEqual(observations,[0,1,0],observations)

    def test_actual_wrapper_metadata_and_nested_namespace_reuse(self):
        command=['runuser','-u','var','--',str(self.stage/'sccache-rustc'),'/home/var/.cargo/bin/rustc','-vV']
        first=subprocess.run(command,env=self.client_env(),capture_output=True,text=True)
        self.assertEqual(first.returncode,0,first.stderr);self.assertIn('rustc ',first.stdout)
        nested=subprocess.run(['unshare','--mount','--pid','--fork','--mount-proc',*command],
                              env=self.client_env(),capture_output=True,text=True)
        self.assertEqual(nested.returncode,0,nested.stderr)
        self.assertEqual(self.service.validate_server(),self.owner)

    def test_concurrent_clients_share_one_owner_and_cannot_lazy_restart(self):
        def invoke(_):
            return subprocess.run(['runuser','-u','var','--',str(self.stage/'sccache-rustc'),
                                   '/home/var/.cargo/bin/rustc','-vV'],env=self.client_env(),capture_output=True)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results=list(pool.map(invoke,range(50)))
        self.assertTrue(all(result.returncode==0 for result in results),[r.stderr for r in results if r.returncode])
        denied=subprocess.run(['runuser','-u','var','--','touch',str(self.service.CACHE_ENDPOINT.parent/'rogue')],
                              capture_output=True)
        self.assertNotEqual(denied.returncode,0)
        self.assertEqual(self.service.validate_server(),self.owner)

    def test_idle_clients_cannot_consume_authority_descriptors_and_actual_old_loop_fails(self):
        before=len(list(Path('/proc',str(self.process.pid),'fd').iterdir()))
        idle=[]
        try:
            for _ in range(128):
                connection=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
                connection.connect(str(self.service.CONTROL));idle.append(connection)
            self.assertEqual(len(list(Path('/proc',str(self.process.pid),'fd').iterdir())),before)
            self.assertEqual(self.service.admitted({}),self.owner)
            self.assertIsNone(self.process.poll())
        finally:
            for connection in idle:connection.close()
        # Execute the captured former listener, with the same real server,
        # under a low descriptor budget. No invented external-tool responses.
        fixture=HERE/'fixtures/cache-policy/former-control-loop.py'
        info=json.loads(fixture.with_name('former-control-loop.origin.json').read_text())
        self.assertEqual(hashlib.sha256(fixture.read_bytes()).hexdigest(),info['sha256'])
        endpoint=self.root/'former-control.sock';notify=self.root/'former-notify.sock'
        script='''import sys,resource,json,os
from pathlib import Path
sys.path.insert(0,sys.argv[1]);import importlib.util
spec=importlib.util.spec_from_file_location('old_authority',Path(sys.argv[1])/'bb-cache.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.CONTROL=Path(sys.argv[2]);m.owner=json.loads(sys.argv[3]);os.environ['NOTIFY_SOCKET']=sys.argv[4]
resource.setrlimit(resource.RLIMIT_NOFILE,(32,32))
exec(Path(sys.argv[5]).read_text(),m.__dict__)
'''
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as ready:
            ready.bind(str(notify));ready.settimeout(3)
            old=subprocess.Popen([sys.executable,'-B','-c',script,str(self.stage),str(endpoint),
                                  json.dumps(self.owner),str(notify),str(fixture)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            clients=[]
            try:
                self.assertEqual(ready.recv(128),b'READY=1')
                for _ in range(64):
                    connection=socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET)
                    connection.setblocking(False);clients.append(connection)
                    try:connection.connect(str(endpoint))
                    except (BlockingIOError,ConnectionRefusedError,FileNotFoundError):pass
                output,error=old.communicate(timeout=3)
                self.assertNotEqual(old.returncode,0,output.decode())
                self.assertIn(b'Too many open files',error)
            finally:
                for connection in clients:connection.close()
                if old.poll() is None:old.kill();old.communicate(timeout=3)

    def test_atomic_requests_reject_bad_or_abandoned_reply_channels_without_fd_leaks(self):
        import array
        before=len(list(Path('/proc',str(self.process.pid),'fd').iterdir()))
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as sender:
            sender.connect(str(self.service.CONTROL))
            for data in [b'invalid',b'verify'*20]:
                receiver,reply=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
                with receiver,reply:
                    receiver.settimeout(3)
                    sender.sendmsg([data],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[reply.fileno()]))])
                    self.assertIn('error',json.loads(receiver.recv(16384)))
            # A closed reply peer, extra descriptors and a non-socket fd are
            # all bounded single-packet work; a subsequent valid request joins
            # the FIFO after them and proves the authority remains alive.
            with open('/dev/null') as file:
                sender.sendmsg([b'verify'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[file.fileno()]))])
            receiver,reply=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            receiver.close()
            with reply:
                sender.sendmsg([b'verify'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[reply.fileno(),reply.fileno()]))])
            self.assertEqual(self.service.admitted({}),self.owner)
        self.assertEqual(len(list(Path('/proc',str(self.process.pid),'fd').iterdir())),before)

    def test_authority_death_refuses_inflight_request_and_actual_old_datagram_hangs(self):
        fixture=HERE/'fixtures/cache-policy/former-reply-channel.py'
        info=json.loads(fixture.with_suffix('.origin.json').read_text())
        self.assertEqual(hashlib.sha256(fixture.read_bytes()).hexdigest(),info['sha256'])
        driver='''import sys,importlib.util
from pathlib import Path
sys.path.insert(0,sys.argv[1])
spec=importlib.util.spec_from_file_location('client',Path(sys.argv[1])/'bb-cache.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.CONTROL=Path(sys.argv[2])
if sys.argv[3]:exec(Path(sys.argv[3]).read_text(),m.__dict__)
try:m.compiler(['/bin/true'])
except m.CachePolicyError as error:print(error,file=sys.stderr);raise SystemExit(224)
'''
        for negative in [False,True]:
            endpoint=self.root/('death-'+str(negative)+'.sock')
            reader,writer=os.pipe()
            listener=socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM)
            listener.bind(str(endpoint));listener.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
            server=os.fork()
            if server==0:
                try:
                    os.close(reader)
                    if negative:exec(fixture.read_text(),self.service.__dict__)
                    validate=self.service.validate_server
                    def joined_validation(**kwargs):
                        validate(**kwargs)
                        os.write(writer,b'1')
                        signal.pause()
                    self.service.validate_server=joined_validation
                    self.service.verify_request(listener,self.owner)
                finally:os._exit(1)
            os.close(writer);listener.close()
            client=subprocess.Popen([sys.executable,'-B','-c',driver,str(self.stage),str(endpoint),
                                     str(fixture) if negative else ''],env=self.client_env(),
                                    stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.assertTrue(select.select([reader],[],[],3)[0],'request reached real server validation')
                self.assertEqual(os.read(reader,1),b'1')
                os.kill(server,signal.SIGKILL);os.waitpid(server,0);server=None
                if negative:
                    with self.assertRaises(subprocess.TimeoutExpired):client.communicate(timeout=0.2)
                else:
                    output,error=client.communicate(timeout=3)
                    self.assertEqual(client.returncode,224,output.decode()+error.decode())
                    self.assertIn(b'kind=CACHE_POLICY',error)
                    self.assertIn(b'authority unavailable',error)
            finally:
                os.close(reader)
                if server is not None:os.kill(server,signal.SIGKILL);os.waitpid(server,0)
                if client.poll() is None:client.kill();client.communicate(timeout=3)
                endpoint.unlink()

    def test_wrong_and_reused_peer_identity_refused(self):
        for changed in [{'identity':'different-birth'},{'pid':self.owner['pid']+1}]:
            with self.assertRaisesRegex(self.policy.CachePolicyError,'peer/birth'):
                self.service.validate_server(expected={**self.owner,**changed})
        # The actual kernel peer is used, not an identity supplied by a client.
        with socket.socket(socket.AF_UNIX) as impostor:
            endpoint=self.root/'impostor.sock';impostor.bind(str(endpoint));impostor.listen()
            with self.assertRaisesRegex(self.policy.CachePolicyError,'wrong server peer'):
                self.service.validate_server(endpoint)

    def test_server_launch_policy_unknown_or_legacy_refused_even_with_good_client_env(self):
        for cap in ['400G',None]:
            directory=self.root/('legacy-'+str(cap));directory.mkdir()
            user=pwd.getpwnam('var');os.chown(directory,user.pw_uid,user.pw_gid)
            endpoint=directory/'sccache.sock'
            # Deliberate authority bypass in this isolated negative fixture.
            bad=self.policy.cache_environment({},endpoint)
            if cap is None:bad.pop('SCCACHE_CACHE_SIZE')
            else:bad['SCCACHE_CACHE_SIZE']=cap
            result=subprocess.run(['/usr/sbin/runuser','-u','var','--',self.service.SCCACHE,'--start-server'],
                                  env=bad,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            with socket.socket(socket.AF_UNIX) as connection:
                connection.connect(str(endpoint));pid,_,_=self.service.peer(connection)
                identity=self.service.birth(pid)
            try:
                with patch.dict(os.environ,{'SCCACHE_CACHE_SIZE':'50G'}):
                    with self.assertRaisesRegex(self.policy.CachePolicyError,'server launch policy'):
                        self.service.validate_server(endpoint)
            finally:
                descriptor=os.pidfd_open(pid)
                try:
                    if self.service.birth(pid)==identity:signal.pidfd_send_signal(descriptor,signal.SIGTERM)
                    __import__('select').select([descriptor],[],[])
                finally:os.close(descriptor)
        # Negative control: the former client-only override says 50G while
        # the real pinned production policy starts with 400G (tested above).
        self.assertEqual(self.policy.cache_environment({})['SCCACHE_CACHE_SIZE'],'50G')

    def test_wrong_policy_through_actual_wrapper_never_executes_child(self):
        marker=self.data/'must-not-exist'
        result=subprocess.run(['runuser','-u','var','--',str(self.stage/'sccache-rustc'),
                               '/usr/bin/touch',str(marker)],
                              env=self.client_env({'SCCACHE_CACHE_SIZE':'400G'}),capture_output=True,text=True)
        self.assertEqual(result.returncode,224,result.stderr)
        self.assertIn('kind=CACHE_POLICY',result.stderr);self.assertFalse(marker.exists())

    def test_migration_drains_queued_clients_before_idle_daemon_restart(self):
        root=self.data/'admit';root.mkdir();(root/'admit.conf').write_text('compile=1 tenant.cloud=1\n')
        user=pwd.getpwnam('var');os.chown(root,user.pw_uid,user.pw_gid)
        os.chown(root/'admit.conf',user.pw_uid,user.pw_gid)
        release=self.data/'release';os.mkfifo(release,0o666);release.chmod(0o666)
        done=self.data/'second-finished'
        env=self.client_env({'BB_ADMIT_ROOT':str(root),'BB_ADMIT_WRAPPED_COMMAND':'1','BB_CACHE_DISABLED':'1'})
        entry=['/usr/sbin/runuser','-u','var','--','/usr/bin/python3','-B',str(self.cpu_fixture.control/'bb-admit.py'),'lane','cloud','--','/usr/bin/python3','-c']
        first=subprocess.Popen([*entry,f'print("ready",flush=True);open({str(release)!r}).read()'],
                               env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        ready=first.stdout.readline()
        if ready!=b'ready\n':
            out,err=first.communicate(timeout=2)
            self.fail('migration client exited before readiness: '+err.decode()+repr(out))
        second=subprocess.Popen([*entry,f'import time;time.sleep(.1);open({str(done)!r},"w").write("finished")'],
                                env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        # Observe the real accepted ticket; timing cannot stand in for a queued
        # handshake. The first command still owns the sole slot.
        admitted=second.stderr.readline()
        self.assertTrue(admitted.startswith(b'BB_INPUTS_ADMITTED '),admitted.decode())
        proof=json.loads(admitted.removeprefix(b'BB_INPUTS_ADMITTED '))
        self.assertIsInstance(proof,dict)
        ticket=second.stderr.readline()
        self.assertTrue(ticket.startswith(b'BB_ADMIT_TICKET '),ticket.decode())
        errors=[]
        def drain():
            try:
                with (root/'upgrade.lock').open('a') as upgrade:
                    fcntl.flock(upgrade,fcntl.LOCK_EX);self.service.drain_admission(root)
            except BaseException as error:errors.append(error)
        thread=threading.Thread(target=drain);thread.start()
        try:
            with release.open('w') as stream:stream.write('complete')
            first_out,first_err=first.communicate(timeout=5)
            second_out,second_err=second.communicate(timeout=5)
            thread.join(5)
            self.assertFalse(thread.is_alive(),'migration must finish after both clients')
            self.assertEqual(errors,[])
            self.assertEqual((first.returncode,second.returncode),(0,0),first_err+second_err)
            self.assertTrue(done.exists())
        finally:
            for process in [first,second]:
                if process.poll() is None:process.terminate();process.wait()
            state=json.loads((root/'daemon.json').read_text());pid=state['pid']
            if self.service.birth(pid)==state['identity']:
                descriptor=os.pidfd_open(pid)
                try:signal.pidfd_send_signal(descriptor,signal.SIGTERM)
                finally:os.close(descriptor)


if __name__ == '__main__':
    if sys.argv[1:]==['--host-controls']:
        result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(HostAuthority))
        raise SystemExit(0 if result.wasSuccessful() else 1)
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(CachePolicy))
    if not result.wasSuccessful():raise SystemExit(1)
    raise SystemExit(subprocess.call(['sudo','-n','/usr/bin/python3','-B',str(Path(__file__).resolve()),'--host-controls']))
