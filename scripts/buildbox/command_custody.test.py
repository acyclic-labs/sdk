"""Command retirement and export ownership inside the enclosing job namespace."""
import importlib.util
import ast
import json
import os
from pathlib import Path
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import tarfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('custody_job', HERE / 'bb-job.py')
job = importlib.util.module_from_spec(spec)
spec.loader.exec_module(job)


class CommandCustodyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='command-custody-', dir='/mnt/bb/tmp')
        self.d = Path(self.temp.name)
        shutil.copyfile(HERE / 'bb_target_store.py', self.d / 'bb_target_store.py')

    def tearDown(self):
        self.temp.cleanup()

    def module(self, negative=False):
        shutil.copyfile(HERE / 'bb_target_store.py', self.d / 'bb_target_store.py')
        source = (HERE / 'bb-job.py').read_text()
        if negative:
            boundary = 'start_new_session=True)'
            self.assertEqual(source.count(boundary), 1)
            source = source.replace(boundary, 'start_new_session=False)')
        path = self.d / ('negative.py' if negative else 'worker.py')
        path.write_text(source)
        return path

    def custody_case(self, interrupted, negative=False):
        fifo = self.d / 'release'
        os.mkfifo(fifo)
        marker = self.d / 'late-effect'
        owned = Path(tempfile.mkdtemp(prefix='job-custody-', dir='/mnt/bb/jobs'))
        runtime = self.d / 'runtime.json'
        code = '''import json,os,select,sys
from pathlib import Path
ready_read,ready_write=os.pipe()
child=os.fork()
if child==0:
 os.close(ready_read)
 release=os.open(sys.argv[1],os.O_RDONLY|os.O_NONBLOCK)
 print(json.dumps({'pid':os.getpid(),'root':os.getppid()}),flush=True)
 os.write(ready_write,b'S');os.close(ready_write)
 select.select([release],[],[])
 if os.read(release,1):Path(sys.argv[2]).write_text('after cleanup')
 os._exit(0)
os.close(ready_write);os.read(ready_read,1);os.close(ready_read)
if sys.argv[3]=='normal':sys.exit(0)
os.waitpid(child,0)
'''
        # Exercise the same production owner/receipt retirement functions in the
        # current namespace. The outer build job already owns its private proc.
        fixture = '''import importlib.util,json,signal,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1]).resolve().parent))
spec=importlib.util.spec_from_file_location('owner',sys.argv[1])
job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
runtime={'hermetic_home':sys.argv[2],'targets_policy':'job-owned'}
def register(command):
 runtime['command']=command;job.atomic(sys.argv[3],runtime)
def interrupted(number,frame):raise SystemExit(128+number)
signal.signal(signal.SIGTERM,interrupted)
command=job.OwnedCommand(sys.argv[4:],dict(__import__('os').environ),register)
try:command.wait()
finally:
 command.close();job.cleanup_runtime(sys.argv[3])
'''
        worker = subprocess.Popen([sys.executable, '-c', fixture, str(self.module(negative)),
            str(owned), str(runtime), sys.executable, '-c', code, str(fifo), str(marker),
            'normal' if interrupted is None else 'signal'], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, start_new_session=True)
        descriptors = []
        try:
            self.assertTrue(select.select([worker.stdout], [], [], 2)[0], 'fixture did not start')
            line = worker.stdout.readline()
            if not line:
                output, errors = worker.communicate(timeout=2)
                self.fail('custody fixture exited before admission: ' + errors.decode())
            descendants = json.loads(line)
            for pid in descendants.values():
                try:
                    descriptors.append(os.pidfd_open(pid))
                except ProcessLookupError:
                    pass
            if interrupted is not None:
                worker.send_signal(interrupted)
            worker.wait(timeout=2)
            if interrupted == signal.SIGKILL:
                job.cleanup_runtime(runtime)
            self.assertFalse(owned.exists())
            if negative:
                self.assertIsNotNone(job.identity(descendants['pid']))
                release = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
                os.write(release, b'S')
                os.close(release)
                worker.communicate(timeout=2)
                self.assertEqual(marker.read_text(), 'after cleanup')
            else:
                for descriptor in descriptors:
                    self.assertTrue(select.select([descriptor], [], [], 2)[0],
                                    'descendant survived cleanup')
                output, errors = worker.communicate(timeout=2)
                self.assertEqual(worker.returncode, 0 if interrupted is None else 128 + interrupted
                                 if interrupted != signal.SIGKILL else -signal.SIGKILL,
                                 errors.decode())
                self.assertFalse(marker.exists())
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait()
            for descriptor in descriptors:
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                os.close(descriptor)
            worker.communicate(timeout=2)
            if runtime.exists():
                job.cleanup_runtime(runtime)
            elif owned.exists():
                job.remove_home(owned)
            fifo.unlink(missing_ok=True)
            marker.unlink(missing_ok=True)

    def test_command_custody_drains_descendants_on_direct_signal_and_parent_death(self):
        for interrupted in [signal.SIGTERM, signal.SIGKILL]:
            with self.subTest(signal=interrupted):
                self.custody_case(interrupted)

    def test_completed_shell_keeps_descendant_custody_until_cleanup(self):
        self.custody_case(None)

    def test_missing_owned_group_allows_side_effects_after_cleanup(self):
        for interrupted in [None, signal.SIGTERM, signal.SIGKILL]:
            with self.subTest(signal=interrupted):
                self.custody_case(interrupted, negative=True)

    def test_command_supervision_has_no_unchecked_cli_entry(self):
        source = (HERE / 'bb-job.py').read_text()
        def probe(text):
            entry = ast.parse(text).body[-1]
            self.assertIsInstance(entry, ast.If)
            code = compile(ast.Module(body=[entry], type_ignores=[]), '<production entry>', 'exec')
            for arguments in [['supervise', '17', '19', 'fixture'], ['unknown-command']]:
                run = unittest.mock.Mock(return_value=0)
                unchecked = unittest.mock.Mock(return_value=0)
                namespace = {'__name__': '__main__', 'sys': type('Args', (), {'argv': ['bb-job.py', *arguments]}),
                             'run': run, 'supervise': unchecked}
                with self.assertRaises(SystemExit):
                    exec(code, namespace)
                run.assert_called_once_with(arguments)
                unchecked.assert_not_called()
        probe(source)
        boundary = "    if sys.argv[1:2]==['home-namespace']:"
        self.assertEqual(source.count(boundary), 1)
        former = source.replace(boundary,
            "    if sys.argv[1:2]==['supervise']: raise SystemExit(supervise(17,19,sys.argv[4:]))\n"
            "    elif sys.argv[1:2]==['home-namespace']:")
        with self.assertRaises(AssertionError):
            probe(former)

    def test_foreign_namespace_never_observes_or_signals_numeric_command_pid(self):
        def probe(module):
            with patch.object(module, 'identity', return_value=None) as observed:
                module.stop_command({'pid': 123, 'identity': 'owned', 'pid_namespace': 'foreign'})
                module.stop_command({'pid': 123, 'identity': 'owned'})
                observed.assert_not_called()
        probe(job)
        source = (HERE / 'bb-job.py').read_text()
        boundary = "if not command or command.get('pid_namespace') != pid_namespace(): return"
        self.assertEqual(source.count(boundary), 1)
        path = self.d / 'foreign-namespace-negative.py'
        path.write_text(source.replace(boundary, 'if not command: return'))
        spec = importlib.util.spec_from_file_location('foreign_negative', path)
        former = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(former)
        with self.assertRaises(AssertionError):
            probe(former)

    def test_command_cleanup_never_waits_for_a_reused_pid(self):
        def probe(module, before, after):
            with (patch.object(module,'identity',side_effect=[before,after]),
                  patch.object(module.os,'pidfd_open',return_value=71) as opened,
                  patch.object(module.os,'close') as closed,
                  patch.object(module.signal,'pidfd_send_signal') as sent,
                  patch.object(module.select,'select') as waited):
                module.stop_command({'pid':123,'identity':'owned','pid_namespace':job.pid_namespace()})
                if before!='owned':
                    opened.assert_not_called();closed.assert_not_called()
                else:
                    closed.assert_called_once_with(71)
                if before==after=='owned':
                    sent.assert_called_once_with(71,signal.SIGKILL,None,1 << 2)
                    waited.assert_called_once_with([71],[],[])
                else:
                    sent.assert_not_called();waited.assert_not_called()
        for before in ['owned','replacement',None]:
            for after in ['owned','replacement',None]:
                with self.subTest(before=before,after=after):probe(job,before,after)
        source=(HERE/'bb-job.py').read_text()
        fixed='''        if identity(command['pid']) != command['identity']: return
'''
        former='''        if identity(command['pid']) != command['identity']:
            select.select([descriptor], [], [])
            return
'''
        self.assertEqual(source.count(fixed),1)
        # Restore the former mismatched-identity path which still waited.
        path=self.d/'reused-pid-negative.py';path.write_text(source.replace(fixed,former))
        spec=importlib.util.spec_from_file_location('reused_pid_negative',path)
        negative=importlib.util.module_from_spec(spec);spec.loader.exec_module(negative)
        with self.assertRaises(AssertionError):probe(negative,'owned','replacement')

    def test_stopped_supervisor_retires_through_both_owners(self):
        import select
        source=(HERE/'bb-job.py').read_text()
        boundary='signal.pidfd_send_signal(descriptor, signal.SIGKILL, None, PIDFD_SIGNAL_PROCESS_GROUP)'
        self.assertEqual(source.count(boundary),1)
        negative=self.d/'stopped-supervisor-negative.py'
        negative.write_text(source.replace(boundary,'signal.pidfd_send_signal(descriptor, signal.SIGTERM)'))
        fixture='''import importlib.util,json,os,signal,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1]).resolve().parent))
spec=importlib.util.spec_from_file_location('custody',sys.argv[1])
job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
child="import json,os,signal;print(json.dumps({'descendant':os.getpid()}),flush=True);signal.pause()"
command=job.OwnedCommand([sys.executable,'-c',child],dict(os.environ),lambda owner:None)
print(json.dumps({'owner':command.command}),flush=True)
sys.stdin.read(1)
os.kill(command.child.pid,signal.SIGSTOP)
_,status=os.waitpid(command.child.pid,os.WUNTRACED)
assert os.WIFSTOPPED(status)
print('stopped',flush=True)
sys.stdin.read(1)
if sys.argv[2]=='receipt':job.stop_command(command.command)
command.close()
print('retired',flush=True)
'''
        def assert_retired(ready):
            self.assertTrue(ready,'stopped supervisor blocked owned-group retirement')
        for mode in ['receipt','command']:
            for broken in [False,True]:
                with self.subTest(mode=mode,broken=broken):
                    worker=subprocess.Popen([sys.executable,'-c',fixture,str(negative if broken else HERE/'bb-job.py'),mode],
                        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    descriptors=[];owner=None
                    try:
                        rows=[json.loads(worker.stdout.readline()) for _ in range(2)]
                        owner=next(row['owner'] for row in rows if 'owner' in row)
                        descendant=next(row['descendant'] for row in rows if 'descendant' in row)
                        for pid in [owner['pid'],descendant]:descriptors.append(os.pidfd_open(pid))
                        worker.stdin.write('S');worker.stdin.flush()
                        self.assertEqual(worker.stdout.readline(),'stopped\n')
                        worker.stdin.write('R');worker.stdin.flush()
                        ready=bool(select.select([worker.stdout],[],[],1)[0])
                        if broken:
                            with self.assertRaisesRegex(AssertionError,'stopped supervisor blocked'):
                                assert_retired(ready)
                            signal.pidfd_send_signal(descriptors[0],signal.SIGCONT)
                        else:assert_retired(ready)
                        output,error=worker.communicate(timeout=2)
                        self.assertEqual(worker.returncode,0,error)
                        self.assertIn('retired',output)
                        for descriptor in descriptors:
                            self.assertTrue(select.select([descriptor],[],[],1)[0],'owned descendant survived retirement')
                    finally:
                        if descriptors:
                            try:signal.pidfd_send_signal(descriptors[0],signal.SIGKILL,None,1 << 2)
                            except ProcessLookupError:pass
                        if worker.poll() is None:worker.kill()
                        worker.communicate(timeout=2)
                        for descriptor in descriptors:os.close(descriptor)

    def worker(self,negative=False,cleanup_failure=False,custody_negative=False,export_negative=False):
        shutil.copyfile(HERE/'job_exports.py',self.d/'job_exports.py')
        shutil.copyfile(HERE/'bb_target_store.py',self.d/'bb_target_store.py')
        source=(HERE/'bb-job.py').read_text()
        source=source.replace("'disk_free':disk()", "'disk_free':[{'path':'fixture','free_percent':50}]")
        if negative:source=source.replace("file=sys.stderr,flush=True", "flush=True")
        if cleanup_failure:source=source.replace('try:remove_home(job)','try:raise OSError("cleanup control")')
        if custody_negative:source=source.replace('start_new_session=True)', 'start_new_session=False)')
        if export_negative:
            boundary="export_spec=env.pop('BB_JOB_EXPORT_SPEC',None)"
            self.assertEqual(source.count(boundary),1)
            source=source.replace(boundary,"export_spec=env.get('BB_JOB_EXPORT_SPEC')")
        p=self.d/('negative.py' if negative else 'worker.py');p.write_text(source);return p
    def run_job(self,argv,negative=False,extra=None,cleanup_failure=False,export_negative=False):
        meta=self.d/'fixture.receipt-meta.v2.json';meta.write_text('{}')
        env={**os.environ,'PATH':'/home/var/.cargo/bin:'+os.environ['PATH'], 'BUILD_SLOT_RECEIPT_META':str(meta),'BUILD_SLOT_EXEC_IN_PAYLOAD':'1','BB_CACHE_DISABLED':'1'}
        env.pop('BB_JOB_ACTIVE',None);env.pop('BB_ADMITTED',None)
        env.pop('BB_JOB_EXPORT_SPEC',None)
        env.update(extra or {})
        result=subprocess.run([sys.executable,str(self.worker(negative,cleanup_failure,export_negative=export_negative)),*argv],cwd=self.d,env=env,capture_output=True)
        return result,json.loads(Path(str(meta)+'.runtime').read_text())

    def test_failed_export_is_infra_and_does_not_contaminate_stdout(self):
        result,runtime=self.run_job([sys.executable,'-c','print("child output",end="")'],extra={'BB_JOB_EXPORT_SPEC':json.dumps({'prefix':'/invalid','paths':[]})})
        self.assertEqual(result.returncode,224,result.stderr.decode())
        self.assertEqual(result.stdout,b'child output')
        self.assertIn(b'[buildbox] export INFRA:',result.stderr)
        self.assertIn('export_error',runtime);self.assertTrue(runtime['targets_removed'])

    def test_export_plan_never_crosses_into_a_nested_job(self):
        import tarfile
        for negative in [False,True]:
            with self.subTest(negative=negative):
                prefix=Path('/mnt/bb/jobs')/f'r19-export-owner-{os.getpid()}-{negative}'
                start=Path(str(prefix)+'.t0');start.touch()
                output=self.d/f'parent-{negative}.txt'
                meta=self.d/f'nested-{negative}'
                meta.write_text('{}')
                code='''import os,subprocess,sys
from pathlib import Path
env=dict(os.environ,BB_JOB_ACTIVE='0',BUILD_SLOT_RECEIPT_META=sys.argv[2])
result=subprocess.run([sys.executable,sys.argv[1],sys.executable,'-c','print("nested output")'],env=env,capture_output=True)
sys.stdout.buffer.write(result.stdout);sys.stderr.buffer.write(result.stderr)
if result.returncode:sys.exit(result.returncode)
Path(sys.argv[3]).write_text('parent output after nested cleanup')
'''
                spec={'prefix':str(prefix),'cwd':str(self.d),'paths':[output.name],'build':False}
                try:
                    result,runtime=self.run_job([sys.executable,'-c',code,str(self.d/'worker.py'),str(meta),str(output)],
                        extra={'BB_CACHE_DISABLED':'1','BB_JOB_EXPORT_SPEC':json.dumps(spec)},export_negative=negative)
                    self.assertEqual(result.returncode,224 if negative else 0,result.stderr.decode())
                    self.assertEqual(result.stdout,b'nested output\n')
                    self.assertTrue(runtime['home_removed'])
                    inner=json.loads(Path(str(meta)+'.runtime').read_text())
                    self.assertTrue(inner['home_removed'])
                    if negative:
                        self.assertIn('missing declared output',inner['export_error'])
                        self.assertFalse(output.exists())
                    else:
                        self.assertNotIn('export_error',inner)
                        rows=json.loads(Path(str(prefix)+'.exports/manifest.json').read_text())
                        self.assertEqual(len(rows),1)
                        with tarfile.open(rows[0]['archive']) as archive:
                            self.assertEqual(archive.extractfile(output.name).read(),b'parent output after nested cleanup')
                finally:
                    start.unlink()
                    shutil.rmtree(str(prefix)+'.exports',ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
