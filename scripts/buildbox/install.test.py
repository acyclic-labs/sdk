import concurrent.futures
import contextlib
import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('installer',HERE/'bb-install.py')
installer=importlib.util.module_from_spec(spec);spec.loader.exec_module(installer)


def python(*argv):
    # sudo resets the environment, including PYTHONDONTWRITEBYTECODE. The
    # interpreter flag must cross every privileged process boundary itself.
    return [sys.executable, '-B', *map(str, argv)]


class InstallTests(unittest.TestCase):
    def test_privileged_import_keeps_source_immutable_without_inherited_environment(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            source=Path(temporary)/'module.py';source.write_text('VALUE=1\n')
            driver='import importlib.util,sys;s=importlib.util.spec_from_file_location("fixture",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);assert m.VALUE==1'
            env={k:v for k,v in os.environ.items() if k!='PYTHONDONTWRITEBYTECODE'}
            cache=source.parent/'__pycache__'
            try:
                subprocess.run(['sudo','-n',*python('-c',driver,source)],env=env,check=True)
                self.assertFalse(cache.exists())
                # The former sudo entry lost the env flag and wrote a root
                # directory into the upload, preventing unprivileged pruning.
                old=python('-c',driver,source);old.remove('-B')
                subprocess.run(['sudo','-n',*old],env=env,check=True)
                self.assertTrue(cache.exists());self.assertEqual(cache.stat().st_uid,0)
            finally:
                if cache.exists():
                    subprocess.run(['sudo','-n',*python('-c','import shutil,sys;shutil.rmtree(sys.argv[1])',cache)],check=True)

    def test_fifty_concurrent_exec_jobs_see_complete_generations(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            def content(value):return f'#!/bin/sh\nprintf "{value}\\n"\n'.encode()
            installer.publish(tool,content('old'))
            barrier=threading.Barrier(51)
            def execute(_):
                barrier.wait()
                for _ in range(8):
                    run=subprocess.run([str(tool)],capture_output=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertIn(run.stdout,[b'old\n',b'new\n'])
            with concurrent.futures.ThreadPoolExecutor(max_workers=50) as workers:
                futures=[workers.submit(execute,i) for i in range(50)]
                # Publication is a separate process, as in production. A fork
                # from a writer thread can inherit another thread's open write
                # fd before CLOEXEC and manufacture ETXTBSY unrelated to rename.
                writer=subprocess.Popen(python('-c',
                    'import importlib.util,sys;from pathlib import Path;'
                    's=importlib.util.spec_from_file_location("i",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                    '[m.publish(Path(sys.argv[2]),bytes.fromhex(sys.argv[3+n%2])) for n in range(80)]',
                    str(HERE/'bb-install.py'),str(tool),content('old').hex(),content('new').hex()))
                barrier.wait()
                for future in futures:future.result()
                self.assertEqual(writer.wait(),0)
            # Deterministic old in-place publication window: exec sees an empty
            # file between truncate and write, even though the path exists.
            tool.chmod(0o755)
            with tool.open('wb'):
                with self.assertRaises(OSError):subprocess.run([str(tool)],check=True)

    def test_failed_qualification_never_publishes(self):
        self.assertIn('sccache-rustc',installer.TOOLS)
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool';installer.publish(tool,b'original')
            with patch.object(installer.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['test'])):
                with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path(temporary),'var')
            self.assertEqual(tool.read_bytes(),b'original')
            self.assertEqual(list(Path(temporary).iterdir()),[tool])

    def test_cache_and_retirement_controls_are_mandatory_before_publication(self):
        for suite in ['prune.test.py','cargo_cache.test.py','receipt-outcome.test.mjs','prune-admission.test.mjs','reaping.test.py','cache_policy.test.py','cpu_leases.test.py','job_caps.test.py','input_closure.test.py','command_custody.test.py']:
            with self.subTest(suite=suite):
                calls=[]
                def run(command,**kwargs):
                    calls.append(command)
                    if command[-1]==suite:raise subprocess.CalledProcessError(1,command)
                with patch.object(installer.subprocess,'run',side_effect=run):
                    with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path('/fixture'),'var')
                self.assertEqual(calls[-1][-1],suite)
                # The former omission admitted an otherwise failing suite.
                old=Path(installer.__file__).read_text().replace(
                    ",\n                ['python3', '"+suite+"']",'').replace(
                    ",\n                ['python3', '-B', '"+suite+"']",'').replace(
                    ",\n                ['python3', '-B', 'cpu_test_fixture.py', '"+suite+"']",'').replace(
                    ",\n                [NODE, '--test', '"+suite+"']",'')
                namespace={};exec(compile(old,'old-qualification','exec'),namespace)
                with patch.object(installer.subprocess,'run',side_effect=run):namespace['qualify'](Path('/fixture'),'var')

    def test_generation_switch_is_one_rename_with_canaries_and_switch_back(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);store=root/'store';store.mkdir()
            (store/'a').write_bytes(b'legacy-a');(store/'legacy-only').write_bytes(b'legacy')
            calls=[];fail=set()
            @contextlib.contextmanager
            def drained(generation):calls.append(('drain',Path(generation).name));yield
            @contextlib.contextmanager
            def services(generation):calls.append(('services',Path(generation).name));yield
            def canary(generation=None):
                stage='staged' if generation else 'live';calls.append((stage,Path(generation or installer.current_generation()).name))
                if stage in fail:raise RuntimeError('kind=GENERATION_CANARY stage='+stage)
            state=lambda:calls.append(('state',))or'prior'
            restored=lambda prior,failed=None:calls.append(('restore',prior))
            with patch.object(installer,'ROOT',root/'opt'),patch.object(installer,'admission_drained',drained), \
                 patch.object(installer,'generation_services',services),patch.object(installer,'canary',canary), \
                 patch.object(installer,'service_state',state),patch.object(installer,'restore_services',restored):
                def read(name):return (store/name).read_bytes() if (store/name).exists() else None
                one=installer.materialize({'a':b'one-a','b':b'one-b','bbcg/systemd-run':b'shim'},'one')
                legacy=installer.deploy(one,store)
                self.assertEqual(calls,[('drain','one'),('staged','one'),('state',),('services','one'),('live','one')])
                self.assertEqual([read('a'),read('b'),read('bbcg/systemd-run'),read('legacy-only')],[b'one-a',b'one-b',b'shim',None])
                self.assertEqual((legacy/'a').read_bytes(),b'legacy-a')  # predecessor retained whole
                self.assertTrue(all(p.is_symlink() for p in store.iterdir()))
                entry=os.lstat(store/'a')
                two=installer.materialize({'a':b'two-a'},'two');calls.clear()
                self.assertEqual(installer.deploy(two,store),one)
                # A later switch only renames CURRENT: entries are untouched and
                # a tool absent from the live generation is absent, not stale.
                self.assertEqual((os.lstat(store/'a').st_ino,read('a'),read('b'),read('bbcg/systemd-run')),(entry.st_ino,b'two-a',None,None))
                self.assertEqual(Path(os.readlink(root/'opt/previous')),one)
                fail.add('live');three=installer.materialize({'a':b'three-a','c':b'c'},'three');calls.clear()
                with self.assertRaisesRegex(RuntimeError,'stage=live'):installer.deploy(three,store)
                self.assertEqual(calls,[('drain','three'),('staged','three'),('state',),('services','three'),('live','three'),('restore','prior')])
                self.assertEqual((installer.current_generation(),read('a'),read('c')),(two,b'two-a',None))
                self.assertEqual((three/'a').read_bytes(),b'three-a')
                # A switch failing before the pointer rename leaves CURRENT
                # unchanged and still restores the services it stopped.
                fail.clear();calls.clear()
                with patch.object(installer,'switch',side_effect=OSError('link failed')),self.assertRaises(OSError):
                    installer.deploy(three,store)
                self.assertEqual((calls[-1],installer.current_generation()),(('restore','prior'),two))
                fail={'staged'};calls.clear()
                with self.assertRaisesRegex(RuntimeError,'stage=staged'):installer.deploy(three,store)
                self.assertEqual((calls,installer.current_generation()),([('drain','three'),('staged','three')],two))
                # An interrupted generation write never becomes visible.
                writes=[]
                def interrupted(*args,**kwargs):
                    writes.append(args)
                    if len(writes)==2:raise OSError('interrupted')
                    return original(*args,**kwargs)
                original=installer.publish
                with patch.object(installer,'publish',interrupted),self.assertRaises(OSError):
                    installer.materialize({'a':b'four-a','b':b'four-b'},'four')
                self.assertFalse((root/'opt/generations/four').exists());self.assertEqual(read('a'),b'two-a')
                # Former per-file publication into a live store, interrupted at
                # the same point, leaves exactly the mixed generation seen on bb2.
                former=root/'former';former.mkdir();(former/'a').write_bytes(b'old-a');(former/'b').write_bytes(b'old-b')
                writes.clear()
                with patch.object(installer,'publish',interrupted),self.assertRaises(OSError):
                    for name,data in [('a',b'new-a'),('b',b'new-b')]:installer.publish(former/name,data)
                self.assertEqual([(former/n).read_bytes() for n in 'ab'],[b'new-a',b'old-b'])

    def test_interrupted_first_adoption_never_removes_live_tools_and_resumes(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);store=root/'store';(store/'dir').mkdir(parents=True)
            for name in 'abc':(store/name).write_bytes(b'legacy-'+name.encode())
            (store/'dir'/'tool').write_bytes(b'legacy-dir')
            seen=[];original=installer.exchange
            def interrupted(a,b):
                original(a,b);seen.append(sorted(p.name for p in store.iterdir() if not p.name.startswith('.')))
                if len(seen)==2:raise KeyboardInterrupt  # Between an exchange and its retirement.
            @contextlib.contextmanager
            def nothing(*args):yield
            with patch.object(installer,'ROOT',root/'opt'),patch.object(installer,'exchange',interrupted), \
                 patch.object(installer,'admission_drained',nothing),patch.object(installer,'generation_services',nothing), \
                 patch.object(installer,'canary',lambda generation=None:None), \
                 patch.object(installer,'service_state',lambda:None),patch.object(installer,'restore_services',lambda state,failed=None:None):
                one=installer.materialize({'a':b'one-a'},'one')
                with self.assertRaises(KeyboardInterrupt):installer.deploy(one,store)
                # Every tool stayed reachable with its legacy bytes throughout.
                self.assertEqual(seen[-1],['a','b','c','dir'])
                self.assertEqual([(store/n).read_bytes() for n in 'abc']+[(store/'dir'/'tool').read_bytes()],
                                 [b'legacy-a',b'legacy-b',b'legacy-c',b'legacy-dir'])
            with patch.object(installer,'ROOT',root/'opt'),patch.object(installer,'admission_drained',nothing), \
                 patch.object(installer,'generation_services',nothing),patch.object(installer,'canary',lambda generation=None:None), \
                 patch.object(installer,'service_state',lambda:None),patch.object(installer,'restore_services',lambda state,failed=None:None):
                installer.deploy(one,store)  # The next run completes the adoption.
                self.assertTrue(all(p.is_symlink() for p in store.iterdir()))
                self.assertEqual(Path(os.readlink(root/'opt/previous')).name[:7],'legacy-')
                self.assertEqual(((store/'a').read_bytes(),(store/'b').exists()),(b'one-a',False))
                self.assertEqual(len(list((root/'opt/retired').iterdir())),4)
        # The former adoption renamed every entry away before linking any: an
        # interruption between those loops left the store empty of tools.
        self.assertNotIn('os.rename(entry,retired',Path(installer.__file__).read_text())

    def test_predecessor_cache_authority_is_restarted_not_migrated(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            legacy=Path(temporary)/'legacy';legacy.mkdir()
            (legacy/'bb-cache.py').write_text('import contextlib\n@contextlib.contextmanager\ndef migration():\n    raise AssertionError("retakes upgrade.lock")\n    yield\n')
            unit=Path(temporary)/'unit';unit.write_text('[Unit]\n');calls=[]
            outgoing=Path(temporary)/'outgoing';outgoing.mkdir()
            (outgoing/'bb-cache.py').write_text('import sys\ndef retire_owners():\n    sys.modules["installer_order"].append(["retire"])\n')
            sys.modules['installer_order']=calls
            run=lambda command,**kw:calls.append(command) or subprocess.CompletedProcess(command,0)
            with patch.object(installer,'CACHE_UNIT',unit),patch.object(installer.subprocess,'run',run), \
                 patch.object(installer,'current_generation',lambda:outgoing):
                with installer.generation_services(legacy):pass
            del sys.modules['installer_order']
            # Retirement stops the unit: a try-restart after it would start nothing.
            self.assertEqual(calls,[['systemctl','is-active','--quiet','buildbox-cache.service'],['retire'],
                                    ['systemctl','restart','buildbox-cache.service']])

    def test_switch_back_retires_the_failed_generations_cache_server_first(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            failed=Path(temporary)/'failed';failed.mkdir();order=[]
            (failed/'bb-cache.py').write_text('import sys\ndef retire_owners():\n    sys.modules["installer_order"].append("retire")\n')
            sys.modules['installer_order']=order
            unit=Path(temporary)/'unit';unit.write_bytes(b'[Unit]\n')
            with patch.object(installer,'CACHE_UNIT',unit), \
                 patch.object(installer.subprocess,'run',lambda command,**kw:order.append(command[1])):
                installer.restore_services({'unit':b'[Unit]\n','enabled':True,'active':True},failed)
            del sys.modules['installer_order']
            # KillMode=process: a restart alone leaves the failed server holding the endpoint.
            self.assertEqual(order,['retire','enable','restart'])

    def test_stale_predecessor_refuses_publication_without_touching_live_tools(self):
        driver='''import importlib.util,sys,json,hashlib,tempfile,os
from pathlib import Path
from unittest.mock import patch
s=importlib.util.spec_from_file_location('i',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
stage=Path(tempfile.mkdtemp(prefix='bb-install-',dir='/mnt/bb/tmp'));live=Path(tempfile.mkdtemp(dir='/mnt/bb/tmp'))
try:
 os.chmod(stage,0o755)
 payload={n:(Path(sys.argv[1]).read_bytes() if n=='bb-install.py' else n.encode()) for n in m.TOOLS}
 for n,d in payload.items():m.publish(stage/n,d,0o444);m.publish(live/n,b'installed-'+n.encode(),0o555)
 m.publish(stage/'.qualified.json',json.dumps({n:hashlib.sha256(d).hexdigest() for n,d in payload.items()}).encode(),0o444)
 m.publish(stage/'.predecessors.json',json.dumps({n:m.file_hash(live/n) for n in m.TOOLS}).encode(),0o444)
 changed=live/m.TOOLS[0];m.publish(changed,b'newer deployment',0o555)  # After qualification.
 before={n:(live/n).read_bytes() for n in m.TOOLS}
 with patch.object(m,'BIN',live),patch.object(m,'deploy',side_effect=AssertionError('deployed')):
  try:m.publish_candidate(stage)
  except ValueError as e:assert 'installed predecessor changed' in str(e),e
  else:raise AssertionError('stale candidate published')
 assert {n:(live/n).read_bytes() for n in m.TOOLS}==before
finally:
 import shutil;shutil.rmtree(stage);shutil.rmtree(live)
'''
        subprocess.run(['sudo','-n',*python('-c',driver,HERE/'bb-install.py')],check=True)

    def test_unmanaged_tools_carry_forward_and_managed_tools_never_do(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            source=Path(temporary)
            for name in ['bb-cargo/cargo','bigstack/rustc','tofu','admission_policy.py','bbcg/systemd-run','retired_tool.py']:
                (source/name).parent.mkdir(parents=True,exist_ok=True);(source/name).write_bytes(name.encode())
            (source/'installed-tools.json').write_text(json.dumps({'sha256':{'retired_tool.py':None}}))
            self.assertEqual(installer.carried(source),{'bb-cargo/cargo':b'bb-cargo/cargo','bigstack/rustc':b'bigstack/rustc','tofu':b'tofu'})
            self.assertEqual(installer.carried(None),{})

    def test_live_publication_has_no_partial_selection(self):
        self.assertNotIn('--only',Path(installer.__file__).read_text())
        self.assertEqual(installer.publish_candidate.__code__.co_argcount,1)

    def test_privileged_publication_refuses_direct_lane_edits(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            script='from pathlib import Path;import sys,os;p=Path(sys.argv[1]);p.write_bytes(b"qualified");os.chown(p,0,0);p.chmod(0o555)'
            subprocess.run(['sudo','-n',*python('-c',script,tool)],check=True)
            with self.assertRaises(PermissionError):tool.write_bytes(b'partial')
            self.assertEqual(tool.read_bytes(),b'qualified')
            # The former var-owned mode admits exactly the same write.
            unprotected=Path(temporary)/'unprotected';unprotected.write_bytes(b'old');unprotected.write_bytes(b'partial')
            self.assertEqual(unprotected.read_bytes(),b'partial')

    def test_actual_predecessor_bootstraps_new_policy_before_publication(self):
        driver='''import importlib.util,sys,json,hashlib,tempfile,shutil
from pathlib import Path
from unittest.mock import patch
source=Path(sys.argv[1]);fixture=source.parent/'fixtures/installer-upgrade'
info=json.loads((fixture/'origin.json').read_text())
assert hashlib.sha256((fixture/'predecessor.py').read_bytes()).hexdigest()==info['sha256']
def load(name,path):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
old=load('predecessor',fixture/'predecessor.py')
root=Path(tempfile.mkdtemp(dir='/mnt/bb/tmp'));live=root/'live';live.mkdir();stages=[]
try:
 # The upgrade boundary needs the actual tool bytes, not three copies of all
 # unrelated regression fixtures. Suites are observed below, not run recursively.
 candidate=root/'source';candidate.mkdir()
 current=load('reviewed_source',source)
 for name in current.TOOLS:old.publish(candidate/name,(source.parent/name).read_bytes(),0o444)
 for name in old.TOOLS:old.publish(live/name,('installed-'+name).encode())
 old.BIN=live;old_qualify=old.qualify;old_calls=[]
 # Execute the predecessor's real freeze/token path. Recursive test execution
 # is observed at its subprocess boundary; each required suite is qualified
 # separately by the enclosing installer suite and before actual deployment.
 def qualify_old(directory,user):
  with patch.object(old.subprocess,'run',side_effect=lambda command,**kw:old_calls.append(command)):
   old_qualify(directory,user)
 old.qualify=qualify_old;stage=old.prepare(candidate);stages.append(stage)
 assert set(json.loads((stage/'.qualified.json').read_text()))==set(old.TOOLS)
 new=load('candidate',stage/'bb-install.py');new.BIN=live;new.secure_bin=lambda:live
 assert len(old.TOOLS)==12 and len(new.TOOLS)==17
 own_file=new.__file__;new.__file__=str(fixture/'predecessor.py')
 try:new.prepare(stage)
 except ValueError as error:assert 'run the reviewed source installer' in str(error)
 else:raise AssertionError('different installer policy accepted')
 finally:new.__file__=own_file
 try:new.publish_candidate(stage)
 except ValueError as error:assert 'qualified candidate changed' in str(error)
 else:raise AssertionError('actual predecessor token qualified new tool set')
 assert (live/'bb-install.py').read_bytes()==b'installed-bb-install.py'
 real_qualify=new.qualify;new_calls=[]
 def qualify_new(directory,user):
  assert not (directory/'.qualified.json').exists()
  with patch.object(new.subprocess,'run',side_effect=lambda command,**kw:new_calls.append(command)):
   real_qualify(directory,user)
 new.qualify=qualify_new
 # The canonical source entry freezes and qualifies its own complete generation;
 # no predecessor whitelist/token is promoted into a new qualification.
 qualified=new.prepare(stage);stages.append(qualified)
 assert {command[-1] for command in new_calls}>={
  'cache_policy.test.py','cpu_leases.test.py','runtime.test.py','install.test.py'}
 assert not {'cache_policy.test.py','cpu_leases.test.py'}&{command[-1] for command in old_calls}
 assert set(json.loads((qualified/'.qualified.json').read_text()))==set(new.TOOLS)
 assert json.loads((qualified/'.predecessors.json').read_text())['cpu_leases.py'] is None
 publisher=load('publisher',qualified/'bb-install.py');publisher.BIN=live;publisher.secure_bin=lambda:live
 import contextlib
 publisher.ROOT=root/'opt';publisher.canary=lambda generation=None:None
 publisher.admission_drained=publisher.generation_services=lambda generation:contextlib.nullcontext()
 publisher.publish_candidate(qualified)
 assert (live/'bb-install.py').read_bytes()==source.read_bytes()
 assert (root/'opt/previous/bb-install.py').read_bytes()==b'installed-bb-install.py'
 def failed(directory,*args):
  stages.append(directory)
  raise RuntimeError('qualification failed control')
 new.qualify=failed
 try:new.prepare(stage)
 except RuntimeError:pass
 else:raise AssertionError('failed qualification accepted')
 assert not (stages[-1]/'.qualified.json').exists()
finally:
 for stage in stages:shutil.rmtree(stage)
 shutil.rmtree(root)
'''
        subprocess.run(['sudo','-n',*python('-c',driver,HERE/'bb-install.py')],check=True)

    def test_writable_home_cannot_replace_pinned_tool_directory(self):
        import uuid
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);alias=root/'bin';alias.mkdir();(alias/'tool').write_text('original')
            store=Path('/opt')/('gate-speed-install-fixture-'+uuid.uuid4().hex)
            driver='''import importlib.util,sys,os,subprocess
from pathlib import Path
s=importlib.util.spec_from_file_location('installer',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
alias=Path(sys.argv[2]);store=Path(sys.argv[3]);os.chown(alias,0,0);m.secure_bin(alias,store,False)
m.secure_bin(alias,store,False) # repeated publication preserves the pinned entry
assert os.statvfs(alias).f_flag & os.ST_RDONLY
m.publish(store/'tool',b'updated');assert (alias/'tool').read_bytes()==b'updated'
probe="from pathlib import Path;import sys,errno;p=Path(sys.argv[1]);\\ntry:p.rename(p.with_name('replaced'))\\nexcept OSError as e:assert e.errno==errno.EBUSY\\nelse:raise AssertionError('tool entry replaced')"
subprocess.run(['runuser','-u','var','--','python3','-B','-c',probe,str(alias)],check=True)
assert (alias/'tool').read_text()=='updated'
'''
            try:
                subprocess.run(['sudo','-n','unshare','--mount','--propagation','private',*python('-c',driver,HERE/'bb-install.py',alias,store)],check=True)
                # The old unmounted root-owned entry is still movable from a
                # writable parent: ownership of files alone is insufficient.
                alias.rename(root/'old-replaced');self.assertTrue((root/'old-replaced/tool').exists())
            finally:
                subprocess.run(['sudo','-n',*python('-c','import shutil,sys;shutil.rmtree(sys.argv[1],ignore_errors=True)',store)],check=True)
                subprocess.run(['sudo','-n',*python('-c','import os,sys;from pathlib import Path;[(os.chown(p,int(sys.argv[2]),int(sys.argv[3]))) for p in Path(sys.argv[1]).iterdir() if p.is_dir()]',root,os.getuid(),os.getgid())],check=True)


if __name__=='__main__':unittest.main()
