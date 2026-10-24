import concurrent.futures
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('installer',HERE/'bb-install.py')
installer=importlib.util.module_from_spec(spec);spec.loader.exec_module(installer)


def isolated_python(*args):
    return [sys.executable,'-I','-B',*args]


class InstallTests(unittest.TestCase):
    def test_privileged_fixture_import_cannot_write_bytecode_into_source(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);root.chmod(0o755)
            module=root/'fixture.py';module.write_text('value=1\n')
            script='import importlib.util,sys;s=importlib.util.spec_from_file_location("fixture",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)'
            try:
                # Negative control reproduces the root-owned pycache left by the former launcher.
                command=isolated_python('-c',script,str(module))
                subprocess.run(['sudo','-n',*[part for part in command if part!='-B']],check=True)
                cache=root/'__pycache__';self.assertEqual(cache.stat().st_uid,0)
                subprocess.run(['sudo','-n','chown','-R',str(os.getuid())+':'+str(os.getgid()),str(cache)],check=True)
                shutil.rmtree(cache)
                subprocess.run(['sudo','-n',*command],check=True)
                self.assertFalse(cache.exists())
            finally:
                if (root/'__pycache__').exists():
                    subprocess.run(['sudo','-n','chown','-R',str(os.getuid())+':'+str(os.getgid()),str(root/'__pycache__')],check=True)

    def test_published_worker_imports_only_the_declared_tool_generation(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary)
            for name in installer.TOOLS:shutil.copyfile(HERE/name,root/name)
            command=[sys.executable,'-I','-B','-c',
                     'import runpy,sys;sys.path.insert(0,sys.argv[1]);'
                     'runpy.run_path(sys.argv[1]+"/bb-job.py",run_name="installed_worker")',str(root)]
            good=subprocess.run(command,cwd=root,capture_output=True)
            self.assertEqual(good.returncode,0,good.stderr.decode())
            # The former publication list omitted a runtime import even though
            # qualification had copied every source file into its stage.
            (root/'job_custody.py').unlink()
            bad=subprocess.run(command,cwd=root,capture_output=True)
            self.assertNotEqual(bad.returncode,0)
            self.assertIn(b"No module named 'job_custody'",bad.stderr)

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
                writer=subprocess.Popen([sys.executable,'-c',
                    'import importlib.util,sys;from pathlib import Path;'
                    's=importlib.util.spec_from_file_location("i",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                    '[m.publish(Path(sys.argv[2]),bytes.fromhex(sys.argv[3+n%2])) for n in range(80)]',
                    str(HERE/'bb-install.py'),str(tool),content('old').hex(),content('new').hex()])
                barrier.wait()
                for future in futures:future.result()
                self.assertEqual(writer.wait(),0)
            # Deterministic old in-place publication window: exec sees an empty
            # file between truncate and write, even though the path exists.
            tool.chmod(0o755)
            with tool.open('wb'):
                with self.assertRaises(OSError):subprocess.run([str(tool)],check=True)

    def test_command_custody_failure_is_a_publication_barrier(self):
        import inspect
        def run(command, **kwargs):
            if 'command_custody.test.py' in command:
                raise subprocess.CalledProcessError(1, command)
            return subprocess.CompletedProcess(command, 0)
        with patch.object(installer.subprocess, 'run', side_effect=run):
            with self.assertRaises(subprocess.CalledProcessError):
                installer.qualify(Path('/unused-frozen-candidate'), 'var')
            # Negative control executes the former qualification policy: the
            # same failed command-custody suite cannot block publication.
            former=inspect.getsource(installer.qualify).replace("['python3', 'command_custody.test.py'], ", '')
            self.assertNotEqual(former, inspect.getsource(installer.qualify))
            namespace=dict(installer.__dict__)
            exec(compile(former, '<former-installer-policy>', 'exec'), namespace)
            namespace['qualify'](Path('/unused-frozen-candidate'), 'var')

    def test_failed_qualification_never_publishes(self):
        self.assertIn('sccache-rustc',installer.TOOLS)
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool';installer.publish(tool,b'original')
            with patch.object(installer.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['test'])):
                with self.assertRaises(subprocess.CalledProcessError):installer.qualify(Path(temporary),'var')
            self.assertEqual(tool.read_bytes(),b'original')
            self.assertEqual(list(Path(temporary).iterdir()),[tool])

    def test_privileged_publication_refuses_direct_lane_edits(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            tool=Path(temporary)/'tool'
            script='from pathlib import Path;import sys,os;p=Path(sys.argv[1]);p.write_bytes(b"qualified");os.chown(p,0,0);p.chmod(0o555)'
            subprocess.run(['sudo','-n',*isolated_python('-c',script,str(tool))],check=True)
            with self.assertRaises(PermissionError):tool.write_bytes(b'partial')
            self.assertEqual(tool.read_bytes(),b'qualified')
            # The former var-owned mode admits exactly the same write.
            unprotected=Path(temporary)/'unprotected';unprotected.write_bytes(b'old');unprotected.write_bytes(b'partial')
            self.assertEqual(unprotected.read_bytes(),b'partial')

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
subprocess.run(['runuser','-u','var','--','python3','-c',probe,str(alias)],check=True)
assert (alias/'tool').read_text()=='updated'
'''
            try:
                subprocess.run(['sudo','-n','unshare','--mount','--propagation','private',*isolated_python('-c',driver,str(HERE/'bb-install.py'),str(alias),str(store))],check=True)
                # The old unmounted root-owned entry is still movable from a
                # writable parent: ownership of files alone is insufficient.
                alias.rename(root/'old-replaced');self.assertTrue((root/'old-replaced/tool').exists())
            finally:
                subprocess.run(['sudo','-n',*isolated_python('-c','import shutil,sys;shutil.rmtree(sys.argv[1],ignore_errors=True)',str(store))],check=True)
                subprocess.run(['sudo','-n',*isolated_python('-c','import os,sys;from pathlib import Path;[(os.chown(p,int(sys.argv[2]),int(sys.argv[3]))) for p in Path(sys.argv[1]).iterdir() if p.is_dir()]',str(root),str(os.getuid()),str(os.getgid()))],check=True)


if __name__=='__main__':unittest.main()
