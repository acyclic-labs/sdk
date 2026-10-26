import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
# Every payload in this suite is inline code or a fixture it writes itself;
# declare that empty closure explicitly instead of inheriting the enclosing job's.
import input_closure
input_closure.declare(os.environ)

HERE=Path(__file__).resolve().parent
from admission_policy import cargo_query, resolve_wrapper


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        from cpu_test_fixture import KernelFixture
        self.fixture=KernelFixture()
        self.entry=self.fixture.control/'bb-admit.py'

    def tearDown(self):self.fixture.close()
    def test_query_identity_does_not_admit_compiles_or_shell_guesses(self):
        for name in ['metadata','tree','locate-project','pkgid','verify-project','read-manifest']:
            self.assertTrue(cargo_query(['cargo','+stable','--locked',name]))
        for argv in [['cargo','test'],['cargo','check'],['cargo','clippy'],['bash','-c','cargo metadata'],['cargo','--config','alias.metadata=check','metadata']]:
            self.assertFalse(cargo_query(argv))

    def test_missing_wrapper_and_interpreter_are_admission_infra(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary); env={**os.environ,'RUSTC_WRAPPER':str(root/'missing')}
            with self.assertRaisesRegex(RuntimeError,'RUSTC_WRAPPER_MISSING admission-INFRA'):resolve_wrapper(env)
            script=root/'tool';script.write_text('#!/missing/interpreter\n');script.chmod(0o755)
            with self.assertRaisesRegex(RuntimeError,'INTERPRETER_MISSING admission-INFRA'):resolve_wrapper({**env,'RUSTC_WRAPPER':str(script)})
            # Old existence-only validation would admit this unusable script.
            self.assertTrue(script.is_file());self.assertTrue(os.access(script,os.X_OK))
            script.write_text('#!/bin/sh\nexit 0\n')
            self.assertEqual(resolve_wrapper({**env,'RUSTC_WRAPPER':str(script)})['path'],str(script))

    def test_relative_wrappers_and_path_are_resolved_in_job_cwd(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);workspace=root/'workspace';(workspace/'tools').mkdir(parents=True)
            script=workspace/'tools/wrapper';script.write_text('#!/bin/sh\nexit 0\n');script.chmod(0o755)
            env={'PATH':'tools:/usr/bin:/bin','RUSTC_WRAPPER':'./tools/wrapper'}
            self.assertEqual(resolve_wrapper(env,cwd=workspace)['path'],str(script))
            self.assertEqual(resolve_wrapper({'PATH':'tools','RUSTC_WRAPPER':'wrapper'},cwd=workspace)['path'],str(script))
            # The daemon is in a different directory; the former lookup fails
            # or chooses its own unrelated wrapper.
            with self.assertRaisesRegex(RuntimeError,'RUSTC_WRAPPER_MISSING'):resolve_wrapper({'PATH':'tools','RUSTC_WRAPPER':'wrapper'},cwd=root)

    def test_env_shebang_missing_interpreter_and_unsupported_forms_are_infra(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            script=Path(temporary)/'wrapper'
            for shebang in ['#!/usr/bin/env missing-interpreter-control','#!/usr/bin/env -S python3 -u','#!/usr/bin/env "']:
                script.write_text(shebang+'\n');script.chmod(0o755)
                with self.assertRaisesRegex(RuntimeError,'admission-INFRA'):resolve_wrapper({'PATH':'/usr/bin:/bin','RUSTC_WRAPPER':str(script)})
                self.assertTrue(os.access('/usr/bin/env',os.X_OK),'old interpreter-only check admits both')
            script.write_text('#!/usr/bin/env python3\n');self.assertEqual(resolve_wrapper({'PATH':'/usr/bin:/bin','RUSTC_WRAPPER':str(script)})['path'],str(script))

    def test_nested_job_malformed_shebang_is_infra_before_child_exec(self):
        from unittest.mock import patch
        spec=importlib.util.spec_from_file_location('job',HERE/'bb-job.py')
        job=importlib.util.module_from_spec(spec);spec.loader.exec_module(job)
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);meta=root/'meta';meta.write_text('{}')
            script=root/'wrapper';script.write_bytes(b'#!/usr/bin/env "\n');script.chmod(0o755)
            env={**os.environ,'BUILD_SLOT_RECEIPT_META':str(meta),'BB_JOB_ACTIVE':'1','RUSTC_WRAPPER':str(script)}
            env.pop('BB_CACHE_DISABLED',None)
            with patch.dict(os.environ,env,clear=True),patch.object(job,'protect_home'),patch.object(job.os,'execvpe') as executed:
                self.assertEqual(job.run(['true'],namespace_ready=True),224)
                executed.assert_not_called()
            # The old parser raises ValueError outside the typed runtime boundary.
            import shlex
            with self.assertRaises(ValueError):shlex.split('/usr/bin/env "')

    def test_shared_daemon_selects_each_workspaces_relative_wrapper(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);queue=root/'admit'
            env={**os.environ,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1','RUSTC_WRAPPER':'./tools/wrapper','PATH':'tools:/usr/bin:/bin'}
            env.pop('BUILD_SLOT_RECEIPT_META',None)
            try:
                for name in ['first','second']:
                    workspace=root/name;(workspace/'tools').mkdir(parents=True)
                    wrapper=workspace/'tools/wrapper';wrapper.write_text('#!/bin/sh\n# '+name+'\n');wrapper.chmod(0o755)
                    run=subprocess.run([sys.executable,str(self.entry),'lane','cloud','--',sys.executable,'-c','import os;print(os.environ["RUSTC_WRAPPER"])'],cwd=workspace,env=env,capture_output=True)
                    self.assertEqual(run.returncode,0,run.stderr.decode());self.assertEqual(run.stdout.decode().strip(),str(wrapper))
                self.assertNotEqual(str(root/'first/tools/wrapper'),run.stdout.decode().strip(),'second job cannot inherit daemon cwd wrapper')
            finally:
                if (queue/'daemon.json').exists():os.kill(json.loads((queue/'daemon.json').read_text())['pid'],signal.SIGTERM)

    def test_metadata_bypasses_saturated_build_queue_and_old_policy_negative(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary); queue=root/'admit';queue.mkdir()
            (queue/'admit.conf').write_text('compile=1 tenant.graphcoder=1 tenant.cloud=1\n')
            env={**os.environ,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1'}
            env.pop('BUILD_SLOT_RECEIPT_META',None);env.pop('RUSTC_WRAPPER',None)
            ready=root/'ready'
            first=subprocess.Popen([sys.executable,str(self.entry),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;import time;Path({str(ready)!r}).touch();time.sleep(60)'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            negative=None
            try:
                deadline=time.monotonic()+3
                while not ready.exists() and time.monotonic()<deadline: time.sleep(.01)
                self.assertTrue(ready.exists())
                (root/'Cargo.toml').write_text('[package]\nname="query-control"\nversion="0.0.0"\n')
                (root/'src').mkdir();(root/'src/lib.rs').write_text('')
                command=['lane','graphcoder','--','/home/var/.cargo/bin/cargo','metadata','--format-version','1','--no-deps']
                good=subprocess.run([sys.executable,str(self.entry),*command],cwd=root,env=env,capture_output=True,timeout=2)
                self.assertEqual(good.returncode,0,good.stderr.decode());self.assertEqual(json.loads(good.stdout)['packages'][0]['name'],'query-control')
                # Fixture mutant executes the same client with bypass removed.
                mutant=root/'old.py';mutant.write_text(self.entry.read_text().replace('if cargo_query(canonical_argv(argv, os.environ)):', 'if False:'))
                negative=subprocess.Popen([sys.executable,str(mutant),*command],cwd=root,env={**env,'PYTHONPATH':str(self.fixture.control)},stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                with self.assertRaises(subprocess.TimeoutExpired):negative.communicate(timeout=.15)
                negative.terminate();negative.communicate(timeout=2)
            finally:
                if negative and negative.poll() is None:negative.kill();negative.communicate()
                first.kill();first.communicate(timeout=2)
                info=queue/'daemon.json'
                if info.exists():os.kill(json.loads(info.read_text())['pid'],signal.SIGTERM)

    def test_daemon_refuses_missing_wrapper_before_child_exec(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary);marker=root/'executed'
            env={**os.environ,'BB_ADMIT_ROOT':str(root/'admit'),'BB_ADMIT_WRAPPED_COMMAND':'1','RUSTC_WRAPPER':str(root/'missing')}
            env.pop('BUILD_SLOT_RECEIPT_META',None)
            result=subprocess.run([sys.executable,str(self.entry),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;Path({str(marker)!r}).touch()'],env=env,capture_output=True)
            try:
                self.assertEqual(result.returncode,224,result.stderr.decode());self.assertEqual(result.stdout,b'')
                self.assertIn(b'RUSTC_WRAPPER_MISSING admission-INFRA',result.stderr);self.assertFalse(marker.exists())
            finally:os.kill(json.loads((root/'admit/daemon.json').read_text())['pid'],signal.SIGTERM)


if __name__=='__main__':unittest.main()
