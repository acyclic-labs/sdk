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

HERE=Path(__file__).resolve().parent
from admission_policy import cargo_query, resolve_wrapper


class AdmissionTests(unittest.TestCase):
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

    def test_metadata_bypasses_saturated_build_queue_and_old_policy_negative(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary); queue=root/'admit';queue.mkdir()
            (queue/'admit.conf').write_text('compile=1 tenant.graphcoder=1 tenant.cloud=1\n')
            env={**os.environ,'BB_ADMIT_ROOT':str(queue),'BB_ADMIT_WRAPPED_COMMAND':'1'}
            env.pop('BUILD_SLOT_RECEIPT_META',None);env.pop('RUSTC_WRAPPER',None)
            ready=root/'ready'
            first=subprocess.Popen([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;import time;Path({str(ready)!r}).touch();time.sleep(60)'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            negative=None
            try:
                deadline=time.monotonic()+3
                while not ready.exists() and time.monotonic()<deadline: time.sleep(.01)
                self.assertTrue(ready.exists())
                (root/'Cargo.toml').write_text('[package]\nname="query-control"\nversion="0.0.0"\n')
                (root/'src').mkdir();(root/'src/lib.rs').write_text('')
                command=['lane','graphcoder','--','/home/var/.cargo/bin/cargo','metadata','--format-version','1','--no-deps']
                good=subprocess.run([sys.executable,str(HERE/'bb-admit.py'),*command],cwd=root,env=env,capture_output=True,timeout=2)
                self.assertEqual(good.returncode,0,good.stderr.decode());self.assertEqual(json.loads(good.stdout)['packages'][0]['name'],'query-control')
                # Fixture mutant executes the same client with bypass removed.
                mutant=root/'old.py';mutant.write_text((HERE/'bb-admit.py').read_text().replace('if cargo_query(canonical_argv(argv, os.environ)):', 'if False:'))
                negative=subprocess.Popen([sys.executable,str(mutant),*command],cwd=root,env={**env,'PYTHONPATH':str(HERE)},stdout=subprocess.PIPE,stderr=subprocess.PIPE)
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
            result=subprocess.run([sys.executable,str(HERE/'bb-admit.py'),'lane','cloud','--',sys.executable,'-c',f'from pathlib import Path;Path({str(marker)!r}).touch()'],env=env,capture_output=True)
            try:
                self.assertEqual(result.returncode,224,result.stderr.decode());self.assertEqual(result.stdout,b'')
                self.assertIn(b'RUSTC_WRAPPER_MISSING admission-INFRA',result.stderr);self.assertFalse(marker.exists())
            finally:os.kill(json.loads((root/'admit/daemon.json').read_text())['pid'],signal.SIGTERM)


if __name__=='__main__':unittest.main()
