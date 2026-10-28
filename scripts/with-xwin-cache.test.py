import importlib.util
import inspect
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import json
import os
import hashlib
import sys
from unittest.mock import patch
import fcntl

spec = importlib.util.spec_from_file_location("xwin_cache", Path(__file__).with_name("with-xwin-cache.py"))
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)
run = subprocess.run
# The installed MSVC shim lives with the external build tooling; the cache only
# copies and hashes it, so a self-contained fixture stands in for it here.
_fixture_compiler = tempfile.NamedTemporaryFile(prefix="msvc-compiler-", suffix=".py", delete=False)
_fixture_compiler.write(b"# fixture MSVC compiler shim\n")
_fixture_compiler.close()
os.environ["SDK_MSVC_COMPILER"] = _fixture_compiler.name


def prepared(command, **kwargs):
    if command[:3] == ["cargo", "xwin", "cache"]:
        return subprocess.CompletedProcess(command, 0)
    return run(command, **kwargs)


class SnapshotTests(unittest.TestCase):
    def test_sdk_preparation_diagnostics_never_pollute_child_json_stdout(self):
        source=Path(cache.__file__).read_text()
        probe='''import importlib.util,json,pathlib,subprocess,sys
spec=importlib.util.spec_from_file_location('candidate',sys.argv[1]);candidate=importlib.util.module_from_spec(spec);spec.loader.exec_module(candidate)
actual=subprocess.run
def prepare(command,**kwargs):
 if command[:3]==['cargo','xwin','cache']:return actual([sys.executable,'-c','print("SDK preparation diagnostic")'],**kwargs)
 return actual(command,**kwargs)
candidate.subprocess.run=prepare
candidate.snapshot(pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3]))
print(json.dumps({'child':'exact JSON'}))
'''
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);seed=self.seed(root)
            for negative in [False,True]:
                file=root/('old.py' if negative else 'production.py')
                file.write_text(source.replace('check=True, stdout=sys.stderr)','check=True)') if negative else source)
                result=run([sys.executable,'-c',probe,str(file),str(seed),str(root/str(negative))],capture_output=True,text=True)
                self.assertEqual(result.returncode,0,result.stderr)
                if negative:
                    with self.assertRaises(json.JSONDecodeError):json.loads(result.stdout)
                    self.assertIn('SDK preparation diagnostic',result.stdout)
                else:
                    self.assertEqual(json.loads(result.stdout),{'child':'exact JSON'})
                    self.assertIn('SDK preparation diagnostic',result.stderr)

    def test_private_sdk_paths_are_stable_without_reinitializing_live_workers(self):
        with tempfile.TemporaryDirectory() as temporary:
            identity=cache.workspace_identity('/work/target/windows',2)
            self.assertEqual(identity,cache.workspace_identity('/work/target/windows',2))
            self.assertNotEqual(identity,cache.workspace_identity('/work/target/linux',2))
            self.assertNotEqual(identity,cache.workspace_identity('/work/target/windows',3))
            root=Path(temporary)/'private-sdk'
            def populate(root,workers):
                (root/'live-sdk').write_text(str(workers))
                (root/'bin').mkdir()
                (root/'bin/clang-cl').write_bytes(cache.compiler_path().read_bytes())
                for index in range(workers):
                    sdk=root/f'worker-{index}'/'xwin'
                    (sdk/'crt').mkdir(parents=True);(sdk/'sdk').mkdir();(sdk/'DONE').write_text('ready')
                    (sdk.parent/'sdk-identity.sha256').write_text('a'*64)
            with patch.object(cache,'populate',side_effect=populate) as initialize:
                for _ in range(2):cache.prepare_workspace(root,2,identity)
                self.assertEqual(initialize.call_count,1)
                self.assertEqual((root/'live-sdk').read_text(),'2')
            (root/'bin/clang-cl').unlink()
            # A marker alone must not permit fallback to another PATH compiler.
            with patch.object(cache,'populate') as initialize:
                with self.assertRaisesRegex(RuntimeError,'incomplete private Windows SDK'):cache.prepare_workspace(root,2,identity)
                initialize.assert_not_called()

    def test_incomplete_private_sdk_is_refused_without_deleting_live_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'private-sdk';identity=cache.workspace_identity('/work/target/windows',2)
            def failed(root,workers):
                (root/'live-sdk').write_text('retain');raise RuntimeError('setup failed')
            with patch.object(cache,'populate',side_effect=failed):
                with self.assertRaisesRegex(RuntimeError,'setup failed'):cache.prepare_workspace(root,2,identity)
            for marker in [None,'invalid-json',json.dumps({**identity,'workers':3})]:
                if marker is not None:(root/'complete.json').write_text(marker)
                with patch.object(cache,'populate') as initialize:
                    with self.assertRaisesRegex(RuntimeError,'incomplete private Windows SDK'):cache.prepare_workspace(root,2,identity)
                    initialize.assert_not_called()
                self.assertEqual((root/'live-sdk').read_text(),'retain')

    def seed(self, root):
        seed = root / "seed"
        for name in ("crt", "sdk"):
            (seed / "xwin" / name).mkdir(parents=True)
            (seed / "xwin" / name / "data").write_text(name)
        (seed / "xwin" / "DONE").write_text("prepared")
        return seed

    def test_workers_own_writes_deletes_and_release_the_seed_before_compilation(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(cache.subprocess, "run", prepared):
            root = Path(temporary)
            seed = self.seed(root)
            first, second = root / "first", root / "second"
            cache.snapshot(seed, first)
            cache.snapshot(seed, second)
            paths = [entry / "xwin" / "crt" / "data" for entry in (seed, first, second)]
            self.assertEqual(len({path.stat().st_ino for path in paths}), 3)
            paths[1].write_text("first owns this write")
            self.assertEqual(paths[0].read_text(), "crt")
            self.assertEqual(paths[2].read_text(), "crt")
            paths[1].unlink()
            self.assertTrue(paths[0].exists() and paths[2].exists())
            with (seed / ".win-test.lock").open("a") as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_sdk_writes_change_identity_even_with_restored_mtime_and_same_done_marker(self):
        with tempfile.TemporaryDirectory() as temporary,patch.object(cache.subprocess,'run',prepared):
            root=Path(temporary);seed=self.seed(root)
            cache.snapshot(seed,root/'first')
            before=(root/'first/sdk-identity.sha256').read_text()
            marker=hashlib.sha256((seed/'xwin/DONE').read_bytes()).hexdigest()
            file=seed/'xwin/crt/data';stat=file.stat();file.write_text('new')
            os.utime(file,ns=(stat.st_atime_ns,stat.st_mtime_ns))
            cache.snapshot(seed,root/'second')
            self.assertNotEqual(before,(root/'second/sdk-identity.sha256').read_text())
            self.assertEqual(marker,hashlib.sha256((seed/'xwin/DONE').read_bytes()).hexdigest(),'DONE-only identity is the negative control')

    def test_seed_mutation_during_snapshot_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);seed=self.seed(root)
            def mutate(command,**kwargs):
                result=prepared(command,**kwargs)
                if command[0]=='cp':(seed/'xwin/crt/data').write_text('changed during copy')
                return result
            with patch.object(cache.subprocess,'run',side_effect=mutate):
                with self.assertRaisesRegex(RuntimeError,'SDK changed during snapshot'):cache.snapshot(seed,root/'worker')
            self.assertFalse((root/'worker/sdk-identity.sha256').exists())

    def test_escaping_sdk_alias_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(cache.subprocess, "run", prepared):
            root = Path(temporary)
            seed = self.seed(root)
            outside = root / "outside"
            outside.write_text("outside snapshot")
            (seed / "xwin" / "sdk" / "alias").symlink_to(outside)
            with self.assertRaisesRegex(RuntimeError, "SDK alias escapes"):
                cache.snapshot(seed, root / "worker")

    def test_seed_preparation_keeps_payload_stdout_machine_readable(self):
        source = inspect.getsource(cache.snapshot)
        former = source.replace(", stdout=sys.stderr", "")
        self.assertNotEqual(source, former)
        namespace = dict(cache.__dict__)
        exec(compile(former, "<former-sdk-stdout-control>", "exec"), namespace)
        for factory, protected in [(cache.snapshot, True), (namespace["snapshot"], False)]:
            with self.subTest(protected=protected), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                seed = self.seed(root)
                with (root / "stdout").open("w+") as machine, (root / "stderr").open("w+") as diagnostics:
                    def noisy_preparation(command, **kwargs):
                        if command[:3] == ["cargo", "xwin", "cache"]:
                            kwargs.setdefault("stdout", machine)
                            return run([sys.executable, "-c", "print('SDK preparation status')"], **kwargs)
                        return run(command, **kwargs)

                    with patch.object(cache.subprocess, "run", noisy_preparation), patch.object(cache.sys, "stderr", diagnostics):
                        factory(seed, root / "worker")
                    run([sys.executable, "-c", "print('{\"reason\":\"compiler-artifact\"}')"], stdout=machine, check=True)
                    machine.seek(0)
                    output = machine.read()
                    diagnostics.seek(0)
                    status = diagnostics.read()
                if protected:
                    self.assertEqual(json.loads(output), {"reason": "compiler-artifact"})
                    self.assertIn("SDK preparation status", status)
                else:
                    with self.assertRaises(json.JSONDecodeError):
                        json.loads(output)
                    self.assertIn("SDK preparation status", output)


if __name__ == "__main__":
    unittest.main()
