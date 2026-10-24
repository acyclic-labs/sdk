"""Controls for the gate-only matrix adapter; no compiler or shared tool edits."""
import importlib.util
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('gate_target_cache', Path(__file__).with_name('gate-target-cache.py'))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class MatrixArtifacts(unittest.TestCase):
    def test_windows_phase_owns_a_separate_private_matrix_root(self):
        source=Path(__file__).with_name('gate-windows-features.sh').read_text()
        prefix=source.split('\necho "GATE_WINDOWS_FEATURES_START',1)[0]
        for legacy,explicit in [(False,False),(True,False),(False,True)]:
            with self.subTest(legacy=legacy,explicit=explicit):
                env={**os.environ,'GATE_FEATURE_TARGET_DIR':'/work/target/matrix-fixture'}
                env.pop('GATE_WINDOWS_FEATURE_TARGET_DIR',None)
                if explicit:env['GATE_WINDOWS_FEATURE_TARGET_DIR']='/work/target/explicit-windows'
                script=prefix+'\nprintf "%s\\n" "$GATE_FEATURE_TARGET_DIR"\n'
                if legacy:script=script.replace('python3 scripts/gate-target-cache.py private "$GATE_FEATURE_TARGET_DIR"','false')
                result=subprocess.run(['bash','-c',script,'fixture','full'],env=env,capture_output=True,text=True)
                self.assertEqual(result.returncode,0,result.stderr)
                expected='/work/target/explicit-windows' if explicit else '/work/target/matrix-fixture'+('' if legacy else '-windows-msvc')
                self.assertEqual(result.stdout.strip(),expected)

    def test_matrix_releases_parent_snapshot_lease_before_cargo_worker_execution(self):
        with tempfile.TemporaryDirectory(dir='/work/target') as temporary:
            root=Path(temporary);command,env=self.fixture(root)
            job=root/'job';target=root/'matrix'
            execute=gate.execute;flock=fcntl.flock
            def contender(command,env,pass_fds=()):
                # A blocking parent lease at execution would deadlock every
                # Cargo worker. Use the production lease with nonblocking flock.
                with patch.object(fcntl,'flock',side_effect=lambda fd,mode:flock(fd,mode|fcntl.LOCK_NB)):
                    with gate.cache.local_target_leases(job,target/'worker-0'):pass
                return execute(command,env,pass_fds)
            with patch.object(gate,'private_inputs',return_value={'repo':'sdk'}), \
                 patch.object(gate.cache.store,'ROOT',root/'store'), \
                 patch.object(gate.cache.store,'request_key',return_value='lease-fixture'), \
                 patch.object(gate.cache.store,'source_unchanged',return_value=True), \
                 patch.object(gate.cache,'record'), patch.object(gate,'execute',side_effect=contender):
                self.assertEqual(gate.matrix(target,command,env),0)
                # Negative: the former parent-held shape excludes the worker.
                with gate.cache.local_target_leases(job,target):
                    with self.assertRaises(BlockingIOError):contender(command,{**env,'CARGO_TARGET_DIR':str(target/'worker-0')})

    def test_runner_does_not_mutate_uploaded_source_and_old_chmod_invalidates_clock(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'runner.sh';source.write_text('#!/bin/bash\nprintf child-output')
            for executable in [False,True]:
                source.chmod(0o755 if executable else 0o644)
                before=source.stat()
                selected=gate.executable_runner(source)
                after=source.stat()
                self.assertEqual((before.st_mode,before.st_mtime_ns,before.st_ctime_ns),
                                 (after.st_mode,after.st_mtime_ns,after.st_ctime_ns))
                self.assertEqual(subprocess.check_output([str(selected)]),b'child-output')
                # The production old command mutates ctime even for mode0755.
                subprocess.run(['chmod','+x',str(source)],check=True)
                self.assertNotEqual(source.stat().st_ctime_ns,after.st_ctime_ns)

    def test_private_pr_preparation_is_empty_and_legacy_mutant_cleans_and_marks(self):
        source=Path(__file__).with_name('pr-gate-checks.sh').read_text()
        function='gate_cache_prepare(){'+source.split('gate_cache_prepare(){',1)[1].split('\ngate_cache_finish(){',1)[0]
        helper=Path(__file__).with_name('gate-target-cache.py')
        with tempfile.TemporaryDirectory(dir='/work/target') as temporary:
            root=Path(temporary); binary=root/'bin';binary.mkdir()
            cargo=binary/'cargo'
            cargo.write_text('''#!/bin/bash
echo "$*" >> "$CALLS"
if [ "$1" = metadata ]; then echo '{"workspace_members":["fixture"],"packages":[{"id":"fixture","name":"fixture"}]}'; fi
''');cargo.chmod(0o755)
            for negative in [False,True]:
                target=root/str(negative)
                script=function+f'''
set -eu
gate_output="{root}"
gate_private_targets=0
if python3 "{helper}" private "{target}"; then gate_private_targets=1; fi
gate_cache_prepare "{target}" fixture-boot
'''
                if negative:script=script.replace('then gate_private_targets=1','then gate_private_targets=0')
                result=subprocess.run(['bash','-c',script],capture_output=True,text=True,
                                      env={**os.environ,'PATH':str(binary)+':'+os.environ['PATH'],'CALLS':str(root/'calls')})
                self.assertEqual(result.returncode,0,result.stdout+result.stderr)
                self.assertEqual(bool(list(target.iterdir())),negative)
                if not negative:self.assertFalse((root/'calls').exists())
                else:
                    self.assertIn('clean --locked -p fixture',(root/'calls').read_text())
                    self.assertTrue((target/'.gate-cache-inflight').is_file())

    def test_real_private_mount_oracle_rejects_flags_ordinary_dirs_and_escaping_targets(self):
        env = dict(os.environ)
        self.assertIsNotNone(gate.private_inputs(env, ['/work/target/feature-proof']))
        self.assertIsNone(gate.private_inputs({**env, 'CARGO_INCREMENTAL': '1'}, ['/work/target/feature-proof']))
        self.assertIsNone(gate.private_inputs(env, ['/tmp/feature-proof']))
        with tempfile.TemporaryDirectory() as temporary:
            fake = Path(temporary) / 'meta.json'
            fake.write_text(json.dumps({'job_prefix': str(Path(temporary) / 'fake')}))
            flagged = {**env, 'BUILD_SLOT_RECEIPT_META': str(fake), 'BB_JOB_ACTIVE': '1'}
            self.assertIsNone(gate.private_inputs(flagged, ['/work/target/feature-proof']))
            # Former flag-only qualification falsely accepts this ordinary dir.
            self.assertEqual(flagged['BB_JOB_ACTIVE'], '1')
            for malformed in [None, [], 'metadata', {'source': []}, {'job_prefix': []}]:
                fake.write_text(json.dumps(malformed))
                self.assertIsNone(gate.private_inputs(flagged, ['/work/target']))

    def test_outputs_have_job_custody_and_do_not_mutate_uploaded_namespace(self):
        output=gate.output_directory()
        self.assertTrue(output.is_relative_to(Path(os.environ['BB_JOB_HOME']).parent))
        self.assertFalse(output.is_relative_to(Path('/work/src')))
        self.assertEqual(output.stat().st_mode & 0o777,0o700)
        with patch.object(gate,'private_inputs',return_value=None):
            self.assertEqual(gate.output_directory(),Path.cwd())
        with self.assertRaisesRegex(ValueError,'outside job custody'):
            gate.output_directory({**os.environ,'TMPDIR':'/tmp'})
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'tmp').mkdir();(root/'outside').mkdir()
            (root/'tmp/gate-output').symlink_to(root/'outside',target_is_directory=True)
            with patch.object(gate,'private_inputs',return_value={}),self.assertRaisesRegex(ValueError,'alias'):
                gate.output_directory({'TMPDIR':str(root/'tmp'),'BB_JOB_HOME':str(root/'home')})

    def test_killed_launcher_cannot_release_a_live_scheduler_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); job=root/'job'; target=root/'target'
            child='import sys;print("ready",flush=True);sys.stdin.read(1)'
            launcher=root/'launcher.py'
            launcher.write_text('''import importlib.util,os,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('gate',sys.argv[1]);gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
with gate.matrix_lifetime(Path(sys.argv[2]),Path(sys.argv[3])) as lease:
 gate.execute([sys.executable,'-c',sys.argv[4]],os.environ,pass_fds=(lease.fileno(),) if sys.argv[5]=='inherit' else ())
''')
            for inherited in [True,False]:
                with self.subTest(inherited=inherited):
                    process=subprocess.Popen([sys.executable,str(launcher),str(Path(gate.__file__)),str(job),str(target),child,
                                              'inherit' if inherited else 'old-shape'],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
                    try:
                        self.assertEqual(process.stdout.readline(),b'ready\n')
                        process.kill();process.wait()
                        path=job/'gate-matrix-leases'/gate.cache.store.digest(str(target.resolve()))
                        with path.open('a') as competitor:
                            if inherited:
                                with self.assertRaises(BlockingIOError):fcntl.flock(competitor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                            else:fcntl.flock(competitor,fcntl.LOCK_EX|fcntl.LOCK_NB)
                        process.stdin.write(b'x');process.stdin.flush()
                        self.assertEqual(process.stdout.read(),b'')
                        with gate.matrix_lifetime(job,target):pass
                    finally:
                        process.stdin.close();process.stdout.close()
                        if process.poll() is None:process.kill();process.wait()

    def fixture(self, root):
        script = root / 'matrix.py'
        script.write_text('''import os,pathlib,sys
target=pathlib.Path(os.environ['CARGO_TARGET_DIR'])
targets=[target,pathlib.Path(str(target)+'-features-1')]
warm=all((item/'compiled').exists() for item in targets)
print('warm='+str(warm),flush=True)
for item in targets:
 item.mkdir(parents=True,exist_ok=True);(item/'compiled').write_text('complete')
with open(os.environ['CALLS'],'a') as out:out.write(('warm' if warm else 'cold')+'\\n')
sys.exit(int(os.environ.get('MATRIX_RC','0')))
''')
        metadata = root/'meta.json'
        metadata.write_text('{}')
        return [sys.executable, str(script)], {'BUILD_SLOT_RECEIPT_META': str(metadata), 'CALLS': str(root/'calls'),
                                             'BB_JOB_HOME': str(root/'job/home'), 'CARGO_TARGET_DIR': str(root/'unused')}

    def test_completed_matrix_seeds_every_worker_but_checks_still_execute_and_status_propagates(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root = Path(temporary); command, env = self.fixture(root)
            records = []
            # Exercise the actual production Session and XFS reflink clone.
            # Key/proof injection isolates the fixture from fleet metadata and
            # publication; the real mount oracle is checked separately above.
            with patch.object(gate, 'private_inputs', return_value={'repo':'sdk'}), \
                 patch.object(gate.cache.store, 'ROOT', root/'store'), \
                 patch.object(gate.cache.store, 'request_key', return_value='matrix-fixture'), \
                 patch.object(gate.cache.store, 'source_unchanged', return_value=True), \
                 patch.object(gate.cache, 'local_target_leases', side_effect=lambda job,target:gate.cache.ExitStack()), \
                 patch.object(gate.cache, 'record', side_effect=lambda _, row:records.append(dict(row))):
                for index in range(2):
                    self.assertEqual(gate.matrix(root/str(index), command, env), 0)
                self.assertEqual([row['status'] for row in records], ['miss','hit'])
                self.assertTrue((root/'1/worker-0/compiled').is_file())
                self.assertEqual((root/'calls').read_text(), 'cold\nwarm\n')
                self.assertTrue((root/'1/worker-0-features-1/compiled').is_file())
                # A restored compile is not a stored PASS: run a failing child.
                self.assertEqual(gate.matrix(root/'2', command, {**env,'MATRIX_RC':'101'}), 101)
                self.assertEqual(records[-1]['status'], 'hit')
                self.assertEqual(records[-1]['matrix_rc'], 101)
                self.assertEqual((root/'calls').read_text(), 'cold\nwarm\nwarm\n')
                # Old per-Cargo snapshots never contain the completed matrix.
                with patch.object(gate.cache.store.Session, 'open', lambda session:session):
                    self.assertEqual(gate.matrix(root/'negative', command, env), 0)
                    self.assertNotEqual(records[-1]['status'], 'hit')
                    self.assertEqual((root/'calls').read_text().splitlines()[-1], 'cold')

    def test_failed_interrupted_or_mutated_matrix_never_publishes(self):
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
            root=Path(temporary); command,env=self.fixture(root)
            for name,status,unchanged in [('failed',101,True),('interrupted',143,True),('mutated',0,False)]:
                with self.subTest(name=name), patch.object(gate,'private_inputs',return_value={'repo':'sdk'}), \
                     patch.object(gate.cache.store,'ROOT',root/'store'), \
                     patch.object(gate.cache.store,'request_key',return_value=name), \
                     patch.object(gate.cache.store,'source_unchanged',side_effect=[True,unchanged] if status==0 else [True]), \
                     patch.object(gate.cache,'local_target_leases',side_effect=lambda job,target:gate.cache.ExitStack()), \
                     patch.object(gate.cache,'record'):
                    self.assertEqual(gate.matrix(root/name,command,{**env,'MATRIX_RC':str(status)}),status)
                    self.assertFalse((root/'store/entries'/name/'complete.json').exists())
            # Exact execution status includes signal termination, not just101.
            self.assertEqual(gate.execute([sys.executable,'-c','import os,signal;os.kill(os.getpid(),signal.SIGTERM)'],os.environ),143)

    def test_added_source_during_matrix_uses_the_shared_publication_guard(self):
        base=Path(__file__).parent/'buildbox'
        modules=[]
        for name,file in [('candidate_store','bb_target_store.py'),('cargo_controls','cargo_cache.test.py')]:
            spec=importlib.util.spec_from_file_location(name,base/file)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);modules.append(module)
        store,controls=modules;guard=store.source_unchanged
        for blind in [False,True]:
            with self.subTest(blind=blind),tempfile.TemporaryDirectory(dir='/work/target') as temporary:
                root=Path(temporary);source,env,meta=controls.LocalTargetLease.source_fixture(root)
                command,driver=self.fixture(root)
                # Preserve the source clock prepared by the shared fixture.
                env.update(driver,SOURCE_ROOT=str(source))
                # Replace the fixture's final exit with a source addition.
                program=Path(command[1]);program.write_text(program.read_text().replace("sys.exit(int(os.environ.get('MATRIX_RC','0')))",
                    "(pathlib.Path(os.environ['SOURCE_ROOT'])/'new.rs').write_text('new input')"))
                with patch.object(gate.cache,'store',store),patch.object(gate,'private_inputs',return_value=meta), \
                     patch.object(store,'ROOT',root/'store'),patch.object(store,'request_key',return_value='matrix-namespace'), \
                     patch.object(store,'source_unchanged',side_effect=lambda m,e:guard(m,e,source)), \
                     patch.object(store,'namespace_unchanged',wraps=store.namespace_unchanged) as inventory, \
                     patch.object(gate.cache,'record'):
                    if blind:inventory.side_effect=lambda *args:True
                    self.assertEqual(gate.matrix(root/'matrix',command,env),0)
                self.assertEqual((root/'store/entries/matrix-namespace/complete.json').is_file(),blind)

    def test_matrix_identity_is_full_source_command_environment_toolchain_and_target(self):
        meta={'repo':'sdk','source':{'content_sha256':'fixture'},'argv':['gate-feature-matrix-v1','node','matrix.mjs','6']}
        env={'BB_SOURCE_TIMES_POLICY':'blob-past-private-v1','BB_TARGET_REQUEST_SHA256':'fixture',
             'CARGO_TARGET_DIR':'/work/target/features','RUSTFLAGS':'original'}
        job=Path('/mnt/bb/jobs/fixture')
        with patch.object(gate.cache.store,'tool_identity',return_value=['original']):
            key=gate.cache.store.request_key(meta,env,job)
            self.assertNotEqual(key,gate.cache.store.request_key({**meta,'source':{'content_sha256':'changed'}},env,job))
            self.assertNotEqual(key,gate.cache.store.request_key({**meta,'argv':meta['argv']+['--each-feature']},env,job))
            self.assertNotEqual(key,gate.cache.store.request_key(meta,{**env,'RUSTFLAGS':'changed'},job))
            self.assertNotEqual(key,gate.cache.store.request_key(meta,{**env,'CARGO_TARGET_DIR':'/work/target/other'},job))
            with patch.object(gate.cache.store,'tool_identity',return_value=['changed']):
                self.assertNotEqual(key,gate.cache.store.request_key(meta,env,job))


if __name__ == '__main__':
    unittest.main()
