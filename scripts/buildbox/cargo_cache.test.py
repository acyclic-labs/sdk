"""Exercise the actual Cargo proxy's kernel lease ownership."""
import fcntl
from contextlib import nullcontext
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import bb_cargo_cache as cache


class LocalTargetLease(unittest.TestCase):
    def setUp(self):
        # This class isolates cache/lease semantics. Real Cargo storage
        # admission is exercised separately below, including the cold path.
        self.admission = patch.object(cache, 'admit_storage')
        self.admission.start()
        self.addCleanup(self.admission.stop)

    @staticmethod
    def source_fixture(root):
        source=root/'src';source.mkdir()
        env={key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
        env.update(GIT_DIR=str(source/'.git'),GIT_WORK_TREE=str(source))
        subprocess.run(['git','init','-q',str(source)],env=env,check=True)
        file=source/'lib.rs';file.write_text('original')
        subprocess.run(['git','add','lib.rs'],cwd=source,env=env,check=True)
        s=file.stat();clock=root/'clock.json'
        clock.write_text(json.dumps({'root':str(source),'content_sha256':'fixture','cacheable':True,
                                     'namespace':{'version':1,'pathspec':[],'skip':'(?!)'},
                                     'files':[['lib.rs',*map(str,[s.st_mode,s.st_size,s.st_mtime_ns,s.st_ctime_ns]),
                                               cache.store.hashlib.sha256(file.read_bytes()).hexdigest()]]}))
        env['BB_SOURCE_TIMES_FILE']=str(clock)
        return source,env,{'source':{'content_sha256':'fixture'}}

    def test_gate_markers_do_not_suppress_snapshot_restore_or_publication(self):
        for former,mutated in [(False,False),(True,False),(False,True)]:
            # Exercise real reflinks on the target store's production filesystem.
            with self.subTest(former=former,mutated=mutated), tempfile.TemporaryDirectory(dir=cache.store.ROOT.parent) as temporary:
                root=Path(temporary);target=root/'target';target.mkdir()
                for name in ['.gate-cache-boot','.gate-cache-inflight']:(target/name).write_text('current-job')
                metadata=root/'meta.json';metadata.write_text(json.dumps({'repo':'sdk'}))
                scaffold=cache.store.SCAFFOLD-({'.gate-cache-boot','.gate-cache-inflight'} if former else set())
                env={'BUILD_SLOT_RECEIPT_META':str(metadata),'BB_JOB_HOME':str(root/'job/home'),'RUSTC_WRAPPER':'sccache'}
                records=[];builds=[];original_run=subprocess.run
                def compile_or_copy(argv,**kwargs):
                    if argv[0]!=cache.CARGO:return original_run(argv,**kwargs)
                    destination=Path(kwargs['env']['CARGO_TARGET_DIR'])
                    builds.append((destination/'artifact').exists())
                    (destination/'artifact').write_text('compiled')
                    return subprocess.CompletedProcess(argv,0)
                with patch.dict(os.environ,env,clear=True), patch.object(cache.store,'ROOT',root/'store'), \
                     patch.object(cache.store,'SCAFFOLD',scaffold), patch.object(cache.store,'private_target',return_value=True), \
                     patch.object(cache.store,'request_key',return_value='fixture'), patch.object(cache,'target_path',return_value=target) as selected, \
                     patch.object(cache.store,'source_unchanged',return_value=not mutated), \
                     patch.object(cache,'local_target_leases',return_value=nullcontext([])), \
                     patch.object(cache.subprocess,'run',side_effect=compile_or_copy), \
                     patch.object(cache,'record',side_effect=lambda _,row:records.append(dict(row))):
                    self.assertEqual(cache.run(['check']),0)
                    if not former and not mutated:
                        restored=root/'restored';restored.mkdir()
                        for name in ['.gate-cache-boot','.gate-cache-inflight']:(restored/name).write_text('next-job')
                        selected.return_value=restored
                        self.assertEqual(cache.run(['check']),0)
                        self.assertEqual(records[-1]['status'],'hit')
                        self.assertEqual(builds,[False,True],'second Cargo invocation must see the restored artifact')
                completed=root/'store/entries/fixture/complete.json'
                self.assertEqual(completed.exists(),not former and not mutated)
                if mutated:self.assertEqual(records[0]['publish'],'source-mutated')
                self.assertEqual(records[0]['status'],'private-target' if former else ('changed-input' if mutated else 'miss'))
                for name in ['.gate-cache-boot','.gate-cache-inflight']:
                    self.assertEqual((target/name).read_text(),'current-job')
                    self.assertFalse((root/'store/entries/fixture/target'/name).exists())

    def test_source_clock_rejects_changed_content_even_when_mtime_is_restored(self):
        store=cache.store
        with tempfile.TemporaryDirectory() as temporary:
            root,env,meta=self.source_fixture(Path(temporary));file=root/'lib.rs';s=file.stat()
            self.assertTrue(store.source_unchanged(meta,env,root))
            file.write_text('mutated!');os.utime(file,ns=(s.st_atime_ns,s.st_mtime_ns))
            self.assertFalse(store.source_unchanged(meta,env,root))
            source=Path(store.__file__).read_text().replace(
                'expected != list(map(str, [s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns]))',
                'expected[:-1] != list(map(str, [s.st_mode, s.st_size, s.st_mtime_ns]))').replace(
                    'hashlib.sha256(file.read_bytes()).hexdigest() != content','False')
            old={};exec(compile(source,'missing-ctime-control','exec'),old)
            self.assertTrue(old['source_unchanged'](meta,env,root))
            self.assertFalse(store.source_unchanged({'source':{'content_sha256':'other'}},env,root))

    def test_namespace_addition_during_actual_cargo_compile_cannot_publish(self):
        store=cache.store;original_run=subprocess.run;guard=store.source_unchanged
        for mutation in ['unchanged','added','indexed','renamed','deleted','link']:
            for blind in ([False,True] if mutation in ['added','indexed'] else [False]):
                with self.subTest(mutation=mutation,blind=blind), tempfile.TemporaryDirectory(dir='/mnt/bb/tmp') as temporary:
                    root=Path(temporary);source,env,meta=self.source_fixture(root)
                    target=root/'target';target.mkdir();metadata=root/'meta.json'
                    metadata.write_text(json.dumps({'repo':'sdk',**meta}))
                    env.update(BUILD_SLOT_RECEIPT_META=str(metadata),BB_JOB_HOME=str(root/'job/home'),
                               CARGO_TARGET_DIR=str(target),RUSTC_WRAPPER='sccache')
                    for name in ['BB_CACHE_DISABLED','BB_TARGET_STORE_DISABLED']:env.pop(name,None)
                    def compiler(argv,**kwargs):
                        if argv[0]!=cache.CARGO:return original_run(argv,**kwargs)
                        (target/'artifact').write_text('compiled')
                        if mutation in ['added','indexed']:
                            (source/'new.rs').write_text('new source input')
                            if mutation=='indexed':original_run(['git','add','new.rs'],cwd=source,env=env,check=True)
                        elif mutation=='renamed':(source/'lib.rs').rename(source/'renamed.rs')
                        elif mutation=='deleted':(source/'lib.rs').unlink()
                        elif mutation=='link':
                            (source/'lib.rs').unlink();(source/'lib.rs').symlink_to('/etc/hostname')
                        return subprocess.CompletedProcess(argv,0)
                    with patch.dict(os.environ,env,clear=True),patch.object(store,'ROOT',root/'store'), \
                         patch.object(store,'private_target',return_value=True), \
                         patch.object(cache,'target_path',return_value=target), \
                         patch.object(cache,'local_target_leases',side_effect=lambda *args:nullcontext([])), \
                         patch.object(store,'request_key',return_value='namespace-fixture'), \
                         patch.object(store,'source_unchanged',side_effect=lambda m,e:guard(m,e,source)), \
                         patch.object(store,'namespace_unchanged',wraps=store.namespace_unchanged) as inventory, \
                         patch.object(subprocess,'run',side_effect=compiler):
                        if blind:inventory.side_effect=lambda *args:True
                        self.assertEqual(cache.run(['check','--locked']),0)
                    published=(root/'store/entries/namespace-fixture/complete.json').is_file()
                    self.assertEqual(published,mutation=='unchanged' or blind)
                    records=[json.loads(line) for line in Path(str(metadata)+'.target-store.jsonl').read_text().splitlines()]
                    self.assertEqual(records[-1]['publish'],'completed' if published else 'source-mutated')

    def test_malformed_clock_or_git_inventory_failure_refuses_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            root,env,meta=self.source_fixture(Path(temporary));clock=Path(env['BB_SOURCE_TIMES_FILE'])
            original=clock.read_text()
            for malformed in [None,[],{'files':[[]]}, {'files':[['..','a','b','c','d','e']]}]:
                clock.write_text(json.dumps(malformed))
                self.assertFalse(cache.store.source_unchanged(meta,env,root))
            clock.write_text(original)
            legacy=json.loads(original);legacy.pop('namespace');clock.write_text(json.dumps(legacy))
            self.assertFalse(cache.store.source_unchanged(meta,env,root))
            clock.write_text(original)
            with patch.object(subprocess,'run',side_effect=subprocess.CalledProcessError(128,['git'])):
                self.assertFalse(cache.store.source_unchanged(meta,env,root))

    def test_request_diagnostics_preserve_key_and_hash_every_component(self):
        store=cache.store
        meta={'repo':'sdk','source':{'content_sha256':'tree'},'argv':['cargo','check']}
        env={'BB_TARGET_REQUEST_SHA256':'payload','SECRET_FIXTURE':'private-value','BB_SOURCE_TIMES_POLICY':'blob-past-private-v1'}
        with patch.object(store,'tool_identity',return_value=['tool-fixture']),patch.object(store,'source_unchanged',return_value=True):
            components={}
            key=store.request_key(meta,env,Path('/job'),components=components)
            self.assertEqual(key,store.request_key(meta,env,Path('/job')))
            self.assertEqual(set(components),{'version','source','argv','cwd','environment','tools','target'})
            self.assertTrue(all(len(value)==64 for value in components.values()))
            self.assertNotIn('private-value',repr(components))
            self.assertIsNone(store.request_key(meta,{**env,'BB_SOURCE_TIMES_POLICY':''},Path('/job')))
            self.assertNotEqual(key,store.request_key({**meta,'source':{'content_sha256':'different-tree'}},env,Path('/job')))

    def test_catalog_lease_reaps_other_key_stages_and_excludes_live_publication(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'staging').mkdir()
            abandoned=root/'staging'/'old-key-crash';abandoned.mkdir()
            (abandoned/'artifact').write_text('partial')
            source=Path(cache.store.__file__).read_text()
            start=source.index('        # Every stage is created')
            end=source.index('        started = time.monotonic()',start)
            old=source[:start]+"        for abandoned in (self.root / 'staging').glob(self.key + '-*'):\n            remove_tree(abandoned)\n"+source[end:]
            namespace={};exec(compile(old,'old-stage-cleanup','exec'),namespace)
            old_session=namespace['Session'](root,'new-key',root/'target')
            old_session.open();old_session.close()
            self.assertTrue(abandoned.exists())
            contender='''import sys
from pathlib import Path
import bb_target_store as store
original=store.fcntl.flock
store.fcntl.flock=lambda fd,flags: original(fd,flags | store.fcntl.LOCK_NB)
session=store.Session(Path(sys.argv[1]),'new-key',Path(sys.argv[1])/'target')
try:session.open()
except BlockingIOError:sys.exit(73)
finally:session.close()
'''
            with (root/'catalog.lock').open('a') as catalog:
                fcntl.flock(catalog,fcntl.LOCK_EX)
                result=subprocess.run([sys.executable,'-c',contender,str(root)],cwd=Path(__file__).parent,capture_output=True,text=True)
                self.assertEqual(result.returncode,73,result.stderr)
                self.assertTrue(abandoned.exists())
            session=cache.store.Session(root,'new-key',root/'target')
            try:session.open();self.assertFalse(abandoned.exists())
            finally:session.close()

    def test_cold_jobs_bypass_store_reads_and_publication_at_both_entries(self):
        class Executed(Exception):pass
        self.assertFalse(cache.store.disabled({}))
        self.assertFalse(cache.store.disabled({'RUSTC_WRAPPER':'sccache'}))
        for flag,value in [('BB_CACHE_DISABLED','1'),('BB_TARGET_STORE_DISABLED','1'),('RUSTC_WRAPPER','')]:
            with self.subTest(flag=flag), patch.dict(os.environ,{flag:value,'BUILD_SLOT_RECEIPT_META':'unused'},clear=True), \
                 patch.object(cache.os,'execv',side_effect=Executed) as execute, \
                 patch.object(cache.store,'private_target',side_effect=AssertionError('store accessed')), \
                 patch.object(cache.store,'Session') as session:
                with self.assertRaises(Executed):cache.run(['check','--locked'])
                execute.assert_called_once_with(cache.CARGO,[cache.CARGO,'check','--locked'])
                self.assertIsNone(cache.store.prepare({},dict(os.environ),Path('/unused')))
                session.assert_not_called()
        # The former proxy reaches store metadata for a cold native job.
        source=Path(cache.__file__).read_text().replace('store.disabled(env)',"env.get('BB_TARGET_STORE_DISABLED') == '1'")
        namespace={};exec(compile(source,'old-cold-proxy','exec'),namespace)
        namespace['admit_storage'] = lambda argv, env: None
        with patch.dict(os.environ,{'BB_CACHE_DISABLED':'1','BUILD_SLOT_RECEIPT_META':'/missing-cold-fixture'},clear=True), \
             patch.object(cache.os,'execv',side_effect=Executed) as execute:
            with self.assertRaises(FileNotFoundError):namespace['run'](['check','--locked'])
            execute.assert_not_called()
        source=Path(cache.store.__file__).read_text().replace('if disabled(env) or',"if env.get('BB_TARGET_STORE_DISABLED') == '1' or")
        namespace={};exec(compile(source,'old-cold-store','exec'),namespace)
        namespace['private_target']=lambda meta: (_ for _ in ()).throw(AssertionError('store accessed'))
        with self.assertRaisesRegex(AssertionError,'store accessed'):
            namespace['prepare']({},{'BB_CACHE_DISABLED':'1'},Path('/unused'))

    def test_independent_jobs_with_the_same_virtual_target_do_not_block(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / 'work' / 'target'
            target.mkdir(parents=True)
            first = cache.local_target_lock(root / 'first', target)
            second = cache.local_target_lock(root / 'second', target)
            self.assertNotEqual(first, second)
            with first.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                result = self.contender(second)
            self.assertEqual(result.returncode, 0, result.stderr)
            # The former parent-based path aliases distinct job identities.
            old_first = target.parent / ('.' + target.name + '.bb-store-lock')
            old_second = target.parent / ('.' + target.name + '.bb-store-lock')
            with old_first.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                self.assertEqual(self.contender(old_second).returncode, 73)

    def test_one_job_target_and_its_aliases_share_one_kernel_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / 'target'
            target.mkdir()
            original = cache.local_target_lock(root / 'job', target)
            alias = cache.local_target_lock(root / 'job', target / '..' / 'target')
            self.assertEqual(original, alias)
            self.assertTrue(original.is_relative_to(root / 'job'))
            self.assertFalse(original.is_relative_to(target))
            with original.open('a') as held:
                fcntl.flock(held, fcntl.LOCK_EX)
                self.assertEqual(self.contender(alias).returncode, 73)
            self.assertEqual(self.contender(alias).returncode, 0)

    @staticmethod
    def contender(path):
        return subprocess.run(
            [sys.executable, '-c', '''import fcntl,sys
with open(sys.argv[1], 'a') as lock:
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(73)
''', str(path)], capture_output=True, text=True, check=False,
        )

    def test_parent_snapshot_excludes_child_writers_but_siblings_can_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root/'target'; target.mkdir()
            child = target/'child'; child.mkdir()
            sibling = target/'sibling'; sibling.mkdir()
            job = root/'job'
            def contender(path):
                return subprocess.run([sys.executable, '-c', '''import sys
from pathlib import Path
import bb_cargo_cache as cache
original=cache.fcntl.flock
cache.fcntl.flock=lambda fd,flags: original(fd,flags | cache.fcntl.LOCK_NB)
try:
 with cache.local_target_leases(Path(sys.argv[1]),Path(sys.argv[2]),Path(sys.argv[3])):pass
except BlockingIOError:sys.exit(73)
''', str(job), str(path), str(target)], cwd=Path(__file__).parent,
                    capture_output=True, text=True)
            with cache.local_target_leases(job,target,target):
                self.assertEqual(contender(child).returncode,73)
            with cache.local_target_leases(job,child,target):
                self.assertEqual(contender(sibling).returncode,0)
                self.assertEqual(contender(target).returncode,73)
            # Former leaf-only ownership permits a child to mutate a parent
            # while its snapshot is being read.
            with cache.local_target_lock(job,target).open('a') as held:
                fcntl.flock(held,fcntl.LOCK_EX)
                self.assertEqual(self.contender(cache.local_target_lock(job,child)).returncode,0)
            self.assertEqual(contender(child).returncode,0)


class CargoStorageAdmission(unittest.TestCase):
    class Executed(Exception):
        pass

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='job-storage-control-', dir='/mnt/bb/jobs')
        self.addCleanup(self.temporary.cleanup)
        self.job = Path(self.temporary.name)
        self.project = self.job / 'project'
        (self.project / 'src').mkdir(parents=True)
        (self.project / 'src/lib.rs').write_text('')
        (self.project / 'Cargo.toml').write_text(
            '[package]\nname="storage-control"\nversion="0.0.0"\nedition="2024"\n[workspace]\n')
        self.metadata = self.job / 'receipt.json'
        self.metadata.write_text('{}')
        self.env = {**os.environ, 'BB_JOB_HOME': str(self.job / 'home'),
                    'CARGO_TARGET_DIR': str(self.job / 'target'),
                    'BUILD_SLOT_RECEIPT_META': str(self.metadata), 'BB_CACHE_DISABLED': '1'}
        self.env.pop('CARGO_BUILD_BUILD_DIR', None)
        self.env.pop('CARGO_BUILD_TARGET_DIR', None)
        previous = Path.cwd()
        os.chdir(self.project)
        self.addCleanup(os.chdir, previous)

    def launched(self, argv, env=None, implementation=None):
        with patch.dict(os.environ, env or self.env, clear=True), \
             patch.object(cache.os, 'execv', side_effect=self.Executed) as execute:
            with self.assertRaises(self.Executed):
                (implementation or cache.run)(argv)
            execute.assert_called_once_with(cache.CARGO, [cache.CARGO, *argv])

    def rejected(self, argv, env=None, message='/mnt/bb filesystem'):
        with patch.dict(os.environ, env or self.env, clear=True), \
             patch.object(cache.os, 'execv') as execute:
            with self.assertRaisesRegex(RuntimeError, message):
                cache.run(argv)
            execute.assert_not_called()

    def test_actual_proxy_rejects_root_targets_before_every_cache_bypass(self):
        source = Path(cache.__file__).read_text().replace(
            '    admitted_target = admit_storage(argv, env)\n',
            "    admitted_target = os.environ.get('CARGO_TARGET_DIR')\n")
        old = {}
        exec(compile(source, 'old-unadmitted-cargo-proxy', 'exec'), old)
        root = '/work/main-green-storage-root-control'
        for flags in ({'BB_CACHE_DISABLED': '1'}, {'BB_CACHE_DISABLED': '0', 'RUSTC_WRAPPER': 'sccache'},
                      {'BB_TARGET_STORE_DISABLED': '1'}, {'RUSTC_WRAPPER': ''},
                      {'BUILD_SLOT_RECEIPT_META': ''}):
            env = {**self.env, **flags, 'CARGO_TARGET_DIR': root}
            with self.subTest(flags=flags):
                self.rejected(['check', '--locked'], env)
                self.launched(['check', '--locked'], env, old['run'])
        self.assertFalse(Path(root).exists(), 'controls must never launch a compiler')

    def test_cli_environment_and_config_select_the_effective_directories(self):
        root = '/work/main-green-storage-root-control'
        for args, env in [(['build', '--target-dir', root], self.env),
                          (['build', '--target-dir=' + root], self.env),
                          (['build'], {**self.env, 'CARGO_TARGET_DIR': root}),
                          (['build'], {**self.env, 'CARGO_BUILD_BUILD_DIR': root}),
                          (['build', '--config', 'build.build-dir="' + root + '"'], self.env),
                          (['build', '--config=build.build-dir="' + root + '"'], self.env)]:
            with self.subTest(args=args, env_override=env != self.env):
                self.rejected(args, env)
        # Cargo's dedicated target environment overrides its config value.
        self.launched(['build', '--config', 'build.target-dir="' + root + '"'])
        without_target = {k: v for k, v in self.env.items() if k != 'CARGO_TARGET_DIR'}
        self.rejected(['build', '--config', 'build.target-dir="' + root + '"'], without_target)
        self.rejected(['build'], {**without_target, 'CARGO_BUILD_TARGET_DIR': root})
        self.assertFalse(Path(root).exists())

    def test_config_files_and_workspace_configuration_use_cargos_parser(self):
        configuration = self.project / '.cargo'
        configuration.mkdir()
        file = configuration / 'config.toml'
        file.write_text('[build]\nbuild-dir="/work/main-green-storage-root-control"\n')
        self.rejected(['check'])
        file.unlink()
        supplied = self.job / 'extra-config.toml'
        supplied.write_text('[build]\nbuild-dir="/work/main-green-storage-root-control"\n')
        self.rejected(['check', '--config', str(supplied)])

    def test_metadata_must_prove_both_output_directories(self):
        complete = {'target_directory': str(self.job / 'target'),
                    'build_directory': str(self.job / 'intermediate')}
        for missing in complete:
            incomplete = {key: value for key, value in complete.items() if key != missing}
            with self.subTest(missing=missing), patch.object(cache.subprocess, 'run', side_effect=[
                    subprocess.CompletedProcess([], 0, stdout=''),
                    subprocess.CompletedProcess([], 0, stdout=json.dumps(incomplete))]):
                self.rejected(['check'], message='both target and build directories')

    def test_global_configuration_keeps_the_compilation_and_storage_identity(self):
        args = ['--config', 'build.build-dir="/work/main-green-storage-root-control"', 'check']
        self.assertEqual(cache.compilation(args), (args, False))
        self.rejected(args)

    def test_effective_device_and_job_custody_cover_aliases_and_foreign_paths(self):
        alias = self.job / 'root-alias'
        alias.symlink_to('/work', target_is_directory=True)
        self.rejected(['check', '--target-dir', str(alias / 'main-green-storage-root-control')])
        with tempfile.TemporaryDirectory(dir='/mnt/bb/jobs', prefix='job-foreign-control-') as other:
            self.rejected(['check', '--target-dir', str(Path(other) / 'target')], message='outside the current job')
            alias.unlink()
            alias.symlink_to(other, target_is_directory=True)
            self.rejected(['check', '--target-dir', str(alias / 'target')], message='outside the current job')
        self.rejected(['check'], {**self.env, 'BB_JOB_HOME': ''}, 'current job custody')
        self.launched(['check', '--target-dir', str(self.job / 'target/nested')])
        self.launched(['check', '--target-dir', str(self.job / 'target/../other-target')])
        with tempfile.TemporaryDirectory(dir='/mnt/bb/tmp', prefix='job-nested-control-') as temporary:
            nested = Path(temporary) / 'jobs' / 'job-owned'
            nested.mkdir(parents=True)
            env = {**self.env, 'BB_JOB_HOME': str(nested / 'home'),
                   'CARGO_TARGET_DIR': str(nested / 'target')}
            self.launched(['check'], env)
            self.rejected(['check', '--target-dir', str(self.job / 'target')], env,
                          message='outside the current job')

    def test_every_compilation_entry_and_runtime_arguments_pass_admission(self):
        for verb in ('build', 'check', 'clippy', 'test', 'bench', 'run', 'doc', 'rustc',
                     'rustdoc', 'package', 'publish', 'fix', 'xwin', 'nextest'):
            with self.subTest(verb=verb):
                self.launched([verb, '--locked'])
        self.launched(['test', '--doc'])
        self.launched(['test', '--', '--target-dir', '/work/runtime-argument-is-not-cargo'])
        self.launched(['check'], {**self.env, 'BB_CACHE_DISABLED': '0', 'RUSTC_WRAPPER': 'sccache'})

    def test_configured_aliases_cannot_hide_target_overrides(self):
        for command in ('build --target-dir /work/main-green-storage-root-control',
                        '!cargo build --target-dir /work/main-green-storage-root-control'):
            self.rejected(['--config', 'alias.storage-control="' + command + '"', 'storage-control'],
                          message='expanded alias command')

    def test_local_install_discovers_the_producers_configuration(self):
        producer = self.job / 'producer'
        (producer / 'src').mkdir(parents=True)
        (producer / 'src/lib.rs').write_text('')
        (producer / 'Cargo.toml').write_text(
            '[package]\nname="install-producer"\nversion="0.0.0"\nedition="2024"\n[workspace]\n')
        (producer / '.cargo').mkdir()
        config = producer / '.cargo/config.toml'
        config.write_text('[build]\nbuild-dir="/work/main-green-storage-root-control"\n')
        source = Path(cache.__file__).read_text().replace(
            "if verb == 'install' and install_path is not None:", 'if False:')
        old = {}
        exec(compile(source, 'missing-install-config-discovery-control', 'exec'), old)
        for args in (['install', '--path', str(producer)], ['install', '--path=' + str(producer)]):
            with self.subTest(args=args):
                self.rejected(args)
                self.launched(args, implementation=old['run'])
        config.unlink()
        self.launched(['install', '--path', str(producer)])

    def test_install_cannot_borrow_an_unrelated_workspaces_configuration(self):
        for args in (['install', 'a-registry-producer'],
                     ['install', '--git', 'https://example.invalid/producer']):
            self.rejected(args, message='local --path producer')
        self.launched(['install', '--list'])


if __name__ == '__main__':
    unittest.main()
