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
            root=Path(temporary);file=root/'lib.rs';file.write_text('original')
            s=file.stat();clock=root/'clock.json'
            clock.write_text(json.dumps({'root':str(root),'content_sha256':'fixture','cacheable':True,
                                         'files':[['lib.rs',*map(str,[s.st_mode,s.st_size,s.st_mtime_ns,s.st_ctime_ns]),store.hashlib.sha256(file.read_bytes()).hexdigest()]]}))
            meta={'source':{'content_sha256':'fixture'}};env={'BB_SOURCE_TIMES_FILE':str(clock)}
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


if __name__ == '__main__':
    unittest.main()
