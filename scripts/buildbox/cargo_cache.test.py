"""Exercise the actual Cargo proxy's kernel lease ownership."""
import fcntl
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import bb_cargo_cache as cache


class LocalTargetLease(unittest.TestCase):
    def test_request_diagnostics_preserve_key_and_hash_every_component(self):
        store=cache.store
        meta={'repo':'sdk','source':{'content_sha256':'tree'},'argv':['cargo','check']}
        env={'BB_TARGET_REQUEST_SHA256':'payload','SECRET_FIXTURE':'private-value'}
        with patch.object(store,'tool_identity',return_value=['tool-fixture']):
            components={}
            key=store.request_key(meta,env,Path('/job'),components=components)
            self.assertEqual(key,store.request_key(meta,env,Path('/job')))
            self.assertEqual(set(components),{'version','source','argv','cwd','environment','tools','target'})
            self.assertTrue(all(len(value)==64 for value in components.values()))
            self.assertNotIn('private-value',repr(components))

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
