"""Exercise the shared source-retirement namespace and its original kernel lease."""
import fcntl
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE=Path(__file__).with_name('prune-wt-sdk.py')
def load(source=SOURCE):
    spec=importlib.util.spec_from_file_location('prune_retirement',source)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir='/mnt/bb/tmp')
        self.directory=Path(self.temp.name)
        self.root=self.directory/'sources';self.root.mkdir()
        self.locks=self.directory/'locks'
        self.module=load()
    def tearDown(self):self.temp.cleanup()
    def prune(self,**options):
        return self.module.prune(self.root,self.directory/'mirror',self.locks,max_count=0,active=set(),**options)
    def source(self,name='source'):
        source=self.root/name;source.mkdir();(source/'content').write_text('owned');return source
    def test_maximum_source_name_has_bounded_retirement_identity(self):
        name='s'*(os.pathconf(self.root,'PC_NAME_MAX')-len('.lock'))
        self.source(name)
        result=self.prune()
        self.assertEqual(result['removed'],[name]);self.assertEqual(list(self.root.iterdir()),[])
    def test_failed_retirement_keeps_original_source_lease_and_never_renames_again(self):
        self.source()
        for _ in range(2):
            with patch.object(self.module,'remove_tree',side_effect=PermissionError('ownership control')):
                with self.assertRaisesRegex(PermissionError,'ownership control'):self.prune()
            self.assertEqual([p.name for p in self.root.iterdir()],['.prune'])
            self.assertEqual([p.name for p in (self.root/'.prune').iterdir()],['source'])
        with (self.locks/'source.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            result=self.prune()
            self.assertEqual(result['held'],['source']);self.assertTrue(result['over_cap'])
            self.assertTrue((self.root/'.prune/source/content').exists())
        with patch.object(Path,'rename',side_effect=AssertionError('retirement renamed again')):
            result=self.prune()
        self.assertEqual(result['removed'],['source']);self.assertEqual(list(self.root.iterdir()),[])
        self.assertEqual(sorted(p.name for p in self.locks.iterdir()),['prune.lock','source.lock'])
    def test_retired_generation_drains_before_same_identity_successor(self):
        self.source()
        with patch.object(self.module,'remove_tree',side_effect=OSError('interrupted')):
            with self.assertRaisesRegex(OSError,'interrupted'):self.prune()
        self.source()
        self.assertEqual(self.prune()['removed'],['source'])
        self.assertEqual(list(self.root.iterdir()),[])
    def test_retired_copies_drain_even_when_below_count_and_age_thresholds(self):
        self.source()
        with patch.object(self.module,'remove_tree',side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):self.prune()
        result=self.module.prune(self.root,self.directory/'mirror',self.locks,active=set())
        self.assertEqual(result['removed'],['source']);self.assertEqual(list(self.root.iterdir()),[])
    def test_abandoned_retirement_at_name_limit_needs_no_longer_lease_name(self):
        name='.prune-'+'x'*(os.pathconf(self.root,'PC_NAME_MAX')-7)
        self.source(name)
        self.assertEqual(self.prune()['remaining'],0);self.assertEqual(list(self.root.iterdir()),[])
    def test_live_retirement_and_symlink_target_are_preserved(self):
        retired=self.root/'.prune';retired.mkdir();(retired/'live').mkdir()
        outside=self.directory/'outside';outside.mkdir();(outside/'keep').write_text('keep')
        (self.root/'.prune-link').symlink_to(outside,target_is_directory=True)
        result=self.module.prune(self.root,self.directory/'mirror',self.locks,max_count=0,active={str(retired/'live/child')})
        self.assertEqual(result['held'],['live']);self.assertTrue(result['over_cap'])
        self.assertTrue((retired/'live').exists());self.assertEqual((outside/'keep').read_text(),'keep')
    def mutation(self,anchor,replacement):
        source=SOURCE.read_text();self.assertEqual(source.count(anchor),1)
        candidate=self.directory/'mutant.py';candidate.write_text(source.replace(anchor,replacement))
        self.module=load(candidate)
    def test_source_derived_tomb_name_is_rejected_negative_control(self):
        self.mutation("tomb=p if p.name.startswith('.prune-') else retirement/p.name", "tomb=root/('.prune-'+p.name)")
        with self.assertRaisesRegex(OSError,'File name too long'):
            self.test_maximum_source_name_has_bounded_retirement_identity()
    def test_retired_lease_alias_is_rejected_negative_control(self):
        self.mutation("else:name=p.name.removesuffix('.prep')", "else:name=('retired-' if parent==retirement else '')+p.name.removesuffix('.prep')")
        with self.assertRaises(AssertionError):
            self.test_failed_retirement_keeps_original_source_lease_and_never_renames_again()
    def test_retirement_namespace_reentry_is_rejected_negative_control(self):
        self.mutation('if p==retirement or not p.is_dir() or p.is_symlink():continue', 'if not p.is_dir() or p.is_symlink():continue')
        self.source()
        with self.assertRaises(OSError):self.prune()

if __name__=='__main__':unittest.main()
