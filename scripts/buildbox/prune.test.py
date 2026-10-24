import fcntl
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('pruner',HERE/'prune-wt-sdk.py')
pruner=importlib.util.module_from_spec(spec);spec.loader.exec_module(pruner)


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory(dir='/mnt/bb/tmp')
        self.d=Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_failed_retirement_keeps_its_name_and_original_source_lease(self):
        root=self.d/'retire';root.mkdir();source=root/'old';source.mkdir()
        (source/'keep').write_text('partial removal evidence')
        locks=self.d/'locks';mirror=self.d/'mirror'
        with patch.object(pruner,'remove_tree',side_effect=PermissionError('ownership control')):
            for _ in range(2):
                result=pruner.prune(root,mirror,locks,max_count=0,active=set())
                self.assertEqual(result['errors'][0]['source'],'old')
                self.assertIn('ownership control',result['errors'][0]['error'])
                self.assertEqual([p.name for p in root.iterdir()],['.prune-old'])
        with (locks/'old.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            result=pruner.prune(root,mirror,locks,max_count=0,active=set())
            self.assertEqual(result['held'],['old']);self.assertTrue(result['over_cap'])
        result=pruner.prune(root,mirror,locks,max_count=0,active=set())
        self.assertEqual(result['removed'],['old']);self.assertEqual(list(root.iterdir()),[])
        # Former naming included the tomb itself and a new PID every time;
        # even the original source lease no longer protected that directory.
        original=root/'.prune-old-123';original.mkdir()
        old={'__name__':'old_pruner'}
        code=Path(pruner.__file__).read_text().replace("name=p.name.removeprefix('.prune-').removesuffix('.prep')","name=p.name.removesuffix('.prep')")
        code=code.replace("tomb=p if p.name.startswith('.prune-') else root/('.prune-'+p.name)","tomb=root/('.prune-'+p.name+'-'+str(os.getpid()))")
        exec(compile(code,'old-retirement','exec'),old)
        with (locks/'old.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_SH)
            with patch.object(old['remove_tree'].__globals__['shutil'],'rmtree',side_effect=PermissionError('old control')):
                self.assertEqual(old['prune'](root,mirror,locks,max_count=0,active=set())['errors'][0]['source'],'.prune-old-123')
        self.assertTrue(next(root.iterdir()).name.startswith('.prune-.prune-old-123-'))

    def test_one_failed_retirement_does_not_prevent_healthy_peer_cleanup(self):
        root=self.d/'copies';root.mkdir()
        for name in ['a-bad','b-good']:(root/name).mkdir()
        remove=pruner.remove_tree
        def retire(path):
            if path.name=='.prune-a-bad':raise PermissionError('foreign owner')
            remove(path)
        with patch.object(pruner,'remove_tree',side_effect=retire):
            result=pruner.prune(root,self.d/'mirror',self.d/'locks',max_count=0,active=set())
        self.assertEqual(result['removed'],['b-good'])
        self.assertEqual([row['source'] for row in result['errors']],['a-bad'])
        self.assertEqual([p.name for p in root.iterdir()],['.prune-a-bad'])
        # The former global exception aborts before b-good is even visited.
        (root/'b-good').mkdir()
        code=Path(pruner.__file__).read_text().replace("errors.append({'source':name,'error':str(error),'errno':error.errno})",'raise')
        old={'__name__':'old_pruner'};exec(compile(code,'abort-all-peers-control','exec'),old)
        with patch.dict(old,remove_tree=retire):
            with self.assertRaisesRegex(PermissionError,'foreign owner'):
                old['prune'](root,self.d/'mirror',self.d/'locks',max_count=0,active=set())
        self.assertTrue((root/'b-good').is_dir())


if __name__=='__main__':unittest.main()
