"""Real isolated Git preparation and restart controls; no Cargo or shared worktree."""
import importlib.util,json,os,shutil,subprocess,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('main_prepare',HERE/'main-prepare.py')
main=importlib.util.module_from_spec(spec);spec.loader.exec_module(main)

class MainPreparation(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(dir='/mnt/bb/tmp',prefix='main-preparation-');self.addCleanup(self.tmp.cleanup)
  self.root=Path(self.tmp.name);self.repo=self.root/'repo';self.work=self.root/'work';self.state=self.root/'state';self.state.mkdir()
  self.repo.mkdir();self.git('init','-qb','main');self.git('config','user.name','fixture');self.git('config','user.email','fixture@example.invalid');self.git('config','commit.gpgsign','false')
  (self.repo/'file').write_text('old');self.git('add','.');self.git('commit','-qm','old');self.old=self.git('rev-parse','HEAD')
  (self.repo/'file').write_text('new');self.git('commit','-qam','new');self.head=self.git('rev-parse','HEAD')
  self.git('remote','add','origin',str(self.repo));self.git('fetch','-q','origin','main')
  self.git('worktree','add','-q','--detach',str(self.work),self.old)
  self.runner=self.root/'runner';self.runner.write_text('fixture runner\n')
 def git(self,*args):return subprocess.check_output(['git','-C',str(self.repo),*args],text=True,stderr=subprocess.PIPE).strip()
 def prepare(self,verify=False):return main.prepare(self.repo,self.work,self.state,self.head,self.runner,verify)
 def test_success_and_prepublication_head_and_runner_verification(self):
  self.prepare();self.prepare(True)
  subprocess.run(['git','-C',str(self.work),'checkout','-q','--detach',self.old],check=True)
  with self.assertRaisesRegex(main.module.PreparationError,'HEAD mismatch'):self.prepare(True)
  subprocess.run(['git','-C',str(self.work),'checkout','-q','--detach',self.head],check=True)
  (self.work/'.gate-checks.sh').write_text('changed')
  with self.assertRaisesRegex(main.module.PreparationError,'runner changed'):self.prepare(True)
 def test_index_lock_and_invalid_git_target_never_prepare(self):
  gitdir=Path(subprocess.check_output(['git','-C',str(self.work),'rev-parse','--absolute-git-dir'],text=True).strip())
  lock=gitdir/'index.lock';lock.touch()
  with self.assertRaisesRegex(main.module.PreparationError,'checkout'):self.prepare()
  self.assertFalse((self.state/'.main-prepared.json').exists());lock.unlink()
  (self.work/'.git').write_text('gitdir: /does/not/exist\n')
  with self.assertRaises(main.module.PreparationError):self.prepare()
 def test_clean_and_runner_copy_errors_stop_before_dispatch(self):
  actual=main.module.subprocess.run
  def fail_clean(argv,**kw):
   return subprocess.CompletedProcess(argv,9,'','forced clean error') if 'clean' in argv else actual(argv,**kw)
  with patch.object(main.module.subprocess,'run',side_effect=fail_clean):
   with self.assertRaisesRegex(main.module.PreparationError,'clean rc=9'):self.prepare()
  self.assertFalse((self.state/'.main-prepared.json').exists())
  self.runner.unlink()
  with self.assertRaises(FileNotFoundError):self.prepare()
  self.assertFalse((self.state/'.main-prepared.json').exists())
 def shell(self,negative=False,cut_off=False):
  lanes=self.root/'lanes';lanes.mkdir(exist_ok=True);state=lanes/'main-gate';state.mkdir(exist_ok=True)
  for name in ['main-prepare.py','gate-prepare.py']:shutil.copyfile(HERE/name,lanes/name)
  for name,text in {'gate-checks.sh':'fixture runner\n','gate-pkgs.sh':'echo --workspace\n','hangs.txt':'','failed-tests.sh':'exit 0\n','gate-baseline.py':'def eligible(text):return True\n'}.items():(lanes/name).write_text(text)
  tools=self.root/'bin';tools.mkdir(exist_ok=True)
  pwsh=tools/'pwsh';pwsh.write_text('#!/bin/bash\necho ran >> '+str(self.root/'dispatch')+'\nprintf "test result: ok. 1 passed; 0 failed\\nGATE_DONE rc=0\\n"\n');pwsh.chmod(0o755)
  if cut_off:pwsh.write_text('#!/bin/bash\necho ran >> '+str(self.root/'dispatch')+'\necho partial; exit 9\n')
  python=tools/'python'
  if not python.exists():python.symlink_to(sys.executable)
  home=self.root/'home';(home/'.codex-fleet').mkdir(parents=True,exist_ok=True)
  source=(HERE/'main-gate.sh').read_text().replace('Q:/wt/main-gate',str(self.work)).replace('Q:/sdk',str(self.repo)).replace('Q:/lanes',str(lanes)).replace('while :; do','for cycle in 1; do').replace('sleep 300','true')
  if negative:
   start=source.index('    if ! python '+str(lanes)+'/main-prepare.py');end=source.index('    # Keep Windows native streams',start)
   source=source[:start]+source[end:]
   source='\n'.join(line for line in source.split('\n') if 'main-prepare.py' not in line)
  script=self.root/'main.sh';script.write_text(source)
  result=subprocess.run(['bash',str(script)],env={**os.environ,'HOME':str(home),'PATH':str(tools)+':'+os.environ['PATH']},capture_output=True,text=True)
  self.assertEqual(result.returncode,0,result.stdout+result.stderr)
  return state
 def test_failed_preparation_is_held_across_restart_until_explicit_recovery(self):
  gitdir=Path(subprocess.check_output(['git','-C',str(self.work),'rev-parse','--absolute-git-dir'],text=True).strip());lock=gitdir/'index.lock';lock.touch()
  state=self.shell();self.assertEqual((state/'.last-input').read_text().strip(),self.head)
  self.assertFalse((self.root/'dispatch').exists());self.assertEqual(len(list((state/'infra').glob('*.log'))),1)
  lock.unlink();self.shell();self.assertFalse((self.root/'dispatch').exists(),'restart must not retry the same held input')
  (state/'.rerun').touch();self.shell();self.assertEqual((self.root/'dispatch').read_text(),'ran\n')
  self.assertTrue((state/(self.head+'.log')).exists())
 def test_old_unchecked_preparation_can_publish_the_previous_checkout(self):
  state=self.shell(negative=True)
  self.assertEqual((self.work/'file').read_text(),'old')
  self.assertTrue((state/(self.head+'.log')).exists(),'negative control reproduces a baseline named for untested main')
 def test_executor_infra_hold_survives_restart(self):
  state=self.shell(cut_off=True)
  self.assertEqual((self.root/'dispatch').read_text(),'ran\n');self.assertEqual(len(list((state/'infra').glob('*.log'))),1)
  self.shell();self.assertEqual((self.root/'dispatch').read_text(),'ran\n','restart cannot launch held main again')
  (state/'.rerun').touch();self.shell();self.assertEqual((self.root/'dispatch').read_text(),'ran\nran\n')
 def test_failed_fetch_is_held_and_deduplicated_without_dispatch(self):
  tools=self.root/'bin';tools.mkdir();git=tools/'git'
  git.write_text('#!/bin/bash\nif [[ " $* " == *" fetch "* ]]; then echo forced-fetch-error >&2; exit 9; fi\nexec '+shutil.which('git')+' "$@"\n');git.chmod(0o755)
  state=self.shell();self.shell();self.assertFalse((self.root/'dispatch').exists())
  alerts=(self.root/'home/.codex-fleet/alerts.log').read_text();self.assertEqual(alerts.count('preparation-INFRA'),1)
  git.unlink();self.shell();self.assertFalse((self.root/'dispatch').exists())
  (state/'.rerun').touch();self.shell();self.assertTrue((self.root/'dispatch').exists())

if __name__=='__main__':unittest.main(verbosity=2)
