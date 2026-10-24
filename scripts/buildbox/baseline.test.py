#!/usr/bin/env python3
"""Isolated baseline dispatch/verdict regression tests; no Rust compilation."""
from pathlib import Path
import contextlib,hashlib,io,json,os,shutil,subprocess,sys,tempfile,types,unittest
HERE=Path(__file__).resolve().parent
class BaselineTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(dir='/mnt/bb/tmp',prefix='r19-baseline-');self.d=Path(self.temp.name)
 def tearDown(self):self.temp.cleanup()
 def qualify(self,negative=False,executor=None):
  root=self.d;work=root/'work';work.mkdir();(root/'bin').mkdir()
  (root/'hangs').write_text('');(root/'gate-baseline.py').write_text('def coverage(text,packages):return set()\n')
  shutil.copyfile(HERE.parent/'gate-test-runner.sh',root/'gate-test-runner.sh')
  metadata={'workspace_members':['fixture'],'packages':[{'name':'acyclic-conformance','id':'fixture','targets':[]}]}
  cargo=root/'bin/cargo';cargo.write_text('''#!/bin/bash
case "$1" in
metadata) printf '%s\n' 'METADATA';;
test)
 echo "$*" >> calls
 if [[ " $* " == *" --no-run "* ]]; then exit 0; fi
 if [[ " $* " == *" --all-targets "* ]]; then
  echo 'thread main panicked: Unauthorized("runtime signing custody is closed")'
  echo 'error: test failed, to rerun pass `-p acyclic-conformance --bench harness`'
  exit 101
 fi
 echo 'test result: ok. 1 passed; 0 failed';;
*) exit 9;;
esac
'''.replace('METADATA',json.dumps(metadata)));cargo.chmod(0o755)
  baseline=root/'old-main.log';baseline.write_text('qualified old main')
  pr=root/'pr.log';pr.write_text('error: test failed, to rerun pass `-p acyclic-conformance --bench harness`\n')
  scope=root/'scope.json';scope.write_text(json.dumps({'tests':['acyclic-conformance']}))
  results=root/'results';results.mkdir()
  # A historical default-target result must not satisfy wider qualification.
  (results/'old.json').write_text(json.dumps({'base':'a'*40,'roots':['acyclic-conformance'],'docs':False,'skips':[]}))
  (results/'old.log').write_text('test result: ok. 1 passed\nGATE_TEST_BASELINE_COMPLETE rc=0\n')
  (results/'interrupted.json').write_text(json.dumps({'base':'a'*40,'roots':['acyclic-conformance'],'docs':False,'skips':[],'target_coverage':'all-targets+doc-artifacts-v3','runner_sha256':hashlib.sha256((root/'gate-test-runner.sh').read_bytes()).hexdigest()}))
  (results/'interrupted.log').write_text('test result: ok. 1 passed\nGATE_TEST_BASELINE_COMPLETE rc=0\n')
  source=(HERE/'coordinator/gate-test-baseline.py').read_text().replace("Path('Q:/lanes/hangs.txt')",f'Path({str(root/"hangs")!r})')
  if negative:source=source.replace("else 'run_tests --all-targets; rc=$?'", "else 'run_tests; rc=$?'")
  module=types.ModuleType('fixture');module.__file__=str(root/'gate-test-baseline.py');exec(compile(source,module.__file__,'exec'),module.__dict__)
  def run(argv,**kw):
   if argv[0]=='git':return subprocess.CompletedProcess(argv,0)
   raise AssertionError('native dispatch must use the single pipe writer')
  def popen(argv,**kw):
   env={**kw['env'],'PATH':str(root/'bin')+':'+os.environ['PATH'],'GATE_MAIN_TARGET_DIR':str(root/'cache')}
   process=subprocess.Popen(['bash',str(work/'.gate-test-baseline.sh')],cwd=work,env=env,stdout=kw['stdout'],stderr=kw['stderr'])
   if executor is not None:
    wait=process.wait
    process.wait=lambda *a,**k:(wait(*a,**k),executor)[1]
   return process
  module.subprocess=types.SimpleNamespace(DEVNULL=subprocess.DEVNULL,STDOUT=subprocess.STDOUT,PIPE=subprocess.PIPE,Popen=popen,run=run,check_output=lambda *a,**k:json.dumps(metadata).encode())
  module.shutil=types.SimpleNamespace(copyfile=shutil.copyfile,copyfileobj=shutil.copyfileobj,which=lambda name:name)
  argv=sys.argv;sys.argv=['baseline',str(root),'a'*40,str(pr),'--scope',str(scope),'--baseline-log',str(baseline),'--worktree',str(work),'--results',str(results)]
  output=io.StringIO()
  try:
   with contextlib.redirect_stdout(output):self.assertEqual(module.main(),0)
  finally:sys.argv=argv
  log=Path(output.getvalue().strip());self.assertNotEqual(log,results/'old.log');self.assertNotEqual(log,results/'interrupted.log')
  text=log.read_text();calls=(work/'calls').read_text()
  self.assertIn(' --doc ',calls)
  return text,calls,module
 def test_main_qualification_matches_all_targets_and_separate_docs(self):
  text,calls,module=self.qualify();self.assertIn(' --all-targets ',calls)
  self.assertIn('GATE_TEST_BASELINE_COMPLETE rc=101',text);self.assertIn('runtime signing custody is closed',text)
  self.assertTrue(module.baseline_complete('error: test failed, to rerun pass `-p p --bench b`\nGATE_TEST_BASELINE_COMPLETE rc=101\nGATE_TEST_BASELINE_EXECUTOR rc=101\n'))
  self.assertFalse(module.baseline_complete('error: could not compile p\nGATE_TEST_BASELINE_COMPLETE rc=101\n'))
 def test_old_default_target_selection_misses_the_main_benchmark(self):
  text,calls,_=self.qualify(True);self.assertNotIn(' --all-targets ',calls)
  self.assertIn('GATE_TEST_BASELINE_COMPLETE rc=0',text);self.assertNotIn('runtime signing custody is closed',text)
 def test_executor_failure_cannot_qualify_a_passing_main_payload(self):
  with self.assertRaisesRegex(RuntimeError,'baseline did not complete'):
   self.qualify(negative=True,executor=224)
  logs=[p for p in (self.d/'results').glob('*.log') if p.name not in ['old.log','interrupted.log']]
  self.assertEqual(len(logs),1)
  self.assertIn('GATE_TEST_BASELINE_EXECUTOR rc=224',logs[0].read_text())
 def test_incomplete_or_mismatched_executor_never_qualifies_or_reuses(self):
  _,_,module=self.qualify()
  for payload in [0,101]:
   footer=f'test result: FAILED. 0 passed; 1 failed\nGATE_TEST_BASELINE_COMPLETE rc={payload}\n'
   self.assertFalse(module.baseline_complete(footer))
   for executor in [-9,224,1 if payload==101 else 101]:
    self.assertFalse(module.baseline_complete(footer+f'GATE_TEST_BASELINE_EXECUTOR rc={executor}\n'))
   self.assertTrue(module.baseline_complete(footer+f'GATE_TEST_BASELINE_EXECUTOR rc={payload}\n'))
  for status in [1,2,224,-9]:
   self.assertFalse(module.baseline_complete(f'test result: ok. 1 passed\nGATE_TEST_BASELINE_COMPLETE rc={status}\nGATE_TEST_BASELINE_EXECUTOR rc={status}\n'))
  for error in ['[buildbox] HOME guard INFRA: unavailable','error: GATE_RUNNER_INFRA missing manifest']:
   self.assertFalse(module.baseline_complete(error+'\ntest result: ok. 1 passed\nGATE_TEST_BASELINE_COMPLETE rc=101\nGATE_TEST_BASELINE_EXECUTOR rc=101\n'))
 def test_pipe_writer_preserves_reopened_native_streams_and_nonzero_status(self):
  module=types.ModuleType('transport')
  source=HERE/'coordinator/gate-test-baseline.py'
  exec(compile(source.read_text(),str(source),'exec'),module.__dict__)
  # Native descendants reopening a regular inherited output get a separate
  # offset. Use that actual descriptor boundary, not a simulated log writer.
  program="""import os
os.write(1,b'wrapper-header\\n')
native=os.open('/proc/self/fd/1',os.O_WRONLY|os.O_TRUNC)
os.write(native,b'cargo output\\x00\\xff\\n')
os.close(native)
os.write(2,b'error: first compiler diagnostic\\npanic lines\\n')
raise SystemExit(101)
"""
  command=[sys.executable,'-B','-c',program]
  expected=b'wrapper-header\ncargo output\x00\xff\nerror: first compiler diagnostic\npanic lines\n'
  log=self.d/'pipe.log'
  self.assertEqual(module.run_logged(command,log,cwd=self.d,env=os.environ),101)
  self.assertEqual(log.read_bytes(),expected)
  # The former direct-file transport loses the header and overwrites bytes.
  legacy=self.d/'direct.log'
  with legacy.open('wb') as output:
   result=subprocess.run(command,cwd=self.d,stdout=output,stderr=subprocess.STDOUT)
  self.assertEqual(result.returncode,101)
  self.assertNotEqual(legacy.read_bytes(),expected)
  self.assertNotIn(b'wrapper-header',legacy.read_bytes())
 def test_partial_stream_is_visible_before_eof_and_interruption_stays_incomplete(self):
  source=HERE/'coordinator/gate-test-baseline.py'
  prefix=b'error: compiler diagnostic before stream interruption\n'
  for negative in [False,True]:
   module=types.ModuleType('transport')
   code=source.read_text()
   if negative:code=code.replace("log.open('wb', buffering=0)","log.open('wb')")
   exec(compile(code,str(source),'exec'),module.__dict__)
   log=self.d/('buffered.log' if negative else 'live.log')
   class Stream:
    first=True
    def read(stream,length):
     if stream.first:stream.first=False;return prefix
     if log.read_bytes()!=prefix:raise AssertionError('diagnostic not visible before EOF')
     raise OSError('fixture stream interrupted')
    def close(stream):pass
   class Child:
    stdout=Stream();terminated=False
    def poll(child):return None
    def terminate(child):child.terminated=True
    def wait(child):return -15
   child=Child()
   module.subprocess=types.SimpleNamespace(PIPE=subprocess.PIPE,STDOUT=subprocess.STDOUT,Popen=lambda *a,**kw:child)
   with self.assertRaisesRegex(AssertionError if negative else OSError,'not visible before EOF' if negative else 'stream interrupted'):
    module.run_logged(['fixture'],log,cwd=self.d,env=os.environ)
   self.assertTrue(child.terminated)
   self.assertEqual(log.read_bytes(),prefix)
   self.assertFalse(module.baseline_complete(log.read_text()))
 def test_actual_verdict_qualifies_non_libtest_targets_without_hiding_new_failures(self):
  root=self.d;(root/'.codex-fleet').mkdir();(root/'flakes').write_text('')
  helper=root/'failed-tests.sh';source=(HERE/'coordinator/failed-tests.sh').read_text()
  checks=root/'checks.sh';checks.write_text('#!/bin/bash\nexit 0\n')
  coordinator=(HERE/'coordinator/pr-gate.sh').read_text();block=coordinator[coordinator.index('pr_fails=$('):]
  block=block.replace('Q:/lanes/failed-tests.sh',str(helper)).replace('Q:/lanes/flakes.txt',str(root/'flakes')).replace('Q:/lanes/gate-checks-verdict.sh',str(checks))
  known='error: test failed, to rerun pass `-p acyclic-conformance --bench harness`\n'
  for negative,new,compiler,expected in [(False,False,False,0),(True,False,False,1),(False,True,False,1),(False,False,True,1)]:
   helper.write_text(source[:source.index('# Non-libtest')]+ '} | sort -u\n' if negative else source)
   pr=root/'pr.log';base=root/'main.log'
   pr.write_text((known.replace('bench harness','bench new_bench') if new else known)+('error: could not compile acyclic-conformance\n' if compiler else '')+'test result: ok. 1 passed; 0 failed\nGATE_DONE rc=101\n')
   base.write_text(known+'test result: ok. 1 passed; 0 failed\n')
   script=f'rc=101;n=1;log={pr};base_log={base};test_base_log="";clippy_base={base};features_base={base}\n'+block
   result=subprocess.run(['bash','-c',script],capture_output=True,text=True,env={**os.environ,'HOME':str(root)})
   self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
   self.assertIn('GATE '+('FAIL' if expected else 'PASS'),result.stdout)
if __name__=='__main__':unittest.main(verbosity=2)
