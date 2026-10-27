"""Linux, no models: native-process fixture plus the real Codex sandbox.

Run on bb2: python3 managed-session.py ACYCLIC GRAPHCODER CODEX_0_164_OR_LATER lifecycle|recursive|resume.
Executables must already be built. Native model-session acceptance belongs to
the real-host e2e suite; this regression never reads native authentication.
"""
import json, os, pathlib, selectors, signal, subprocess, sys, tempfile, time, traceback
from native_events import native_event_fixture
from native_fixture import fixture_environment
if len(sys.argv) != 5:
 raise SystemExit(__doc__)
acyclic, graphcoder, real_codex = map(lambda path: str(pathlib.Path(path).resolve()), sys.argv[1:4])
scenario=sys.argv[4]
assert scenario in ('lifecycle','recursive','resume')
fixture_path, native_session_id = native_event_fixture('codex')
started=time.monotonic()
def interrupted(signum, frame):
 traceback.print_stack(frame)
 raise SystemExit('fixture interrupted')
signal.signal(signal.SIGTERM,interrupted)
with tempfile.TemporaryDirectory(prefix='.plugins-session-proof-',dir=pathlib.Path.home()) as temporary:
 root=pathlib.Path(temporary);workspace=root/'workspace';workspace.mkdir();state=root/'state';state.mkdir();bin_dir=root/'bin';bin_dir.mkdir()
 env=fixture_environment(root, {'PATH':str(bin_dir)+':'+os.environ['PATH'],'FIXTURE_CODEX':real_codex,'FIXTURE_EVENTS':str(fixture_path),'FIXTURE_ACYCLIC':acyclic,'FIXTURE_WORKSPACE':str(workspace),'FIXTURE_OUTSIDE':str(state/'denied')})
 (workspace/'AGENTS.md').write_text('native instruction discovery fixture')
 fake=bin_dir/'codex'
 fake.write_text('''#!/usr/bin/env python3
import json,os,pathlib,subprocess,sys,time,traceback
def failed(kind,error,stack):
 pathlib.Path('.volumes/scratch/native-error.txt').write_text(''.join(traceback.format_exception(kind,error,stack)))
 sys.__excepthook__(kind,error,stack)
sys.excepthook=failed
args=sys.argv[1:]
if 'exec' not in args:
 assert args==['--sandbox','workspace-write','--ask-for-approval','never','--add-dir',os.environ['FIXTURE_WORKSPACE']],args
 cwd=pathlib.Path.cwd()
 assert str(cwd)==os.environ['ACYCLIC_WORKSPACE']==os.environ['FIXTURE_WORKSPACE']
 assert json.loads(os.environ['ACYCLIC_HOST_ARGS'])==args
 (cwd/'root-launch.json').write_text(json.dumps({'task':os.environ['ACYCLIC_TASK'],'args':args}))
 sys.exit(0)
assert args[6:8]==['exec','--json'],args
assert args[args.index('--sandbox')+1]=='workspace-write'
assert args[args.index('--ask-for-approval')+1]=='never'
cwd=pathlib.Path.cwd()
assert args[args.index('--cd')+1]==str(cwd)
assert cwd.is_relative_to(pathlib.Path(os.environ['FIXTURE_WORKSPACE'])/'.volumes/agents')
assert (cwd/'AGENTS.md').read_text()=='native instruction discovery fixture'
first=sys.stdin.read(1)
if first=='B':
 (cwd/'ready').write_text('ready')
 time.sleep(30)
prompt=first+sys.stdin.read()
assert prompt in ('failed','cancelled','recursive','first','resumed')
(cwd/'native-launch.json').write_text(json.dumps({'cwd':str(cwd),'args':args}))
sys.stdout.write(pathlib.Path(os.environ['FIXTURE_EVENTS']).read_text());sys.stdout.flush()
base=[os.environ['FIXTURE_CODEX'],'sandbox','-c','sandbox_mode="workspace-write"','-c','approval_policy="never"','-c','sandbox_workspace_write.writable_roots='+json.dumps([args[args.index('--add-dir')+1]]),'--']
if prompt=='resumed': assert (cwd/'.volumes/scratch/native-private.txt').read_text()=='first'
checked=subprocess.run(base+['sh','-c',
 'set -eu; "$1" agents >/dev/null; printf "%s" "$4" > .volumes/scratch/native-private.txt; '
 'if (printf denied > "$2") 2>/dev/null; then exit 41; fi; test ! -e "$2"; printf native > "$3"; '
 'if test "$4" = failed; then exit 17; fi',
 'fixture',os.environ['FIXTURE_ACYCLIC'],os.environ['FIXTURE_OUTSIDE'],prompt+'.txt',prompt],capture_output=True,text=True)
assert checked.returncode==(17 if prompt=='failed' else 0),checked.stderr
if prompt=='recursive':
 nested=subprocess.Popen([os.environ['FIXTURE_ACYCLIC'],'__mcp','codex'],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
 def call(identifier,name):
  arguments={'reference':'nested'}
  if name=='spawn':arguments['prompt']='first'
  nested.stdin.write((json.dumps({'id':identifier,'method':'tools/call','params':{'name':name,'arguments':arguments}})+'\\n').encode());nested.stdin.flush()
  response=json.loads(nested.stdout.readline());result=response['result'];assert not result.get('isError'),result
  return json.loads(result['content'][0]['text'])
 try:
  grandchild=call(1,'spawn');finished=call(2,'wait')
  assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Succeeded') is not None,finished
  merged=subprocess.run(base+[os.environ['FIXTURE_ACYCLIC'],'fs','merge','agents/nested'],capture_output=True,text=True)
  assert merged.returncode==0,merged.stderr
  assert (cwd/'first.txt').read_text()=='native'
  assert not (cwd/'.acyclic-sdk').exists(),'nested custody was authored into the child project'
 finally:
  nested.stdin.close();assert nested.wait(timeout=2)==0
if prompt=='cancelled':
 child=subprocess.Popen(['sh','-c','sleep 30'])
 (cwd/'descendant.pid').write_text(str(child.pid))
 (cwd/'ready').write_text('ready')
 time.sleep(30)
if prompt=='failed': sys.exit(17)
if prompt=='resumed':
 event=json.loads(pathlib.Path(os.environ['FIXTURE_EVENTS']).read_text())
 assert args[args.index('resume')+1]==event['thread_id']

''');fake.chmod(0o755)
 owner=None;bridge=None;pending=b'';responses={};sequence=0
 def start_owner():
  global owner
  log=(root/'owner.log').open('w+')
  owner=subprocess.Popen([graphcoder,'--host-workspace','--host-state-directory',str(state),'--workspace',str(workspace)],cwd=workspace,env=env,stdout=log,stderr=log)
  deadline=time.monotonic()+2
  while not (workspace/'.acyclic-sdk/host-files/owner.json').exists():
   assert owner.poll() is None,(root/'owner.log').read_text()[-1000:]
   assert time.monotonic()<deadline,'owner readiness';time.sleep(.01)
 def client(action):
  result=subprocess.run([acyclic,'__workspace'],input=json.dumps(action),text=True,cwd=workspace,env=env,capture_output=True,timeout=2)
  assert result.returncode==0,(result.stdout[-1000:],result.stderr[-1000:]);return json.loads(result.stdout)
 def start_bridge(task):
  global bridge,pending,responses
  pending=b'';responses={}
  inherited=json.loads((workspace/'root-launch.json').read_text())['args']
  bridge=subprocess.Popen([acyclic,'__mcp','codex'],cwd=workspace,env={**env,'ACYCLIC_HOST_ARGS':json.dumps(inherited),'ACYCLIC_WORKSPACE':str(workspace),'ACYCLIC_TASK':task},stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(root/'bridge.log').open('w+'))
 def submit(name,reference,prompt=None):
  global sequence
  sequence+=1;args={'reference':reference}
  if prompt is not None:args['prompt']=prompt
  bridge.stdin.write((json.dumps({'jsonrpc':'2.0','id':sequence,'method':'tools/call','params':{'name':name,'arguments':args}})+'\n').encode());bridge.stdin.flush();return sequence
 def response(identifier):
  global pending
  deadline=time.monotonic()+3
  with selectors.DefaultSelector() as ready:
   ready.register(bridge.stdout,selectors.EVENT_READ)
   while identifier not in responses:
    if b'\n' not in pending:
     remaining=deadline-time.monotonic();assert remaining>0 and ready.select(remaining),('MCP response',identifier,(root/'bridge.log').read_text()[-1200:])
     chunk=os.read(bridge.stdout.fileno(),65536);assert chunk,'bridge exited';pending+=chunk
    while b'\n' in pending:
     line,pending=pending.split(b'\n',1);message=json.loads(line);responses[message['id']]=message
  message=responses.pop(identifier);assert 'error' not in message,message
  result=message['result'];assert not result.get('isError'),result
  return json.loads(result['content'][0]['text'])
 def tool(name,reference,prompt=None):return response(submit(name,reference,prompt))
 def close_bridge():
  global bridge
  if bridge and bridge.poll() is None:bridge.stdin.close();assert bridge.wait(timeout=2)==0,(root/'bridge.log').read_text()[-1200:]
  bridge=None
 def stop_owner():
  global owner
  if owner and owner.poll() is None:
   owner.terminate()
   try:status=owner.wait(timeout=2)
   except subprocess.TimeoutExpired:
    owner.kill();owner.wait(timeout=2);raise
   assert status==0,(root/'owner.log').read_text()[-1200:]
  owner=None
 try:
  # Native global policy flags must precede exec; this is checked without a model.
  base=[real_codex,'--sandbox','workspace-write','--ask-for-approval','never','--add-dir',str(workspace),'exec','--json','--cd',str(workspace)]
  for tail in (['--help'],['resume','fixture-id','--help']):
   parsed=subprocess.run(base+tail,cwd=workspace,env=env,capture_output=True,text=True,timeout=2)
   assert parsed.returncode==0,parsed.stderr[-1000:]
  start_owner()
  launched=subprocess.run([acyclic,'host','codex','--','--sandbox','workspace-write','--ask-for-approval','never'],cwd=workspace,env=env,capture_output=True,text=True,timeout=2)
  assert launched.returncode==0,(launched.stdout,launched.stderr)
  parent=json.loads((workspace/'root-launch.json').read_text())['task'];start_bridge(parent)
  print('NATIVE_FIXTURE_PASS root-launch',flush=True)
  if scenario=='lifecycle':
   failed=tool('spawn','failed','failed');finished=tool('wait','failed')
   assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Failed'),finished
   assert not (workspace/'failed.txt').exists(),'failed child write leaked before merge'
   client({'kind':'fs','task':parent,'command':'merge agents/failed'});assert (workspace/'failed.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS failed-capture-merge',flush=True)
   cancelled=tool('spawn','cancelled','cancelled');view=pathlib.Path(cancelled['cwd']);deadline=time.monotonic()+2
   print('NATIVE_FIXTURE_PASS cancelled-spawn',time.monotonic()-started,flush=True)
   while not (view/'ready').exists():assert time.monotonic()<deadline,'child native write readiness';time.sleep(.01)
   print('NATIVE_FIXTURE_PASS cancelled-native-ready',time.monotonic()-started,flush=True)
   waiter=submit('wait','cancelled');stopped=tool('cancel','cancelled');response(waiter)
   assert stopped['control']=='Interrupt' and stopped['status']['settled'],stopped
   pid=int((view/'descendant.pid').read_text());proc=pathlib.Path('/proc')/str(pid)/'stat'
   assert not proc.exists() or proc.read_text().split()[2]=='Z','descendant still running after cancel'
   assert not (workspace/'cancelled.txt').exists(),'cancelled child write leaked before merge'
   client({'kind':'fs','task':parent,'command':'merge agents/cancelled'});assert (workspace/'cancelled.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS cancelled-capture-merge',flush=True)
   blocked=tool('spawn','blocked-input','B'+'x'*32767);view=pathlib.Path(blocked['cwd']);deadline=time.monotonic()+2
   while not (view/'ready').exists():assert time.monotonic()<deadline,'blocked input readiness';time.sleep(.01)
   stopped=tool('cancel','blocked-input');assert stopped['status']['settled'] and stopped['control']=='Interrupt',stopped
   print('NATIVE_FIXTURE_PASS blocked-input-cancel',flush=True)
  if scenario=='recursive':
   recursive=tool('spawn','recursive','recursive');finished=tool('wait','recursive')
   assert not (pathlib.Path(recursive['cwd'])/'.acyclic-sdk').exists(),'coordination metadata entered project history'
   error=pathlib.Path(recursive['cwd'])/'.volumes/scratch/native-error.txt'
   assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Succeeded') is not None,(finished,error.read_text() if error.exists() else 'no fixture error')
   assert (pathlib.Path(recursive['cwd'])/'first.txt').read_text()=='native'
   client({'kind':'fs','task':parent,'command':'merge agents/recursive'});assert (workspace/'recursive.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS recursive-native-session-merge',flush=True)
  if scenario=='resume':
   original=tool('spawn','retained','first');tool('wait','retained');old_view=original['cwd']
   close_bridge();stop_owner();start_owner();start_bridge(parent)
   resumed=tool('resume','retained','resumed');tool('wait','retained')
   assert resumed['cwd']!=old_view,'retained view was not recreated'
   launch=json.loads((pathlib.Path(resumed['cwd'])/'native-launch.json').read_text());assert 'resume' in launch['args'] and native_session_id in launch['args']
   assert not (workspace/'resumed.txt').exists(),'resumed child write leaked before merge'
   client({'kind':'fs','task':parent,'command':'merge agents/retained'});assert (workspace/'resumed.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS resume-recreated-view-merge',flush=True)
   archived=tool('archive','retained');assert archived['control']=='Archive',archived
  close_bridge();stop_owner()
  print(json.dumps({'suite':'managed-native-session-'+scenario,'native_agent_fixture':True,'real_codex_workspace_write_sandbox':True,'model_calls':0,'cwd_and_discovery':True,'outside_denied':True,'seconds':time.monotonic()-started}),flush=True)
 except BaseException:
  for name in ('owner.log','bridge.log'):
   path=root/name
   if path.exists():print('FIXTURE_'+name,path.read_text()[-4000:],flush=True)
  for view in (workspace/'.volumes/agents').glob('*'):
   error=view/'.volumes/scratch/native-error.txt'
   if error.is_file():print('FIXTURE_NATIVE_ERROR',view.name,error.read_text()[-4000:],flush=True)
  raise
 finally:
  if bridge and bridge.poll() is None:
   bridge.stdin.close()
   try:bridge.wait(timeout=2)
   except subprocess.TimeoutExpired:bridge.kill();bridge.wait(timeout=2)
  stop_owner()
assert time.monotonic()-started<10,'native supervision fixture exceeded ten seconds'
