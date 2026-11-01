"""Linux, no models: native-process fixture plus the real Codex sandbox.

Run on Linux: python3 managed-session.py ACYCLIC GRAPHCODER CODEX_0_164_OR_LATER lifecycle|recursive|resume.
Executables must already be built. Native model-session acceptance belongs to
the real-host e2e suite; this regression never reads native authentication.
"""
import json, os, pathlib, selectors, signal, socket, subprocess, sys, tempfile, time, traceback
from native_events import native_event_fixture
from native_fixture import fixture_environment, start_workspace_owner, stop_workspace_owner
from native_mcp import McpClient
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
with tempfile.TemporaryDirectory(prefix='.plugins-session-proof-',dir=pathlib.Path.home()) as temporary, socket.socket() as barrier:
 try:barrier.bind(('127.0.0.1',0));barrier.listen()
 except OSError as error:
  raise RuntimeError(f'step=arm native readiness: {error}; recovery command: python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX {scenario}') from error
 root=pathlib.Path(temporary);workspace=root/'workspace';workspace.mkdir();state=root/'state';state.mkdir();bin_dir=root/'bin';bin_dir.mkdir()
 env=fixture_environment(root, {'PATH':str(bin_dir)+':'+os.environ['PATH'],'FIXTURE_CODEX':real_codex,'FIXTURE_EVENTS':str(fixture_path),'FIXTURE_ACYCLIC':acyclic,'FIXTURE_WORKSPACE':str(workspace),'FIXTURE_OUTSIDE':str(state/'denied'),'FIXTURE_HOST_SUPPORT':str(pathlib.Path(__file__).resolve().parent)})
 env['FIXTURE_BARRIER']=str(barrier.getsockname()[1])
 (workspace/'AGENTS.md').write_text('native instruction discovery fixture')
 fake=bin_dir/'codex'
 fake.write_text('''#!/usr/bin/env python3
import json,os,pathlib,socket,subprocess,sys,traceback
sys.path.insert(0,os.environ['FIXTURE_HOST_SUPPORT'])
from native_mcp import McpClient
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
def hold_for_cancel():
 try:
  with socket.create_connection(('127.0.0.1',int(os.environ['FIXTURE_BARRIER']))) as gate:
   gate.sendall((json.dumps({'cwd':str(cwd)})+'\\n').encode())
   gate.recv(1)
 except OSError as error:
  raise RuntimeError('step=hold native fixture: '+str(error)+'; recovery command: python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX lifecycle') from error
 raise RuntimeError('step=hold native fixture: readiness owner closed or sent unexpected input; recovery command: python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX lifecycle')
if first=='B':
 hold_for_cancel()
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
 try:
  mcp=McpClient(nested,'python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX recursive')
  grandchild=mcp.call('spawn','nested','first');finished=mcp.call('wait','nested')
  assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Succeeded') is not None,finished
  merged=subprocess.run(base+[os.environ['FIXTURE_ACYCLIC'],'fs','merge','agents/nested'],capture_output=True,text=True)
  assert merged.returncode==0,merged.stderr
  assert (cwd/'first.txt').read_text()=='native'
  assert not (cwd/'.acyclic-sdk').exists(),'nested custody was authored into the child project'
 finally:
  try:nested.stdin.close()
  finally:assert nested.wait()==0
if prompt=='cancelled':
 # Only original process-tree cancellation may end this descendant hold.
 child=subprocess.Popen([sys.executable,'-c','import signal; signal.pause()'])
 (cwd/'descendant.pid').write_text(str(child.pid))
 hold_for_cancel()
if prompt=='failed': sys.exit(17)
if prompt=='resumed':
 event=json.loads(pathlib.Path(os.environ['FIXTURE_EVENTS']).read_text())
 assert args[args.index('resume')+1]==event['thread_id']

''');fake.chmod(0o755)
 owner=None;bridge=None;mcp=None;gates=[]
 def start_owner():
  global owner
  with (root/'owner.log').open('w+') as log:
   owner=start_workspace_owner(graphcoder,workspace,state,root,env,log)
 def client(action):
  result=subprocess.run([acyclic,'__workspace'],input=json.dumps(action),text=True,cwd=workspace,env=env,capture_output=True)
  assert result.returncode==0,(result.stdout[-1000:],result.stderr[-1000:]);return json.loads(result.stdout)
 def start_bridge(task):
  global bridge,mcp
  inherited=json.loads((workspace/'root-launch.json').read_text())['args']
  bridge=subprocess.Popen([acyclic,'__mcp','codex'],cwd=workspace,env={**env,'ACYCLIC_HOST_ARGS':json.dumps(inherited),'ACYCLIC_WORKSPACE':str(workspace),'ACYCLIC_TASK':task},stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(root/'bridge.log').open('w+'))
  mcp=McpClient(bridge,'python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX '+scenario)
 def submit(name,reference,prompt=None):
  return mcp.submit(name,reference,prompt)
 def response(identifier):return mcp.result(identifier)
 def tool(name,reference,prompt=None):return mcp.call(name,reference,prompt)
 def native_ready(view,waiter):
  recovery='python3 plugin/tests/hosts/managed-session.py ACYCLIC GRAPHCODER CODEX '+scenario
  try:
   with selectors.DefaultSelector() as ready:
    ready.register(barrier,selectors.EVENT_READ,'ready')
    ready.register(bridge.stdout,selectors.EVENT_READ,'terminal')
    observed=ready.select()
    if any(key.data=='terminal' for key,_ in observed):
     raise RuntimeError(f'step=observe native readiness: child settled before readiness: {response(waiter)}; recovery command: {recovery}')
   gate,_=barrier.accept();gates.append(gate)
   with gate.makefile('rb') as messages:event=json.loads(messages.readline())
  except (OSError,ValueError) as error:
   raise RuntimeError(f'step=observe native readiness: {error}; recovery command: {recovery}') from error
  if event!={'cwd':str(view)}:
   raise RuntimeError(f'step=match native readiness: {event}; recovery command: {recovery}')
 def close_bridge():
  global bridge,mcp
  mcp=None
  previous_bridge,bridge=bridge,None
  if previous_bridge:
   try:previous_bridge.stdin.close()
   finally:assert previous_bridge.wait()==0,(root/'bridge.log').read_text()[-1200:]
 def stop_owner():
  global owner
  previous,owner=owner,None
  if previous:stop_workspace_owner(previous)
 try:
  # Native global policy flags must precede exec; this is checked without a model.
  base=[real_codex,'--sandbox','workspace-write','--ask-for-approval','never','--add-dir',str(workspace),'exec','--json','--cd',str(workspace)]
  for tail in (['--help'],['resume','fixture-id','--help']):
   parsed=subprocess.run(base+tail,cwd=workspace,env=env,capture_output=True,text=True)
   assert parsed.returncode==0,parsed.stderr[-1000:]
  start_owner()
  launched=subprocess.run([acyclic,'host','codex','--','--sandbox','workspace-write','--ask-for-approval','never'],cwd=workspace,env=env,capture_output=True,text=True)
  assert launched.returncode==0,(launched.stdout,launched.stderr)
  parent=json.loads((workspace/'root-launch.json').read_text())['task'];start_bridge(parent)
  print('NATIVE_FIXTURE_PASS root-launch',flush=True)
  if scenario=='lifecycle':
   failed=tool('spawn','failed','failed');finished=tool('wait','failed')
   assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Failed'),finished
   assert not (workspace/'failed.txt').exists(),'failed child write leaked before merge'
   client({'kind':'fs','task':parent,'command':'merge agents/failed'});assert (workspace/'failed.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS failed-capture-merge',flush=True)
   cancelled=tool('spawn','cancelled','cancelled');view=pathlib.Path(cancelled['cwd'])
   print('NATIVE_FIXTURE_PASS cancelled-spawn',time.monotonic()-started,flush=True)
   waiter=submit('wait','cancelled');native_ready(view,waiter)
   print('NATIVE_FIXTURE_PASS cancelled-native-ready',time.monotonic()-started,flush=True)
   # Cancellation retires this view. Retain the observed process identity
   # while its held readiness channel proves the original native session is live.
   pid=int((view/'descendant.pid').read_text())
   stopped=tool('cancel','cancelled');response(waiter)
   assert stopped['control']=='interrupt' and stopped['status']['settled'],stopped
   proc=pathlib.Path('/proc')/str(pid)/'stat'
   assert not proc.exists() or proc.read_text().split()[2]=='Z','descendant still running after cancel'
   assert not (workspace/'cancelled.txt').exists(),'cancelled child write leaked before merge'
   client({'kind':'fs','task':parent,'command':'merge agents/cancelled'});assert (workspace/'cancelled.txt').read_text()=='native'
   print('NATIVE_FIXTURE_PASS cancelled-capture-merge',flush=True)
   blocked=tool('spawn','blocked-input','B'+'x'*32767);view=pathlib.Path(blocked['cwd'])
   waiter=submit('wait','blocked-input');native_ready(view,waiter)
   stopped=tool('cancel','blocked-input');response(waiter)
   assert stopped['status']['settled'] and stopped['control']=='interrupt',stopped
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
   archived=tool('archive','retained');assert archived['control']=='archive',archived
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
  try:
   for gate in gates:gate.close()
  finally:
   try:close_bridge()
   finally:stop_owner()
assert time.monotonic()-started<10,'native supervision fixture exceeded ten seconds'
