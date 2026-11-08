"""Concurrent spawns from a managed child, through the production MCP adapter.

Run on Linux or macOS: python3 concurrent-spawn.py ACYCLIC GRAPHCODER HOST.
HOST is codex, claude-code or pi. Native sessions are stand-ins that run the
host's session-start hook the way the native CLI does. A depth-1 child sends
two spawn calls before reading either reply, as a host that runs one
message's tool calls concurrently does; both replies must arrive, name
distinct children, and each grandchild must run and settle. No model, timer
or sleep: every step waits on a reply, a process exit or a pipe.
"""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
from native_fixture import fixture_environment, start_workspace_owner, stop_workspace_owner
from native_mcp import McpClient

RECOVERY = 'python3 plugin/tests/hosts/concurrent-spawn.py ACYCLIC GRAPHCODER HOST'
POLICIES = {
    'codex': ['--sandbox', 'workspace-write', '--ask-for-approval', 'never'],
    'claude-code': ['--settings', '{"sandbox":{"enabled":true,"allowUnsandboxedCommands":false}}',
                    '--permission-mode', 'default', '--model', 'claude-haiku-5-5'],
    'pi': ['--tools', 'read,write,bash'],
}
STAND_IN = r'''#!/usr/bin/env python3
import json,os,pathlib,subprocess,sys
sys.path.insert(0,os.environ['FIXTURE_HOST_SUPPORT'])
from native_mcp import McpClient
args=sys.argv[1:];host=os.environ['FIXTURE_HOST'];cwd=pathlib.Path.cwd()
acyclic=os.environ['FIXTURE_ACYCLIC']
if {'codex':'exec','claude-code':'-p','pi':'--mode'}[host] not in args:
 (cwd/'root.json').write_text(json.dumps({'task':os.environ['ACYCLIC_TASK'],
                                         'flags':json.loads(os.environ['ACYCLIC_HOST_ARGS'])}))
 sys.exit(0)
assert cwd.is_relative_to(pathlib.Path(os.environ['FIXTURE_WORKSPACE'])/'.volumes/agents'),cwd
# The native CLI runs its session-start hook before it reads the prompt.
started=subprocess.run([acyclic,'__hook',host,'SessionStart'],input=json.dumps({
 'hook_event_name':'SessionStart','session_id':cwd.name,'cwd':str(cwd)}),
 capture_output=True,text=True,check=True)
assert 'hookSpecificOutput' in json.loads(started.stdout),started.stdout
prompt=sys.stdin.read()
if prompt=='parent':
 nested=subprocess.Popen([acyclic,'__mcp',host],stdin=subprocess.PIPE,stdout=subprocess.PIPE)
 try:
  mcp=McpClient(nested,os.environ['FIXTURE_RECOVERY'])
  # Both calls are on the wire before either reply is read.
  calls=[mcp.submit('spawn',name,name) for name in ('left','right')]
  spawned=[mcp.result(call) for call in calls]
  assert len({row['task'] for row in spawned})==2,spawned
  assert len({row['cwd'] for row in spawned})==2,spawned
  for name,row in zip(('left','right'),spawned):
   finished=mcp.call('wait',name)
   assert finished['reply']['kind']=='answered' and finished['reply']['outcome'].get('Succeeded') is not None,finished
   assert (pathlib.Path(row['cwd'])/(name+'.txt')).read_text()==name,row
 finally:
  try:nested.stdin.close()
  finally:assert nested.wait()==0
else:
 assert prompt in ('left','right'),prompt
 (cwd/(prompt+'.txt')).write_text(prompt)
sys.stdout.write(pathlib.Path(os.environ['FIXTURE_EVENTS']).read_text());sys.stdout.flush()
'''


def main():
    binary, graphcoder = (str(pathlib.Path(p).resolve()) for p in sys.argv[1:3])
    host = sys.argv[3]
    flags = POLICIES[host]
    here = pathlib.Path(__file__).resolve().parent
    completion = next(record for record in json.loads((here / 'fixtures/native-completion.json').read_text())['records']
                      if record['host'] == host)
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='.plugins-concurrent-spawn-', dir=pathlib.Path.home()) as temporary:
        root = pathlib.Path(temporary)
        workspace = root / 'workspace'
        workspace.mkdir()
        bins = root / 'bin'
        bins.mkdir()
        events = root / 'native-events.jsonl'
        events.write_text(completion['stdout'])
        env = fixture_environment(root, {'PATH': str(bins) + os.pathsep + os.environ['PATH'],
               'FIXTURE_EVENTS': str(events), 'FIXTURE_HOST': host, 'FIXTURE_ACYCLIC': binary,
               'FIXTURE_WORKSPACE': str(workspace), 'FIXTURE_HOST_SUPPORT': str(here),
               'FIXTURE_RECOVERY': RECOVERY})
        executable = bins / {'codex': 'codex', 'claude-code': 'claude', 'pi': 'pi'}[host]
        executable.write_text(STAND_IN)
        executable.chmod(0o755)
        with (root / 'owner.log').open('w+') as owner_log, (root / 'bridge.log').open('w+') as bridge_log:
            owner = start_workspace_owner(graphcoder, workspace, root / 'state', root, env, owner_log)
            bridge = None
            try:
                launch = subprocess.run([binary, 'host', host, '--', *flags], cwd=workspace,
                                        env=env, capture_output=True, text=True)
                assert launch.returncode == 0, launch.stderr
                parent = json.loads((workspace / 'root.json').read_text())
                bridge = subprocess.Popen([binary, '__mcp', host], cwd=workspace,
                                          env={**env, 'ACYCLIC_HOST_ARGS': json.dumps(parent['flags']),
                                               'ACYCLIC_WORKSPACE': str(workspace),
                                               'ACYCLIC_TASK': parent['task']},
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=bridge_log)
                mcp = McpClient(bridge, RECOVERY)
                mcp.call('spawn', 'parent', 'parent')
                finished = mcp.call('wait', 'parent')
                assert finished['reply']['kind'] == 'answered', finished
                assert finished['reply']['outcome'].get('Succeeded') is not None, finished
                bridge.stdin.close()
                assert bridge.wait() == 0
                bridge = None
                previous, owner = owner, None
                stop_workspace_owner(previous)
            except BaseException:
                for log in (owner_log, bridge_log):
                    log.seek(0)
                    print('FIXTURE_LOG', log.name, log.read()[-4000:], flush=True)
                raise
            finally:
                try:
                    if bridge is not None:
                        bridge.terminate()
                        bridge.wait()
                finally:
                    if owner is not None:
                        stop_workspace_owner(owner)
        print(json.dumps({'suite': 'concurrent-spawn-' + host, 'model_calls': 0,
                          'concurrent_spawns_settled': True, 'seconds': time.monotonic() - started}))


if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    main()
