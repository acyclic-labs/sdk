"""Probe-only scale proof through each production MCP host adapter.

Run on bb2 (or ivar): python3 shared-mount-scale.py ACYCLIC GRAPHCODER HOST.
HOST is codex, claude-code or pi. Native sessions are explicit stand-ins;
the separate plugin-e2e lane owns the real sandbox/model matrix. GraphCoder
must enable test-support for the shared resource sampler/oracle request.
"""
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import tempfile
import time
import traceback
from native_events import native_event_fixture
from native_fixture import fixture_environment


def interrupted(signum, frame):
    traceback.print_stack(frame)
    raise SystemExit('step=shared native scale interrupted; recovery: python3 plugin/tests/hosts/shared-mount-scale.py ACYCLIC GRAPHCODER HOST')


signal.signal(signal.SIGTERM, interrupted)


def main():
    binary, graphcoder = (str(pathlib.Path(p).resolve()) for p in sys.argv[1:3])
    host = sys.argv[3]
    policies = {
        'codex': ['--sandbox', 'workspace-write', '--ask-for-approval', 'never'],
        'claude-code': ['--settings', '{"sandbox":{"enabled":true,"allowUnsandboxedCommands":false}}',
                        '--permission-mode', 'default', '--model', 'claude-haiku-5-5'],
        'pi': ['--tools', 'read,write,bash'],
    }
    fixture_path, _ = native_event_fixture(host)
    flags = policies[host]
    started = time.monotonic()
    with (
        tempfile.TemporaryDirectory(prefix='.plugins-shared-scale-', dir=pathlib.Path.home()) as temporary,
        socket.socket() as barrier,
    ):
        root = pathlib.Path(temporary)
        workspace = root / 'workspace'
        workspace.mkdir()
        bins = root / 'bin'
        bins.mkdir()
        barrier.bind(('127.0.0.1', 0))
        barrier.listen(4)
        env = fixture_environment(root, {'PATH': str(bins) + os.pathsep + os.environ['PATH'],
               'FIXTURE_EVENTS': str(fixture_path), 'FIXTURE_FLAGS': json.dumps(flags), 'FIXTURE_HOST': host,
               'FIXTURE_WORKSPACE': str(workspace),
               'FIXTURE_BARRIER': str(barrier.getsockname()[1]),
               'ACYCLIC_CACHED_DESCRIPTORS': '1', 'ACYCLIC_ADMITTED_DESCRIPTORS': '1'})
        (workspace / 'AGENTS.md').write_text('shared scale discovery fixture')
        executable = bins / {'codex': 'codex', 'claude-code': 'claude', 'pi': 'pi'}[host]
        executable.write_text(r'''#!/usr/bin/env python3
import json,os,pathlib,socket,sys
args=sys.argv[1:];host=os.environ['FIXTURE_HOST'];cwd=pathlib.Path.cwd()
child={'codex':'exec','claude-code':'-p','pi':'--mode'}[host] in args
flags=json.loads(os.environ['FIXTURE_FLAGS'])
for key,value in zip(flags[::2],flags[1::2]):
 assert args[args.index(key)+1]==value,(key,args)
if not child:
 (cwd/'root.json').write_text(json.dumps({'task':os.environ['ACYCLIC_TASK'],
                                         'flags':json.loads(os.environ['ACYCLIC_HOST_ARGS'])}))
 sys.exit(0)
assert cwd.is_relative_to(pathlib.Path(os.environ['FIXTURE_WORKSPACE'])/'.volumes/agents')
assert (cwd/'AGENTS.md').read_text()=='shared scale discovery fixture'
prompt=sys.stdin.read()
(cwd/(prompt+'.txt')).write_text(prompt)
(cwd/'.volumes/scratch/private.txt').write_text(prompt)
sys.stdout.write(pathlib.Path(os.environ['FIXTURE_EVENTS']).read_text());sys.stdout.flush()
with socket.create_connection(('127.0.0.1',int(os.environ['FIXTURE_BARRIER']))) as gate:
 gate.sendall((prompt+'\n').encode())
 while True:
  message=gate.recv(1)
  if message!=b'?': break
  gate.sendall(b'!')
sys.stderr.buffer.write(b'e'*262144 + ('diagnostic-tail-'+host).encode())
sys.stderr.flush()
sys.exit(17)
''')
        executable.chmod(0o755)
        with (root / 'owner.log').open('w+') as owner_log, (root / 'bridge.log').open('w+') as bridge_log:
            owner = subprocess.Popen([graphcoder, '--host-workspace', '--host-state-directory',
                                      str(root / 'state'), '--workspace', str(workspace)],
                                     cwd=workspace, env=env, stdout=owner_log, stderr=owner_log)
            bridge = None
            gates = []
            try:
                deadline = time.monotonic() + 2
                while not (workspace / '.acyclic-sdk/host-files/owner.json').is_file():
                    assert owner.poll() is None, (root / 'owner.log').read_text()[-2000:]
                    assert time.monotonic() < deadline, 'owner readiness'
                    time.sleep(.001)
                launch = subprocess.run([binary, 'host', host, '--', *flags], cwd=workspace,
                                        env=env, capture_output=True, text=True, timeout=2)
                assert launch.returncode == 0, launch.stderr
                parent = json.loads((workspace / 'root.json').read_text())
                task = parent['task']

                def client(action):
                    print(json.dumps({'host': host, 'step': 'workspace command', 'action': action}), flush=True)
                    result = subprocess.run([binary, '__workspace'], cwd=workspace, env=env,
                                            input=json.dumps(action), text=True,
                                            capture_output=True, timeout=2)
                    assert result.returncode == 0, result.stderr
                    return json.loads(result.stdout)

                bridge = subprocess.Popen([binary, '__mcp', host], cwd=workspace,
                                          env={**env, 'ACYCLIC_HOST_ARGS': json.dumps(parent['flags']),
                                               'ACYCLIC_WORKSPACE': str(workspace), 'ACYCLIC_TASK': task},
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                          stderr=bridge_log, text=True)
                sequence = 0

                def tool(name, reference, prompt=None):
                    nonlocal sequence
                    sequence += 1
                    arguments = {'reference': reference}
                    if prompt is not None:
                        arguments['prompt'] = prompt
                    bridge.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': sequence,
                                                   'method': 'tools/call',
                                                   'params': {'name': name, 'arguments': arguments}}) + '\n')
                    bridge.stdin.flush()
                    print(json.dumps({'host': host, 'step': 'await tool', 'tool': name, 'reference': reference}), flush=True)
                    message = json.loads(bridge.stdout.readline())
                    assert message['id'] == sequence and 'error' not in message, message
                    result = message['result']
                    assert not result.get('isError'), result
                    print(json.dumps({'host': host, 'step': 'tool completed', 'tool': name, 'reference': reference}), flush=True)
                    return json.loads(result['content'][0]['text'])

                paths = []
                samples = []

                def assert_live():
                    for gate in gates:
                        gate.sendall(b'?')
                        assert gate.recv(1) == b'!', 'sample requires every native session alive'

                for population in (2, 4):
                    for child in range(len(paths), population):
                        reference = 'child' + str(child)
                        location = tool('spawn', reference, reference)
                        gate, _ = barrier.accept()
                        gates.append(gate)
                        identity = gate.makefile('rb')
                        assert identity.readline() == (reference + '\n').encode(), reference
                        identity.close()
                        path = pathlib.Path(location['cwd'])
                        assert (path / (reference + '.txt')).read_text() == reference
                        assert (path / '.volumes/scratch/private.txt').read_text() == reference
                        paths.append(path)
                    assert_live()
                    sample = client({'kind': 'resource_probe', 'task': task, 'agents': population,
                                     'previous': samples[0]['counts'] if samples else None})
                    assert_live()
                    sample['live_sessions'] = population
                    samples.append(sample)
                    print(json.dumps({'host': host, 'sample': sample,
                                      'sampled_process': 'shared_mount_owner'}), flush=True)
                # The same N sessions remain alive through the 2N sample.
                # Sample the mount owner with the shared oracle; descendant
                # host-runtime resources are outside that oracle's contract.
                assert len({path.parent for path in paths}) == 1, paths
                assert samples[1]['exceeded'] == [], samples
                for gate in gates:
                    gate.sendall(b'.')
                for child, path in enumerate(paths):
                    reference = 'child' + str(child)
                    observed = tool('wait', reference)
                    assert observed['status']['settled'], observed
                    failure = observed['results'][0]['outcome']['Failed']['message']
                    assert failure.endswith('diagnostic-tail-' + host), failure
                    assert len(failure.encode()) < 4300, len(failure.encode())
                    client({'kind': 'fs', 'task': task, 'command': 'merge agents/' + reference})
                    print(json.dumps({'host': host, 'step': 'verify merge', 'reference': reference}), flush=True)
                    assert (workspace / (reference + '.txt')).read_text() == reference
                    assert (path / (reference + '.txt')).read_text() == reference
                bridge.stdin.close()
                assert bridge.wait(timeout=2) == 0
                owner.terminate()
                assert owner.wait(timeout=2) == 0
                elapsed = time.monotonic() - started
                assert elapsed < 10, elapsed
                print(json.dumps({'host': host, 'samples': samples, 'seconds': elapsed,
                                  'model_calls': 0, 'native_session_standins': True,
                                  'sessions_alive_at_both_samples': True,
                                  'sampled_process': 'shared_mount_owner',
                                  'shared_project_and_private_views': True}))
            finally:
                for gate in gates:
                    gate.close()
                for process in (bridge, owner):
                    if process is not None and process.poll() is None:
                        process.terminate()
                        process.wait(timeout=2)


if __name__ == '__main__':
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    main()
