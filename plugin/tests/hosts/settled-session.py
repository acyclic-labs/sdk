"""Exercise the production MCP shim against an observable mailbox fixture.

Run on bb2: python3 settled-session.py ACYCLIC [HOST]. No host, model or login.
The mailbox rejects recovery/execution for settled empty or stopped sessions;
queued input must reach its claim; an orphaned in-flight turn must recover first.
"""
import collections
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from native_fixture import fixture_environment


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    started = time.monotonic()
    policies = {
        'codex': ['--sandbox', 'workspace-write', '--ask-for-approval', 'never'],
        'claude-code': ['--settings', '{"sandbox":{"enabled":true}}',
                        '--permission-mode', 'default', '--model', 'claude-haiku-5-5'],
        'pi': ['--tools', 'read,write,bash'],
    }
    if len(sys.argv) == 3:
        policies = {sys.argv[2]: policies[sys.argv[2]]}
    else:
        assert len(sys.argv) == 2
    cases = {
        'empty': {'settled': True, 'pending': 0, 'control': 'resume'},
        'stopped': {'settled': True, 'pending': 1, 'control': 'interrupt'},
        'unstarted-stopped': {'settled': None, 'pending': 1, 'control': 'interrupt'},
        'fresh': {'settled': None, 'pending': 1, 'control': 'resume'},
        'queued': {'settled': True, 'pending': 1, 'control': 'resume'},
        'orphaned': {'settled': False, 'pending': 0, 'control': 'resume'},
    }
    idle = ('empty', 'stopped', 'unstarted-stopped')
    background = {'background-' + name: cases[name] for name in idle}
    cases.update(background)
    cases['awaited'] = {'settled': False, 'pending': 0, 'control': 'resume'}
    results = []
    for host, flags in policies.items():
        with tempfile.TemporaryDirectory(prefix='plugins-settled-session-') as temporary:
            root = pathlib.Path(temporary).resolve()
            workspace = root / 'workspace'
            directory = workspace / '.acyclic-sdk/host-files'
            (directory / 'requests').mkdir(parents=True)
            (directory / 'results').mkdir()
            session = str(uuid.uuid4())
            parent = str(uuid.uuid4())
            (directory / 'owner.json').write_text(json.dumps(
                {'version': 1, 'session': session, 'workspace': str(workspace)}))
            stop = threading.Event()
            background_done = threading.Event()
            counts = collections.Counter()
            errors = []

            def serve():
                seen = set()
                listed = False
                waiting_listed = False
                while not stop.is_set():
                    for path in (directory / 'requests').glob('*.json'):
                        if path.name in seen:
                            continue
                        request = json.loads(path.read_text())
                        seen.add(path.name)
                        assert request['session'] == session
                        action = request['action']
                        if action['kind'] == 'sessions':
                            if not listed:
                                result = {'ok': list(background)}
                                listed = True
                            elif counts['awaited', 'observe'] == 1 and not waiting_listed:
                                result = {'ok': ['awaited']}
                                waiting_listed = True
                            else:
                                result = {'ok': []}
                        else:
                            assert action['kind'] == 'session', action
                            reference = action['reference']
                            kind = action['action']['kind']
                            counts[reference, kind] += 1
                            case = cases[reference]
                            if reference == 'awaited':
                                assert kind == 'observe', (reference, kind)
                                case = {**case, 'settled': counts[reference, kind] >= 2}
                            if (reference in idle or reference in background) and kind in ('recover', 'next'):
                                errors.append((reference, kind))
                                result = {'error': 'settled session entered recovery or execution'}
                            elif kind == 'next':
                                result = {'ok': None}
                            else:
                                result = {'ok': {'task': parent, 'cwd': str(workspace),
                                    'status': None if case['settled'] is None else {'settled': case['settled']},
                                    'pending': case['pending'],
                                    'control': case['control'], 'results': []}}
                        destination = directory / 'results' / path.name
                        pending = destination.with_suffix('.pending')
                        pending.write_text(json.dumps(result))
                        pending.replace(destination)
                        if all(counts[name, 'observe'] for name in background):
                            background_done.set()
                    stop.wait(.001)

            def owner():
                try:
                    serve()
                except BaseException as error:
                    errors.append(repr(error))

            thread = threading.Thread(target=owner)
            thread.start()
            env = fixture_environment(root, {'ACYCLIC_HOST_ARGS': json.dumps(flags),
                   'ACYCLIC_WORKSPACE': str(workspace), 'ACYCLIC_TASK': parent})
            bridge = subprocess.Popen([binary, '__mcp', host], cwd=workspace, env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                for identifier, reference in enumerate((name for name in cases
                        if name not in background and name != 'awaited'), 1):
                    tool = {'queued': 'send', 'orphaned': 'resume'}.get(reference, 'spawn')
                    bridge.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': identifier,
                        'method': 'tools/call', 'params': {'name': tool,
                        'arguments': {'reference': reference, 'prompt': 'input'}}}) + '\n')
                    bridge.stdin.flush()
                    response = json.loads(bridge.stdout.readline())
                    assert response['id'] == identifier, response
                    assert not response['result'].get('isError'), (host, reference, response)
                    value = json.loads(response['result']['content'][0]['text'])
                    assert value['pending'] == cases[reference]['pending'], value
                background_done.wait()
                bridge.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': 7,
                    'method': 'tools/call', 'params': {'name': 'wait',
                    'arguments': {'reference': 'awaited'}}}) + '\n')
                bridge.stdin.flush()
                response = json.loads(bridge.stdout.readline())
                assert response['id'] == 7 and not response['result'].get('isError'), response
                observed = json.loads(response['result']['content'][0]['text'])
                assert observed['status']['settled'], observed
                bridge.stdin.close()
                assert bridge.wait() == 0, bridge.stderr.read()
                assert not errors, errors
                for reference in cases:
                    if reference == 'awaited':
                        assert counts[reference, 'observe'] == 2, (
                            'pending wait repeated the refresh observation', host, counts)
                        continue
                    assert counts[reference, 'observe'] == 1, (
                        'settled-session observation was repeated', host, reference, counts)
                for reference in idle:
                    assert counts[reference, 'spawn'] > 0, (host, reference, counts)
                    assert counts[reference, 'recover'] == counts[reference, 'next'] == 0, counts
                for reference in background:
                    assert counts[reference, 'recover'] == counts[reference, 'next'] == 0, counts
                for reference in ('fresh', 'queued'):
                    assert counts[reference, 'recover'] == 0 and counts[reference, 'next'] > 0, counts
                assert counts['orphaned', 'recover'] > 0 and counts['orphaned', 'next'] > 0, counts
                assert counts['queued', 'send'] == 1 and counts['orphaned', 'control'] == 1, counts
                results.append({'host': host, 'cases': len(cases), 'settled_mutations': 0,
                                'queued_claimed_and_orphaned_recovered': True})
            finally:
                if bridge.poll() is None:
                    bridge.kill()
                    bridge.wait()
                stop.set()
                thread.join()
    print(json.dumps({'cases': results, 'model_calls': 0, 'seconds': time.monotonic() - started}))


if __name__ == '__main__':
    main()
