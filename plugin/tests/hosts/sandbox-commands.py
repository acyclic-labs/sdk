#!/usr/bin/env python3
"""Native-host command contract.

Run on the host OS with prebuilt binaries; never reads or copies a login.
Codex and Pi probes are model-free. The coordinator owns real Claude Code
sandbox qualification. Pi uses its real RPC Bash tool; --pi-sandbox-codex
launches Pi under the pinned Codex workspace-write profile to test inheritance.
"""
import argparse
import json
import os
from pathlib import Path
import selectors
import shlex
import shutil
import subprocess
import sys
import tempfile
import time


def checked(argv, *, cwd, env, payload=None):
    result = subprocess.run(argv, cwd=cwd, env=env, input=payload, text=True,
                            capture_output=True, timeout=8)
    assert result.returncode == 0, (argv, result.stdout[-2000:], result.stderr[-2000:])
    return result.stdout


def inside(config):
    """Every operation here runs inside the host's native command boundary."""
    cfg = json.loads(Path(config).read_text())
    workspace, view = Path(cfg['workspace']), Path(cfg['view'])
    binary, parent, child = cfg['binary'], cfg['parent'], cfg['child']
    assert Path.cwd() == workspace, 'native tool lost the parent workspace cwd'
    assert view.is_relative_to(workspace / '.volumes/agents')
    assert (view / 'parent.txt').read_text() == 'parent'
    (view / 'child.txt').write_text('sandbox-child')
    blocked = False
    try:
        Path(cfg['outside']).write_text('must be refused by the host')
    except OSError:
        blocked = True
    assert blocked == cfg['outside_write_blocked'], 'host sandbox boundary changed'

    def cli(args, cwd=workspace, payload=None):
        env = dict(os.environ)
        env.pop('ACYCLIC_TASK', None)
        return checked([binary, *args], cwd=cwd, env=env, payload=payload)

    # Both explicit identities and cwd discovery go through the same client.
    cli(['agents'], cwd=view)
    hook = json.loads(cli(['__hook', cfg['host'], 'Stop'], cwd=view,
                          payload=json.dumps({'cwd': str(view), 'task': child})))
    assert hook.get('continue') is not False, hook
    cli(['--task', child, 'capture'], cwd=view)
    cli(['--task', parent, 'fs', 'merge', 'agents/sandbox-child'])
    assert (workspace / 'child.txt').read_text() == 'sandbox-child'
    cli(['--task', parent, 'agents'])
    cli(['--task', parent, 'log'])
    cli(['--task', parent, 'discard', 'agents/sandbox-child'])
    assert (workspace / 'child.txt').read_text() == 'sandbox-child'
    doctor = json.loads(cli(['doctor', cfg['host']]))
    assert doctor['additional_sandbox_roots'] == []

    # A legacy external data directory cannot masquerade as an admitted mailbox.
    negative = Path(cfg['negative'])
    for args in [['agents'], ['log'], ['discard', 'agents/sandbox-child'],
                 ['fs', 'merge', 'agents/sandbox-child'], ['capture']]:
        result = subprocess.run([binary, '--task', parent, *args], cwd=negative,
                                text=True, capture_output=True, timeout=2)
        assert result.returncode != 0, ('external data endpoint admitted', args)
    rejected = json.loads(cli(['__hook', cfg['host'], 'Stop'], cwd=negative,
                              payload=json.dumps({'cwd': str(negative), 'task': parent})))
    assert rejected.get('continue') is False, rejected
    external = Path(cfg['external_data'])
    assert not list((external / 'requests').iterdir()), 'negative published a request outside'
    assert not list((external / 'results').iterdir()), 'negative published a result outside'
    result = dict(host=cfg['host'], commands=True, captured=True, merged=True,
                  discarded=True, hook_command=True, external_data_rejected=True,
                  outside_write_blocked=blocked, model_calls=0)
    (workspace / 'sandbox-result.json').write_text(json.dumps(result))


def codex_sandbox(binary, workspace):
    return [binary, 'sandbox', '-c', 'sandbox_mode="workspace-write"', '-c',
            'approval_policy="never"', '-c', 'sandbox_workspace_write.writable_roots=' +
            json.dumps([str(workspace)]), '--']


def pi_command(binary, command, *, cwd, env, log, prefix=()):
    native = subprocess.Popen([*prefix, binary, '--mode', 'rpc', '--no-session', '--no-skills',
                               '--no-prompt-templates', '--no-themes'], cwd=cwd, env=env,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                              text=True, bufsize=1)
    try:
        native.stdin.write(json.dumps({'id': 'sandbox', 'type': 'bash', 'command': command}) + '\n')
        native.stdin.flush()
        with selectors.DefaultSelector() as ready:
            ready.register(native.stdout, selectors.EVENT_READ)
            deadline = time.monotonic() + 8
            while True:
                remaining = deadline - time.monotonic()
                assert remaining > 0 and ready.select(remaining), 'Pi command did not complete'
                line = native.stdout.readline()
                assert line, 'Pi exited without command result'
                message = json.loads(line)
                if message.get('id') == 'sandbox':
                    assert message.get('success'), message
                    assert message['data']['exitCode'] == 0, message
                    break
        native.stdin.close()
        assert native.wait(timeout=2) == 0
    finally:
        if native.poll() is None:
            native.kill()
            native.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inside')
    parser.add_argument('--host', choices=['codex', 'pi'])
    parser.add_argument('--binary')
    parser.add_argument('--graphcoder')
    parser.add_argument('--native')
    parser.add_argument('--package')
    parser.add_argument('--scratch')
    parser.add_argument('--pi-sandbox-codex')
    args = parser.parse_args()
    if args.inside:
        inside(args.inside)
        return
    assert all([args.host, args.binary, args.graphcoder, args.native, args.package, args.scratch])
    assert not args.pi_sandbox_codex or args.host == 'pi'
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='plugin-sandbox-', dir=args.scratch) as fixture:
        root = Path(fixture)
        workspace, state = root / 'workspace', root / 'trusted-state'
        workspace.mkdir()
        state.mkdir()
        (workspace / 'parent.txt').write_text('parent')
        # Whitelist environment: never inspect/copy the user's login or keys.
        env = {'PATH': str(Path(args.native).parent) + ':' + os.environ['PATH'], 'LANG': 'C.UTF-8'}
        for name in ['HOME', 'USERPROFILE', 'CODEX_HOME', 'CLAUDE_CONFIG_DIR', 'XDG_STATE_HOME',
                     'XDG_CONFIG_HOME', 'XDG_DATA_HOME', 'PLUGIN_DATA', 'CLAUDE_PLUGIN_DATA', 'PI_CODING_AGENT_DIR']:
            path = root / name.lower()
            path.mkdir()
            env[name] = str(path)
        with (root / 'owner.log').open('w+') as owner_log, (root / 'native.log').open('w+') as native_log:
            owner_env = dict(env)
            for name in ['HOME', 'USERPROFILE', 'XDG_STATE_HOME', 'XDG_CONFIG_HOME', 'XDG_DATA_HOME']:
                path = root / 'owner-profile' / name.lower()
                path.mkdir(parents=True)
                owner_env[name] = str(path)
            owner = subprocess.Popen([args.graphcoder, '--host-workspace', '--host-state-directory', str(state),
                                      '--workspace', str(workspace)], cwd=workspace, env=owner_env,
                                     stdout=owner_log, stderr=owner_log)
            try:
                descriptor = workspace / '.acyclic-sdk/host-files/owner.json'
                deadline = time.monotonic() + 2
                while not descriptor.exists():
                    assert owner.poll() is None, 'workspace owner exited'
                    assert time.monotonic() < deadline, 'workspace owner not ready'
                    time.sleep(0.01)

                def action(value):
                    return json.loads(checked([args.binary, '__workspace'], cwd=workspace, env=env,
                                              payload=json.dumps(value)))

                parent = action({'kind': 'open', 'host': args.host, 'session': 'sandbox-commands'})
                child = action({'kind': 'fork', 'parent': parent['task'], 'reference': 'sandbox-child'})
                view = Path(child['cwd'])
                env['ACYCLIC_TASK'] = parent['task']
                negative = workspace / '.acyclic-sdk/sandbox-negative'
                negative.mkdir()
                external = root / 'legacy-data'
                external.mkdir()
                (external / 'requests').mkdir()
                (external / 'results').mkdir()
                # Descriptor must name the negative workspace; only its location is wrong.
                endpoint = json.loads(descriptor.read_text())
                endpoint['workspace'] = str(negative)
                (external / 'owner.json').write_text(json.dumps(endpoint))
                (negative / '.acyclic-sdk').mkdir()
                (negative / '.acyclic-sdk/host-files').symlink_to(external, target_is_directory=True)
                # Count only request/result projections, not the descriptor itself.
                config = workspace / 'sandbox-config.json'
                config.write_text(json.dumps(dict(host=args.host, workspace=str(workspace), view=str(view),
                    binary=args.binary, parent=parent['task'], child=child['task'], negative=str(negative),
                    external_data=str(external), outside=str(state / 'forbidden'),
                    outside_write_blocked=args.host != 'pi' or bool(args.pi_sandbox_codex))))
                probe = workspace / 'sandbox-commands.py'
                shutil.copyfile(__file__, probe)
                command = shlex.join([sys.executable, str(probe), '--inside', str(config)])
                if args.host == 'codex':
                    checked([*codex_sandbox(args.native, workspace),
                             sys.executable, str(probe), '--inside', str(config)],
                            cwd=workspace, env=env)
                else:
                    prefix = ()
                    if args.pi_sandbox_codex:
                        profile = workspace / '.acyclic-sdk/pi-fixture'
                        profile.mkdir()
                        env['PI_CODING_AGENT_DIR'] = str(profile)
                        prefix = codex_sandbox(args.pi_sandbox_codex, workspace)
                    checked([args.binary, 'install', 'pi', '--plugin-root', args.package, '--settings',
                             str(Path(env['PI_CODING_AGENT_DIR']) / 'extensions/acyclic.ts')], cwd=workspace, env=env)
                    pi_command(args.native, command, cwd=workspace, env=env, log=native_log, prefix=prefix)
                    session = action({'kind': 'hook', 'host': 'pi', 'event': 'NativeSession',
                                      'input': {'task': parent['task']}})
                    assert session['id'], 'native Pi extension did not attach its session'
                result_path = workspace / 'sandbox-result.json'
                assert result_path.exists(), (root / 'native.log').read_text()[-5000:]
                result = json.loads(result_path.read_text())
                result['inherited_codex_workspace_write'] = bool(args.pi_sandbox_codex)
                for name in ['XDG_STATE_HOME', 'XDG_DATA_HOME', 'PLUGIN_DATA', 'CLAUDE_PLUGIN_DATA']:
                    assert not list(Path(env[name]).iterdir()), f'client wrote external {name}'
                result['seconds'] = time.monotonic() - started
                assert result['seconds'] < 10, result
                print(json.dumps(result), flush=True)
            finally:
                owner.terminate()
                owner.wait(timeout=2)


if __name__ == '__main__':
    main()
