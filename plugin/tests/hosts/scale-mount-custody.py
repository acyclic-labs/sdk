"""Real-binary Linux scale proof with an actual concurrent FUSE registration.

python3 scale-mount-custody.py ACYCLIC GRAPHCODER HOST namespace|foreign|owned
namespace restores the former namespace-wide oracle; it must reject the foreign
mount. foreign must pass; owned must fail the same zero-growth oracle.
"""
import json
import pathlib
import shutil
import subprocess
import sys
import types


def main():
    script = pathlib.Path(__file__).with_name('shared-mount-scale.py')
    binary, owner, host, mode = sys.argv[1:]
    if mode not in ('namespace', 'foreign', 'owned'):
        raise ValueError('step=select mount control; recovery: select namespace, foreign or owned')
    scale = types.ModuleType('shared_scale')
    scale.__file__ = str(script)
    exec(compile(script.read_bytes(), str(script), 'exec'), scale.__dict__)
    original_run, original_exceeded = subprocess.run, scale.exceeded
    if mode == 'namespace':
        scale.VIEW_LIMITS = {**scale.VIEW_LIMITS, 'mounts': 0}
        del scale.VIEW_LIMITS['fixture_mounts']
    lease, directory, observed = None, None, None
    samples = 0

    def run(argv, **kwargs):
        nonlocal samples, lease, directory
        if len(argv) > 1 and argv[1] == '__workspace':
            action = json.loads(kwargs['input'])
            if action['kind'] == 'resources':
                samples += 1
                if samples == 2:
                    root = pathlib.Path(kwargs['cwd']).parent
                    directory = (root / 'injected owner' if mode == 'owned'
                                 else root.with_name(root.name + '-other'))
                    directory.mkdir()
                    lease = scale.MountLease(directory)
                    fixture = script.parents[3] / 'scripts/graphcoder-e2e/mount-fixture.py'
                    command = [sys.executable, str(fixture), str(directory / 'mounted space\ttab')]
                    result = original_run(command, env=kwargs['env'], capture_output=True, text=True)
                    if result.returncode:
                        raise RuntimeError(f'step=register actual control mount: {result.stderr}; recovery: {command}')
                    assert len(lease.mounts()) == 1, 'control must register one actual OS mount'
                    print(json.dumps({'control': mode, 'native_mount': json.loads(result.stdout),
                                      'records': lease.mounts()}), flush=True)
        return original_run(argv, **kwargs)

    def exceeded(small, large):
        nonlocal observed
        observed = original_exceeded(small, large)
        # Exhausted mount-fixture has closed its device; retire its actual
        # registration before the scenario removes any fixture directories.
        lease.retire()
        return observed

    subprocess.run, scale.exceeded = run, exceeded
    sys.argv = [str(script), binary, owner, host]
    expected = {'namespace': ['mounts'], 'foreign': [], 'owned': ['fixture_mounts']}[mode]
    try:
        try:
            scale.main()
        except AssertionError:
            if not expected or observed != expected:
                raise
        else:
            assert not expected, f'{mode} negative control was accepted'
        assert observed == expected, (mode, observed, expected)
        print(json.dumps({'control': mode, 'host': host, 'refused_resources': observed,
                          'qualified': True}), flush=True)
    finally:
        subprocess.run = original_run
        if lease is not None:
            try:
                lease.retire()
            finally:
                lease.close()
        if directory is not None:
            shutil.rmtree(directory)


if __name__ == '__main__':
    if len(sys.argv) != 5:
        raise SystemExit(__doc__)
    main()
