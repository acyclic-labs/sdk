"""Exercise the real pinned Pi RPC Bash stream; no model or credential setup."""
import importlib.util
import os
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest


class NativePiRpc(unittest.TestCase):
    def test_streamed_output_is_not_a_command_result(self):
        native = Path(os.environ['ACYCLIC_NATIVE_PI']).resolve(strict=True)
        source = Path(os.environ.get('ACYCLIC_PI_RPC_SOURCE',
                                    str(Path(__file__).with_name('sandbox-commands.py'))))
        spec = importlib.util.spec_from_file_location('sandbox_contract', source)
        contract = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(contract)
        root = Path(os.environ['E2E_SCRATCH_ROOT'])
        with tempfile.TemporaryDirectory(prefix='pi-rpc-', dir=root) as fixture:
            # The public test also gets its profile from the single shared
            # factory; a direct invocation cannot inherit the user's login.
            factory = Path(__file__).resolve().parents[3] / 'scripts/graphcoder-e2e/scratch.mjs'
            import json
            profile = subprocess.run(['node', str(factory), 'profile', str(Path(fixture) / 'profile')],
                                     check=True, text=True, capture_output=True)
            env = {name: os.environ[name] for name in ['PATH', 'LANG'] if name in os.environ}
            env.update(json.loads(profile.stdout))
            with (Path(fixture) / 'native.stderr').open('w+') as log:
                contract.pi_command(str(native), 'printf NATIVE_PI_RPC_COMPLETED',
                                    cwd=fixture, env=env, log=log)


if __name__ == '__main__':
    try:
        outcome = unittest.main(exit=False)
        if not outcome.result.wasSuccessful():
            raise RuntimeError('native Pi RPC completion assertion failed')
    except Exception as error:
        import shlex
        print(f'step=qualify actual Pi RPC: {error}; recovery='
              + shlex.join([sys.executable, *sys.argv]), file=sys.stderr)
        raise SystemExit(1)
