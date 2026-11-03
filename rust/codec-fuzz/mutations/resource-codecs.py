"""Exercise the original writer corpus against unchanged production-code oracles."""
import hashlib
from pathlib import Path
import subprocess
import sys

package = Path(__file__).resolve().parents[1]
root = package.parents[1]
oracle = package / 'src/harness_resources.rs'
oracle_digest = hashlib.sha256(oracle.read_bytes()).digest()
command = ['cargo', 'run', '--locked', '--example', 'capture', '--',
           '--only-resources', '--extend-goldens']


def run(step, negative=False):
    if hashlib.sha256(oracle.read_bytes()).digest() != oracle_digest:
        raise RuntimeError(f'{step}: fuzz oracle changed; recovery: git diff -- src/harness_resources.rs')
    result = subprocess.run(command, cwd=package, capture_output=True, text=True)
    print(result.stdout, end='', flush=True)
    print(result.stderr, end='', flush=True)
    failed_assertion = 'panicked at' in result.stderr and 'assertion' in result.stderr
    if (negative and (result.returncode == 0 or not failed_assertion)) or (
        not negative and result.returncode != 0
    ):
        raise RuntimeError(f'{step}: expected {"semantic assertion failure" if negative else "successful writer replay"}, exit={result.returncode}; recovery: cargo run --locked --example capture -- --only-resources --extend-goldens')
    print(f'{step}: {"REJECTED" if negative else "PASS"}', flush=True)


def main():
    run('original')
    for step, relative, before, after in [
        ('cache-version', 'executor/model_cache.rs',
         '(observation.version == Self::VERSION).then_some(observation)', 'Some(observation)'),
        ('summary-version', 'conversation/epochs/summary.rs',
         'if wire.version != 1 {', 'if false {'),
        ('snapshot-identity', 'conversation/epochs/snapshot.rs',
         'if wire.epoch.key.digest_through(pending.through) != pending.summary {', 'if false {'),
    ]:
        production = root / 'rust/crates/harness-core/src' / relative
        original = production.read_bytes()
        source = original.decode()
        if source.count(before) != 1:
            raise RuntimeError(f'{step}: expected one production guard; recovery: git diff -- {production}')
        try:
            production.write_bytes(source.replace(before, after).encode())
            run(step, negative=True)
        finally:
            production.write_bytes(original)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'resource-codecs qualification failed: {error}; recovery: python3 mutations/resource-codecs.py after inspecting the original output and restoring source guards', file=sys.stderr)
        sys.exit(1)
