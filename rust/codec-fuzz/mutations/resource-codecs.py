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


def run(step, diagnostic=None):
    if hashlib.sha256(oracle.read_bytes()).digest() != oracle_digest:
        raise RuntimeError(f'{step}: fuzz oracle changed; recovery: git diff -- src/harness_resources.rs')
    result = subprocess.run(command, cwd=package, capture_output=True, text=True)
    print(result.stdout, end='', flush=True)
    print(result.stderr, end='', flush=True)
    failed_oracle = (
        'panicked at' in result.stderr
        and 'src/harness_resources.rs:' in result.stderr.replace('\\', '/')
        and diagnostic is not None and diagnostic in result.stderr
    )
    if (diagnostic is not None and (result.returncode == 0 or not failed_oracle)) or (
        diagnostic is None and result.returncode != 0
    ):
        raise RuntimeError(f'{step}: expected {diagnostic or "successful writer replay"}, exit={result.returncode}; recovery: cargo run --locked --example capture -- --only-resources --extend-goldens')
    print(f'{step}: {"REJECTED" if diagnostic is not None else "PASS"}', flush=True)


def main():
    mutations = [
        ('cache-version', 'executor/model_cache.rs',
         '(observation.version == Self::VERSION).then_some(observation)', 'Some(observation)',
         'unsupported cache observation version admitted'),
        ('summary-version', 'conversation/epochs/summary.rs',
         'if wire.version != 1 {', 'if false {', 'unsupported summary version admitted'),
        ('snapshot-identity', 'conversation/epochs/snapshot.rs',
         'if wire.epoch.key.digest_through(pending.through) != pending.summary {', 'if false {',
         'foreign snapshot preparation identity admitted'),
        ('snapshot-completion', 'conversation/epochs/snapshot.rs',
         'completion: pending.completion.as_ref().map(SummaryOutcome::completion),',
         'completion: None,', 'writer snapshot lost epoch or producer custody'),
        ('snapshot-generation', 'conversation/epochs/protocol.rs',
         'if prepared.generation != generation {', 'if false {',
         'foreign pending epoch generation admitted'),
        ('snapshot-range-start', 'conversation/epochs/protocol.rs',
         'prepared.through <= cursor || prepared.through > head',
         'prepared.through > head', 'already covered pending epoch range admitted'),
        ('snapshot-range-end', 'conversation/epochs/protocol.rs',
         'prepared.through <= cursor || prepared.through > head',
         'prepared.through <= cursor', 'pending epoch range beyond source head admitted'),
    ]
    arguments = sys.argv[1:]
    selection = arguments[1:]
    if arguments and (arguments[0] != '--only' or not selection
                      or len(set(selection)) != len(selection)
                      or not set(selection) <= {row[0] for row in mutations}):
        raise RuntimeError('select mutation: expected --only GUARD ...; recovery: python3 mutations/resource-codecs.py --only snapshot-range-start snapshot-range-end')
    run('original')
    for step, relative, before, after, diagnostic in mutations:
        if selection and step not in selection:
            continue
        production = root / 'rust/crates/harness-core/src' / relative
        original = production.read_bytes()
        source = original.decode()
        if source.count(before) != 1:
            raise RuntimeError(f'{step}: expected one production guard; recovery: git diff -- {production}')
        try:
            production.write_bytes(source.replace(before, after).encode())
            run(step, diagnostic)
        finally:
            production.write_bytes(original)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(f'resource-codecs qualification failed: {error}; recovery: python3 mutations/resource-codecs.py after inspecting the original output and restoring source guards', file=sys.stderr)
        sys.exit(1)
