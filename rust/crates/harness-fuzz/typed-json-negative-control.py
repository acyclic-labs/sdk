"""Check actual typed JSON admission mutations through the shared corpus property."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[3]


class ControlArguments(argparse.ArgumentParser):
    def error(self, message):
        raise SystemExit('step=select typed JSON control; ' + message
                         + '; recovery=run python3 rust/crates/harness-fuzz/'
                         'typed-json-negative-control.py --help')


parser = ControlArguments(description=__doc__)
selection = parser.add_mutually_exclusive_group()
selection.add_argument('--raw-domain-prior', metavar='REVISION',
                       help='compare the prior shared oracle before checking borrowed raw JSON admission')
selection.add_argument('--depth-prior', metavar='REVISION',
                       help='check the prior depth false positive, actual depth omission and exact restored corpus')
selection.add_argument('--first-cause-prior', metavar='REVISION',
                       help='check prior-oracle miss, actual first-cause loss and exact restored corpus')
args = parser.parse_args()
source = root / 'rust/crates/harness-core/src/executor/json/records.rs'
oracle = root / 'rust/crates/harness/tests/support/bounded_json.rs'
patch = root / 'rust/crates/harness-fuzz/negative-controls/typed-json-ignore-syntax-admission.patch'
out = root / 'target/typed-json-syntax-control'
if args.raw_domain_prior:
    source = root / 'rust/crates/harness-core/src/executor/json/admission.rs'
    patch = root / 'rust/crates/harness-fuzz/negative-controls/typed-json-expand-borrowed-raw.patch'
    out = root / 'target/typed-json-raw-domain-control'
if args.depth_prior:
    source = root / 'rust/crates/harness-core/src/executor/json/admission.rs'
    patch = root / 'rust/crates/harness-fuzz/negative-controls/typed-json-ignore-depth.patch'
    out = root / 'target/typed-json-depth-control'
if args.first_cause_prior:
    source = root / 'rust/crates/harness-core/src/executor/json/admission.rs'
    patch = root / 'rust/crates/harness-fuzz/negative-controls/typed-json-drop-first-cause.patch'
    out = root / 'target/typed-json-first-cause-control'
command = ['cargo', 'test', '--locked', '-p', 'acyclic-harness-core', '--lib',
           'executor::json::tests::object_work::checked_codec_fuzz_corpus',
           '--', '--exact', '--nocapture']
if args.depth_prior or args.first_cause_prior:
    command = ['cargo', 'nextest', 'run', '--locked', '--profile', 'ci', '--retries', '0',
               '-p', 'acyclic-harness-core', '--lib', '-E',
               'test(=executor::json::tests::object_work::checked_codec_fuzz_corpus)']
step = 'read original typed JSON source'

def digest(data):
    return hashlib.sha256(data).hexdigest()

try:
    original = source.read_bytes()
    original_oracle = oracle.read_bytes()
    oracle_hash = digest(oracle.read_bytes())
    lock = root / 'Cargo.lock'
    lock_hash = digest(lock.read_bytes())
    out.mkdir(parents=True, exist_ok=True)
    result = {'source': digest(original), 'oracle': oracle_hash, 'patch': digest(patch.read_bytes()),
              'lock': lock_hash, 'command': command, 'phases': [],
              'corpus': [{'name': path.name, 'sha256': digest(path.read_bytes())}
                         for path in sorted((root / 'rust/crates/harness-fuzz/corpus/bounded_json').iterdir())
                         if path.is_file()]}
    try:
        step = 'apply selected typed JSON production mutation'
        subprocess.run(['git', 'apply', '--check', str(patch)], cwd=root, check=True)
        subprocess.run(['git', 'apply', str(patch)], cwd=root, check=True)
        mutated = source.read_bytes()
        phases = [
            ('diagnostic-admission-refusal-red', 101,
             'typed JSON decode must preserve caller charge refusal at frontier 5'),
            ('restored-original-corpus', 0, 'test result: ok. 1 passed; 0 failed'),
        ]
        if args.raw_domain_prior:
            step = 'read prior shared oracle from the selected revision'
            prior = subprocess.check_output(['git', 'show', args.raw_domain_prior + ':'
                                             + oracle.relative_to(root).as_posix()], cwd=root)
            result['prior_oracle'] = {'revision': args.raw_domain_prior, 'sha256': digest(prior)}
            phases = [('prior-oracle-mutant-miss', 0, 'test result: ok. 1 passed; 0 failed'),
                      ('raw-domain-red', 101, 'typed raw JSON must admit independently valid raw input'),
                      ('restored-original-corpus', 0, 'test result: ok. 1 passed; 0 failed')]
        if args.depth_prior:
            step = 'read prior shared oracle for the actual depth false positive'
            prior = subprocess.check_output(['git', 'show', args.depth_prior + ':'
                                             + oracle.relative_to(root).as_posix()], cwd=root)
            result['prior_oracle'] = {'revision': args.depth_prior, 'sha256': digest(prior)}
            phases = [('prior-depth-false-positive', 100, 'typed raw JSON must admit independently valid raw input'),
                      ('depth-omission-red', 100, 'typed raw JSON must refuse documents beyond the declared depth'),
                      ('restored-original-corpus', 0, '1 test run: 1 passed')]
        if args.first_cause_prior:
            step = 'read prior shared oracle for the actual first-cause omission'
            prior = subprocess.check_output(['git', 'show', args.first_cause_prior + ':'
                                             + oracle.relative_to(root).as_posix()], cwd=root)
            result['prior_oracle'] = {'revision': args.first_cause_prior, 'sha256': digest(prior)}
            phases = [('prior-oracle-mutant-miss', 0, '1 test run: 1 passed'),
                      ('first-cause-loss-red', 100, 'materialized JSON admission must preserve the first caller cause'),
                      ('restored-original-corpus', 0, '1 test run: 1 passed')]
        for name, expected, assertion in phases:
            source.write_bytes(original if name in ['prior-depth-false-positive', 'restored-original-corpus'] else mutated)
            oracle.write_bytes(prior if name in ['prior-oracle-mutant-miss', 'prior-depth-false-positive'] else original_oracle)
            step = name
            log = out / (name + '.log')
            with log.open('wb') as stream:
                completed = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT)
            output = log.read_text(errors='replace')
            print(output[-6000:], flush=True)
            phase = {'name': name, 'exit': completed.returncode,
                     'source': digest(source.read_bytes()), 'oracle': digest(oracle.read_bytes()),
                     'log': digest(log.read_bytes())}
            executables = re.findall(r'Running unittests .*? \(([^)]+)\)', output)
            if args.depth_prior or args.first_cause_prior:
                executables = re.findall(r'command:.*?(/[^\s]+/acyclic_harness_core-[a-f0-9]+)', output)
            if executables:
                executable = root / executables[-1]
                if executable.is_file():
                    phase['executable'] = {'path': str(executable), 'sha256': digest(executable.read_bytes()),
                                           'bytes': executable.stat().st_size, 'exported': False}
            if 'executable' not in phase:
                phase['executable_identity'] = 'not emitted by the test runner; no binary hash inferred'
            result['phases'].append(phase)
            (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
            if completed.returncode != expected or assertion not in output:
                raise RuntimeError('expected semantic assertion absent; inspect ' + str(log))
            if expected in [100, 101] and ('panicked at ' not in output or 'test result: FAILED.' not in output):
                raise RuntimeError('mutation did not reach the runtime assertion; inspect ' + str(log))
    finally:
        source.write_bytes(original)
        oracle.write_bytes(original_oracle)
        result['restored_source'] = digest(source.read_bytes()) == digest(original)
        result['restored_oracle'] = digest(oracle.read_bytes()) == oracle_hash
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        if digest(oracle.read_bytes()) != oracle_hash or digest(lock.read_bytes()) != lock_hash:
            raise RuntimeError('oracle or Cargo.lock changed during the control')
    print('PASS: actual typed admission mutation rejected; exact restored corpus passes', flush=True)
except Exception as error:
    raise SystemExit('step=' + step + '; ' + str(error)
                     + '; recovery=inspect ' + str(out / 'result.json') + ' and its phase logs, '
                     'correct the diagnosed source or command, then run python3 '
                     'rust/crates/harness-fuzz/typed-json-negative-control.py')
