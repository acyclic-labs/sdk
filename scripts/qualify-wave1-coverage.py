#!/usr/bin/env python3
"""Qualify the real HST availability guard and endpoint refusal on Linux.

This is reporting-policy evidence, not proof of any complete HST history. The
unchanged tests also run against an actual mutation of the exported Rust guard.
All external observations below are captured from the executed binaries.
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.wave1-coverage-records'
ARCHIVE = ROOT / '.wave1-coverage.tar'
SOURCE = ROOT / 'rust/crates/stream/src/conformance/wave1/coverage.rs'
TEST = ROOT / 'rust/crates/stream/tests/wave1_coverage.rs'
RECOVERY = 'python3 scripts/qualify-wave1-coverage.py'
started = time.monotonic()
step = 'preflight'
original = None


def need(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(name, data):
    (OUT / name).write_text(json.dumps(data, indent=2) + '\n')


def snapshot():
    paths = {ROOT / name for name in ('Cargo.toml', 'Cargo.lock', 'MODULE.bazel', 'MODULE.bazel.lock', '.bazelrc')}
    paths.add(Path(__file__).resolve())
    paths.update((ROOT / 'rust').rglob('*.rs'))
    paths.update((ROOT / 'rust').rglob('Cargo.toml'))
    paths.add(ROOT / 'rust/crates/stream/BUILD.bazel')
    return {path.relative_to(ROOT).as_posix(): sha(path) for path in sorted(paths)}


def run(args, label, expected=0, env=None, fast=False):
    begin = time.monotonic()
    result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True)
    elapsed = time.monotonic() - begin
    (OUT / (label + '.out')).write_bytes(result.stdout)
    (OUT / (label + '.err')).write_bytes(result.stderr)
    need(result.returncode == expected, f'{label}: exit={result.returncode}, expected={expected}; inspect captured {label}.out/.err')
    if fast:
        need(elapsed < 10, label + ': suite exceeded10s')
    write_json(label + '.json', dict(argv=args, exit=result.returncode, seconds=elapsed))
    return result


def build(label, target):
    download = '--remote_download_toplevel'
    run(['bazel', 'build', download, target], label)
    result = run(['bazel', 'cquery', download, '--output=files', target], label + '-query')
    artifacts = result.stdout.decode().splitlines()
    need(len(artifacts) == 1, label + ': expected exactly one built executable')
    root = run(['bazel', 'info', 'execution_root'], label + '-execution-root')
    path = Path(root.stdout.decode().strip()) / artifacts[0]
    need(path.is_file(), label + ': executable missing')
    write_json(label + '-executable.json', dict(path=str(path), sha256=sha(path), oracle_sha256=sha(TEST), production_sha256=sha(SOURCE)))
    return path


try:
    need(not OUT.exists() and not ARCHIVE.exists(), 'existing coverage output must be admitted/reused, never overwritten or identically rerun')
    need(shutil.which('bazel'), 'Bazel missing from admitted environment')
    package = tomllib.loads((ROOT / 'rust/crates/stream/Cargo.toml').read_text())
    need(package['package']['name'] == 'acyclic-stream' and {'conformance'} <= package['features'].keys(), 'affected crate/features differ')
    for path in (SOURCE, TEST, ROOT / 'rust/crates/stream/examples/wave1-conformance.rs', ROOT / 'Cargo.lock'):
        need(path.is_file() and path.stat().st_size > 0, 'required input absent/empty: ' + str(path))
    original = SOURCE.read_bytes()
    before = b'Err(Unqualified {\n        missing: hst_coverage(),\n    })'
    after = b'Ok(())'
    need(original.count(before) == 1, 'production mutation anchor must select exactly one HST qualification guard')
    plan = json.loads((ROOT / 'docs/proofs/wave1-history-plan.json').read_bytes())
    need([row['id'] for row in plan['cases']] == [f'HST-{number:02}' for number in range(1, 13)], 'adopted HST identities differ')
    OUT.mkdir()
    inputs = snapshot()
    write_json('source.json', inputs)
    write_json('job.json', dict(source_tree=subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=ROOT, text=True).strip(), affected_crate='acyclic-stream'))
    (OUT / 'production.rs').write_bytes(original)
    (OUT / 'oracle.rs').write_bytes(TEST.read_bytes())
    step = 'positive_compile_and_test'
    write_json('positive-compiled-source.json', snapshot())
    target = '//rust/crates/stream:provider-contracts_cases'
    executable = build('positive-compile', target)
    result = run([str(executable), 'wave1_coverage::', '--nocapture', '--test-threads=1'], 'positive', fast=True)
    need(b'3 passed; 0 failed; 0 ignored;' in result.stdout, 'positive: exactly three selected tests required')
    step = 'endpoint_compile_and_refusal'
    runner = build('runner-compile', '//rust/crates/stream:wave1-conformance_example')
    environment = os.environ.copy()
    for name in ('ACYCLIC_CONFORMANCE_ENDPOINT', 'ACYCLIC_CONFORMANCE_TOKEN', 'ACYCLIC_CONFORMANCE_NAMESPACE', 'ACYCLIC_CONFORMANCE_CASE'):
        environment.pop(name, None)
    environment['ACYCLIC_CONFORMANCE_REQUIRE_HST'] = '1'
    refused = run([str(runner)], 'endpoint-unqualified', expected=1, env=environment, fast=True)
    rows = [json.loads(line) for line in refused.stdout.splitlines()]
    need([row['history'] for row in rows] == [row['id'] for row in plan['cases']] and all(row['outcome'] == 'Unqualified' and row['reason'] == 'CanonicalHistoryNotExported' for row in rows), 'endpoint: missing/incorrect HST refusal rows')
    need(b'step=require_hst_qualification' in refused.stderr and b'step=read_environment' not in refused.stderr, 'endpoint: qualification must refuse before provider configuration')
    environment['ACYCLIC_CONFORMANCE_REQUIRE_HST'] = 'invalid'
    invalid = run([str(runner)], 'endpoint-invalid-option', expected=1, env=environment, fast=True)
    need(not invalid.stdout and b'step=parse_hst_requirement' in invalid.stderr and b'recovery:' in invalid.stderr, 'endpoint: invalid request must fail typed before work')
    step = 'mutant_compile_and_same_oracle'
    SOURCE.write_bytes(original.replace(before, after))
    (OUT / 'mutant.rs').write_bytes(SOURCE.read_bytes())
    mutated_inputs = snapshot()
    changes = [name for name in inputs if inputs[name] != mutated_inputs[name]]
    need(changes == [SOURCE.relative_to(ROOT).as_posix()], 'mutation changed other production/test inputs')
    write_json('mutant-compiled-source.json', mutated_inputs)
    mutant = build('mutant-compile', target)
    result = run([str(mutant), 'wave1_coverage::', '--nocapture', '--test-threads=1'], 'mutant', expected=101, fast=True)
    need(b'2 passed; 1 failed; 0 ignored;' in result.stdout and b'qualification_request_refuses_all_unexported_histories ... FAILED' in result.stdout and b'complete HST qualification must refuse unexported histories' in result.stdout + result.stderr, 'mutant must fail the unchanged HST refusal assertion, not setup')
    SOURCE.write_bytes(original)
    need(snapshot() == inputs, 'inputs were not restored exactly')
    write_json('receipt.json', dict(kind='hst_reporting_policy_not_complete_history_proof', positive_proofs=0, complete_hst_exports=0, source_sha256=sha(OUT / 'source.json'), oracle_sha256=sha(TEST), mutation=dict(path=SOURCE.relative_to(ROOT).as_posix(), before=before.decode(), after=after.decode()), selected=3, seconds=time.monotonic() - started))
    step = 'export_real_records'
    with tarfile.open(ARCHIVE, 'w') as archive:
        for path in sorted(OUT.iterdir()):
            archive.add(path, arcname=OUT.name + '/' + path.name)
    print(json.dumps(dict(step=step, status='reporting_policy_qualified_only', archive=ARCHIVE.name, archive_sha256=sha(ARCHIVE), positive_proofs=0, complete_hst_exports=0)))
except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError, tarfile.TarError) as error:
    print(f'VERIFY_COVERAGE_FAILURE step={step} error={error}; recovery: {RECOVERY}', file=sys.stderr)
    sys.exit(1)
finally:
    if original is not None and SOURCE.read_bytes() != original:
        SOURCE.write_bytes(original)
