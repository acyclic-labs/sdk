#!/usr/bin/env python3
"""Select qualified main evidence, preferring completed phases at the exact base.

An unrelated red phase cannot invalidate completed workspace tests. Partial
workspace discovery still cannot claim coverage; legacy green runs retain only
package roots actually observed in their output.
"""
import argparse
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import re
import subprocess


def completed_payload(text):
    payload = re.findall(r'^GATE_DONE rc=(-?\d+)$', text, re.M)
    executor = re.findall(r'^GATE_EXECUTOR rc=(-?\d+)$', text, re.M)
    return len(payload) == len(executor) == 1 and payload == executor and int(payload[0]) in (0, 1, 101)


def eligible(text):
    if not completed_payload(text) or not re.search(r'^GATE_DONE rc=0\s*$', text, re.M):
        return False
    if not re.search(r'^test result:', text, re.M):
        return False
    if re.search(r'^error(?:\[E\d+\])?:', text, re.M):
        return False
    if re.search(r'^GATE_(?:MAIN_START|TEST_START|TEST_COVERAGE)', text, re.M):
        # The initial wide rollout emitted coverage after *any* test result.
        # Cargo success for both entire invocations is the completion proof.
        for phase in ('targets', 'docs'):
            if not re.search(r'^GATE_TEST_WALL phase=' + phase + r' .*\brc=0\s*$', text, re.M):
                return False
        if not re.search(r'^GATE_TEST_COVERAGE all-targets\+doc packages=\S+\s*$', text, re.M):
            return False
    return True


def coverage(text, packages):
    if not (eligible(text) or check_complete(text, 'tests')):
        raise ValueError('incomplete main baseline')
    marker = re.search(r'^GATE_TEST_COVERAGE all-targets\+doc packages=(\S+)\s*$', text, re.M)
    if marker:
        return set(marker[1].split(','))
    # Do not recompute an old dependency closure from today's Cargo manifests:
    # a package added since the baseline has no historical test coverage.
    seen = set(re.findall(r'^\s*Doc-tests (\S+)', text, re.M))
    seen.update(re.findall(r'^\s*Running unittests .*[/\\]([^/\\]+)-[0-9a-f]+(?:\.exe)?\)', text, re.M))
    return {p for p in packages if p.replace('-', '_') in seen}


def check_complete(text, phase):
    if not completed_payload(text):
        return False
    if phase == 'tests':
        # Qualify the test phase independently of Loom/features/browser/clippy.
        # Cargo rc=0 for both whole invocations proves no target was cut off,
        # failed to compile or silently omitted. Never infer coverage merely
        # from the test summaries that preceded an interrupted build.
        if not re.search(r'^test result:', text, re.M):
            return False
        for name in ('targets', 'docs'):
            statuses = re.findall(r'^GATE_TEST_WALL phase=' + name + r' .*\brc=(-?\d+)\s*$', text, re.M)
            if statuses != ['0']:
                return False
        covered = re.findall(r'^GATE_TEST_COVERAGE all-targets\+doc packages=(\S+)\s*$', text, re.M)
        return len(covered) == 1
    if phase == 'clippy':
        spec = importlib.util.spec_from_file_location('gate_clippy_completion', Path(__file__).with_name('gate-clippy.py'))
        clippy = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(clippy)
        # A completed red run with a valid receipt is exact-base evidence too:
        # its findings are main's, so the PR comparison ratchets against them.
        return clippy.complete(text)
    # The native matrix owns the first wall marker; later Windows matrix
    # results must never complete an interrupted or failed native phase.
    native = re.split(r'^GATE_WINDOWS_FEATURES_START(?: |$)', text, maxsplit=1, flags=re.M)[0]
    matrix = re.search(r'^GATE_FEATURES feature matrix: (\d+)/(\d+) sets ok', native, re.M)
    if not matrix:
        return False
    wall = re.search(r'^GATE_FEATURES_WALL .*\brc=(-?\d+)\b', native, re.M)
    if not wall:
        return False
    status = re.search(r'^GATE_FEATURES rc=(-?\d+)$', native, re.M)
    if status and status[1] != wall[1]:
        return False
    if wall[1] == '0' and matrix[1] == matrix[2]:
        return True
    # A completed red matrix still qualifies its known failing selections.
    # A cut-off matrix (missing per-selection results) supplies no baseline.
    # One completion follows both --lib and --all-targets for a selection.
    # Failed commands are not failed sets: either command can fail, and the
    # producer repeats each failure in its final summary. Their count cannot
    # establish whether the matrix finished. Require every distinct selection
    # instead; duplicate completion lines cannot hide a missing selection.
    completed = re.findall(r'^GATE_FEATURE_CHECK feature check: worker=\d+ seconds=[0-9.]+ rebuilt=\d+ manifest=(.+) flags=(.+)$', native, re.M)
    passed, total = map(int, matrix.groups())
    return bool(int(wall[1]) > 0 and 0 <= passed <= total
                and len(completed) == len(set(completed)) == total)


def qualified_phase(text, base, phase):
    """Use the same exact-source and completion contract at every boundary."""
    if not re.fullmatch(r'[0-9a-f]{40}', base):
        return False
    verification = re.findall(r'^GATE_MAIN_VERIFIED (.+)$', text, re.M)
    try:
        proof = json.loads(verification[0]) if len(verification) == 1 else None
    except ValueError:
        return False
    if not isinstance(proof, dict) or proof.get('head') != base or not re.fullmatch(
            r'[0-9a-f]{64}', str(proof.get('runner_sha256', ''))):
        return False
    if re.search(r'^error: GATE_RUNNER_INFRA |^\[buildbox\] (?:HOME guard|cleanup|export) INFRA:', text, re.M):
        return False
    return check_complete(text, phase)


def publication_identity(base, paths):
    """Hash the bytes we admitted, before the queue changes any custody."""
    if len(paths) != 2:
        raise ValueError('baseline-ready requires exact-main Clippy and feature logs')
    evidence = {'head': base, 'phases': {}}
    for phase, path in zip(('clippy', 'features'), paths):
        data = Path(path).read_bytes()
        if not qualified_phase(data.decode('utf-8', errors='replace'), base, phase):
            raise ValueError(f'baseline-ready refuses incomplete or wrong-head {phase} evidence: {path}')
        evidence['phases'][phase] = sha256(data).hexdigest()
    return json.dumps(evidence, sort_keys=True, separators=(',', ':'))


def select(checkout, base, directory, alerts, phase='tests', exact=False):
    logs = list(directory.glob('[0-9a-f]*.log'))
    if phase != 'tests' or exact:
        logs.extend((directory / 'wide').glob('[0-9a-f]*.log'))
    if phase == 'clippy' and exact:
        # A one-phase migration qualification can replace a damaged legacy
        # stream without rebuilding unrelated test and feature phases.
        logs.extend((directory / 'clippy').glob('[0-9a-f]*.log'))
    for log in sorted(logs, key=lambda p: p.stat().st_mtime, reverse=True):
        if not re.fullmatch(r'[0-9a-f]{7,40}', log.stem):
            continue
        if exact and log.stem != base:
            continue
        status = r'MAIN_GATE GREEN ' if phase == 'tests' and not exact else r'MAIN_(?:GATE|WIDE) [A-Z]+ '
        if not re.search(status + re.escape(log.stem) + r':', alerts):
            continue
        text = log.read_text(encoding='utf-8', errors='replace')
        if not (qualified_phase(text, base, phase) if exact else
                eligible(text) if phase == 'tests' else check_complete(text, phase)):
            continue
        if subprocess.run(['git', '-C', str(checkout), 'merge-base', '--is-ancestor', log.stem, base], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            return log
    raise RuntimeError(f'baseline selection step failed: no complete {"exact-base" if exact else "ancestor"} main {phase} baseline for {base}; '
                       f'recovery command: python Q:/lanes/gate-baseline.py {checkout} {base} --phase {phase}' + (' --exact' if exact else ''))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('checkout', type=Path)
    parser.add_argument('base')
    parser.add_argument('--phase', choices=('tests', 'clippy', 'features'), default='tests')
    parser.add_argument('--exact', action='store_true')
    parser.add_argument('--directory', type=Path, default=Path('Q:/lanes/main-gate'))
    parser.add_argument('--alerts', type=Path, default=Path.home() / '.codex-fleet/alerts.log')
    args = parser.parse_args()
    print(select(args.checkout, args.base, args.directory, args.alerts.read_text(encoding='utf-8', errors='replace'), args.phase, args.exact).as_posix())


if __name__ == '__main__':
    main()
