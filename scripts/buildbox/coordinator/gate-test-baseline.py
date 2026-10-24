#!/usr/bin/env python3
"""Qualify new failures in affected test roots not covered by the full main gate.

Run only those failing crates, once on pinned main. Never rerun the PR, and never
change the flake list. Reuse a complete baseline for the same SHA/roots/skips.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess


def run_logged(command, log, cwd, env):
    """One writer owns the file; native shells inherit only a pipe.

    PowerShell/native descendants can reopen inherited file handles with
    independent positions. Even stderr=STDOUT cannot make that file safe.
    Reopening a pipe preserves its byte stream instead of overwriting a log.
    """
    # Do not retain short diagnostics until EOF: an interrupted coordinator
    # must leave the bytes already received in its partial, unqualified log.
    with log.open('wb', buffering=0) as output:
        process = subprocess.Popen(command, cwd=cwd, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        try:
            shutil.copyfileobj(process.stdout, output)
            output.flush()
            os.fsync(output.fileno())
            return process.wait()
        except BaseException:
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
            process.wait()
            raise
        finally:
            process.stdout.close()


def baseline_complete(output):
    payloads = re.findall(r'^GATE_TEST_BASELINE_COMPLETE rc=(-?\d+)$', output, re.M)
    executors = re.findall(r'^GATE_TEST_BASELINE_EXECUTOR rc=(-?\d+)$', output, re.M)
    # A footer without the completed client boundary is incomplete, including
    # a coordinator killed before appending its executor result.
    if len(payloads) != 1 or len(executors) != 1:
        return False
    payload, executor = int(payloads[0]), int(executors[0])
    if payload not in (0, 101) or payload != executor:
        return False
    build_failed = re.search(r'^error: (?:could not compile|failed to |the lock file|no matching|package ID)', output, re.M)
    isolation_failed = re.search(r'^error: GATE_(?:HOME_LEAK|RUNNER_INFRA) |^BB_DISK_LOW |^\[buildbox\] (?:cleanup|export|HOME guard) INFRA:', output, re.M)
    return bool(not build_failed and not isolation_failed and (payload == 0 or re.search(r'^test result:|^error: test failed, to rerun pass ', output, re.M)))



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('checkout', type=Path)
    parser.add_argument('base')
    parser.add_argument('log', type=Path)
    parser.add_argument('--scope', type=Path, required=True)
    parser.add_argument('--baseline-log', type=Path, required=True)
    parser.add_argument('--worktree', type=Path, default=Path('Q:/wt/pr-gate-baseline'))
    parser.add_argument('--results', type=Path, default=Path('Q:/lanes/pr-gate/test-baselines'))
    args = parser.parse_args()
    lanes = Path(__file__).resolve().parent
    scope = json.loads(args.scope.read_text())
    text = args.log.read_text(encoding='utf-8', errors='replace')
    reruns = [line for line in text.splitlines() if 'to rerun pass' in line]
    only_docs = bool(reruns) and all(line.startswith('error: doctest failed') for line in reruns)
    # Use exactly the caller's qualified main log; never independently select
    # a newer partial workspace run. Historical coverage comes from real output.
    spec = importlib.util.spec_from_file_location('gate_baseline', lanes / 'gate-baseline.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    main_text = args.baseline_log.read_text(encoding='utf-8', errors='replace')
    covered = module.coverage(main_text, scope['tests'])
    if not args.base.startswith(args.baseline_log.stem):
        # Historical coverage cannot rule out a main regression introduced
        # since that run. Qualify failing roots at this PR's exact pinned base;
        # complete cached results for the same base/roots are reused.
        covered = set()
    extra = set(scope['tests']) - covered
    text = args.log.read_text(encoding='utf-8', errors='replace')
    failing_roots = set(re.findall(r'^error: .*?to rerun pass [`\'"].*?-p ([A-Za-z0-9_-]+)', text, re.M))
    compiler_roots = set(re.findall(r'^error: could not compile [`\'\"]([A-Za-z0-9_-]+)', text, re.M))
    # Lint compiler errors also need a main qualification for newly covered
    # packages; an old workspace clippy run may not contain a new target.
    compiler_roots.update(re.findall(r'^GATE_CLIPPY_RAW error: could not compile [`\'\"]([A-Za-z0-9_-]+)', text, re.M))
    # A dependency's main compile failure can block a selected root without
    # itself belonging to the changed-package set. Qualify every compiler root
    # on pinned main, including roots covered by the older narrow baseline.
    roots = sorted((extra & failing_roots) | compiler_roots)
    if not roots:
        return 0
    # No need to compile non-doc targets when all reported failures are doctests.
    reruns = [line for line in text.splitlines() if 'to rerun pass' in line and any(re.search(r'-p ' + re.escape(p) + r'(?![A-Za-z0-9_-])', line) for p in roots)]
    only_docs = bool(reruns) and all(line.startswith('error: doctest failed') for line in reruns) and not re.search(r'^error: could not compile ', text, re.M)
    skips = [line.strip() for line in Path('Q:/lanes/hangs.txt').read_text().splitlines() if line.strip() and not line.startswith('#')]
    key_data = {'base': args.base, 'roots': roots, 'docs': only_docs, 'skips': skips, 'target_coverage': 'all-targets+doc-artifacts-v3', 'runner_sha256': hashlib.sha256((lanes / 'gate-test-runner.sh').read_bytes()).hexdigest()}
    if compiler_roots:
        key_data['check_targets'] = True
        key_data['check_only'] = not bool(failing_roots)
    key = hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()[:24]
    args.results.mkdir(parents=True, exist_ok=True)
    log = args.results / (key + '.log')

    def complete(path):
        if not path.exists():
            return False
        output = path.read_text(encoding='utf-8', errors='replace')
        return baseline_complete(output)

    if not log.exists():
        # A completed broader focused run on this exact main base already has
        # the requested package results. Reuse it instead of rebuilding the
        # same libraries for a different spelling of the root set.
        for candidate in sorted(args.results.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True):
            try:
                data = json.loads(candidate.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                continue
            if any(data.get(k) != key_data[k] for k in ['base', 'skips', 'docs', 'target_coverage', 'runner_sha256']):
                continue
            if bool(data.get('check_targets')) != bool(compiler_roots) or bool(data.get('check_only')) != bool(compiler_roots and not failing_roots):
                continue
            if set(roots) <= set(data.get('roots', [])) and complete(candidate.with_suffix('.log')):
                print(candidate.with_suffix('.log').as_posix())
                return 0

    if not complete(log):
        # Never replace a failed/incomplete attempt as if it had not happened.
        if log.exists():
            raise RuntimeError('incomplete baseline retained; inspect before a new attempt: ' + log.as_posix())
        if not args.worktree.exists():
            subprocess.run(['git', '-C', str(args.checkout), 'worktree', 'add', '--detach', str(args.worktree), args.base], check=True, stdout=subprocess.DEVNULL)
        else:
            subprocess.run(['git', '-C', str(args.worktree), 'checkout', '--detach', '-f', args.base], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(['git', '-C', str(args.worktree), 'clean', '-qfdx', '-e', 'target'], check=True)
        metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--offline', '--no-deps', '--format-version', '1'], cwd=args.worktree))
        members = {p['name'] for p in metadata['packages'] if p['id'] in metadata['workspace_members']}
        existing = [p for p in roots if p in members]
        if compiler_roots - members:
            # Registry/dependency errors have no workspace -p selector. Check
            # the PR's selected roots on main instead of declaring no baseline.
            existing = sorted(set(existing) | (set(scope['tests']) & members))
        (args.results / (key + '.json')).write_text(json.dumps({**key_data, 'existing_on_base': existing}, indent=2) + '\n', encoding='utf-8', newline='\n')
        if not existing:
            log.write_text('GATE_TEST_BASELINE_SKIPPED absent-on-base\nGATE_TEST_BASELINE_COMPLETE rc=0\nGATE_TEST_BASELINE_EXECUTOR rc=0\n', encoding='utf-8', newline='\n')
        else:
            shutil.copyfile(lanes / 'gate-test-runner.sh', args.worktree / '.gate-test-runner.sh')
            script = '''#!/bin/bash
exec 2>&1
started=$SECONDS
cache=${GATE_MAIN_TARGET_DIR:-/mnt/bb/targets/sdk/gate-main}
if [[ "$cache" == /mnt/bb/targets/*/* ]]; then
  unit=${cache#/mnt/bb/targets/}; unit=${unit//\\//%}
  mkdir -p /mnt/bb/locks/unit
  exec 8>>"/mnt/bb/locks/unit/$unit.lock"
  flock -s 8 || exit 2
fi
mkdir -p "$cache" || exit 2
exec 7>"$cache/.gate.lock"
flock -x 7 || exit 2
export CARGO_TARGET_DIR="$cache/tests"
trap 'touch "$cache"' EXIT
echo "GATE_TEST_BASELINE_START base=@BASE@ roots=@ROOTS@"
@CHECK@
cargo metadata --locked --offline --no-deps --format-version 1 > .gate-baseline-metadata.json || exit 2
export GATE_FUZZ_EXECUTABLES="$PWD/.gate-fuzz-executables"
: > "$GATE_FUZZ_EXECUTABLES" || exit 2
export GATE_BASE_TEST_RUNNER=${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER:-}
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/.gate-test-runner.sh"
chmod +x "$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER" || exit 2
run_tests() {
  if [[ " $* " == *" --all-targets "* ]]; then
    bash "$CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER" --build .gate-baseline-metadata.json "$GATE_FUZZ_EXECUTABLES" -- \
      cargo test --locked @PACKAGES@ --all-features --no-fail-fast "$@" || return $?
  fi
  cargo test --locked @PACKAGES@ --all-features --no-fail-fast "$@" -- @SKIPS@ 2>&1 | tee -a .gate-test-baseline.out
  status=("${PIPESTATUS[@]}"); result=${status[0]}
  if [ "${status[1]}" != 0 ]; then echo 'error: failed to retain baseline output'; result=2; fi
  return "$result"
}
rc=0
@TARGET_TESTS@
run_tests --doc; docs_rc=$?
[ "$rc" != 0 ] || rc=$docs_rc
echo "GATE_TEST_BASELINE_WALL seconds=$((SECONDS-started))"
echo "GATE_TEST_BASELINE_COMPLETE rc=$rc"
exit "$rc"
'''.replace('@CHECK@', '''cargo check --locked @PACKAGES@ --all-targets --all-features --profile test 2>&1 | tee .gate-test-baseline-check.out
status=("${PIPESTATUS[@]}"); rc=${status[0]}
if [ "${status[1]}" != 0 ]; then echo 'error: failed to retain baseline check output'; rc=2; fi
if [ "$rc" != 0 ]; then echo "GATE_TEST_BASELINE_COMPLETE rc=$rc"; exit "$rc"; fi
''' + ('''echo 'GATE_TEST_BASELINE_COMPLETE rc=0'
exit 0
''' if not failing_roots else '') if compiler_roots else '').replace('@BASE@', args.base).replace('@ROOTS@', ' '.join(existing)).replace('@PACKAGES@', ' '.join('-p ' + p for p in existing)).replace('@TARGET_TESTS@', '' if only_docs else 'run_tests --all-targets; rc=$?').replace('@SKIPS@', ' '.join('--skip ' + p for p in skips))
            (args.worktree / '.gate-test-baseline.sh').write_text(script, encoding='utf-8', newline='\n')
            env = dict(os.environ, BUILD_SLOT_TIER='merge', WSLENV='BUILD_SLOT_TIER')
            result = run_logged([shutil.which('pwsh'), '-NoProfile', '-File', 'Q:/lanes/bs.ps1', '--env', 'BUILD_SLOT_BUILDBOX_HOST=bb2', '--env', 'BUILD_SLOT_TIER=merge', '--env', 'BUILD_SLOT_LABEL=pr-gate-baseline', '--env', 'BUILD_SLOT_PHASE=pr-baseline', '--env', 'BUILD_SLOT_PR=' + os.environ.get('GATE_BASELINE_PRS', ''), '--env', 'BUILD_SLOT_GATE_LOG=' + log.as_posix(), 'bash', '.gate-test-baseline.sh'], log, cwd=args.worktree, env=env)
            with log.open('a', encoding='utf-8', newline='\n') as output:
                output.write(f'GATE_TEST_BASELINE_EXECUTOR rc={result}\n')
    if not complete(log):
        raise RuntimeError('baseline did not complete: ' + log.as_posix())
    print(log.as_posix())
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
