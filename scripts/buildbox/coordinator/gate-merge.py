#!/usr/bin/env python3
"""Fence GitHub merges to the exact PR heads present in the tested tree.

The caller owns pr-gate/.lock throughout preparation, execution and merging.
The pin is local coordinator state, never parsed from a job's stdout.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys

spec = importlib.util.spec_from_file_location('gate_prepare', Path(__file__).resolve().with_name('gate-prepare.py'))
preparation = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preparation)
queue_spec = importlib.util.spec_from_file_location('gate_queue', Path(__file__).resolve().with_name('gate-queue.py'))
queue = importlib.util.module_from_spec(queue_spec)
queue_spec.loader.exec_module(queue)


def git(repo, *args):
    return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()


def record(repo, worktree, state, prs):
    heads = {str(p): git(repo, 'rev-parse', '--verify', f'refs/pr-gate/{p}^{{commit}}') for p in prs}
    for head in heads.values():
        subprocess.run(['git', '-C', str(worktree), 'merge-base', '--is-ancestor', head, 'HEAD'], check=True)
    pin = {'commit': git(worktree, 'rev-parse', 'HEAD'), 'heads': heads}
    target = state / '.prepared-heads.json'
    tmp = target.with_suffix('.new')
    tmp.write_text(json.dumps(pin) + '\n', encoding='utf-8')
    os.replace(tmp, target)


def merge(repo, worktree, state, prs, alerts):
    pin = json.loads((state / '.prepared-heads.json').read_text(encoding='utf-8'))
    if pin['commit'] != git(worktree, 'rev-parse', 'HEAD') or set(pin['heads']) != set(map(str, prs)):
        raise ValueError('tested tree/group does not match prepared head pins')
    if not all(re.fullmatch(r'[0-9a-f]{40,64}', h) for h in pin['heads'].values()):
        raise ValueError('invalid prepared head pin')
    # GitHub checks head identity atomically with the merge, so a push after
    # local validation cannot merge untested commits. A stale peer cannot
    # prevent the other independently pinned peers from merging.
    failed = []
    for pr in prs:
        result = subprocess.run(['gh', 'pr', 'merge', str(pr), '--squash', '--admin',
                                 '--match-head-commit', pin['heads'][str(pr)]],
                                cwd=repo, capture_output=True, text=True)
        from datetime import datetime, timezone
        with alerts.open('a', encoding='utf-8') as log:
            log.write(f'{datetime.now(timezone.utc).isoformat()} pr-gate MERGE1 #{pr} head={pin["heads"][str(pr)]} merge_rc={result.returncode} {result.stdout}{result.stderr}\n')
        if result.returncode:
            failed.append((pr, f'gh pr merge rc={result.returncode}: {result.stdout}{result.stderr}'.strip()))
    if failed:
        # Failed merges remain explicit queue members, rather than silently
        # disappearing or immediately retrying an unchanged refused head.
        g = preparation.Gate(repo, worktree, state, alerts)
        for pr, reason in failed:
            g.park(pr, 'merge-failed', reason, head=pin['heads'][str(pr)])
            queue.add(state, 'held-pending', [pr])
            g.alert('GATE_MERGE_HELD', f'#{pr} head={pin["heads"][str(pr)]}: {reason}; gate-speed annotation: held until a changed head or explicit repository recovery')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['record', 'merge'])
    parser.add_argument('prs', nargs='+', type=int)
    parser.add_argument('--repo', type=Path, default=Path('Q:/sdk'))
    parser.add_argument('--worktree', type=Path, default=Path('Q:/wt/pr-gate'))
    parser.add_argument('--state', type=Path, default=Path('Q:/lanes/pr-gate'))
    parser.add_argument('--alerts', type=Path, default=Path.home()/'.codex-fleet/alerts.log')
    args = parser.parse_args()
    try:
        if args.operation == 'record': record(args.repo, args.worktree, args.state, args.prs)
        else: merge(args.repo, args.worktree, args.state, args.prs, args.alerts)
        return 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, subprocess.CalledProcessError) as error:
        print(f'GATE preparation-INFRA: exact-head merge fence: {error}', file=sys.stderr)
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
