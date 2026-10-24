#!/usr/bin/env python3
"""Local preparation only: no build-box probes, rebases or metadata repair.

Exit 3: a main-only index conflict; 6: held preparation infrastructure;
7: batch-only conflict (defer the member, never rebase it).
All state mutations are serialized by the caller's pr-gate/.lock.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


class PreparationError(Exception):
    pass


class Gate:
    def __init__(self, repo, worktree, state, alerts):
        self.repo, self.worktree, self.state, self.alerts = map(Path, (repo, worktree, state, alerts))
        self.state.mkdir(parents=True, exist_ok=True)
        self.base = self.oid(self.repo, 'origin/main')

    def git(self, where, *args):
        r = subprocess.run(['git', '-C', str(where), *args], capture_output=True,
                           text=True, encoding='utf-8', errors='replace', timeout=180)
        return r

    def oid(self, where, ref):
        r = self.git(where, 'rev-parse', '--verify', ref)
        return r.stdout.strip() if r.returncode == 0 else 'unavailable'

    def checked(self, where, *args):
        r = self.git(where, *args)
        if r.returncode:
            raise PreparationError(f'git {args[0]} rc={r.returncode}: {r.stdout}{r.stderr}'.strip())
        return r.stdout.strip()

    def read(self, name, default=None):
        p = self.state / name
        return json.loads(p.read_text(encoding='utf-8')) if p.exists() else default

    def write(self, name, value):
        p = self.state / name
        tmp = p.with_name(p.name + '.new')
        tmp.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
        os.replace(tmp, p)

    def alert(self, kind, text):
        with self.alerts.open('a', encoding='utf-8') as f:
            f.write(f'{datetime.now(timezone.utc).isoformat()} pr-gate {kind} {text}\n')

    def validate(self):
        # A valid HEAD alone is insufficient: check the actual linked Git and
        # common directories before any checkout/clean could modify files.
        root = Path(self.checked(self.repo, 'rev-parse', '--path-format=absolute', '--git-common-dir')).resolve()
        common = Path(self.checked(self.worktree, 'rev-parse', '--path-format=absolute', '--git-common-dir')).resolve()
        actual = Path(self.checked(self.worktree, 'rev-parse', '--show-toplevel')).resolve()
        gitdir = Path(self.checked(self.worktree, 'rev-parse', '--absolute-git-dir')).resolve()
        if root != common or actual != self.worktree.resolve() or not gitdir.is_dir():
            raise PreparationError(f'invalid designated worktree/common directory: worktree={actual} git={gitdir} common={common} expected={root}')
        self.checked(self.worktree, 'rev-parse', '--verify', 'HEAD^{commit}')
        self.checked(self.worktree, 'ls-files', '--stage')  # reject a corrupt index
        return {'worktree': str(actual), 'git_dir': str(gitdir), 'common_dir': str(common)}

    def refresh(self, prs):
        self.validate()
        self.checked(self.repo, 'fetch', '-q', '--no-tags', 'origin', 'main',
                     *[f'+refs/pull/{p}/head:refs/pr-gate/{p}' for p in prs])
        self.base = self.checked(self.repo, 'rev-parse', '--verify', 'origin/main^{commit}')

    def hold(self, prs, reason):
        heads = {str(p): self.oid(self.repo, f'refs/pr-gate/{p}') for p in prs}
        value = {'prs': heads, 'base': self.base, 'reason': reason}
        key = hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()
        old = self.read('preparation-hold.json', {})
        if old.get('key') != key:
            self.alert('GATE_PREPARATION_INFRA', f'#{"+".join(map(str, prs))} preparation-INFRA: gate-speed annotation: {reason} key={key}; explicit repository recovery required')
        self.write('preparation-hold.json', {**value, 'key': key})
        print('GATE preparation-INFRA: gate-speed annotation: ' + reason + '; candidates preserved; explicit repository recovery required')
        return 6

    def reset(self):
        self.checked(self.worktree, 'checkout', '-q', '--detach', '-f', self.base)
        self.checked(self.worktree, 'clean', '-qfdx', '-e', 'target')

    def merge(self, pr, label):
        r = self.git(self.worktree, '-c', 'rerere.enabled=false', '-c', 'rerere.autoupdate=false',
                     'merge', '-q', '--no-edit', f'refs/pr-gate/{pr}')
        if not r.returncode:
            return False
        entries = self.checked(self.worktree, 'ls-files', '-u')
        files = self.checked(self.worktree, 'diff', '--name-only', '--diff-filter=U')
        diff = self.checked(self.worktree, 'diff', '--cc')
        log = self.state / f'conflict-{pr}.log'
        log.write_text(f'phase={label} base={self.base} head={self.oid(self.repo, f"refs/pr-gate/{pr}")}\n' +
                       r.stdout + r.stderr + '\nUnmerged index:\n' + entries + '\n' + files + '\n' + diff, encoding='utf-8')
        print(log.read_text(encoding='utf-8'), end='')
        # Nonzero merge without unmerged index is operational, even if its
        # English diagnostic contains the word "conflict".
        if not entries:
            raise PreparationError(f'operational merge error for #{pr} ({label}), rc={r.returncode}: {r.stdout}{r.stderr}'.strip())
        self.checked(self.worktree, 'merge', '--abort')
        return True

    def park(self, pr, kind, reason, *, head=None):
        holds = self.read('head-holds.json', {})
        # A delayed merge verdict belongs to its tested head. A queue producer
        # may already have fetched a newer head into the shared mutable ref.
        head = self.oid(self.repo, f'refs/pr-gate/{pr}') if head is None else head
        key = {'head': head, 'base': self.base, 'kind': kind, 'reason': reason}
        fresh = holds.get(str(pr)) != key
        holds[str(pr)] = key
        self.write('head-holds.json', holds)
        return fresh

    def eligible(self, pr):
        head=self.oid(self.repo, f'refs/pr-gate/{pr}')
        journal=self.state.parent/'auto-rebase'/str(pr)/'state.json'
        if journal.exists():
            try: skipped=json.loads(journal.read_text(encoding='utf-8'))
            except (OSError,ValueError) as e:raise PreparationError(f'invalid rebase hold journal for #{pr}: {e}')
            if skipped.get('protected') and skipped.get('phase')=='skipped' and skipped.get('attempt',{}).get('head')==head:
                return False  # protected ownership must change its head, not just main
        hold = self.read('head-holds.json', {}).get(str(pr))
        if hold and hold['kind'] == 'merge-failed':
            return hold['head'] != head
        return not hold or hold['head'] != head or hold['base'] != self.base

    def release_failed_merges(self):
        holds = self.read('head-holds.json', {})
        self.write('head-holds.json', {p: hold for p, hold in holds.items() if hold['kind'] != 'merge-failed'})

    def prepare(self, prs):
        self.refresh(prs)
        # Verify every PR against main ALONE before constructing the batch.
        for p in prs:
            self.reset()
            if self.merge(p, 'main-only'):
                (self.state / '.conflict').write_text(str(p) + '\n')
                if self.park(p, 'main-conflict', 'unmerged index against main'):
                    self.alert('GATE_CONFLICT', f'#{p} unmerged index against main={self.base}; log={self.state / f"conflict-{p}.log"}')
                print(f'GATE FAIL #{"+".join(map(str, prs))}: #{p} conflicts (rebase needed)')
                return 3
        self.reset()
        for p in prs:
            if self.merge(p, 'batch-only'):
                (self.state / '.conflict').write_text(str(p) + '\n')
                print(f'GATE_BATCH_DEFER #{p}: conflicts only with a batch peer; no rebase')
                return 7
        (self.state / '.prepared-base').write_text(self.base + '\n')
        return 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('operation', choices=['prepare', 'refresh', 'eligible', 'park', 'hold', 'recover'])
    p.add_argument('prs', type=int, nargs='*')
    p.add_argument('--repo', default='Q:/sdk')
    p.add_argument('--worktree', default='Q:/wt/pr-gate')
    p.add_argument('--state', default='Q:/lanes/pr-gate')
    p.add_argument('--alerts', default=str(Path.home() / '.codex-fleet/alerts.log'))
    p.add_argument('--reason', default='policy')
    a = p.parse_args()
    g = Gate(a.repo, a.worktree, a.state, a.alerts)
    if a.operation != 'recover' and g.read('preparation-hold.json'):
        print('GATE preparation-INFRA: gate-speed annotation: held pending explicit repository recovery')
        return 6
    try:
        if a.operation == 'hold':
            return g.hold(a.prs, a.reason)
        if a.operation == 'recover':
            lock=g.state/'.lock'
            try:lock.mkdir()
            except FileExistsError:
                print('GATE recovery deferred: a gate owns the lock');return 75
            try:
                detail = g.validate()
                g.refresh([])
                g.reset()  # checkout AND clean must succeed before releasing hold
                g.release_failed_merges()
                # Explicit recovery command, not an automatic retry loop.
                (g.state / 'preparation-hold.json').unlink(missing_ok=True)
                g.write('recovery.json', {'at': datetime.now(timezone.utc).isoformat(), **detail})
                print('GATE_PREPARATION_RECOVERED ' + json.dumps(detail))
                return 0
            finally:lock.rmdir()
        if a.operation == 'eligible':
            return 0 if g.eligible(a.prs[0]) else 1
        if a.operation == 'park':
            g.park(a.prs[0], 'policy', a.reason)
            return 0
        if a.operation == 'refresh':
            g.refresh(a.prs)
            return 0
        return g.prepare(a.prs)
    except (PreparationError, subprocess.TimeoutExpired, OSError) as e:
        return g.hold(a.prs, str(e))


if __name__ == '__main__':
    raise SystemExit(main())
