import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location('gate_merge', HERE/'gate-merge.py')
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class MergeFence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.repo = self.root/'repo'
        self.repo.mkdir()
        self.state = self.root/'state'
        self.state.mkdir()
        self.alerts = self.root/'alerts'
        self.git('init', '-q')
        self.git('config', 'user.name', 'fixture')
        self.git('config', 'user.email', 'fixture@example.test')
        self.git('config', 'commit.gpgsign', 'false')
        (self.repo/'file').write_text('base')
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.head = self.git('rev-parse', 'HEAD')
        for pr in [1, 2]: self.git('update-ref', f'refs/pr-gate/{pr}', self.head)

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], text=True).strip()

    def test_atomic_match_refuses_changed_head_and_preserves_peer(self):
        gate.record(self.repo, self.repo, self.state, [1, 2])
        calls = []
        real = subprocess.run
        def remote(args, **kwargs):
            if args[0] != 'gh': return real(args, **kwargs)
            calls.append(args)
            # GitHub's compare-and-merge: #1 has changed remotely; #2 is healthy.
            fenced = '--match-head-commit' in args and args[-1] == self.head
            return subprocess.CompletedProcess(args, 1 if args[3] == '1' and fenced else 0, '', '')
        with patch.object(gate.subprocess, 'run', side_effect=remote):
            gate.merge(self.repo, self.repo, self.state, [1, 2], self.alerts)
        self.assertEqual([c[3] for c in calls], ['1', '2'])
        self.assertTrue(all(c[-2:] == ['--match-head-commit', self.head] for c in calls))
        text = self.alerts.read_text()
        self.assertIn('#1 head='+self.head+' merge_rc=1', text)
        self.assertIn('#2 head='+self.head+' merge_rc=0', text)
        self.assertEqual((self.state/'held-pending.txt').read_text().split(), ['1'])
        hold = json.loads((self.state/'head-holds.json').read_text())
        self.assertEqual(hold['1']['kind'], 'merge-failed')
        self.assertNotIn('2', hold)
        # Negative control: an unfenced call accepts the changed head.
        self.assertEqual(remote(['gh', 'pr', 'merge', '1']).returncode, 0)

    def test_refreshed_new_head_is_not_held_by_the_older_tested_merge_refusal(self):
        gate.record(self.repo, self.repo, self.state, [1, 2])
        self.git('update-ref', 'refs/remotes/origin/main', self.head)
        (self.repo/'file').write_text('new queued head')
        self.git('commit', '-qam', 'new queued head')
        newer = self.git('rev-parse', 'HEAD')
        self.git('update-ref', 'refs/pr-gate/1', newer)
        self.git('checkout', '-q', '--detach', self.head)
        real = subprocess.run
        def remote(args, **kwargs):
            if args[0] != 'gh': return real(args, **kwargs)
            return subprocess.CompletedProcess(args, 1 if args[3] == '1' else 0,
                                               '', 'head changed' if args[3] == '1' else '')
        for negative in [False, True]:
            state = self.state if not negative else self.root/'negative-refreshed-head'
            if negative:
                state.mkdir()
                (state/'.prepared-heads.json').write_text((self.state/'.prepared-heads.json').read_text())
            namespace = {'__file__':str(HERE/'gate-merge.py'), '__name__':'merge_fresh_head'}
            source = (HERE/'gate-merge.py').read_text()
            if negative:
                source = source.replace("g.park(pr, 'merge-failed', reason, head=pin['heads'][str(pr)])",
                                        "g.park(pr, 'merge-failed', reason)")
            exec(compile(source, str(HERE/'gate-merge.py'), 'exec'), namespace)
            with patch.object(gate.subprocess, 'run', side_effect=remote):
                namespace['merge'](self.repo, self.repo, state, [1, 2], self.alerts)
            hold = json.loads((state/'head-holds.json').read_text())
            self.assertEqual(hold['1']['head'], newer if negative else self.head)
            self.assertNotIn('2', hold)
            self.assertEqual((state/'held-pending.txt').read_text().split(), ['1'])
            g = gate.preparation.Gate(self.repo, self.repo, state, self.alerts)
            self.assertEqual(g.eligible(1), not negative)
        self.git('update-ref', 'refs/pr-gate/1', self.head)
        g = gate.preparation.Gate(self.repo, self.repo, self.state, self.alerts)
        self.assertFalse(g.eligible(1))  # unchanged tested head stays held
        self.git('update-ref', 'refs/pr-gate/1', newer)
        self.assertTrue(g.eligible(1))

    def test_api_failures_are_held_until_changed_head_or_explicit_recovery(self):
        gate.record(self.repo, self.repo, self.state, [1, 2])
        real = subprocess.run
        def refused(args, **kwargs):
            if args[0] != 'gh': return real(args, **kwargs)
            return subprocess.CompletedProcess(args, 1, '', 'authentication/API refused')
        with patch.object(gate.subprocess, 'run', side_effect=refused):
            gate.merge(self.repo, self.repo, self.state, [1, 2], self.alerts)
        self.assertEqual((self.state/'held-pending.txt').read_text().split(), ['1', '2'])
        g = gate.preparation.Gate(self.repo, self.repo, self.state, self.alerts)
        self.assertFalse(g.eligible(1)); self.assertFalse(g.eligible(2))
        g.base = 'new-main-does-not-repair-authentication'
        self.assertFalse(g.eligible(1))
        (self.repo/'file').write_text('changed')
        self.git('commit', '-qam', 'new head')
        self.git('update-ref', 'refs/pr-gate/1', self.git('rev-parse', 'HEAD'))
        self.assertTrue(g.eligible(1)); self.assertFalse(g.eligible(2))
        g.release_failed_merges()
        self.assertTrue(g.eligible(2))
        # Negative control executes the previous shape: API refusals are logged
        # but no failed merge is retained as a queue member or held head.
        negative = self.root/'negative'; negative.mkdir()
        (negative/'.prepared-heads.json').write_text(json.dumps({'commit':self.git('rev-parse','HEAD'),
            'heads':{'1':self.git('rev-parse','refs/pr-gate/1')}}))
        namespace = {'__file__':str(HERE/'gate-merge.py'), '__name__':'merge_negative'}
        source = (HERE/'gate-merge.py').read_text().replace('if result.returncode:\n            failed.append',
                                                           'if False:\n            failed.append')
        exec(compile(source, str(HERE/'gate-merge.py'), 'exec'), namespace)
        with patch.object(gate.subprocess, 'run', side_effect=refused):
            namespace['merge'](self.repo,self.repo,negative,[1],self.alerts)
        self.assertFalse((negative/'held-pending.txt').exists())

    def test_missing_malformed_wrong_group_or_changed_tree_never_merges(self):
        target = self.state/'.prepared-heads.json'
        for value in [None, '{', json.dumps({'commit':self.head, 'heads':{'2':self.head}}),
                      json.dumps({'commit':'a'*40, 'heads':{'1':self.head}}),
                      json.dumps({'commit':self.head, 'heads':{'1':'--admin'}})]:
            if value is None: target.unlink(missing_ok=True)
            else: target.write_text(value)
            with patch.object(gate, 'git', return_value=self.head), patch.object(gate.subprocess, 'run') as run:
                with self.assertRaises((OSError, ValueError)):
                    gate.merge(self.repo, self.repo, self.state, [1], self.alerts)
                run.assert_not_called()

    def test_pin_cannot_claim_a_head_absent_from_prepared_tree(self):
        (self.repo/'file').write_text('new')
        self.git('commit', '-qam', 'new')
        newer = self.git('rev-parse', 'HEAD')
        self.git('update-ref', 'refs/pr-gate/1', newer)
        self.git('checkout', '-q', '--detach', self.head)
        with self.assertRaises(subprocess.CalledProcessError):
            gate.record(self.repo, self.repo, self.state, [1])
        self.assertFalse((self.state/'.prepared-heads.json').exists())

    def test_bad_pin_holds_once_without_reexecution_and_requires_recovery(self):
        argv = [sys.executable, str(HERE/'gate-prepare.py'), 'hold', '1', '2',
                '--repo', str(self.repo), '--worktree', str(self.repo),
                '--state', str(self.state), '--alerts', str(self.alerts),
                '--reason', 'invalid prepared head pin']
        for _ in range(2):
            result = subprocess.run(argv, capture_output=True, text=True)
            self.assertEqual(result.returncode, 6, result.stderr)
        self.assertEqual(self.alerts.read_text().count('GATE_PREPARATION_INFRA'), 1)
        hold = json.loads((self.state/'preparation-hold.json').read_text())
        self.assertEqual(set(hold['prs']), {'1', '2'})
        self.assertIn('invalid prepared head pin', hold['reason'])

    def test_direct_and_bisect_dispatch_merge_only_pinned_groups(self):
        # Verdict evaluation must retain the tree whose generation was pinned.
        # Resetting it before merge would make every valid pin stale.
        source = (HERE/'pr-gate.sh').read_text()
        self.assertNotIn('git -C $W checkout', source)
        for bisect in [False, True]:
            with self.subTest(bisect=bisect):
                root = self.root/str(bisect)
                root.mkdir()
                state = root/'state'; state.mkdir()
                tools = root/'tools'; tools.mkdir()
                bins = root/'bin'; bins.mkdir()
                (state/'pending.txt').write_text('1\n2\n')
                (tools/'merge-hold.txt').write_text('')
                (tools/'gate-prepare.py').write_text('raise SystemExit(0)\n')
                (tools/'gate-merge.py').symlink_to(HERE/'gate-merge.py')
                (tools/'gate-queue.py').symlink_to(HERE/'gate-queue.py')
                record = f'python "{HERE}/gate-merge.py" record "$@" --repo "{self.repo}" --worktree "{self.repo}" --state "$GATE_STATE"\n'
                (tools/'pr-gate.sh').write_text(('if [ "$#" -gt 1 ]; then exit 1; fi\n' if bisect else '')+record)
                gh = bins/'gh'
                gh.write_text('#!/bin/bash\nif [ "$2" = view ]; then echo READY; else printf "%s\\n" "$*" >> "$GATE_STATE/merges"; [ "$3" != 1 ]; fi\n')
                gh.chmod(0o755)
                (bins/'python').symlink_to(sys.executable)
                env = {**os.environ, 'GATE_LANES':str(tools), 'GATE_STATE':str(state),
                       'GATE_REPO':str(self.repo), 'GATE_WORKTREE':str(self.repo),
                       'GATE_ALERTS':str(state/'alerts'), 'PATH':str(bins)+':'+os.environ['PATH']}
                subprocess.run(['bash', str(HERE/'gate-drain.sh')], env=env, check=True)
                merged = (state/'merges').read_text().splitlines()
                self.assertEqual(len(merged), 2)
                self.assertTrue(all('--match-head-commit '+self.head in line for line in merged))
                self.assertEqual((state/'held-pending.txt').read_text().split(), ['1'])
                self.assertEqual(json.loads((state/'head-holds.json').read_text())['1']['kind'], 'merge-failed')
                self.assertIn('#2 head='+self.head+' merge_rc=0', (state/'alerts').read_text())
                self.assertFalse((state/'.lock').exists())


if __name__ == '__main__': unittest.main(verbosity=2)
