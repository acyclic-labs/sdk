import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class DispatchTests(unittest.TestCase):
    def run_drain(self, body, *, draft=False, negative=False, unchecked=False, onepass=False, first='', pending='1\n2\n3\n'):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = root/'state'; state.mkdir()
            bins = root/'bin'; bins.mkdir()
            tools = root/'tools'; tools.mkdir()
            (bins/'python').symlink_to(sys.executable)
            gh = bins/'gh'
            gh.write_text('#!/bin/bash\nif [ -f "$GATE_STATE/draft-$3" ]; then echo DRAFT; else echo READY; fi\n')
            gh.chmod(0o755)
            for name in ['gate-queue.py']:
                (tools/name).symlink_to(HERE/name)
            (tools/'gate-prepare.py').write_text('raise SystemExit(0)\n')
            (tools/'gate-merge.py').write_text('import os,sys\nfrom pathlib import Path\nwith (Path(os.environ["GATE_STATE"])/"merged").open("a") as f:f.write(" ".join(sys.argv[2:sys.argv.index("--repo")])+"\\n")\n')
            (tools/'merge-hold.txt').write_text('')
            (tools/'pr-gate.sh').write_text('echo "$*" >> "$GATE_STATE/runs"\n'+body)
            (tools/'route.sh').write_text('echo "$*" >> "$GATE_STATE/routes"\n')
            (state/'pending.txt').write_text(pending)
            (state/'first.txt').write_text(first)
            if draft:
                (state/'draft-1').touch()
            source = (HERE/'gate-drain.sh').read_text()
            if negative:
                source = source.replace('deferred+=("$c")', 'enqueue "$c"')
            if unchecked:
                source = source.replace('for p in "$@"; do\n    state=', 'for p in "$@"; do\n    ready+=("$p"); continue\n    state=')
            if onepass:
                # Bound the observation to one drain, as the contract requires;
                # eventual success in another drain does not prove fairness.
                source = source.replace('  rmdir "$L/.lock"; trap - EXIT', '  rmdir "$L/.lock"; trap - EXIT\n  break')
            script = tools/'gate-drain.sh'; script.write_text(source)
            env = {**os.environ, 'GATE_LANES':str(tools), 'GATE_STATE':str(state),
                   'PATH':str(bins)+':'+os.environ['PATH']}
            run = subprocess.run(['bash', str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(run.returncode, 0, run.stdout+run.stderr)
            values = {p.name:p.read_text() for p in state.iterdir() if p.is_file() and p.suffix != '.sqlite3'}
            self.assertFalse((state/'.lock').exists())
            return values, run.stdout

    def test_universal_batch_collision_runs_solo_within_one_drain(self):
        body = '''if [ "$(wc -l < "$GATE_STATE/runs")" -gt 4 ]; then exit 6; fi
if [ "$#" -gt 1 ] && [[ " $* " = *' 1 '* ]]; then echo 1 > "$GATE_STATE/.conflict"; exit 7; fi
exit 0
'''
        good, _ = self.run_drain(body, onepass=True)
        self.assertEqual(good['runs'].splitlines(), ['1 2 3', '2 3', '1'])
        self.assertEqual(good['merged'].splitlines(), ['2 3', '1'])
        self.assertEqual(good['pending.txt'], '')
        old, _ = self.run_drain(body, negative=True, onepass=True)
        self.assertNotIn('1', old['runs'].splitlines())
        self.assertIn('1', old['pending.txt'].split())

    def test_bisect_deferrals_are_solo_and_do_not_route_a_batch(self):
        body = '''if [ "$*" = '1 2 3' ]; then exit 1; fi
if [ "$*" = '1 2' ]; then echo 1 > "$GATE_STATE/.conflict"; exit 7; fi
exit 0
'''
        good, _ = self.run_drain(body)
        self.assertEqual(good['runs'].splitlines(), ['1 2 3', '1 2', '2', '1', '3'])
        self.assertNotIn('routes', good)

    def test_draft_is_removed_before_preparation_and_batch_dispatch(self):
        good, text = self.run_drain('exit 0\n', draft=True)
        self.assertEqual(good['runs'].splitlines(), ['2 3'])
        self.assertEqual(good['held-pending.txt'].split(), ['1'])
        self.assertIn('draft at batch formation', text)
        old, _ = self.run_drain('exit 0\n', draft=True, unchecked=True)
        self.assertEqual(old['runs'].splitlines(), ['1 2 3'])

    def test_draft_transition_is_rechecked_before_recursive_dispatch(self):
        good, text = self.run_drain('if [ "$#" -gt 1 ]; then touch "$GATE_STATE/draft-1"; exit 1; fi\nexit 0\n', pending='1\n2\n')
        self.assertEqual(good['runs'].splitlines(), ['1 2', '2'])
        self.assertEqual(good['held-pending.txt'].split(), ['1'])
        self.assertIn('draft at batch formation', text)
        old, _ = self.run_drain('if [ "$#" -gt 1 ]; then touch "$GATE_STATE/draft-1"; exit 1; fi\nexit 0\n', unchecked=True, pending='1\n2\n')
        self.assertEqual(old['runs'].splitlines(), ['1 2', '1', '2'])

    def test_preparation_failure_preserves_unvisited_solo_and_peers(self):
        body = 'if [ "$#" -gt 1 ]; then echo 1 > "$GATE_STATE/.conflict"; exit 7; fi\nexit 6\n'
        good, _ = self.run_drain(body, pending='1\n2\n')
        self.assertEqual(good['runs'].splitlines(), ['1 2', '2'])
        self.assertEqual(good['pending.txt'].split(), ['1', '2'])

    def test_impossible_solo_batch_collision_holds_instead_of_looping(self):
        good, text = self.run_drain('echo 1 > "$GATE_STATE/.conflict"; exit 7\n', pending='1\n')
        self.assertEqual(good['runs'].splitlines(), ['1'])
        self.assertEqual(good['pending.txt'].split(), ['1'])
        self.assertIn('batch-only collision reported for a solo', text)

    def test_priority_members_each_gate_solo_before_general_queue(self):
        good, _ = self.run_drain('exit 0\n', first='1\n2\n')
        self.assertEqual(good['runs'].splitlines(), ['1', '2', '3'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
