"""Real Git repositories and captured Cargo/main output qualify the base."""
import importlib.util
import contextlib
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
FIXTURES = HERE/'fixtures'
spec = importlib.util.spec_from_file_location('baseline', HERE/'gate-baseline.py')
baseline = importlib.util.module_from_spec(spec); spec.loader.exec_module(baseline)
PHASES = (FIXTURES/'main-32b799-phases.txt').read_text()


def completed_clippy_phase(text):
    # A test-only repaired envelope around an actual complete captured finding;
    # the original truncated external fixture remains unchanged and refused.
    warning = next(line for line in (FIXTURES/'main-32b799-truncated-clippy.txt').read_text().splitlines()
                   if line.startswith('GATE_CLIPPY_FINDING '))
    block = warning + '\n'
    original = next(line for line in text.splitlines() if line.startswith('GATE_CLIPPY_FINDING '))
    return text.replace(original, block + f'GATE_CLIPPY_COMPLETE rc=0 records=1 sha256={sha256(block.encode()).hexdigest()}\nGATE_FEATURES rc=0')
TEST_PHASES = (FIXTURES/'main-32b799-test-phases.txt').read_text()
GREEN_PHASES = (FIXTURES/'main-7989-green-test-phases.txt').read_text()
RED_FEATURES = (FIXTURES/'main-da7eee-red-features.txt').read_text()
CAPTURED_HEAD = '32b79969bc504e875bb6e9f65611bf0d02ea45e9'


class ExactBaselineTests(unittest.TestCase):
    def test_1504_captured_scope_cannot_use_its_historical_baseline_labels(self):
        captured=(FIXTURES/'route-1504/checks.txt').read_text()
        scope=json.loads(captured.splitlines()[0].removeprefix('GATE_SCOPE '))
        self.assertEqual(scope['base'],CAPTURED_HEAD)
        self.assertIn('GATE_CLIPPY rc=0\n',captured)
        self.assertIn('7989aaf9c465c04e0f2044854afefa8cd4136344.log',captured)
        ancestor='ecd0e67effe6d71631ba4acb66c918074f40df59'
        self.assertIn('features=Q:/lanes/main-gate/wide/'+ancestor+'.log',captured)
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'wide').mkdir();(root/'clippy').mkdir()
            native=root/'wide'/(CAPTURED_HEAD+'.log');native.write_text(PHASES)
            lint=root/'clippy'/(CAPTURED_HEAD+'.log')
            lint.write_text((FIXTURES/'main-32b799-complete-clippy.txt').read_text())
            stale=root/'wide'/(ancestor+'.log')
            stale.write_text(completed_clippy_phase(PHASES).replace(CAPTURED_HEAD,ancestor))
            os.utime(stale,(max(native.stat().st_mtime,lint.stat().st_mtime)+10,)*2)
            alerts=f'MAIN_WIDE RED {CAPTURED_HEAD}: captured\nMAIN_WIDE RED {ancestor}: captured\n'
            from unittest.mock import patch
            with patch.object(baseline.subprocess,'run',return_value=subprocess.CompletedProcess([],0)):
                for phase,expected in [('clippy',lint),('features',native)]:
                    self.assertEqual(baseline.select(root,scope['base'],root,alerts,phase,True),expected)
                    # Removing exact selection reproduces the historical
                    # ancestor comparison even though this base has results.
                    self.assertEqual(baseline.select(root,scope['base'],root,alerts,phase,False),stale)
                native.unlink()
                with self.assertRaisesRegex(RuntimeError,'no complete exact-base main features'):
                    baseline.select(root,scope['base'],root,alerts,'features',True)

    def test_real_requalified_clippy_replaces_damaged_stream_only_for_its_exact_phase(self):
        complete = (FIXTURES/'main-32b799-complete-clippy.txt').read_text()
        self.assertTrue(baseline.check_complete(complete, 'clippy'))
        self.assertFalse(baseline.check_complete(complete, 'tests'))
        self.assertFalse(baseline.check_complete(complete, 'features'))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root/'clippy').mkdir(); (root/'wide').mkdir()
            log = root/'clippy'/(CAPTURED_HEAD+'.log'); log.write_text(complete)
            (root/'wide'/(CAPTURED_HEAD+'.log')).write_text(PHASES)
            from unittest.mock import patch
            with patch.object(baseline.subprocess, 'run', return_value=subprocess.CompletedProcess([],0)):
                self.assertEqual(baseline.select(root,CAPTURED_HEAD,root,
                    'MAIN_WIDE RED '+CAPTURED_HEAD+': actual captured run', 'clippy', True),log)
            self.assertFalse(baseline.check_complete(complete.replace('GATE_CLIPPY_COMPLETE ', 'lost '), 'clippy'))
        # A real Clippy error on main still completes its exact-base phase when
        # its receipt is valid; the former rc=0 requirement refused it and
        # deferred every PR on that base.
        self.assertEqual(complete.count('GATE_CLIPPY rc=0\n'), 1)
        self.assertEqual(complete.count('GATE_CLIPPY_COMPLETE rc=0 '), 1)
        red = complete.replace('GATE_CLIPPY rc=0\n', 'GATE_CLIPPY rc=101\n').replace('GATE_CLIPPY_COMPLETE rc=0 ', 'GATE_CLIPPY_COMPLETE rc=101 ')
        self.assertTrue(baseline.check_complete(red, 'clippy'))
        self.assertFalse(baseline.check_complete(red.replace('GATE_CLIPPY_COMPLETE rc=101 ', 'GATE_CLIPPY_COMPLETE rc=0 '), 'clippy'))

    def test_historical_green_fallback_requires_its_executor_receipt(self):
        self.assertTrue(baseline.eligible(GREEN_PHASES))
        for replacement in ['', 'GATE_EXECUTOR rc=-9', 'GATE_EXECUTOR rc=101']:
            broken = GREEN_PHASES.replace('GATE_EXECUTOR rc=0', replacement)
            self.assertFalse(baseline.eligible(broken))
            with self.assertRaisesRegex(ValueError, 'incomplete main baseline'):
                baseline.coverage(broken, [])
        # Removing just the shared receipt guard restores the former false
        # qualification of a main log left before coordinator completion.
        source = (HERE/'gate-baseline.py').read_text()
        old = source.replace('not completed_payload(text) or ', '')
        self.assertNotEqual(source, old)
        namespace = {}; exec(compile(old, 'old-green-receipt-control', 'exec'), namespace)
        self.assertTrue(namespace['eligible'](GREEN_PHASES.replace('GATE_EXECUTOR rc=0', '')))

    def test_exact_red_main_reuses_completed_tests_without_a_focused_build(self):
        self.assertFalse(baseline.eligible(TEST_PHASES), 'overall main remains RED')
        self.assertTrue(baseline.check_complete(TEST_PHASES, 'tests'))
        covered = baseline.coverage(TEST_PHASES, [])
        self.assertIn('acyclic-harness-core', covered)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); repo = root/'repo'; repo.mkdir()
            env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
            def git(*args):
                return subprocess.check_output(['git', '-C', str(repo), *args], env=env, stderr=subprocess.DEVNULL).decode().strip()
            git('init', '-q')
            git('-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                'commit', '-q', '--allow-empty', '-m', 'real main phase output identity fixture')
            head = git('rev-parse', 'HEAD')
            logs = root/'logs'; (logs/'wide').mkdir(parents=True)
            exact = logs/'wide'/(head+'.log')
            actual = TEST_PHASES.replace(CAPTURED_HEAD, head)
            exact.write_text(actual)
            alerts = f'MAIN_WIDE RED {head}: captured real test phases\n'
            self.assertEqual(baseline.select(repo, head, logs, alerts, 'tests', True), exact)
            alert_file = root/'alerts'; alert_file.write_text(alerts)
            (root/'.codex-fleet').mkdir()
            source = (HERE/'pr-gate.sh').read_text()
            dispatch = source[source.index('if ! base_log=$('):source.index('clippy_base=$(')]
            dispatch = dispatch.replace('python Q:/lanes/gate-baseline.py',
                f'"{sys.executable}" "{HERE / "gate-baseline.py"}" --directory "{logs}" --alerts "{alert_file}"')
            for negative in [False, True]:
                selected = dispatch.replace(' --phase tests --exact', '') if negative else dispatch
                run = subprocess.run(['bash', '-c', f'W="{repo}";base={head};log="{root / "job.log"}";n=1\n'+selected],
                                     env={**os.environ, 'HOME':str(root)}, capture_output=True, text=True)
                self.assertEqual(run.returncode, 5 if negative else 0, run.stdout+run.stderr)
                if not negative:
                    self.assertIn('GATE_BASELINE log='+str(exact), (root/'job.log').read_text())
            # The old GREEN-only path rejects the same completed phase and
            # makes the coordinator rebuild roots already tested on main.
            with self.assertRaisesRegex(RuntimeError, 'recovery command:'):
                baseline.select(repo, head, logs, alerts, 'tests', False)
            spec = importlib.util.spec_from_file_location('focused_baseline', HERE/'gate-test-baseline.py')
            focused = importlib.util.module_from_spec(spec); spec.loader.exec_module(focused)
            pr = root/'pr.log'; pr.write_text((FIXTURES/'baseline-delta-pr.txt').read_text())
            scope = root/'scope.json'; scope.write_text(json.dumps({'tests': sorted(covered)}))
            result_dir = root/'focused-results'
            argv = [str(HERE/'gate-test-baseline.py'), str(repo), head, str(pr), '--scope', str(scope),
                    '--baseline-log', str(exact), '--results', str(result_dir)]
            from unittest.mock import patch
            with patch.object(sys, 'argv', argv), patch.object(focused, 'run_logged', side_effect=AssertionError('duplicate main build')), contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(focused.main(), 0)
            self.assertEqual(output.getvalue(), '')
            self.assertFalse(result_dir.exists(), 'completed exact main phases need no focused job')
            # Executor loss, contradictory phase footers and missing coverage
            # must never borrow completion from unrelated successful phases.
            target = next(line for line in actual.splitlines(keepends=True) if line.startswith('GATE_TEST_WALL phase=targets '))
            for broken in [actual.replace('GATE_EXECUTOR rc=1', ''),
                           actual.replace('GATE_EXECUTOR rc=1', 'GATE_EXECUTOR rc=-9'),
                           actual.replace(target, target.replace('rc=0', 'rc=101')),
                           actual + target,
                           '\n'.join(line for line in actual.splitlines() if not line.startswith('GATE_TEST_COVERAGE all-targets+doc '))]:
                self.assertFalse(baseline.check_complete(broken, 'tests'))
                exact.write_text(broken)
                with self.assertRaisesRegex(RuntimeError, 'recovery command:'):
                    baseline.select(repo, head, logs, alerts, 'tests', True)

    def test_completed_red_matrix_counts_sets_not_failed_commands(self):
        self.assertTrue(baseline.check_complete(RED_FEATURES, 'features'))
        # This is actual main109/110: one all-targets command failed and is
        # printed both immediately and in the final summary.
        lines = RED_FEATURES.splitlines(keepends=True)
        failed = [line for line in lines if line.startswith('GATE_FEATURES FAILED cargo ')]
        completed = [line for line in lines if line.startswith('GATE_FEATURE_CHECK feature check: ')]
        self.assertEqual(len(failed), 2)
        self.assertEqual(len(completed), 110)
        self.assertTrue(baseline.check_complete(RED_FEATURES+''.join(failed), 'features'),
                        'repeated command summaries do not change set completion')
        missing = RED_FEATURES.replace(completed[-1], '')
        self.assertFalse(baseline.check_complete(missing, 'features'))
        self.assertFalse(baseline.check_complete(missing+completed[0], 'features'),
                         'duplicate completed sets cannot replace an uncompleted set')
        # The actual former command-count comparison rejects this complete run.
        source = (HERE/'gate-baseline.py').read_text()
        old = source.replace('and len(completed) == len(set(completed)) == total',
            "and len(completed) == total and len(re.findall(r'^GATE_FEATURES FAILED cargo ', native, re.M)) == total-passed")
        self.assertNotEqual(old, source)
        namespace = {'__name__': 'old_command_count_control'}
        exec(compile(old, 'old-command-count-control', 'exec'), namespace)
        self.assertFalse(namespace['check_complete'](RED_FEATURES, 'features'))

    def test_exact_check_selection_never_uses_a_newer_dated_ancestor(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); repo = root/'repo'; repo.mkdir()
            env = {k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
            def git(*args):
                return subprocess.check_output(['git', '-C', str(repo), *args], env=env, stderr=subprocess.DEVNULL).decode().strip()
            git('init', '-q')
            def commit():
                git('-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                    'commit', '-q', '--allow-empty', '-m', 'captured-output identity fixture')
                return git('rev-parse', 'HEAD')
            old = commit(); new = commit()
            logs = root/'logs'; (logs/'wide').mkdir(parents=True)
            ancestor = logs/(old+'.log'); exact = logs/'wide'/(new+'.log')
            qualified = completed_clippy_phase(PHASES)
            ancestor.write_text(qualified.replace(CAPTURED_HEAD, old))
            exact.write_text(qualified.replace(CAPTURED_HEAD, new))
            os.utime(ancestor, (exact.stat().st_mtime+10,)*2)
            alerts = f'MAIN_WIDE RED {old}: captured\nMAIN_WIDE RED {new}: captured\n'
            alert_file = root/'alerts'; alert_file.write_text(alerts)
            for phase in ['clippy', 'features']:
                self.assertEqual(baseline.select(repo,new,logs,alerts,phase,True),exact)
                self.assertEqual(baseline.select(repo,new,logs,alerts,phase,False),ancestor,
                                 'former ancestor selection compares the wrong source')
                # Execute the actual coordinator call too. Removing --exact
                # reproduces selection of the newer-dated ancestor.
                statement = next(line for line in (HERE/'pr-gate.sh').read_text().splitlines()
                                 if line.startswith(phase+'_base=$(python '))
                statement = statement.replace('python Q:/lanes/gate-baseline.py',
                    f'"{sys.executable}" "{HERE / "gate-baseline.py"}" --directory "{logs}" --alerts "{alert_file}"')
                for negative in [False, True]:
                    script = f'W="{repo}";base={new};log="{root / "job.log"}"\n'
                    script += statement.replace(' --exact', '') if negative else statement
                    script += '\nprintf "%s" "$'+phase+'_base"\n'
                    run = subprocess.run(['bash','-c',script],capture_output=True,text=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertEqual(run.stdout,str(ancestor if negative else exact))
                original = exact.read_text()
                for broken in [original.replace('GATE_EXECUTOR rc=1', 'GATE_EXECUTOR rc=-9'),
                               original.replace('GATE_EXECUTOR rc=1', ''),
                               original.replace('"head": "'+new+'"', '"head": "'+old+'"')]:
                    exact.write_text(broken)
                    with self.assertRaisesRegex(RuntimeError, 'recovery command:'):
                        baseline.select(repo,new,logs,alerts,phase,True)
                exact.write_text(original)

    def test_native_completion_cannot_be_borrowed_from_windows(self):
        self.assertFalse(baseline.check_complete(PHASES, 'clippy'), 'Cargo success cannot complete truncated findings')
        self.assertTrue(baseline.check_complete(PHASES, 'features'))
        lines = PHASES.splitlines(keepends=True)
        wall = next(line for line in lines if line.startswith('GATE_FEATURES_WALL '))
        # The captured native rc marker is joined to an unfinished lint line.
        # Its own complete wall receipt is independent of the Windows phase.
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, wall.replace('rc=0', 'rc=101')), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, ''), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace('GATE_EXECUTOR rc=1', ''), 'features'))
        self.assertFalse(baseline.check_complete(PHASES.replace(wall, 'GATE_FEATURES rc=101\n'+wall), 'features'))

    def test_focused_current_results_replace_obsolete_failure_names(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('pr_fails=$('):]
        pr_text = (FIXTURES/'baseline-delta-pr.txt').read_text()
        main_text = (FIXTURES/'baseline-delta-main.txt').read_text()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root/'.codex-fleet').mkdir(); (root/'flakes').write_text('')
            (root/'checks.sh').write_text('exit 0\n')
            (root/'failed-tests.sh').write_text((HERE/'failed-tests.sh').read_text())
            block = block.replace('Q:/lanes/failed-tests.sh', str(root/'failed-tests.sh')).replace(
                'Q:/lanes/flakes.txt', str(root/'flakes')).replace('Q:/lanes/gate-checks-verdict.sh', str(root/'checks.sh'))
            pr = root/'pr.log'; pr.write_text(pr_text)
            stale = root/'old.log'; stale.write_text(pr_text)
            focused = root/'current.log'; focused.write_text(main_text)
            for negative, focus, expected in [(False,str(focused),1),(True,str(focused),0),(False,'',5)]:
                actual = block.replace('effective_test_base=$test_base_log', 'effective_test_base=$base_log') if negative else block
                script = f'rc=101;n=1;base={CAPTURED_HEAD};W=fixture;log={pr};base_log={stale};test_base_log="{focus}";clippy_base={focused};features_base={focused}\n'+actual
                result = subprocess.run(['bash','-c',script],env={**os.environ,'HOME':str(root)},capture_output=True,text=True)
                self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
                if expected == 1:
                    self.assertIn('new failures vs exact main (32b79969bc)', result.stdout)
                    self.assertIn('core::element_sizes::conversation_message', result.stdout)
                if expected == 5:
                    self.assertIn('exact-base test comparison step unavailable', result.stdout)
                    self.assertIn('recovery command:', result.stdout)

    def test_same_failures_on_exact_base_are_excluded_by_name(self):
        source = (HERE/'pr-gate.sh').read_text()
        block = source[source.index('pr_fails=$('):]
        captured = (FIXTURES/'baseline-delta-pr.txt').read_text()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); (root/'.codex-fleet').mkdir()
            (root/'flakes').write_text('')
            (root/'checks.sh').write_text('exit 0\n')
            (root/'failed-tests.sh').write_text((HERE/'failed-tests.sh').read_text())
            block = block.replace('Q:/lanes/failed-tests.sh', str(root/'failed-tests.sh')).replace(
                'Q:/lanes/flakes.txt', str(root/'flakes')).replace('Q:/lanes/gate-checks-verdict.sh', str(root/'checks.sh'))
            pr = root/'pr.log'; pr.write_text(captured)
            base = root/(CAPTURED_HEAD+'.log')
            for negative in [False, True]:
                # Mutate only which captured result the base supplies. Actual
                # named failures present on both sides must yield no new test.
                base.write_text((FIXTURES/'baseline-delta-main.txt').read_text() if negative else captured)
                header = f'rc=101;n=1;base={CAPTURED_HEAD};W=fixture;log={pr};base_log={base};test_base_log="";clippy_base={base};features_base={base}\n'
                result = subprocess.run(['bash','-c',header+block],env={**os.environ,'HOME':str(root)},capture_output=True,text=True)
                self.assertEqual(result.returncode,1 if negative else 0,result.stdout+result.stderr)
                self.assertIn('GATE_COMPARISON_BASE base='+CAPTURED_HEAD,(pr).read_text())
                if not negative:
                    self.assertIn('2 pre-existing on main',result.stdout)

    def test_both_phase_emitters_separate_an_unterminated_diagnostic(self):
        lint = next(line for line in PHASES.splitlines() if line.startswith('GATE_CLIPPY_FINDING ')).split('GATE_FEATURES rc=')[0]
        for name in ['main-gate-checks.sh','pr-gate-checks.sh']:
            source = (HERE.parent.parent/name).read_text()
            statement = next(line.strip() for line in source.splitlines() if "printf '\\nGATE_FEATURES rc=" in line)
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); (root/'.gate-features.rc').write_text('0\n')
                for negative in [False,True]:
                    emit = 'echo "GATE_FEATURES rc=0"' if negative else statement
                    run = subprocess.run(['bash','-c','printf "%s" "$CAPTURED_LINT"; '+emit],
                                         env={**os.environ,'CAPTURED_LINT':lint,'gate_output':str(root)},capture_output=True,text=True)
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertEqual('GATE_FEATURES rc=0' in run.stdout.splitlines(),not negative)


if __name__ == '__main__':
    unittest.main(verbosity=2)
