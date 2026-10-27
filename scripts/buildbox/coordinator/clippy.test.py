#!/usr/bin/env python3
"""Captured Cargo short errors exercise emitters, public verdict and coordinator."""
import os
import importlib.util
import json
import shlex
from hashlib import sha256
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import contextlib
import io

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / 'fixtures'
RAW = (FIXTURES / 'authority104-clippy-errors.txt').read_text()
BASE = (FIXTURES / 'authority104-main-clippy-status.txt').read_text()
spec = importlib.util.spec_from_file_location('clippy', HERE / 'gate-clippy.py')
clippy = importlib.util.module_from_spec(spec); spec.loader.exec_module(clippy)


def sealed(text):
    records = ''.join(line + '\n' for line in text.splitlines()
                      if line.startswith(('GATE_CLIPPY_FINDING ', 'GATE_CLIPPY_ERROR ')))
    status = next(line.split('=')[1] for line in text.splitlines() if line.startswith('GATE_CLIPPY rc='))
    return text + f'GATE_CLIPPY_COMPLETE rc={status} records={len(records.splitlines())} sha256={sha256(records.encode()).hexdigest()}\n'



class ClippyDiagnostics(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.raw = self.root / 'cargo.log'; self.raw.write_text(RAW)
        self.base = self.root / 'main.log'; self.base.write_text(sealed(BASE))
        self.log = self.root / 'pr.log'
        for name in ['gate-checks.sh', 'gate-checks-results.sh', 'gate-clippy.py', 'failed-tests.sh']:
            shutil.copyfile(HERE / name, self.root / name)

    def verdict(self):
        return subprocess.run(['bash', str(self.root / 'gate-checks-results.sh'), str(self.log), str(self.base)],
                              capture_output=True, text=True)

    def test_captured_short_errors_fail_even_through_old_emission(self):
        emitted = subprocess.run(['python3', str(HERE / 'gate-clippy.py'), 'emit', str(self.raw), '101'],
                                 capture_output=True, text=True, check=True)
        self.assertEqual(emitted.stdout.count('GATE_CLIPPY_ERROR '), 2)
        self.assertEqual(emitted.stdout.count('error[E0277]'), 2)
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        # The live legacy emitters retained raw diagnostics but lost parsed
        # error[E...] markers. Both this case and the new emitter must fail.
        for parsed in ['', emitted.stdout]:
            self.log.write_text('GATE_CLIPPY rc=101\n' + raw + parsed)
            result = self.verdict()
            self.assertEqual(result.returncode, 1 if parsed else 4, result.stdout + result.stderr)
            if parsed:
                self.assertEqual(result.stdout.count('clippy-error: '), 2)
                self.assertIn('error[E0277]', result.stdout)
        shutil.copyfile(FIXTURES / 'authority104-old-results.sh.txt', self.root / 'old.sh')
        self.log.write_text('GATE_CLIPPY rc=101\n' + raw)
        old = subprocess.run(['bash', str(self.root / 'old.sh'), str(self.log), str(self.base)], capture_output=True, text=True)
        self.assertEqual(old.returncode, 0, 'actual old public verdict falsely accepts these captured compiler errors')

    def test_missing_diagnostics_and_operational_status_are_typed_infra(self):
        for status in ['101', '127', '-9']:
            self.log.write_text('GATE_CLIPPY rc=' + status + '\n')
            result = self.verdict()
            self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
            self.assertIn('recovery command:', result.stdout)

    def test_both_actual_emitters_decode_captured_short_errors(self):
        (self.root / '.gate-clippy.out').write_text(RAW)
        (self.root / '.gate-clippy.rc').write_text('101\n')
        hook = 'python3 scripts/buildbox/coordinator/gate-clippy.py emit '
        for name, end in [('main-gate-checks.sh', 'wait "$features"'),
                          ('pr-gate-checks.sh', 'if [ -n "$features" ]; then')]:
            source = (HERE.parents[1] / name).read_text()
            start = source.index('echo "GATE_CLIPPY rc=')
            emission = source[start:source.index(end, start)]
            for negative in [False, True]:
                code = emission.replace(hook, ': # ' + hook) if negative else emission
                output = subprocess.run(['bash', '-c', code], cwd=HERE.parents[2], capture_output=True, text=True,
                                        env={**os.environ, 'gate_output': str(self.root)})
                self.assertEqual(output.returncode, 0, output.stdout + output.stderr)
                markers = [line for line in output.stdout.splitlines() if line.startswith('GATE_CLIPPY_ERROR ')]
                self.assertEqual(len(markers), 0 if negative else 2, name)
                self.log.write_text(output.stdout)
                self.assertEqual(self.verdict().returncode, 4 if negative else 1)

    def test_warning_ratchet_and_known_source_errors_remain_comparable(self):
        # The warning bytes come from the same real Cargo capture.
        warning = next(line for line in (FIXTURES / 'main-32b799-phases.txt').read_text().splitlines()
                       if line.startswith('GATE_CLIPPY_FINDING '))
        self.base.write_text(sealed(BASE + warning + '\n'))
        self.log.write_text(sealed(BASE + warning + '\n'))
        self.assertEqual(self.verdict().returncode, 0)
        self.base.write_text(sealed(BASE))
        self.assertEqual(self.verdict().returncode, 1)
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        emitted = subprocess.check_output(['python3', str(HERE/'gate-clippy.py'), 'emit', str(self.raw), '101'], text=True)
        self.base.write_text('GATE_CLIPPY rc=101\n' + raw + emitted)
        self.log.write_text(self.base.read_text())
        self.assertEqual(self.verdict().returncode, 0)

    def test_coordinator_preserves_source_and_infra_verdicts(self):
        source = (HERE / 'pr-gate.sh').read_text()
        chunk = source[source.index('pr_fails=$('):].replace('Q:/lanes/failed-tests.sh', str(self.root / 'failed-tests.sh')).replace(
            'Q:/lanes/flakes.txt', str(self.root / 'flakes')).replace('Q:/lanes/gate-checks-verdict.sh', str(self.root / 'gate-checks-results.sh'))
        (self.root / 'flakes').write_text(''); (self.root / '.codex-fleet').mkdir()
        raw = ''.join('GATE_CLIPPY_RAW ' + line + '\n' for line in RAW.splitlines())
        for status, diagnostics, expected in [('101', raw, 1), ('101', '', 4), ('0', '', 0)]:
            current = 'GATE_DONE rc=0\nGATE_CLIPPY rc=' + status + '\n' + diagnostics
            if status == '101' and diagnostics:
                current += subprocess.check_output(['python3', str(HERE/'gate-clippy.py'), 'emit', str(self.raw), status], text=True)
            elif status == '0':
                current = sealed(current)
            self.log.write_text(current)
            header = f'rc=0;n=1;base=main;log={self.log};base_log={self.base};test_base_log="";clippy_base={self.base};features_base={self.base}\n'
            run = subprocess.run(['bash', '-c', header + chunk], capture_output=True, text=True, env={**os.environ, 'HOME': str(self.root)})
            self.assertEqual(run.returncode, expected, run.stdout + run.stderr)
        old = chunk.replace('*) v="INFRA #$n: clippy/feature interpretation', '*) v="FAIL #$n: clippy/feature interpretation')
        self.log.write_text('GATE_DONE rc=0\nGATE_CLIPPY rc=101\n')
        negative = subprocess.run(['bash', '-c', header + old], capture_output=True, text=True, env={**os.environ, 'HOME': str(self.root)})
        self.assertEqual(negative.returncode, 1, 'former nonzero-as-source classification hides typed INFRA')

    def test_captured_truncation_cannot_qualify_or_fail_unrelated_source(self):
        truncated = (FIXTURES/'main-32b799-truncated-clippy.txt').read_text()
        self.assertFalse(clippy.complete(truncated))
        self.base.write_text(truncated)
        self.log.write_text(sealed(BASE))
        result = self.verdict()
        self.assertEqual(result.returncode, 4, result.stdout + result.stderr)
        self.assertIn('diagnostic stream is incomplete', result.stdout)
        # The former status-only baseline admission accepts these real bytes.
        self.assertIn('GATE_CLIPPY rc=0\n', truncated)

    def test_receipt_detects_missing_changed_and_duplicate_records(self):
        warning = next(line for line in (FIXTURES/'main-32b799-phases.txt').read_text().splitlines()
                       if line.startswith('GATE_CLIPPY_FINDING ')) + '\n'
        good = sealed(BASE + warning)
        self.assertTrue(clippy.complete(good))
        for broken in [good.replace(warning, ''), good.replace(warning, warning + warning),
                       good.replace('GATE_CLIPPY_FINDING 1 ', 'GATE_CLIPPY_FINDING 2 '),
                       good.split('GATE_CLIPPY_COMPLETE')[0]]:
            self.assertFalse(clippy.complete(broken))

    def test_legacy_payload_is_attested_against_real_cargo_bytes_before_retirement(self):
        # Retain the captured Cargo stream; only its presentation envelope is
        # varied. The mutation models the observed mid-stream producer loss.
        shutil.copyfile(self.raw, self.root/'.gate-clippy.out')
        emitted = subprocess.check_output(['python3', str(HERE/'gate-clippy.py'), 'emit', str(self.raw), '101'], text=True)
        records = emitted.split('GATE_CLIPPY_COMPLETE')[0]
        for negative in [False, True]:
            self.log.write_text('GATE_CLIPPY rc=101\n' + (records[:len(records)//2] if negative else records) + '\nGATE_DONE rc=101\n')
            driver = self.root/'payload.sh'
            driver.write_text('cat "'+str(self.log)+'"\nexit 101\n')
            result = subprocess.run(['python3', str(HERE/'gate-clippy.py'), 'run', 'bash', str(driver)],
                                    cwd=self.root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 101, result.stdout+result.stderr)
            self.assertTrue(clippy.complete(result.stdout))
            self.assertEqual(result.stdout.count('GATE_DONE rc='), 1)
            self.assertEqual(result.stdout.count('GATE_CLIPPY_ERROR '), 2)
            self.assertEqual(clippy.findings(result.stdout),clippy.findings('GATE_CLIPPY rc=101\n'+emitted))
        # No retained Cargo file means no authority to repair a legacy stream.
        (self.root/'.gate-clippy.out').unlink()
        missing = subprocess.run(['python3',str(HERE/'gate-clippy.py'),'run','bash',str(driver)],
                                 cwd=self.root,capture_output=True,text=True)
        self.assertEqual(missing.returncode,224)
        self.assertIn('recovery command:',missing.stdout)
        # The production adapter must keep attestation inside the admitted job.
        source = (HERE/'pr-gate.sh').read_text()
        self.assertIn('exec python3 .gate-clippy.py run bash .gate-pr-payload.sh', source)

    def test_real_base_journal_and_recovery_findings_are_not_pr_regressions(self):
        captured = (FIXTURES/'main-32b799-complete-clippy.txt').read_text()
        self.base.write_text(captured)
        self.log.write_text(captured)
        self.assertEqual(self.verdict().returncode, 0)
        for path in ['tests/execution_journal.rs:', 'context_continuation/recovery.rs:']:
            self.assertIn(path, captured)
        # The captured damaged stream is exactly what the old public reader
        # accepted as a baseline. It blames unchanged main for its lost bytes.
        self.base.write_text((FIXTURES/'main-32b799-truncated-clippy.txt').read_text())
        self.assertEqual(self.verdict().returncode, 4, 'partial baseline defers, never blames the PR')
        old = self.root/'old.sh'
        shutil.copyfile(FIXTURES/'authority104-old-results.sh.txt', old)
        result = subprocess.run(['bash', str(old), str(self.log), str(self.base)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('tests/execution_journal.rs:', result.stdout)
        self.assertIn('context_continuation/recovery.rs:', result.stdout)

    def test_payload_adapter_preserves_bytes_and_refuses_missing_tools(self):
        driver = self.root/'bytes.py'
        driver.write_text("import os\nos.write(1, bytes([255])+b' captured child output\\nGATE_DONE rc=0\\n')\n")
        result = subprocess.run(['python3', str(HERE/'gate-clippy.py'), 'run', 'python3', str(driver)], capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, bytes([255])+b' captured child output\nGATE_DONE rc=0\n')
        missing = subprocess.run(['python3', str(HERE/'gate-clippy.py'), 'run', str(self.root/'absent-tool')], capture_output=True, text=True)
        self.assertEqual(missing.returncode, 224)
        self.assertIn('payload start step failed:', missing.stdout)
        self.assertIn('recovery command:', missing.stdout)

    def test_legacy_repair_consumes_the_reported_directory_without_reallocating(self):
        scripts = self.root / 'scripts'
        scripts.mkdir()
        shutil.copyfile(HERE.parents[1] / 'gate-target-cache.py', scripts / 'gate-target-cache.py')
        output = self.root / 'actual-output'
        output.mkdir()
        (output / '.gate-clippy.out').write_text(RAW)
        self.log.write_text(f'GATE_OUTPUT_DIRECTORY {output}\nGATE_CLIPPY rc=101\nGATE_DONE rc=101\n')
        driver = self.root / 'payload.sh'
        driver.write_text(f'cat "{self.log}"\nexit 101\n')
        for adapter, expected in [(HERE / 'gate-clippy.py', 101),
                                  (FIXTURES / 'gate1509-footer/former-adapter.py.txt', 224)]:
            result = subprocess.run([sys.executable, str(adapter), 'run', 'bash', str(driver)],
                                    cwd=self.root, capture_output=True, text=True)
            self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
            if expected == 101:
                self.assertTrue(clippy.complete(result.stdout))
                self.assertEqual(result.stdout.count('GATE_CLIPPY_ERROR '), 2)
        # A helper's presence does not authorize inventing a second allocation,
        # even if unrelated diagnostics happen to exist in cwd.
        shutil.copyfile(self.raw, self.root / '.gate-clippy.out')
        for header in ['', f'GATE_OUTPUT_DIRECTORY {output}\n' * 2,
                       'GATE_OUTPUT_DIRECTORY relative/path\n',
                       f'GATE_OUTPUT_DIRECTORY {self.root / "absent"}\n']:
            self.log.write_text(header + 'GATE_CLIPPY rc=101\nGATE_DONE rc=101\n')
            result = subprocess.run([sys.executable, str(HERE / 'gate-clippy.py'), 'run', 'bash', str(driver)],
                                    cwd=self.root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 224, result.stdout + result.stderr)
            self.assertFalse(clippy.complete(result.stdout))

    def test_captured_cutoff_retains_actual_status_and_withheld_protocol(self):
        fixture = FIXTURES / 'gate1509-footer'
        captured = (fixture / 'tail.bin').read_bytes()
        origin = json.loads((fixture / 'origin.json').read_text())
        self.assertEqual(sha256(captured).hexdigest(), origin['sha256'])
        # The old adapter's terminal error/footer are its output, not evidence
        # of the inner child outcome. Replay only the preceding real bytes.
        captured = captured.split(b'error: GATE_RUNNER_INFRA Clippy stream attestation')[0]
        stream = self.root / 'cutoff.bin'
        stream.write_bytes(captured)
        driver = self.root / 'cutoff.py'
        driver.write_text('import os,signal,sys\nfrom pathlib import Path\n'
                          'sys.stdout.buffer.write(Path(sys.argv[1]).read_bytes());sys.stdout.buffer.flush()\n'
                          'os.kill(os.getpid(),signal.SIGKILL)\n')
        result = subprocess.run([sys.executable, str(HERE / 'gate-clippy.py'), 'run', sys.executable,
                                 str(driver), str(stream)], capture_output=True)
        self.assertEqual(result.returncode, 224, result.stderr)
        text = result.stdout.decode('utf-8')
        marker = next(line.removeprefix('GATE_PAYLOAD_OBSERVED ') for line in text.splitlines()
                      if line.startswith('GATE_PAYLOAD_OBSERVED '))
        observed = json.loads(marker)
        self.assertEqual(observed, {'child_status': -9, 'exit_status': 137, 'footers': [],
                                    'bytes': len(captured), 'sha256': sha256(captured).hexdigest()})
        self.assertIn(captured, result.stdout)
        self.assertFalse(clippy.complete(text))
        self.assertEqual(text.count('\nGATE_DONE rc='), 1)
        old = subprocess.run([sys.executable, str(fixture / 'former-adapter.py.txt'), 'run',
                              sys.executable, str(driver), str(stream)], capture_output=True)
        self.assertEqual(old.returncode, 224)
        self.assertNotIn(b'GATE_PAYLOAD_OBSERVED ', old.stdout,
                         'the actual former adapter loses the child status')
        # Mismatched completion markers are retained under an inert prefix;
        # they cannot be mistaken for an attested candidate completion.
        stream.write_bytes(b'GATE_DONE rc=0\n')
        done = subprocess.run([sys.executable, str(HERE / 'gate-clippy.py'), 'run',
                               sys.executable, str(driver), str(stream)], capture_output=True, text=True)
        self.assertIn('GATE_PAYLOAD_RETAINED GATE_DONE rc=0\n', done.stdout)
        self.assertEqual(done.stdout.count('\nGATE_DONE rc='), 1)
        stream.write_bytes(captured.rstrip(b'\n'))
        unterminated = subprocess.run([sys.executable, str(HERE / 'gate-clippy.py'), 'run',
                                       sys.executable, str(driver), str(stream)], capture_output=True, text=True)
        marker = next(line.removeprefix('GATE_PAYLOAD_OBSERVED ') for line in unterminated.stdout.splitlines()
                      if line.startswith('GATE_PAYLOAD_OBSERVED '))
        self.assertEqual(json.loads(marker)['child_status'], -9)

    def test_actual_bun_formatter_cannot_change_the_following_writers_flags(self):
        # Exercise the reviewed formatter and actual Bun/rustfmt on a tracked
        # Rust source. No Cargo compiler or handwritten tool output is used.
        scripts = self.root / 'scripts'
        scripts.mkdir()
        for name in ['gate-rust-format.sh', 'qualify-rustfmt.mjs', 'rustfmt-manifests.py']:
            shutil.copyfile(HERE.parents[1] / name, scripts / name)
        (self.root / 'Cargo.toml').write_text('[package]\nname="stdio-ownership"\nversion="0.0.0"\nedition="2024"\n')
        (self.root / 'src').mkdir()
        (self.root / 'src/main.rs').write_text('fn main() {}\n')
        env = {key: value for key, value in os.environ.items() if not key.startswith('GIT_')}
        subprocess.run(['git', 'init', '-q'], cwd=self.root, env=env, check=True)
        subprocess.run(['git', 'add', '.'], cwd=self.root, env=env, check=True)
        driver = self.root / 'formatter.sh'
        driver.write_text('bash scripts/gate-rust-format.sh\nrc=$?\n' + shlex.join([
            sys.executable, '-c', 'import os;print("FOLLOWING_WRITER_BLOCKING",os.get_blocking(1),flush=True)',
        ]) + '\nprintf "GATE_DONE rc=%s\\n" "$rc"\nexit "$rc"\n')
        for adapter, blocking in [(HERE / 'gate-clippy.py', True),
                                  (FIXTURES / 'gate1509-footer/former-adapter.py.txt', False)]:
            result = subprocess.run([sys.executable, str(adapter), 'run', 'bash', str(driver)],
                                    cwd=self.root, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn('GATE_RUSTFMT rc=0', result.stdout)
            self.assertIn('FOLLOWING_WRITER_BLOCKING ' + str(blocking), result.stdout)

    def test_bun_boundary_preserves_a_captured_large_clippy_stream(self):
        captured = (FIXTURES / 'main-32b799-complete-clippy.txt').read_text()
        projected = ''.join(line + '\n' for line in captured.splitlines()
                            if line.startswith(('GATE_CLIPPY ', 'GATE_CLIPPY_FINDING ',
                                                'GATE_CLIPPY_ERROR ', 'GATE_CLIPPY_COMPLETE ')))
        self.assertGreater(len(projected), 65536)
        stream = self.root / 'captured-clippy.txt'
        stream.write_text(projected)
        driver = self.root / 'large-stream.sh'
        driver.write_text(shlex.join(['bun', '-e', 'process.stdout.write("actual Bun output\\n")']) + '\n' +
                          shlex.join([sys.executable, '-c',
                                      'from pathlib import Path;import sys;sys.stdout.buffer.write(Path(sys.argv[1]).read_bytes())',
                                      str(stream)]) + '\nrc=$?\nprintf "GATE_DONE rc=%s\\n" "$rc"\nexit "$rc"\n')
        result = subprocess.run([sys.executable, str(HERE / 'gate-clippy.py'), 'run', 'bash', str(driver)],
                                cwd=self.root, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(clippy.complete(result.stdout))
        self.assertEqual(clippy.findings(result.stdout), clippy.findings(captured))

    def test_bun_status_and_real_relay_write_failure_are_preserved(self):
        with clippy.payload_environment() as env:
            result = subprocess.run(['bun', '-e', 'process.stderr.write("actual Bun failure\\n");process.exit(17)'],
                                    env=env, capture_output=True)
            self.assertEqual(result.returncode, 17, result.stderr)
            self.assertEqual(result.stdout, b'')
            self.assertEqual(result.stderr, b'actual Bun failure\n')
            with open('/dev/full', 'wb', buffering=0) as full:
                failed = subprocess.run(['bun', '-e', 'process.stdout.write("unretainable output\\n")'],
                                        env=env, stdout=full, stderr=subprocess.PIPE)
            self.assertEqual(failed.returncode, 224, failed.stderr)
            self.assertIn(b'GATE_RUNNER_INFRA Bun stdio relay step failed:', failed.stderr)
            self.assertIn(b'recovery command:', failed.stderr)

    def test_bun_transport_setup_refuses_before_start_with_a_recovery_command(self):
        output = io.StringIO()
        with patch.object(clippy.tempfile, 'TemporaryDirectory', side_effect=PermissionError('private job tmp refused')):
            with patch.object(clippy.subprocess, 'Popen') as spawn, contextlib.redirect_stdout(output):
                self.assertEqual(clippy.run_payload(['bash', 'candidate with spaces.sh']), 224)
                spawn.assert_not_called()
        self.assertIn('Bun stdio isolation step failed: private job tmp refused', output.getvalue())
        self.assertIn("'candidate with spaces.sh'", output.getvalue())
        self.assertIn('recovery command:', output.getvalue())


if __name__ == '__main__':
    unittest.main(verbosity=2)
