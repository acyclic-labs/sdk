"""Exercise proof drift guards using actual admitted/source/git artifacts."""
import copy
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location('proof_coverage', ROOT / 'scripts/check-proof-coverage.py')
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
MANIFEST = json.loads((ROOT / POLICY.MANIFEST).read_bytes())
SOURCE = 'rust/crates/stream/src/conformance/wave1/coverage.rs'
CAPTURES = ROOT / 'scripts/tests/fixtures/proof-policy/entrypoint-discovery-v1'


class ProofCoverage(unittest.TestCase):
    def test_contract_proof_discovery_uses_the_production_formatter(self):
        source = 'rust/crates/filesystem/proofs/lease-path/lib.rs'
        proofs = POLICY.scan_file(source, ROOT)['proofs']
        names = [row['name'] for row in proofs if row['kind'] == 'kani']
        self.assertIn('every_identity_fits_its_fixed_hex_component', names)
        self.assertEqual(len(names), len(set(names)))

    def test_real_reporting_source_has_eleven_receipt_backed_entries(self):
        result = POLICY.check_review(ROOT, MANIFEST, [SOURCE])
        self.assertEqual((result['changed_rust_files'], result['entries']), (1, 11))

    def test_missing_file_proof_entry_is_refused(self):
        with self.assertRaisesRegex(ValueError, 'missing proof entries'):
            POLICY.check_review(ROOT, dict(MANIFEST, reviewed_files={}), [SOURCE])

    def test_omitted_duplicate_and_retyped_entries_are_refused(self):
        for label in ('omit', 'duplicate', 'retype'):
            with self.subTest(mutation=label):
                manifest = copy.deepcopy(MANIFEST)
                entries = manifest['reviewed_files'][SOURCE]['entries']
                if label == 'omit':
                    entries.pop()
                elif label == 'duplicate':
                    entries.append(copy.deepcopy(entries[-1]))
                else:
                    entries[-1]['kind'] = 'file_review'
                with self.assertRaisesRegex(ValueError, 'proof entry multiset differs'):
                    POLICY.check_review(ROOT, manifest, [SOURCE])

    def test_empty_invariant_is_refused(self):
        manifest = copy.deepcopy(MANIFEST)
        manifest['reviewed_files'][SOURCE]['entries'][0]['invariants'] = []
        with self.assertRaisesRegex(ValueError, 'entry lacks invariant'):
            POLICY.check_review(ROOT, manifest, [SOURCE])

    def test_stale_file_digest_is_refused(self):
        manifest = copy.deepcopy(MANIFEST)
        manifest['reviewed_files'][SOURCE]['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'reviewed file digest'):
            POLICY.check_review(ROOT, manifest, [SOURCE])

    def test_unrelated_invariant_cannot_borrow_reporting_proof(self):
        manifest = copy.deepcopy(MANIFEST)
        manifest['reviewed_files'][SOURCE]['entries'][0]['invariants'] = ['actors.checkpoint-alarm']
        with self.assertRaisesRegex(ValueError, 'receipt does not prove the registered invariant'):
            POLICY.check_review(ROOT, manifest, [SOURCE])

    def test_missing_or_unsupported_receipt_is_refused(self):
        for label in ('missing', 'unsupported'):
            with self.subTest(mutation=label):
                manifest = copy.deepcopy(MANIFEST)
                if label == 'missing':
                    manifest['proofs'] = {}
                else:
                    manifest['proofs']['hst-reporting-v1']['adapter'] = 'unimplemented'
                with self.assertRaisesRegex(ValueError, 'missing proof receipt|unsupported proof adapter'):
                    POLICY.check_review(ROOT, manifest, [SOURCE])

    def test_actual_captured_unsafe_sites_and_wildcard_are_discovered(self):
        sources = json.loads((CAPTURES / 'sources.json').read_bytes())['sources']
        for source in sources:
            self.assertEqual(POLICY.digest(CAPTURES / source['path']), source['sha256'])
        unsafe = POLICY.candidates(CAPTURES, sources[0]['path'])
        wildcard = POLICY.candidates(CAPTURES, sources[1]['path'])
        self.assertEqual([(kind, line) for kind, line, _ in unsafe if kind == 'unsafe'], [('unsafe', 18), ('unsafe', 21)])
        self.assertIn(('catch_all_arm', 374, 'observe_body'), wildcard)

    def test_dependency_input_cannot_borrow_reporting_proof(self):
        source = 'rust/crates/native-runtime/src/held_sync.rs'
        manifest = copy.deepcopy(MANIFEST)
        manifest['reviewed_files'][source] = dict(sha256=POLICY.digest(ROOT / source), entries=[dict(kind=kind, line=line, name=name, proof='hst-reporting-v1', invariants=[POLICY.HST_POLICY_INVARIANT], safety_argument='Captured safety comment requires its own proof') for kind, line, name in POLICY.candidates(ROOT, source)])
        with self.assertRaisesRegex(ValueError, 'receipt did not exercise the reviewed source'):
            POLICY.check_review(ROOT, manifest, [source])

    def test_comments_and_raw_strings_do_not_create_entries(self):
        data = (ROOT / SOURCE).read_text()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / SOURCE
            target.parent.mkdir(parents=True)
            # Metamorphic language fixture derived from real source; no provider
            # or external-tool output is fabricated.
            target.write_text('/*\n' + data.replace('*/', '* /') + '\n*/\n' + 'const TEXT: &str = r########"' + data + '"########;\n')
            self.assertEqual(POLICY.candidates(root, SOURCE), [('file_review', 1, '')])

    def git_repo(self, root):
        for args in [('init', '-q'), ('config', 'user.email', 'verify-test@example.invalid'), ('config', 'user.name', 'Verify test'), ('config', 'commit.gpgsign', 'false')]:
            POLICY.git(root, *args)
        inventory = root / POLICY.MANIFEST
        inventory.parent.mkdir(parents=True)
        inventory.write_text(json.dumps(dict(schema_version=1, reviewed_files={}, proofs={})))
        POLICY.git(root, 'add', '.')
        POLICY.git(root, 'commit', '-qm', 'real temporary baseline')
        return POLICY.git(root, 'rev-parse', 'HEAD').decode().strip()

    def test_real_git_new_source_requires_proof_entry_and_dirty_tree_refuses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = self.git_repo(root)
            self.assertEqual(POLICY.check(root, base)['changed_rust_files'], 0)
            target = root / SOURCE
            target.parent.mkdir(parents=True)
            shutil.copyfile(ROOT / SOURCE, target)
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'actual source added without proof registration')
            with self.assertRaisesRegex(ValueError, 'missing proof entries'):
                POLICY.check(root, base)
            target.write_bytes(target.read_bytes() + b'\n')
            with self.assertRaisesRegex(ValueError, 'tracked worktree differs'):
                POLICY.check(root, base)

    def test_real_git_rename_requires_new_entry_and_deletion_needs_none(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.git_repo(root)
            old = root / SOURCE
            old.parent.mkdir(parents=True)
            shutil.copyfile(ROOT / SOURCE, old)
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'actual source baseline')
            base = POLICY.git(root, 'rev-parse', 'HEAD').decode().strip()
            new = SOURCE.replace('coverage.rs', 'renamed.rs')
            POLICY.git(root, 'mv', SOURCE, new)
            POLICY.git(root, 'commit', '-qm', 'real source rename')
            with self.assertRaisesRegex(ValueError, 'missing proof entries'):
                POLICY.check(root, base)
            POLICY.git(root, 'rm', new)
            POLICY.git(root, 'commit', '-qm', 'source deletion')
            self.assertEqual(POLICY.check(root, base)['changed_rust_files'], 0)

    def test_real_git_source_type_change_cannot_bypass_regular_file_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.git_repo(root)
            target = root / SOURCE
            target.parent.mkdir(parents=True)
            shutil.copyfile(ROOT / SOURCE, target)
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'real regular source baseline')
            base = POLICY.git(root, 'rev-parse', 'HEAD').decode().strip()
            target.unlink()
            target.symlink_to(ROOT / SOURCE)
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'real source replaced by symbolic link')
            self.assertEqual(POLICY.git(root, 'diff', '--name-status', base, 'HEAD').decode().split(), ['T', SOURCE])
            with self.assertRaisesRegex(ValueError, 'Rust source must be a regular file: ' + SOURCE):
                POLICY.check(root, base)

    def test_real_manifest_feature_change_cannot_pass_with_unchanged_rust(self):
        capture = ROOT / 'scripts/tests/fixtures/proof-policy/canonical-exports-v1/source/rust/crates/demo'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.git_repo(root)
            package = root / 'rust/crates/demo'
            shutil.copytree(capture, package)
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'real captured feature-dependent API baseline')
            base = POLICY.git(root, 'rev-parse', 'HEAD').decode().strip()
            self.assertEqual(POLICY.check(root, base)['changed_rust_files'], 0)
            manifest = package / 'Cargo.toml'
            original = manifest.read_bytes()
            self.assertEqual(original.count(b'default = ["base"]'), 1)
            # This is an adversarial source change to the actual captured
            # manifest, not invented Cargo/compiler output.
            manifest.write_bytes(original.replace(b'default = ["base"]', b'default = ["base", "extra"]'))
            POLICY.git(root, 'add', '.')
            POLICY.git(root, 'commit', '-qm', 'enable captured extra API without changing Rust')
            self.assertEqual(POLICY.git(root, 'diff', '--name-only', base, 'HEAD', '--', '*.rs'), b'')
            with self.assertRaisesRegex(ValueError, 'export profile inputs require canonical generated-export proof: rust/crates/demo/Cargo.toml'):
                POLICY.check(root, base)

    def test_real_manifest_deletion_or_rename_requires_export_evidence(self):
        capture = ROOT / 'scripts/tests/fixtures/proof-policy/canonical-exports-v1/source/Cargo.toml'
        for operation in ('delete', 'rename'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.git_repo(root)
                shutil.copyfile(capture, root / 'Cargo.toml')
                POLICY.git(root, 'add', '.')
                POLICY.git(root, 'commit', '-qm', 'actual captured workspace manifest')
                base = POLICY.git(root, 'rev-parse', 'HEAD').decode().strip()
                if operation == 'delete':
                    POLICY.git(root, 'rm', 'Cargo.toml')
                else:
                    # A rename-only diff can hide the removed manifest's name.
                    POLICY.git(root, 'mv', 'Cargo.toml', 'Cargo.saved')
                POLICY.git(root, 'commit', '-qm', 'remove an effective export input')
                with self.assertRaisesRegex(ValueError, 'export profile inputs require canonical generated-export proof: Cargo.toml'):
                    POLICY.check(root, base)

    def test_cli_missing_base_is_typed(self):
        result = subprocess.run([sys.executable, str(ROOT / 'scripts/check-proof-coverage.py')], capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertIn('step=read_arguments', result.stderr)
        self.assertIn('recovery: python scripts/check-proof-coverage.py --help', result.stderr)


if __name__ == '__main__':
    try:
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(ProofCoverage))
        if not result.wasSuccessful():
            raise ValueError('proof coverage assertions failed; inspect named cases above')
    except (OSError, ValueError, ImportError, subprocess.SubprocessError) as error:
        print(f'VERIFY_COVERAGE_TEST_FAILURE step=test_proof_coverage error={error}; recovery: python scripts/test-proof-coverage.py', file=sys.stderr)
        sys.exit(1)
