#!/usr/bin/env python3
"""Exercise proof-credit refusals using captured real BB2 evidence.

The inputs are the committed captures and manifest. Negative controls mutate
copies of those real observations; no provider/tool-output fixture is invented.
This tests the integrity policy, not the Rust implementation or runtime proof.
"""
import copy
import importlib.util
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Imported helpers must not create artifacts before the checker loads.
sys.dont_write_bytecode = True
from proof_fixture_paths import repository_path

ROOT = Path(__file__).resolve().parents[1]


class EvidencePolicy(unittest.TestCase):
    def test_qualification_gate_refuses_the_real_open_inventory(self):
        with self.assertRaisesRegex(ValueError, "unqualified obligations:"):
            POLICY.check(True)

    def test_one_mode_cannot_claim_qualification_without_a_positive_receipt(self):
        original_read = Path.read_text
        inventory_path = ROOT / "docs/proofs/wave1-inventory.json"
        for mode in ("memory", "local", "hosted", "wasm_browser"):
            with self.subTest(mode=mode):
                manifest = copy.deepcopy(MANIFEST)
                manifest["obligations"][0]["modes"][mode]["status"] = "qualified"

                def read_text(path, *args, **kwargs):
                    if path == inventory_path:
                        return json.dumps(manifest)
                    return original_read(path, *args, **kwargs)

                with patch.object(Path, "read_text", read_text), self.assertRaisesRegex(ValueError, "positive qualification receipt validation has not been implemented"):
                    POLICY.check(False)

    def test_captured_old_source_controls_are_valid(self):
        for receipt in MANIFEST["evidence"].values():
            with self.subTest(content=receipt["build_slot_content_digest"]):
                POLICY.check_evidence(receipt)

    def test_empty_incomplete_duplicate_and_wrong_witnesses_are_refused(self):
        original = MANIFEST["evidence"]["old-stream-controls"]
        variants = {
            "empty": [],
            "incomplete": original["witnesses"][:-1],
            "duplicate": original["witnesses"] + original["witnesses"][:1],
            "wrong step": [dict(witness, step="not_the_observed_step") for witness in original["witnesses"]],
            "zero occurrences": [dict(witness, occurrences=0) for witness in original["witnesses"]],
        }
        for label, witnesses in variants.items():
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "witness"):
                POLICY.check_evidence(dict(original, witnesses=witnesses))

    def test_rehashed_log_mutations_do_not_bypass_semantic_or_selection_checks(self):
        original = MANIFEST["evidence"]["old-stream-controls"]
        variants = [
            ("running 10 tests", "running 0 tests", "selected test count", {}),
            ("invariant=SubtreeMembership", "invariant=PreCommitCut", "semantic witnesses", {}),
            ("finished in 0.15s", "finished in 10.00s", "out-of-budget", {"runtime_seconds": 10.0}),
        ]
        for before, after, reason, metadata in variants:
            with self.subTest(mutation=before), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.copy_evidence(root, original)
                target = repository_path(root, original["repo_log"])
                data = target.read_bytes()
                self.assertIn(before.encode(), data)
                target.write_bytes(data.replace(before.encode(), after.encode()))
                receipt = dict(original, log_sha256=POLICY.sha256(target), **metadata)
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, reason):
                    POLICY.check_evidence(receipt)

    def test_mismatched_executed_oracle_is_refused(self):
        receipt = copy.deepcopy(MANIFEST["evidence"]["old-stream-controls"])
        receipt["executed_oracle_sources"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "executed oracle capture"):
            POLICY.check_evidence(receipt)

    def test_real_rejected_append_run_is_source_review_only(self):
        POLICY.check_source_review(MANIFEST["source_reviews"]["fs-append-initial"])

    def test_rehashed_source_review_cannot_claim_runtime_or_mutant_credit(self):
        original = MANIFEST["source_reviews"]["fs-append-initial"]
        for field in ("positive_proofs", "negative_controls_detected"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                receipt = self.copy_review(root, original)
                review = json.loads(receipt.read_text(encoding="utf-8"))
                review[field] = 1
                receipt.write_text(json.dumps(review), encoding="utf-8")
                reference = dict(original, sha256=POLICY.sha256(receipt))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify"):
                    POLICY.check_source_review(reference)

    def test_rejected_append_run_cannot_be_relabelled_as_executed_mutant(self):
        original = MANIFEST["source_reviews"]["fs-append-initial"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = self.copy_review(root, original)
            review = json.loads(receipt.read_text(encoding="utf-8"))
            target = repository_path(root, review["rejected_run"]["repo_log"])
            data = target.read_bytes()
            self.assertEqual(data.count(b'"stage": "positive"'), 1)
            target.write_bytes(data.replace(b'"stage": "positive"', b'"stage": "negative"'))
            review["rejected_run"]["log_sha256"] = POLICY.sha256(target)
            receipt.write_text(json.dumps(review), encoding="utf-8")
            reference = dict(original, sha256=POLICY.sha256(receipt))
            with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "mutation execution"):
                POLICY.check_source_review(reference)

    @staticmethod
    def copy_file(root, relative):
        target = repository_path(root, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(repository_path(ROOT, relative), target)
        return target

    def test_owner_observations_cannot_be_relabelled_as_proofs(self):
        original = MANIFEST["owner_observations"]["updates-v1"]
        POLICY.check_observations(original)
        for field in ("positive_proofs", "negative_controls_detected"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original["repo_receipt"])
                receipt = json.loads(target.read_text())
                receipt[field] = 1
                target.write_text(json.dumps(receipt))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify redesigned"):
                    POLICY.check_observations(reference)

    def test_real_handover_preserves_snapshot_and_final_failure_scope(self):
        POLICY.check_handover(MANIFEST["handover_observations"]["updates-v2"])

    def test_real_compiled_hst_guard_and_endpoint_refuse_complete_qualification(self):
        POLICY.check_hst_reporting(MANIFEST["hst_reporting"])

    def test_historical_hst_receipt_does_not_read_current_execution_inputs(self):
        original_sha256 = POLICY.sha256
        current_inputs = {
            ROOT / name for name in (
                "rust/crates/stream/src/conformance/wave1/coverage.rs",
                "rust/crates/stream/src/conformance/wave1.rs",
                "rust/crates/stream/tests/wave1_coverage.rs",
                "rust/crates/stream/examples/wave1-conformance.rs",
                "scripts/qualify-wave1-coverage.py",
            )
        }

        def captured_sha256(path):
            self.assertNotIn(path, current_inputs, "historical validation read current execution input")
            return original_sha256(path)

        with patch.object(POLICY, "sha256", captured_sha256):
            POLICY.check_hst_reporting(MANIFEST["hst_reporting"])

    def test_current_reference_cannot_borrow_historical_dependency_hash(self):
        receipt = repository_path(ROOT, MANIFEST["hst_reporting"]["repo_receipt"])
        executed = json.loads(receipt.with_name("source.json").read_bytes())
        reference = copy.deepcopy(MANIFEST["entrypoints"][0])
        historical_hash = executed[reference["file"]]
        self.assertNotEqual(historical_hash, reference["sha256"])
        reference["sha256"] = historical_hash
        with self.assertRaisesRegex(ValueError, "source hash changed"):
            POLICY.check_reference(reference)

    def test_rehashed_hst_rows_cannot_promote_or_omit_a_history(self):
        original = MANIFEST["hst_reporting"]
        variants = (("promote", "cannot promote a missing history"), ("omit", "missing or reordered stable history"))
        for label, reason in variants:
            with self.subTest(mutation=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original["repo_receipt"])
                admission = json.loads(target.read_text())
                for capture in admission["captures"]:
                    copied = self.copy_file(root, capture["repo_path"])
                    if copied.name == "endpoint-unqualified.out":
                        rows = [json.loads(line) for line in copied.read_text().splitlines()]
                        if label == "promote":
                            rows[0]["outcome"] = "Qualified"
                        else:
                            rows.pop()
                        copied.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
                        capture.update(sha256=POLICY.sha256(copied), bytes=copied.stat().st_size)
                source = json.loads(repository_path(root, original["repo_receipt"]).with_name("source.json").read_bytes())
                for name in source:
                    if name.endswith(("conformance/wave1/coverage.rs", "conformance/wave1.rs", "tests/wave1_coverage.rs", "examples/wave1-conformance.rs", "scripts/qualify-wave1-coverage.py")):
                        self.copy_file(root, name)
                self.copy_file(root, "docs/proofs/wave1-history-plan.json")
                self.copy_file(root, "docs/proofs/evidence/hosted-history-plan-source.md")
                target.write_text(json.dumps(admission))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, reason):
                    POLICY.check_hst_reporting(reference)

    def test_rehashed_handover_cannot_promote_older_pass_or_signed_wip(self):
        original = MANIFEST["handover_observations"]["updates-v2"]
        variants = [
            ("full_obligation_qualified", True, "cannot qualify runtime"),
            ("streams.qualified_pin", "41d8b46a5e3e18786d601b125b586b5b7f413506", "cannot become a qualified pin"),
            ("streams.latest_tested_snapshot", "1a7ae0d2e73652a46ef59b030ce6b52f16b36307", "cannot be relabelled"),
            ("streams.latest_tested_outcome", "passed", "rejection cannot become a pass"),
            ("actors.state", "qualified", "sketch cannot qualify runtime"),
        ]
        for key, value, reason in variants:
            with self.subTest(field=key), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original["repo_receipt"])
                receipt = json.loads(target.read_text())
                for capture in receipt["captures"]:
                    self.copy_file(root, capture["repo_path"])
                parts = key.split(".")
                destination = receipt if len(parts) == 1 else receipt[parts[0]]
                destination[parts[-1]] = value
                target.write_text(json.dumps(receipt))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, reason):
                    POLICY.check_handover(reference)

    def test_partial_eof_control_cannot_claim_complete_a5(self):
        original = MANIFEST["partial_proofs"]["append-eof"]
        POLICY.check_append_eof(original)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.copy_file(root, original["repo_receipt"])
            receipt = json.loads(target.read_text())
            receipt["full_obligation_qualified"] = True
            target.write_text(json.dumps(receipt))
            reference = dict(original, sha256=POLICY.sha256(target))
            with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify complete A5"):
                POLICY.check_append_eof(reference)

    def test_rehashed_mac_receipt_must_still_use_linux_artifact(self):
        original = MANIFEST["owner_observations"]["updates-v1"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.copy_file(root, original["repo_receipt"])
            receipt = json.loads(target.read_text())
            for capture in receipt["captures"]:
                copied = self.copy_file(root, capture["repo_path"])
                if copied.name == "current-wasm-mac-receipt.json":
                    mac = json.loads(copied.read_text())
                    key = next(key for key in mac["artifacts"] if key.endswith(".wasm"))
                    mac["artifacts"][key] = "0" * 64
                    copied.write_text(json.dumps(mac))
                    capture.update(sha256=POLICY.sha256(copied), bytes=copied.stat().st_size)
            target.write_text(json.dumps(receipt))
            reference = dict(original, sha256=POLICY.sha256(target))
            with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "Node same-artifact identity"):
                POLICY.check_observations(reference)

    def test_real_actor_store_is_partial_with_three_semantic_failures(self):
        POLICY.check_actor_store(MANIFEST["actor_store_observations"]["actor-store-v1"])

    def test_rehashed_actor_store_cannot_promote_unsupported_scope(self):
        original = MANIFEST["actor_store_observations"]["actor-store-v1"]
        fields = ("runtime_qualified", "readiness_qualified", "keep_contract_qualified", "qualified_modes", "complete_executable_provenance", "full_obligation_qualified")
        for field in fields:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original["repo_receipt"])
                receipt = json.loads(target.read_bytes())
                for capture in receipt["captures"]:
                    self.copy_file(root, capture["repo_path"])
                receipt[field] = ["hosted"] if field == "qualified_modes" else True
                target.write_text(json.dumps(receipt))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, "ROOT", root), self.assertRaisesRegex(ValueError, "cannot qualify|missing Store executable"):
                    POLICY.check_actor_store(reference)

    def test_real_streams_checkpoint_preserves_native_and_bun_scope(self):
        POLICY.check_stream_checkpoint(MANIFEST['stream_checkpoint_observations']['streams-v3'])

    def test_rehashed_streams_checkpoint_cannot_promote_scope(self):
        original = MANIFEST['stream_checkpoint_observations']['streams-v3']
        for field in ('qualified_modes', 'current_head_qualified', 'exact_compiled_artifacts_verified', 'positive_proofs'):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                target = self.copy_file(root, original['repo_receipt'])
                receipt = json.loads(target.read_bytes())
                for capture in receipt['captures']:
                    self.copy_file(root, capture['repo_path'])
                receipt[field] = ['hosted'] if field == 'qualified_modes' else True
                target.write_text(json.dumps(receipt))
                reference = dict(original, sha256=POLICY.sha256(target))
                with patch.object(POLICY, 'ROOT', root), self.assertRaisesRegex(ValueError, 'cannot qualify a mode'):
                    POLICY.check_stream_checkpoint(reference)

    def test_rehashed_postprocessed_page_cannot_claim_production_control(self):
        original = MANIFEST['stream_checkpoint_observations']['streams-v3']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.copy_file(root, original['repo_receipt'])
            receipt = json.loads(target.read_bytes())
            for capture in receipt['captures']:
                self.copy_file(root, capture['repo_path'])
            receipt['controls']['independent_reads'] = 'production_fault'
            target.write_text(json.dumps(receipt))
            reference = dict(original, sha256=POLICY.sha256(target))
            with patch.object(POLICY, 'ROOT', root), self.assertRaisesRegex(ValueError, 'postprocessed page'):
                POLICY.check_stream_checkpoint(reference)

    def copy_evidence(self, root, receipt):
        self.copy_file(root, receipt["repo_log"])
        for source in receipt["executed_oracle_sources"]:
            self.copy_file(root, source["repo_path"])

    def copy_review(self, root, reference):
        target = self.copy_file(root, reference["repo_receipt"])
        review = json.loads(target.read_text(encoding="utf-8"))
        self.copy_file(root, review["rejected_run"]["repo_log"])
        for source in review["reviewed_sources"]:
            self.copy_file(root, source["repo_path"])
        return target


if __name__ == "__main__":
    try:
        SPEC = importlib.util.spec_from_file_location("wave1_inventory", ROOT / "scripts/check-wave1-inventory.py")
        POLICY = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(POLICY)
        MANIFEST = json.loads((ROOT / "docs/proofs/wave1-inventory.json").read_text(encoding="utf-8"))
        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(EvidencePolicy))
        if not result.wasSuccessful():
            raise ValueError("evidence-policy tests failed; inspect named cases above")
    except (OSError, ValueError, ImportError) as error:
        print(f"VERIFY_POLICY_FAILURE step=test_wave1_inventory error={error}; recovery: python scripts/test-wave1-inventory.py", file=sys.stderr)
        sys.exit(1)
