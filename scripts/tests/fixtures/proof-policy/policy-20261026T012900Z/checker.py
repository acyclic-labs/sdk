#!/usr/bin/env python3
"""Check evidence integrity; --require-qualified also refuses open obligations.

This is not yet the full public-entrypoint/unsafe/status-mapper CI policy. The
wave-1 inventory explicitly tracks that remaining milestone. Never infer positive
proof from a source hash, captured negative-control failure or this command's pass.
"""
import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECOVERY = "python scripts/check-wave1-inventory.py"


def require(holds, message):
    if not holds:
        raise ValueError(message)


def source_path(relative):
    path = (ROOT / relative).resolve()
    require(path.is_relative_to(ROOT), f"path escapes repository: {relative}")
    return path


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def check_reference(reference):
    path = source_path(reference["file"])
    require(sha256(path) == reference["sha256"], f"source hash changed: {reference['file']}")
    lines = path.read_text(encoding="utf-8").splitlines()
    require(1 <= reference["line"] <= len(lines), f"source line invalid: {reference}")


def check_evidence(receipt):
    log = source_path(receipt["repo_log"])
    require(sha256(log) == receipt["log_sha256"], f"captured log changed: {log}")
    output = log.read_text(encoding="utf-8")
    identity = f"BUILD_SLOT_TREE={receipt['production_commit']} {receipt['build_slot_base_tree']} {receipt['build_slot_content_digest']}"
    require(identity in output, f"missing exact source identity: {log}")
    count = receipt["test_count"]
    require(count > 0 and receipt["failed"] == count, "negative control must select nonzero tests and detect all expected cases")
    require(f"running {count} tests" in output, "wrong selected test count")
    require(f"0 passed; {count} failed; 0 ignored; 0 measured; 0 filtered out" in output, "wrong control outcomes")
    require("Error: Provider" not in output and "error[E" not in output, "build/provider failure cannot be a negative-control witness")
    runtime = re.search(r"finished in ([0-9.]+)s", output)
    require(runtime is not None and float(runtime[1]) == receipt["runtime_seconds"] < 10, "missing or out-of-budget selected test runtime")
    if receipt["log_format"] == "display":
        pattern = r'Error: "wave1 case=(\w+) step=(\w+) invariant=(\w+); recovery:'
    elif receipt["log_format"] == "debug":
        pattern = r'Error: Violation \{ case: (\w+), step: "([^"]+)", invariant: (\w+) \}'
    else:
        raise ValueError("unknown captured log format")
    witnesses = re.findall(pattern, output)
    require(len(witnesses) == count, "missing semantic violation witnesses")
    expected = receipt["witnesses"]
    require(expected and all(isinstance(witness["occurrences"], int) and witness["occurrences"] > 0 for witness in expected), "missing or invalid expected semantic witnesses")
    require(len({(witness["case"], witness["step"], witness["invariant"]) for witness in expected}) == len(expected), "duplicate expected semantic witness")
    counts = {(witness["case"], witness["step"], witness["invariant"]): witness["occurrences"] for witness in expected}
    require(Counter(witnesses) == counts, "captured semantic witnesses differ from exact expected multiset")
    for reference in receipt["executed_oracle_sources"]:
        require(sha256(source_path(reference["repo_path"])) == reference["sha256"], "executed oracle capture changed")


def check_source_review(reference):
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "source-review receipt changed")
    review = json.loads(path.read_text(encoding="utf-8"))
    require(review["schema_version"] == 1 and review["kind"] == "source_review_not_runtime_qualification", "unsupported source review")
    require(review["positive_proofs"] == review["negative_controls_detected"] == 0, "source review cannot qualify runtime or mutation proof")
    require(review["reviewed_sources"] and review["open_proofs"], "source review has no sources or open proof obligations")
    for source in review["reviewed_sources"]:
        check_reference({"file": source["repo_path"], "line": source["line"], "sha256": source["sha256"]})
    run = review["rejected_run"]
    log = source_path(run["repo_log"])
    require(sha256(log) == run["log_sha256"], "rejected-run capture changed")
    output = log.read_text(encoding="utf-8")
    identity = f"BUILD_SLOT_TREE={run['base_commit']} {run['base_tree']} {run['content_digest']}"
    require(identity in output, "rejected-run source identity absent")
    suite = run["appended_suite"]
    require(suite["selected"] == 6 and suite["passed"] == 5 and suite["failed"] == 1 and suite["ignored"] == 0, "unexpected rejected append outcomes")
    require(f"test {suite['failing_test']} ... FAILED" in output and f"Error: {suite['failure']}" in output, "missing exact rejected-case witness")
    require(f"test result: FAILED. 5 passed; 1 failed; 0 ignored; 0 measured; 1504 filtered out; finished in {suite['runtime_seconds']:.2f}s" in output, "wrong rejected-case runtime or selection")
    require(not run["mutation_executed"] and '"stage": "negative"' not in output, "rejected positive run cannot claim mutation execution")


def check(require_qualified):
    inventory = json.loads((ROOT / "docs/proofs/wave1-inventory.json").read_text(encoding="utf-8"))
    require(inventory["schema_version"] == 1 and inventory["scope"] == "approved_redesign_wave1", "unsupported inventory schema/scope")
    entries = inventory["obligations"]
    require(entries, "empty obligation inventory")
    ids = [entry["id"] for entry in entries]
    require(len(set(ids)) == len(ids), "duplicate obligation id")
    risks = {name: index for index, name in enumerate(("data_loss_or_corruption", "authority_or_security", "liveness", "resource_bounds", "ergonomics"))}
    require(entries == sorted(entries, key=lambda entry: (risks[entry["risk"]], entry["id"])), "risk order differs from contract")
    references = inventory["entrypoints"] + [inventory["runner"]]
    for entry in entries:
        require(entry["invariant"] and entry["proof_specification"] and entry["negative_controls"], f"missing contract or negative control: {entry['id']}")
        require(set(entry["modes"]) == {"memory", "local", "hosted", "wasm_browser"}, f"missing mode: {entry['id']}")
        references.extend(entry["source"])
        if "source_review" in entry:
            require(entry["source_review"] in inventory["source_reviews"], f"missing source review: {entry['id']}")
        for control in entry["negative_controls"]:
            if control["status"] == "detected":
                require(control["receipt"] in inventory["evidence"], f"missing control receipt: {entry['id']}")
    for reference in references:
        check_reference(reference)
    for receipt in inventory["evidence"].values():
        check_evidence(receipt)
    for reference in inventory.get("source_reviews", {}).values():
        check_source_review(reference)
    cleanliness = inventory["cleanliness"]
    capture = source_path(cleanliness["repo_capture"])
    require(sha256(capture) == cleanliness["capture_sha256"], "captured Clippy diagnostics changed")
    records = [json.loads(line) for line in capture.read_text(encoding="utf-8").splitlines() if line.startswith("{")]
    require(any(record.get("reason") == "build-finished" and record.get("success") for record in records), "Clippy did not complete")
    messages = [record for record in records if record.get("reason") == "compiler-message"]
    require(len(messages) == len(cleanliness["diagnostics"]) == 4, "captured baseline Clippy diagnostic count differs")
    require(not cleanliness["strict_clean_pass"] and cleanliness["new_proof_diagnostics"] == 0, "cannot relabel baseline warnings as a strict cleanliness pass")
    require(all(not any("conformance/wave1" in span["file_name"] or "wave1_keys" in span["file_name"] or "wave1-conformance" in span["file_name"] for span in record["message"]["spans"] if span.get("is_primary")) for record in messages), "new proof Clippy diagnostic present")
    summary = inventory["qualification_summary"]
    require(summary["obligations"] == len(entries), "obligation count differs")
    require(summary["negative_tests_detected"] == max(receipt["test_count"] for receipt in inventory["evidence"].values()), "negative tests counted twice across repeated runs")
    open_entries = [entry["id"] for entry in entries if any(mode["status"] != "qualified" for mode in entry["modes"].values())]
    # Passing modes will need immutable positive receipts in a later milestone.
    require(summary["positive_proofs"] == 0 and len(open_entries) == len(entries), "positive qualification receipt validation has not been implemented")
    if require_qualified:
        require(not open_entries, "unqualified obligations: " + ", ".join(open_entries))
    print(json.dumps({"status": "artifact_integrity_pass", "obligations": len(entries), "references": len(references), "negative_tests_detected": summary["negative_tests_detected"], "positive_proofs": 0, "scope": "integrity only; not full public API coverage or runtime qualification"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-qualified", action="store_true")
    arguments = parser.parse_args()
    try:
        check(arguments.require_qualified)
    except (OSError, ValueError, KeyError, TypeError) as error:
        recovery = RECOVERY + (" --require-qualified" if arguments.require_qualified else "")
        print(f"VERIFY_INVENTORY_FAILURE step=check_wave1_inventory error={error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
