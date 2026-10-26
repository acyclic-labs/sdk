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
from proof_fixture_paths import repository_path, repository_relative

ROOT = Path(__file__).resolve().parents[1]
RECOVERY = "python scripts/check-wave1-inventory.py"


def require(holds, message):
    if not holds:
        raise ValueError(message)


def source_path(relative):
    return repository_path(ROOT, relative)


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


def check_observations(reference):
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "owner-observation receipt changed")
    receipt = json.loads(path.read_text(encoding="utf-8"))
    require(receipt["kind"] == "owner_observations_not_wave1_qualification", "unsupported owner observations")
    require(receipt["positive_proofs"] == receipt["negative_controls_detected"] == 0, "owner observations cannot qualify redesigned proofs or controls")
    captures = receipt["captures"]
    require(captures and len({capture["repo_path"] for capture in captures}) == len(captures), "empty or duplicate owner captures")
    for capture in captures:
        source = source_path(capture["repo_path"])
        require(sha256(source) == capture["sha256"] and source.stat().st_size == capture["bytes"], "owner capture hash/length changed")
        require(capture["origin"], "owner capture lacks real-output provenance")
    base = path.parent
    def load(relative):
        target = base / relative
        require(target.relative_to(ROOT).as_posix() in {repository_relative(capture["repo_path"]) for capture in captures}, "observation uses uncaptured receipt")
        return json.loads(target.read_text(encoding="utf-8"))
    observations = {item["id"]: item for item in receipt["observations"]}
    require(len(observations) == len(receipt["observations"]) == 5, "wrong owner observation selection")
    fs = observations["fs339-memory-append"]
    admission = load("fs339/admission.json")
    run = admission["checks"][0]
    require(fs["selected"] == fs["passed"] == 6 and 0 < fs["runtime_seconds"] < 10 and run["exit"] == 0, "wrong observed append selection/runtime")
    require(fs["archive_sha256"] == admission["archive_sha256"] and fs["executable_sha256"] == run["executable_sha256"] and fs["source_manifest_sha256"] == run["compiled_source_manifest_sha256"], "append observed artifact differs")
    output = (base / "fs339/append-segments.out").read_text(encoding="utf-8")
    require("running 6 tests" in output and "6 passed; 0 failed; 0 ignored;" in output, "append observation lacks actual six-case pass")
    for name, expected in admission["archive_members"].items():
        captured = base / "fs339" / name.removeprefix(".bb-fs-segments-records/")
        if captured.is_file():
            require(sha256(captured) == expected, "append archive member changed")
    client = observations["client-node-existing-wasm"]
    qualification = load("client/qualification.json")
    mac = load("client/macos/current-wasm-mac-receipt.json")
    artifacts = {name.removeprefix(".bb-client-current-wasm-runtime/"): item["sha256"] for name, item in qualification["admitted_runtime"].items()}
    require(client["same_artifacts"] == mac["artifacts"] == artifacts, "Node same-artifact identity differs")
    require(client["head"] == qualification["head"] and client["archive_sha256"] == qualification["archive_sha256"], "Node source/archive identity differs")
    require(client["linux_node_cases"] == client["macos_node_cases"] == qualification["node"]["cases"] == json.loads(mac["output"])["cases"] == 1, "Node observed selection differs")
    require(client["bun_cases"] == qualification["bun_cases"] == 8 and mac["rc"] == 0 and 0 < mac["seconds"] < 10 and all(run["rc"] == 0 and 0 < run["seconds"] < 10 for run in qualification["results"]), "WASM observed result/runtime differs")
    require((base / "client/macos/source-qualification.sha256").read_text().strip() == sha256(base / "client/qualification.json"), "macOS source qualification link differs")
    for kind, count in (("bounded", 2), ("queue", 5)):
        old = base / f"client/before/{kind}/.bb-client-{kind}-before-records"
        output = (old / "negative.log").read_text(encoding="utf-8")
        require(f" 0 pass\n {count} fail" in output and f"Ran {count} tests" in output, "old TS control selection differs")
        if kind == "bounded":
            metrics = load("client/before/bounded/.bb-client-bounded-before-records/pagination-metrics.json")
            require(sha256(base / "client/before/bounded/typescript/packages/harness/src/pagination.ts") == metrics["source_sha256"], "old PageWindow source differs from executed metrics")
            require([row["count"] for row in metrics["rows"]] == [100, 1000, 10000] and all(row["arrays"] == row["reads"] == 100 for row in metrics["rows"]), "old PageWindow copy observations differ")
        else:
            metrics = load("client/before/queue/.bb-client-queue-before-records/metrics.json")
            require([row["count"] for row in metrics["rows"]] == [1000, 10000, 100000] and all(row["capacity"] == 1 and row["pending"] == row["count"] for row in metrics["rows"]), "old queue producer observations differ")
    require(observations["rbfs-session-transport"]["status"] == "owner_reported_no_raw_receipt", "Cloud report cannot claim raw execution admission")


def check_hst_reporting(reference):
    """Validate captured execution at its historical source identity.

    Current source admission is independent: check_reference validates current
    inventory references, and the entrypoint guard binds proof credit only to
    the reporting module hash exercised by the captured oracle. Dependency
    evolution cannot rewrite or invalidate this historical receipt.
    """
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "HST reporting admission changed")
    admission = json.loads(path.read_text())
    require(admission["kind"] == "hst_reporting_policy_admission_not_complete_history_proof" and not admission["full_obligation_qualified"] and admission["positive_proofs"] == admission["complete_hst_exports"] == 0, "HST reporting cannot qualify complete histories")
    for capture in admission["captures"]:
        captured = source_path(capture["repo_path"])
        require(sha256(captured) == capture["sha256"] and captured.stat().st_size == capture["bytes"], "HST reporting capture changed")
    base = path.parent
    load = lambda name: json.loads((base / name).read_bytes())
    producer = load("receipt.json")
    source, positive, mutant = (load(name) for name in ("source.json", "positive-compiled-source.json", "mutant-compiled-source.json"))
    require(source == positive and source.keys() == mutant.keys(), "HST reporting compiled input selection differs")
    require(sha256(base / "source.json") == producer["source_sha256"] == admission["source_manifest_sha256"], "HST reporting source manifest differs")
    changed = [name for name in source if source[name] != mutant[name]]
    mutation = producer["mutation"]
    require(changed == [mutation["path"]], "HST reporting mutation changes another input")
    original = (base / "production.rs").read_bytes()
    before, after = mutation["before"].encode(), mutation["after"].encode()
    require(after == b"Ok(())" and before == b"Err(Unqualified {\n        missing: hst_coverage(),\n    })" and original.count(before) == 1 and original.replace(before, after) == (base / "mutant.rs").read_bytes(), "HST reporting control must bypass exactly the production refusal guard")
    require(sha256(base / "production.rs") == source[mutation["path"]] and sha256(base / "mutant.rs") == mutant[mutation["path"]], "HST reporting production/mutant identities differ")
    for label, expected in (("positive", 0), ("mutant", 101)):
        run, executable = load(label + ".json"), load(label + "-compile-executable.json")
        require(run["exit"] == expected and 0 < run["seconds"] < 10 and run["argv"][0] == executable["path"], "HST reporting executable/runtime differs")
        require(executable["oracle_sha256"] == producer["oracle_sha256"] == sha256(base / "oracle.rs") and executable["production_sha256"] == sha256(base / ("production.rs" if label == "positive" else "mutant.rs")), "HST reporting production/oracle executable receipt differs")
    require("3 passed; 0 failed; 0 ignored;" in (base / "positive.out").read_text() and "2 passed; 1 failed; 0 ignored;" in (base / "mutant.out").read_text() and "qualification_request_refuses_all_unexported_histories ... FAILED" in (base / "mutant.out").read_text() and "complete HST qualification must refuse unexported histories" in (base / "mutant.err").read_text(), "HST reporting control lacks exact semantic failure")
    plan = json.loads(source_path("docs/proofs/wave1-history-plan.json").read_bytes())
    require(sha256(source_path(plan["adopted_source"]["repo_path"])) == plan["adopted_source"]["sha256"], "adopted HST source changed")
    require(source[plan["availability_source"]["repo_path"]] == plan["availability_source"]["sha256"], "historical HST availability source differs")
    rows = [json.loads(line) for line in (base / "endpoint-unqualified.out").read_bytes().splitlines()]
    require([row["history"] for row in rows] == [row["id"] for row in plan["cases"]] == [f"HST-{number:02}" for number in range(1, 13)], "HST reporting missing or reordered stable history")
    require(all(row["outcome"] == "Unqualified" and row["reason"] == "CanonicalHistoryNotExported" for row in rows), "HST reporting cannot promote a missing history")
    aliases = {"AppendRetry": "append_retry", "CommitRetry": "commit_retry", "AbsentZeroTail": "absent_zero_tail", "SubtreeFork": "subtree_fork", "ComposedFork": "composed_fork"}
    require(all(row["partial_case_exports"] == [aliases[case] for case in history["partial_ordinary_cases"]] for row, history in zip(rows, plan["cases"])), "HST partial exports differ from adopted plan")
    for label in ("endpoint-unqualified", "endpoint-invalid-option"):
        run = load(label + ".json")
        require(run["exit"] == 1 and 0 < run["seconds"] < 10 and run["argv"][0] == load("runner-compile-executable.json")["path"], "HST endpoint refusal executable/runtime differs")
    require("step=require_hst_qualification" in (base / "endpoint-unqualified.err").read_text() and "step=read_environment" not in (base / "endpoint-unqualified.err").read_text(), "HST endpoint must refuse before provider setup")
    require(not (base / "endpoint-invalid-option.out").read_bytes() and "step=parse_hst_requirement" in (base / "endpoint-invalid-option.err").read_text(), "HST invalid option must fail typed before work")
    require(admission["build_slot_tree"] == load("job.json")["build_slot_tree"] and admission["build_slot_tree"] in (base / "build-slot.log").read_text(), "HST reporting exact job identity differs")
    require(producer["positive_proofs"] == producer["complete_hst_exports"] == 0 and producer["selected"] == admission["selected_rust_tests"] == 3 and admission["endpoint_unqualified_rows"] == 12 and 0 < producer["seconds"] < 570, "HST reporting scope/selection/job budget differs")


def check_handover(reference):
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "handover receipt changed")
    receipt = json.loads(path.read_text(encoding="utf-8"))
    require(receipt["kind"] == "handover_observations_not_runtime_qualification", "unsupported handover observations")
    require(not receipt["full_obligation_qualified"] and receipt["positive_proofs"] == receipt["negative_controls_detected"] == 0, "handover cannot qualify runtime proof")
    for capture in receipt["captures"]:
        captured = source_path(capture["repo_path"])
        require(sha256(captured) == capture["sha256"] and captured.stat().st_size == capture["bytes"], "handover capture changed")
    base = path.parent
    streams = receipt["streams"]
    require(streams["state"] == "signed_wip_not_qualified" and streams["qualified_pin"] is None, "signed WIP cannot become a qualified pin")
    require((base / "streams/signature.txt").read_text().startswith(streams["signed_transfer"] + " G "), "Streams transfer signature capture differs")
    require(streams["older_positive_snapshot"] != streams["latest_tested_snapshot"] and streams["signed_transfer"] not in (streams["older_positive_snapshot"], streams["latest_tested_snapshot"]), "distinct snapshot observations cannot be relabelled as final head")
    good = (base / "streams/streams-tests.log").read_text()
    require(streams["older_positive_counts"] == [375, 10, 1] and streams["older_positive_seconds"] == [9.45, 0.06, 0.24], "older Streams selection differs")
    require(all(f"{count} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in {seconds:.2f}s" in good for count, seconds in zip(streams["older_positive_counts"], streams["older_positive_seconds"])), "older Streams captured outcomes differ")
    bad = (base / "streams/streams-rust-all.log").read_text()
    require(streams["latest_tested_outcome"] == "rejected" and streams["latest_passed"] == 374 and streams["latest_failed"] == 6 and streams["latest_seconds"] == 9.77, "latest Streams rejection cannot become a pass")
    require("374 passed; 6 failed; 0 ignored;" in bad and "left: Some(Err(InvalidResponse))" in bad and "right: Some(Err(Unavailable))" in bad and bad.count("error: LimitExceeded") == 3, "latest Streams failure witnesses differ")
    loom = (base / "streams/streams-loom.log").read_text()
    require(streams["loom_cases"] == 4 and "4 passed; 0 failed;" in loom and loom.count("should panic ... ok") == 2, "Streams Loom observation differs")
    require(all("tail_record: None" in (base / "streams/source" / name).read_text() for name in ("grpc/client.rs", "wasm/remote.rs")), "Streams unfinished projection source differs; new review required")
    require(receipt["actors"]["state"] == "sketch_only_no_signed_implementation_or_runtime_receipt", "actor sketch cannot qualify runtime proof")
    require(receipt["rb_fs"]["state"] == "owner_reported_no_raw_receipt", "hosted report cannot qualify raw execution")


def check_stream_checkpoint(reference):
    path = source_path(reference['repo_receipt'])
    require(sha256(path) == reference['sha256'], 'Streams checkpoint receipt changed')
    receipt = json.loads(path.read_bytes())
    require(receipt['kind'] == 'streams_checkpoint_partial_observation', 'unsupported Streams checkpoint kind')
    require(not receipt['qualified_modes'] and not receipt['current_head_qualified'] and not receipt['exact_compiled_artifacts_verified'] and receipt['positive_proofs'] == 0, 'Streams checkpoint cannot qualify a mode, final head or executable provenance')
    require(receipt['controls'] == dict(root_only='production_fault', exclusions='production_fault', page_bytes='production_fault', independent_reads='model_postprocessing'), 'Streams postprocessed page cannot become a production negative control')
    for capture in receipt['captures']:
        captured = source_path(capture['repo_path'])
        require(sha256(captured) == capture['sha256'] and captured.stat().st_size == capture['bytes'], 'Streams checkpoint capture changed')
    base = path.parent
    ids = receipt['identities']
    require(ids == json.loads((base / 'identities.json').read_bytes()), 'Streams checkpoint identities differ from real git capture')
    require(ids['native_snapshot']['tree'] == ids['native_signed']['tree'] and ids['typescript_snapshot']['tree'] == ids['current_signed']['tree'], 'Streams snapshot is not tree-identical to its signed source')
    require(ids['native_signed']['tree'] != ids['current_signed']['tree'], 'Streams native observation cannot be relabelled as the newer head')
    for name in ('native_signed', 'current_signed'):
        require((base / (name + '-signature.txt')).read_text().startswith(ids[name]['commit'] + ' G '), 'Streams signed-source identity differs')
    native = (base / 'logs/owner-qualified-20261026T0245Z.log').read_text()
    require(native.startswith('bb2run: streams at ' + ids['native_snapshot']['commit'][:10]), 'Streams native snapshot output differs')
    require(receipt['native'] == dict(library_tests=381, library_seconds=8.98, wave1_tests=10, wave1_seconds=0.04, recovery_tests=1, recovery_seconds=0.65, loom_tests=8, loom_seconds=0.01), 'Streams native selection/runtime differs')
    for count, seconds in ((381, 8.98), (10, 0.04), (1, 0.65), (8, 0.01)):
        require(f'{count} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in {seconds:.2f}s' in native, 'Streams native result is not in captured output')
    for name in ('old_root_only_fork', 'old_fork_without_exclusions', 'independent_status_reads', 'uncharged_status_records'):
        require('test negative_control_' + name + ' - should panic ... ok' in native, 'Streams Loom control selection differs')
    ts = (base / 'logs/owner-typescript-20261026T0258Z.log').read_text()
    require(ts.startswith('bb2run: streams at ' + ids['typescript_snapshot']['commit'][:10]) and '105 pass\n 0 fail' in ts and 'Ran 105 tests across 14 files. [6.37s]' in ts, 'Streams TypeScript actual selection differs')
    require(receipt['typescript'] == dict(tests=105, files=14, seconds=6.37, runtime='Bun; not Node', generated_wasm_hash=None), 'Streams Bun result cannot qualify Node or an unidentified generated artifact')
    source = base / 'source/rust/crates/stream'
    model = (source / 'src/memory/recovery_model.rs').read_text()
    require('page.children[1].tail_record =' in model and 'block_on(provider.children_page(request)).unwrap().children[1]' in model, 'independent-page control review changed')
    require('builder.max_permutations = None;' in model and 'builder.max_duration = None;' in model and all(f'check_with_bound(Some({bound})' in model for bound in (0, 1, 2)), 'Streams bounded schedule policy changed')
    commit = (source / 'src/memory/commit.rs').read_text()
    provider = (source / 'src/memory/provider.rs').read_text()
    require('Fault::RootOnlyFork' in commit and 'Fault::IgnoreForkExclusions' in commit and 'Fault::IgnorePageRecordBytes' in provider, 'Streams production fault locations differ')


def check_actor_store(reference):
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "actor Store admission changed")
    receipt = json.loads(path.read_bytes())
    require(receipt["kind"] == "partial_actor_store_observation_not_full_a4_qualification" and not receipt["full_obligation_qualified"] and receipt["positive_proofs"] == receipt["negative_controls_detected"] == 0, "partial Store output cannot qualify full A4")
    require(not receipt["qualified_modes"] and not receipt["runtime_qualified"] and not receipt["readiness_qualified"] and not receipt["keep_contract_qualified"], "Store cannot qualify Runtime, readiness, Keep or another mode")
    require(not receipt["complete_executable_provenance"] and receipt["positive"]["executable_sha256"] is None, "missing Store executable hashes cannot become complete provenance")
    require(receipt["owner"] == "rb-actors" and receipt["retired_owner"] == "b-runtime", "actor ownership transfer differs")
    for capture in receipt["captures"]:
        captured = source_path(capture["repo_path"])
        require(sha256(captured) == capture["sha256"] and captured.stat().st_size == capture["bytes"], "actor Store capture changed")
    base = path.parent
    load = lambda name: json.loads((base / "run" / name).read_bytes())
    formatted = load("formatted.json")
    require(formatted == load("restored.json") == receipt["formatted_source"] and len(formatted) == 6 and receipt["formatted_files_match_signed_pin"] and receipt["restored_source_matches"], "actor Store restoration/source join differs")
    for name, digest in formatted.items():
        require(sha256(base / "source" / name) == digest, "actor Store formatted source differs")
    require(sha256(base / "run/sources.json") == receipt["source_manifest_sha256"] and sha256(base / "producer.sh") == load("sources.json")[".bb-a4-store.sh"], "actor Store producer/input identity differs")
    require(receipt["store_pin"] == "252c28c27fd490186f7e17e1d55b47ffe82a9e9a" and receipt["ready_pin"] == "4056b65d72ed901caef674a3ec01d93d630800fe", "actor Store and readiness pins differ")
    for pin in (receipt["store_pin"], receipt["ready_pin"]):
        require((base / ("signature-" + pin[:12] + ".txt")).read_text().startswith(pin + " G "), "actor signed source capture differs")
    positive = (base / "run/positive.log").read_text()
    tests = ["memory_atomic_history", "memory_epoch_alarm_pause_fences", "bounded_alarm_histories", "local_atomic_history", "reopen_preserves_the_complete_committed_tuple", "local_epoch_alarm_pause_fences"]
    require(receipt["positive"]["selected"] == 6 and receipt["positive"]["runtime_reported_seconds"] == 0.05 and receipt["positive"]["modes"] == ["memory", "local"] and load("positive.json")["exit"] == 0 and "6 passed; 0 failed; 0 ignored;" in positive and "finished in 0.05s" in positive and all(f"local::tests::{test} ... ok" in positive for test in tests), "actor Store positive selection/outcomes differ")
    expected = [("epoch", "epoch fencing", "memory_epoch_alarm_pause_fences"), ("alarm", "alarm generation fencing", "memory_epoch_alarm_pause_fences"), ("split", "one checkpoint/effect transaction", "memory_atomic_history")]
    require(len(receipt["observed_controls"]) == 3, "actor Store control selection differs")
    for observed, (label, assertion, test) in zip(receipt["observed_controls"], expected):
        log = (base / "run" / (label + "-negative.log")).read_text()
        require((observed["label"], observed["assertion"], observed["test"]) == (label, assertion, test) and observed["mode"] == "memory" and observed["executable_sha256"] is None and observed["mutant_source_sha256"] is None, "actor Store control scope/provenance differs")
        require(load(label + "-negative.json")["exit"] == 101 and "0 passed; 1 failed; 0 ignored;" in log and "finished in 0.00s" in log and assertion in log and f"local::tests::{test} ... FAILED" in log and "error[E" not in log, "actor Store control lacks exact semantic failure")
    source = (base / "source/rust/crates/actors/src/local.rs").read_text()
    require("AlarmChange::Keep if matches!(trigger, Trigger::Alarm { .. }) => Some(None)" in source, "old Store Keep mismatch requires a new review")


def check_append_eof(reference):
    path = source_path(reference["repo_receipt"])
    require(sha256(path) == reference["sha256"], "append EOF receipt changed")
    receipt = json.loads(path.read_text(encoding="utf-8"))
    require(receipt["kind"] == "partial_append_eof_proof_not_full_a5_qualification", "unsupported append EOF receipt")
    require(not receipt["full_obligation_qualified"] and receipt["positive_proofs"] == receipt["negative_controls_detected"] == 0, "partial EOF proof cannot qualify complete A5")
    for capture in receipt["captures"]:
        captured = source_path(capture["repo_path"])
        require(sha256(captured) == capture["sha256"] and captured.stat().st_size == capture["bytes"], "append EOF capture changed")
    base = path.parent
    oracle = receipt["oracle"]
    source = receipt["mutation"]["path"]
    for run_id in ("341", "342"):
        admission = json.loads((base / run_id / "admission.json").read_text())
        observed = receipt["runs"][run_id]
        check = admission["checks"][0]
        require(observed["archive_sha256"] == admission["archive_sha256"] and 0 < observed["seconds"] < 10, "append EOF run identity/runtime differs")
        require(all(observed[key] == check[key] for key in ("exit", "seconds", "executable_sha256", "compiled_source_manifest_sha256")), "append EOF executable receipt differs")
        for name, expected in admission["archive_members"].items():
            captured = base / run_id / name.removeprefix(".bb-fs-segments-records/")
            if captured.is_file():
                require(sha256(captured) == expected, "append EOF archived member differs")
        require(sha256(base / run_id / oracle["file"]) == oracle["sha256"], "append EOF oracle differs across production/control")
    production = (base / "341" / source).read_bytes()
    require(production == (base / "342" / source).read_bytes(), "append EOF original sources differ")
    before, after = receipt["mutation"]["before"], receipt["mutation"]["after"]
    require(before.endswith("cancellation, true)") and after == before.replace("cancellation, true)", "cancellation, false)"), "append EOF mutation selects wrong operation")
    require(production.count(before.encode()) == 1 and production.replace(before.encode(), after.encode()) == (base / "342/mutant.rs").read_bytes(), "append EOF mutation changes more than terminal observation")
    good = (base / "341/append-segments.out").read_text()
    bad = (base / "342/negative-fence.out").read_text()
    require("running 10 tests" in good and "10 passed; 0 failed; 0 ignored;" in good and f"::{oracle['test']} ... ok" in good and receipt["runs"]["341"]["exit"] == 0, "append EOF positive selection differs")
    require("running 1 test" in bad and "0 passed; 1 failed; 0 ignored;" in bad and f"::{oracle['test']} ... FAILED" in bad and "assertion failed: matches!(appender.rebase" in bad and "TransactionRebase::Conflicted" in bad and receipt["runs"]["342"]["exit"] == 101, "append EOF control lacks exact rebase-conflict witness")


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
    references.extend(inventory["hst_reporting"]["source"])
    guard = inventory["entrypoint_guard"]
    require(guard["status"] == "delta_registration_only_pending_canonical_joins_and_ci", "source drift policy cannot become full API or CI qualification")
    require(sha256(source_path(guard["manifest"])) == guard["manifest_sha256"], "entrypoint registry changed")
    references.extend(guard["source"])
    for entry in entries:
        require(entry["invariant"] and entry["proof_specification"] and entry["negative_controls"], f"missing contract or negative control: {entry['id']}")
        require(set(entry["modes"]) == {"memory", "local", "hosted", "wasm_browser"}, f"missing mode: {entry['id']}")
        require(all(mode["status"] != "qualified" for mode in entry["modes"].values()), f"positive qualification receipt validation has not been implemented: {entry['id']}")
        references.extend(entry["source"])
        if "source_review" in entry:
            require(entry["source_review"] in inventory["source_reviews"], f"missing source review: {entry['id']}")
        if "partial_proof" in entry:
            require(entry["partial_proof"] in inventory["partial_proofs"], f"missing partial proof: {entry['id']}")
        for observation in entry.get("handover_observations", []):
            require(observation in inventory["handover_observations"], f"missing handover observation: {entry['id']}")
        if entry["id"].startswith("actors."):
            require(entry["owner"] == "rb-actors" and entry.get("actor_store_observation") in inventory["actor_store_observations"], "actor entry requires current owner and scoped Store observation")
        if entry["id"].startswith("streams."):
            require(entry.get("stream_checkpoint_observation") in inventory["stream_checkpoint_observations"], "Streams entry requires its scoped checkpoint observation")
        for control in entry["negative_controls"]:
            if control["status"] == "detected":
                require(control["receipt"] in inventory["evidence"], f"missing control receipt: {entry['id']}")
    for reference in references:
        check_reference(reference)
    for receipt in inventory["evidence"].values():
        check_evidence(receipt)
    for reference in inventory.get("source_reviews", {}).values():
        check_source_review(reference)
    for reference in inventory.get("owner_observations", {}).values():
        check_observations(reference)
    for reference in inventory.get("handover_observations", {}).values():
        check_handover(reference)
    check_hst_reporting(inventory["hst_reporting"])
    for reference in inventory.get("actor_store_observations", {}).values():
        check_actor_store(reference)
    for reference in inventory.get("stream_checkpoint_observations", {}).values():
        check_stream_checkpoint(reference)
    for reference in inventory.get("partial_proofs", {}).values():
        check_append_eof(reference)
    cleanliness = inventory["cleanliness"]
    capture = source_path(cleanliness["repo_capture"])
    require(sha256(capture) == cleanliness["capture_sha256"], "captured Clippy diagnostics changed")
    records = [json.loads(line) for line in capture.read_text(encoding="utf-8").splitlines() if line.startswith("{")]
    require(any(record.get("reason") == "build-finished" and record.get("success") for record in records), "Clippy did not complete")
    messages = [record for record in records if record.get("reason") == "compiler-message"]
    require(len(messages) == len(cleanliness["diagnostics"]) == 4, "captured baseline Clippy diagnostic count differs")
    require(not cleanliness["strict_clean_pass"] and cleanliness["new_proof_diagnostics"] == 0, "cannot relabel baseline warnings as a strict cleanliness pass")
    require(all(not any("conformance/wave1" in span["file_name"] or "wave1_keys" in span["file_name"] or "wave1_coverage" in span["file_name"] or "wave1-conformance" in span["file_name"] for span in record["message"]["spans"] if span.get("is_primary")) for record in messages), "new proof Clippy diagnostic present")
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
