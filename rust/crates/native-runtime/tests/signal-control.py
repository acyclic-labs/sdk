"""Check production signal/wake/owner models against actual source defects.

Run in a disposable checkout: python3 rust/crates/native-runtime/tests/signal-control.py /tmp/native-signal-control
Select a retained control with --wake, --owners, --host, --host-events, --ownership, --claim, --descriptor, --owner-envelope, --durability, --durability-io, --native-terminal or --native-capacity.
An optional comma-separated mutation selection shares one restored positive run.
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys


def main():
    if len(sys.argv) not in (2, 3, 4) or (len(sys.argv) >= 3 and sys.argv[2] not in ("--wake", "--owners", "--host", "--host-events", "--ownership", "--claim", "--descriptor", "--owner-envelope", "--durability", "--durability-io", "--native-terminal", "--native-capacity")):
        raise RuntimeError("arguments: expected evidence directory, optional control and mutation; recovery: pass /tmp/native-signal-control [--wake|--owners|--host|--host-events|--ownership|--claim|--descriptor|--owner-envelope|--durability|--durability-io|--native-terminal|--native-capacity [mutation]]")
    mode = sys.argv[2] if len(sys.argv) >= 3 else "--signal"
    root = Path(__file__).resolve().parents[4]
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    signal = root / "rust/crates/native-runtime/src/process_tree/events/signal.rs"
    outcome = signal.with_name("outcome.rs")
    wake = root / "rust/crates/native-runtime/src/wake.rs"
    owners = root / "rust/crates/native-runtime/src/health/owners.rs"
    host = root / "rust/crates/native-runtime/src/testing/host.rs"
    ownership = root / "rust/crates/native-runtime/src/ownership/mod.rs"
    claim = root / "rust/crates/native-runtime/src/claim/protocol.rs"
    descriptor = root / "rust/crates/native-runtime/src/descriptor/cache.rs"
    envelope = root / "rust/crates/native-runtime/src/owner_record/mod.rs"
    durability = root / "rust/crates/native-runtime/src/testing/durability.rs"
    blocking = root / "rust/crates/native-runtime/src/blocking/mod.rs"
    completion = root / "rust/crates/native-runtime/src/completion/state.rs"
    admission = root / "rust/crates/native-runtime/src/workers/admission.rs"
    target, sources, selection = {
        "--signal": ("process_signal_loom.rs", (signal, outcome), "--native-signal"),
        "--wake": ("wake_loom.rs", (wake,), "--native-wake"),
        "--owners": ("owner_count_loom.rs", (owners, owners.parent / "owners/model.rs"), "--native-owner"),
        "--host": ("../src/testing/host/models.rs", (host, host.parent / "host/pool.rs"), "--native-host"),
        "--host-events": ("../src/testing/host/tests.rs", (host,), "--native-host-events"),
        "--ownership": ("../src/ownership/model.rs", (ownership,), "--ownership"),
        "--claim": ("../src/claim/model.rs", (claim,), "--native-claim"),
        "--descriptor": ("../src/descriptor/model.rs", (descriptor,), "--native-descriptor"),
        "--owner-envelope": ("../src/owner_record/tests.rs", (envelope,), "--native-owner-envelope"),
        "--durability": ("../src/testing/durability/models.rs", (durability,), "--native-durability"),
        "--durability-io": ("../src/testing/durability/tests.rs", (durability,), "--native-durability-io"),
        "--native-terminal": ("../src/testing/queue_model.rs", (blocking, completion), "--native-terminal"),
        "--native-capacity": ("../src/testing/queue_model.rs", (admission,), "--native-capacity"),
    }[mode]
    oracle = root / "rust/crates/native-runtime/tests" / target
    qualifier = root / "scripts/qualify-mcp-loom.mjs"
    originals = {path: path.read_bytes() for path in (*sources, oracle, qualifier)}
    digest = lambda data: hashlib.sha256(data).hexdigest()
    command = ["node", "scripts/qualify-mcp-loom.mjs", selection]
    report = {"inputs": {str(path.relative_to(root)): digest(data) for path, data in originals.items()}, "steps": []}

    def run(label, failure=None, test=None):
        selected = command
        if test:
            # Reuse the one qualification builder and its original environment;
            # select the unchanged oracle that decides this specific defect.
            selected = ["node", "--input-type=module", "-e", """
import { spawnSync } from 'node:child_process';
import { qualificationMain } from './scripts/qualify-mcp-loom.mjs';
qualificationMain([process.argv[1]], (command, args, options) => spawnSync(command,
  command === 'cargo' ? args : [process.argv[2], '--exact', '--nocapture'], options));
""", "--", selection, test if "::" in test else oracle.parent.name + "::model::" + test]
        result = subprocess.run(selected, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log = output / (label + ".log")
        log.write_bytes(result.stdout)
        text = result.stdout.decode(errors="replace")
        report["steps"].append({"step": label, "argv": selected, "exit": result.returncode, "log_sha256": digest(result.stdout)})
        print(f"{label}: exit={result.returncode}; log={log}", flush=True)
        # Keep the original compiler/model diagnostic in the caller's transcript
        # even when this disposable checkout's evidence directory is removed.
        sys.stdout.buffer.write(result.stdout)
        sys.stdout.buffer.flush()
        valid = (result.returncode != 0 and "panicked at" in text and failure in text) if failure else result.returncode == 0
        if failure and mode == "--host":
            valid = valid and "thread 'testing::host::models::physical_occupants_outside_the_schedule_cannot_block_the_driver' " in text
        if not valid:
            raise RuntimeError(f"{label}: semantic oracle not satisfied; recovery: inspect {shlex.quote(str(log))}, correct source or command, then {shlex.join(command)}")

    mutations = [
        ("first-terminal", signal, b"if state.outcome.is_some() {", b"if false && state.outcome.is_some() {", "first terminal event changed"),
        ("registration", signal, b"state.waker = Some(context.waker().clone());", b"state.waker = None;", "deadlock; threads ="),
        ("failure-cause", outcome, b"Self::ServiceFailed(error) => Err(io::Error::new(error.kind(), error)),", b'Self::ServiceFailed(error) => Err(io::Error::new(error.kind(), "replacement cause")),', "service failure cause changed"),
    ]
    if mode == "--wake":
        mutations = [
            ("retained-token", wake, b"PoisonError::into_inner) = true;", b"PoisonError::into_inner) = false;", "deadlock; threads ="),
            ("notification", wake, b"self.changed.notify_one();", b"if false { self.changed.notify_one(); }", "deadlock; threads ="),
            ("consumption", wake, b"*notified = false;", b"*notified = true;", "wake returned without its producer"),
        ]
    elif mode == "--owners":
        mutations = [
            ("separate-election", owners,
             b"self.0.fetch_sub(1, Ordering::AcqRel) == 1",
             b"let last = self.0.load(Ordering::Acquire) == 1; self.0.fetch_sub(1, Ordering::AcqRel); last",
             "last owners elected:"),
        ]
    elif mode == "--host":
        mutations = [
            ("queued-callback-wake", host,
             b"let parked_driver = state.take_start_waiter();",
             b"let parked_driver = { state.waiting_start = false; None::<Waker> };",
             "deadlock; threads ="),
        ]
    elif mode == "--host-events":
        mutations = [
            ("invented-callback", host,
             b"let parked_driver = state.take_start_waiter();",
             b"state.ready = true; let parked_driver = state.take_start_waiter();",
             "parking invented callback readiness"),
        ]
    elif mode == "--ownership":
        mutations = [
            ("cleanup-order", ownership,
             b"    _owner: Owner,\n    fences: crate::sync::Mutex<Vec<Fence>>,",
             b"    fences: crate::sync::Mutex<Vec<Fence>>,\n    _owner: Owner,",
             "cleanup deleted a reopened store", "cleanup_precedes_reopen_under_every_interleaving"),
            ("fence-retention", ownership,
             b"            .push(fence);", b"            .clear();\n        drop(fence);",
             "fence released before shared cleanup", "concurrent_fence_registration_survives_last_store_release"),
            ("failed-initialization", ownership,
             b"if result.is_ok() {", b"if true {",
             "failed initialization retained its reopen fence", "failed_initialization_releases_its_fence_after_physical_work"),
            ("release-order", ownership,
             b"struct SharedOwner<Owner, Fence> {\n", b"struct SharedOwner<Owner, Fence> {\n    release: ReleaseSignal,\n",
             "release preceded physical owner or retained-fence cleanup", "release_waits_for_the_actual_owner_and_retained_fence"),
            ("async-release-wake", ownership,
             b"self.0.asynchronous.notify_waiters();", b"// omitted original asynchronous notification",
             "deadlock; threads =", "release_waits_for_the_actual_owner_and_retained_fence"),
        ]
    elif mode == "--claim":
        mutations = [
            ("unreadable-record", claim,
             b"Contents::decode(&host.read(&file)?);",
             b"Contents::decode(&host.read(&file).unwrap_or_default());",
             "two live owners", "an_unreadable_claim_file_refuses"),
            ("unknown-owner", claim,
             b'Liveness::Unknown => Some(contention("store claimed by a process of unknown liveness")),',
             b"Liveness::Unknown => None,",
             "two live owners", "an_owner_of_unknown_liveness_refuses"),
        ]
    elif mode == "--descriptor":
        mutations = [
            ("replaced-file", descriptor,
             b"if self.cache.files.identity(&file)? != identity {",
             b"if self.cache.files.identity(&file)? != identity && false {",
             "another file under the cached name was used", "a_file_replaced_while_closed_is_refused"),
            ("close-error", descriptor,
             b".get_or_insert(error.kind());",
             b".take().unwrap_or(error.kind());",
             "a write lost as its descriptor closed was never reported", "a_flush_that_fails_as_a_descriptor_closes_fails_the_next_use"),
        ]
    elif mode == "--owner-envelope":
        mutations = [
            ("integrity-bypass", envelope,
             b"if magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes())) {",
             b"if false && (magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes()))) {",
             "corrupt record decoded"),
        ]
    elif mode == "--durability":
        mutations = [
            ("crash-retention", durability,
             b"writes.failed = fault\n            .as_ref()\n            .filter(|fault| fault.fault != DurableFault::Interrupted)\n            .cloned();",
             b"writes.failed = None;",
             "durable write admitted after crash"),
        ]
    elif mode == "--durability-io":
        mutations = [
            ("physical-write", durability,
             b"        write(offered)", b"        Ok(offered.len())",
             "assertion `left == right` failed"),
        ]
    elif mode == "--native-terminal":
        mutations = [
            ("worker-failure-cause", blocking,
             b".result = Some(result);",
             b".result = Some(result.map_err(|error| io::Error::new(error.kind(), error.to_string())));",
             "original worker failure cause replaced"),
            ("terminal-accounting", completion,
             b"        state.task_pending = false;", b"        state.task_pending = true;",
             "terminal accounting lost"),
            ("terminal-wake", completion,
             b"        let waker = state.waker.take();", b"        let waker = None;",
             "terminal wake lost"),
        ]
    elif mode == "--native-capacity":
        mutations = [
            ("queue-accounting", admission,
             b"admission.queued_jobs = admission.queued_jobs.saturating_sub(1);",
             b"admission.queued_jobs = admission.queued_jobs.saturating_sub(0);",
             "queue accounting lost", "testing::queue_model::capacity_release_races_registration_on_the_real_queue"),
            ("capacity-wake", admission,
             b"admission.notifying = true;\n        admission.notify_next = admission.head.clone();",
             b"admission.notifying = true;\n        admission.notify_next = None;",
             "capacity wake lost", "testing::queue_model::capacity_release_races_registration_on_the_real_queue"),
            ("capacity-broadcast", admission,
             b"admission.notify_next = state.next.clone();",
             b"admission.notify_next = None;",
             "capacity broadcast lost a live waiter", "testing::queue_model::release_broadcast_reaches_every_waiter_across_concurrent_cancellation"),
        ]
    if len(sys.argv) == 4:
        selected = set(sys.argv[3].split(","))
        if not selected <= {mutation[0] for mutation in mutations}:
            raise RuntimeError("selection: unknown mutation; recovery: select a retained mutation name from this controller")
        mutations = [mutation for mutation in mutations if mutation[0] in selected]
    try:
        for mutation in mutations:
            label, path, before, after, failure, *test = mutation
            original = originals[path]
            if original.count(before) != 1:
                raise RuntimeError(f"{label}: expected exactly one production match; recovery: inspect {path} and update the exact mutation")
            mutant = original.replace(before, after)
            if mode == "--ownership" and label == "release-order":
                # Move the actual field; do not create a second signal.
                tail = b"    // Last: signal after the owner's destructor and every retained fence.\n    release: ReleaseSignal,\n"
                if mutant.count(tail) != 1:
                    raise RuntimeError("release-order: field anchor changed; recovery: inspect SharedOwner before updating the retained mutation")
                mutant = mutant.replace(tail, b"")
            (output / (label + "-mutant.rs")).write_bytes(mutant)
            report["steps"].append({"mutation": label, "path": str(path.relative_to(root)), "sha256": digest(mutant)})
            try:
                path.write_bytes(mutant)
                run(label, failure, test[0] if test else None)
            finally:
                path.write_bytes(original)
        run("positive")
    finally:
        report["inputs_unchanged"] = all(path.read_bytes() == data for path, data in originals.items())
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not report["inputs_unchanged"]:
        raise RuntimeError("restore: source/oracle mismatch; recovery: restore original checkout bytes before further validation")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-signal-control: {error}; recovery: inspect retained evidence and rerun with a new evidence directory after correcting the cause", file=sys.stderr)
        sys.exit(1)
