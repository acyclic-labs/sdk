#!/usr/bin/env python3
"""Qualify the actual ready queue, activation and shared observation oracles."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "rust/crates/actors/src/local/ready.rs"
SUITE = "local::ready::tests::schedules::"
BASE = ["cargo", "test", "--locked", "-p", "acyclic-actors", "--features",
        "model-checking", "--lib"]
RECOVERY = "inspect original diagnostics, correct source or command, then python3 scripts/qualify-actors-ready.py"


def run(arguments, expected, diagnostic=None, *, model=True, recovery="--activation"):
    base = BASE if model else ["cargo", "test", "--locked", "--all-features",
                               "-p", "acyclic-actors", "--lib"]
    command = base + arguments + ["--"] + ([] if model else ["--exact"]) + ["--nocapture"]
    print("STEP actual Actor oracle: " + repr(command), flush=True)
    environment = dict(os.environ)
    if model:
        environment["RUSTC_WORKSPACE_WRAPPER"] = str(
            ROOT / "scripts" / ("loom-rustc.cmd" if os.name == "nt" else "loom-rustc.sh"))
    result = subprocess.run(command, cwd=ROOT, env=environment,
                            capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(output, flush=True)
    if expected == "positive":
        valid = result.returncode == 0 and f"{6 if model else 1} passed; 0 failed" in output
    else:
        valid = (result.returncode != 0 and "0 passed; 1 failed" in output
                 and arguments[0] in output and diagnostic in output)
    if not valid:
        raise RuntimeError("Actor oracle did not establish " + expected
                           + "; recovery: inspect the original command and diagnostic, "
                           "correct source or command, then python3 scripts/qualify-actors-ready.py"
                           + ("" if model else " " + recovery))


def activation():
    source = ROOT / "rust/crates/actors/src/conformance.rs"
    oracle = ROOT / "rust/crates/actors/src/local/runtime/tests.rs"
    original = source.read_bytes()
    original_oracle = oracle.read_bytes()
    anchor = b'Some(format!("r{sequence}"))'
    probe = b') -> Result<acyclic_stream::Record, StreamError> {\n    provider'
    normalized = original_oracle.replace(b"\r\n", b"\n")
    if original.count(anchor) != 1 or normalized.count(probe) != 1:
        raise RuntimeError("activation producer anchor changed; recovery: " + RECOVERY + " --activation")
    name = "local::runtime::tests::foreign_created_actors_activate_without_placement_or_scan"
    print("Activation producer sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    print("Activation oracle sha256=" + hashlib.sha256(original_oracle).hexdigest(), flush=True)
    try:
        source.write_bytes(original.replace(anchor, b'Some(format!("wrong{sequence}"))'))
        run([name], "unchanged-oracle rejection",
            "foreign-created actor published another first delivery", model=False)
        source.write_bytes(original)
        oracle.write_bytes(normalized.replace(probe, probe.replace(
            b"    provider", b"    let _ = provider.tail(path.clone()).await;\n    provider")))
        run([name], "unchanged-oracle rejection",
            "first-publication observation must not probe a head", model=False)
        oracle.write_bytes(original_oracle)
        run([name], "positive", model=False)
    finally:
        source.write_bytes(original)
        oracle.write_bytes(original_oracle)


def observation():
    source = ROOT / "rust/crates/actors/src/conformance.rs"
    original = source.read_bytes()
    normalized = original.replace(b"\r\n", b"\n")
    begin = normalized.index(b"async fn observe<P:")
    end = normalized.index(b"\nimpl<'a, T: Backend", begin)
    observer = normalized[begin:end]
    cursor = b".map_ok(acyclic_stream::records_only)"
    closed = b"None => return Err(Error::from(StreamError::Unavailable).into()),"
    if observer.count(cursor) != 1 or observer.count(closed) != 1:
        raise RuntimeError("shared observation source changed; recovery: " + RECOVERY + " --observation")
    # End the original provider cursor early; keep its real opening and records.
    truncated = observer.replace(cursor, b".map_ok(|follow| acyclic_stream::records_only(follow).take(0))")
    name = "local::sockets::tests::socket_conformance_passes_on_local_runtime"
    print("Shared observation source sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        source.write_bytes(normalized[:begin] + truncated.replace(closed, b"None => break,")
                           + normalized[end:])
        run([name], "original closed-follow misclassification",
            'Error: Violation { case: "echo"', model=False, recovery="--observation")
        source.write_bytes(normalized[:begin] + truncated + normalized[end:])
        run([name], "typed closed-follow refusal", "Error: Target(Stream(Unavailable))",
            model=False, recovery="--observation")
        source.write_bytes(original)
        run([name], "positive", model=False, recovery="--observation")
    finally:
        source.write_bytes(original)


def completion(selected=None):
    """Qualify the real dispatcher cut, its output oracle and operation cost."""
    source = ROOT / "rust/crates/actors/src/local/runtime.rs"
    original = source.read_bytes()
    normalized = original.replace(b"\r\n", b"\n")
    name = "local::runtime::tests::completion::completion_joins_discovery_delivery_and_producer_lifetime"
    custody = (b"        let completions = self\n"
               b"            .completions()\n            .receive\n            .lock()\n"
               b"            .unwrap_or_else(std::sync::PoisonError::into_inner)\n"
               b"            .take()\n            .ok_or(Error::CompletionStopped);\n")
    controls = {
        "premature-ack": (b"receive\n            .await\n            .map_err(|_| Error::CompletionStopped)?\n            .map_err(Error::CompletionFailed)",
         b"let _ = receive;\n        Ok(())",
         "completion escaped"),
        "extra-tail": (b"completion_tail = match provider.tail(self.store.root().clone()).await {",
         b"let _ = provider.tail(self.store.root().clone()).await;\n"
         b"completion_tail = match provider.tail(self.store.root().clone()).await {",
         "assertion `left == right` failed"),
        "unpolled-host": (custody + b"        self.run_owned(report, completions)\n",
         b"        async move {\n" + custody
         + b"        self.run_owned(report, completions).await\n        }\n",
         "completion producer survived its unpolled host"),
    }
    chosen = list(controls) if selected is None else list(selected)
    if len(chosen) != len(set(chosen)) or any(control not in controls for control in chosen):
        raise RuntimeError("select distinct completion controls from " + ", ".join(controls)
                           + "; recovery: " + RECOVERY + " --completion")
    print("Completion producer sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        for control in chosen:
            anchor, plant, diagnostic = controls[control]
            if normalized.count(anchor) != 1:
                raise RuntimeError("completion producer anchor changed; recovery: " + RECOVERY + " --completion")
            source.write_bytes(normalized.replace(anchor, plant))
            run([name], "unchanged-oracle rejection", diagnostic,
                model=False, recovery="--completion")
        source.write_bytes(original)
        restored_tests = [
            name,
            "local::runtime::tests::conformance_suite_passes_on_memory_runtime",
            "local::runtime::tests::conformance_suite_passes_on_durable_local_runtime",
            "local::runtime::tests::negative_control_conformance_rejects_a_host_that_loses_alarm_changes",
            "local::runtime::tests::clock::controlled_clock_drives_original_alarms_and_reports_producer_loss",
        ]
        command = ["cargo", "nextest", "run", "--locked", "-p", "acyclic-actors",
                   "--all-features", "--lib", "-E", " | ".join(f"test(={test})" for test in restored_tests),
                   "--no-capture", "--no-tests=fail"]
        print("STEP restored Actor completion controls: " + repr(command), flush=True)
        if subprocess.run(command, cwd=ROOT).returncode:
            raise RuntimeError("restored Actor completion controls failed; recovery: " + RECOVERY + " --completion")
        if selected is None:
            observation()
        command = ["cargo", "check", "--locked", "-p", "acyclic-actors", "--tests",
                   "--all-features", "--target", "x86_64-pc-windows-gnu"]
        print("STEP Windows Actor cross-check: " + repr(command), flush=True)
        if subprocess.run(command, cwd=ROOT).returncode:
            raise RuntimeError("Windows Actor cross-check failed; recovery: " + RECOVERY + " --completion")
        command = ["cargo", "check", "--locked", "-p", "acyclic-cloudflare-conformance",
                   "--target", "wasm32-unknown-unknown"]
        print("STEP hosted completion adapter cross-check: " + repr(command), flush=True)
        if subprocess.run(command, cwd=ROOT).returncode:
            raise RuntimeError("hosted completion adapter cross-check failed; recovery: " + RECOVERY + " --completion")
    finally:
        source.write_bytes(original)


def restart_completion():
    """Cross the real alarm deadline between restarted delivery and observation."""
    source = ROOT / "rust/crates/actors/src/conformance.rs"
    original = source.read_bytes()
    normalized = original.replace(b"\r\n", b"\n")
    anchor = (b'    restarted.send("emit").await?;\n'
              b'    target.complete(&restarted.actor).await?;\n')
    if normalized.count(anchor) != 1:
        raise RuntimeError("restart completion anchor changed; recovery: "
                           + RECOVERY + " --restart-completion")
    # Run the original dispatcher and SAME clock, with r2 already committed.
    # This is a valid wall-clock interleaving, not a fabricated output fixture.
    crossed = anchor + (b'    target.advance_to(target.clock().now_unix_ms()'
                        b'.saturating_add(2 * ALARM_MS)).await?;\n'
                        b'    target.complete(&restarted.actor).await?;\n')
    name = "local::runtime::tests::conformance_suite_passes_on_memory_runtime"
    print("Restart completion source sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        probe = normalized.replace(anchor, crossed)
        source.write_bytes(probe.replace(crossed, crossed
                           + b'    restarted.expect(&["r0", "r2"]).await?;\n'))
        run([name], "original deadline-crossing rejection",
            'Error: Violation { case: "restarted"', model=False,
            recovery="--restart-completion")
        source.write_bytes(probe)
        run([name], "positive", model=False, recovery="--restart-completion")
        source.write_bytes(original)
        completion(())
    finally:
        source.write_bytes(original)


def main():
    if sys.argv[1:] == ["--restart-completion"]:
        restart_completion()
        return
    if sys.argv[1:2] == ["--completion"]:
        completion(None if len(sys.argv) == 2 else sys.argv[2:])
        return
    if sys.argv[1:] == ["--observation"]:
        observation()
        return
    if sys.argv[1:] == ["--activation"]:
        activation()
        return
    original = SOURCE.read_bytes()
    source = original.replace(b"\r\n", b"\n")
    controls = {
        "queued-wake": (b"Some(Phase::Queued { .. }) | None => None,",
         b"Some(Phase::Queued { .. }) => { self.ready_count += 1; None }, None => None,",
         "wake_racing_replay_and_park_cannot_lose_ready_work", "queued wakes must coalesce"),
        "running-wake": (b"*notified = true;", b"*notified = false;",
         "wake_racing_replay_and_park_cannot_lose_ready_work",
         "wake must remain ready across parking"),
        "dispatcher": (b"index.waiter = Some(context.waker().clone());", b"index.waiter = None;",
         "dispatcher_registers_its_waiter_with_the_empty_check",
         "empty-to-wait handoff must retain append notification"),
        "generation": (b"if alarm.generation < entry.alarm.generation",
         b"if false",
         "clear_racing_due_selection_cannot_restore_the_old_generation",
         "registration.update_alarm"),
        "cancellation": (b"if let Some(entry) = index.entries.remove(&self.id) {",
         b"if let Some(entry) = index.entries.remove(&0) {",
         "cancelled_registration_cannot_wake_a_new_placement",
         "cancelled registration must leave only the current placement"),
        "due": (b"if let Some(wake) = index.notify(id) {",
         b"if let Some(wake) = None::<Waker> {",
         "due_alarm_racing_running_work_parking_retains_one_ready_entry",
         "a due alarm must survive either order of the parking handoff"),
        "earlier-alarm": (
         b"// An earlier deadline must wake the single dispatcher so it can rearm.\n        let waiter = index.waiter.take();",
         b"// An earlier deadline must wake the single dispatcher so it can rearm.\n        let waiter = None::<Waker>;",
         "earlier_alarm_racing_dispatcher_wait_retains_a_rearm_signal",
         "the sole dispatcher must observe the earlier deadline or retain its wake"),
    }
    selected = sys.argv[1:] or list(controls)
    if len(selected) != len(set(selected)) or any(name not in controls for name in selected):
        raise RuntimeError("select distinct controls from " + ", ".join(controls)
                           + "; recovery: " + RECOVERY)
    if any(source.count(anchor) != 1 for anchor, _, _, _ in controls.values()):
        raise RuntimeError("ready queue production anchor changed; recovery: inspect "
                           "Index::notify, ReadySet::poll_ready and Registration::update_alarm "
                           "before python3 scripts/qualify-actors-ready.py")
    print("Ready queue production sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        for name in selected:
            anchor, replacement, model, diagnostic = controls[name]
            SOURCE.write_bytes(source.replace(anchor, replacement))
            print("STEP actual production-source plant: " + name, flush=True)
            run([SUITE + model], "unchanged-oracle rejection", diagnostic)
        SOURCE.write_bytes(original)
        run([SUITE], "positive")
    finally:
        SOURCE.write_bytes(original)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("STEP ready queue qualification failed: " + str(error), file=sys.stderr)
        sys.exit(1)
