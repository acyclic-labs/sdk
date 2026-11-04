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
    name = "local::runtime::tests::conformance_suite_passes_on_memory_runtime"
    print("Shared observation source sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        source.write_bytes(normalized[:begin] + truncated.replace(closed, b"None => break,")
                           + normalized[end:])
        run([name], "original closed-follow misclassification",
            'Error: Violation { case: "ordered"', model=False, recovery="--observation")
        source.write_bytes(normalized[:begin] + truncated + normalized[end:])
        run([name], "typed closed-follow refusal", "Error: Target(Stream(Unavailable))",
            model=False, recovery="--observation")
        source.write_bytes(original)
        run([name], "positive", model=False, recovery="--observation")
    finally:
        source.write_bytes(original)


def main():
    if sys.argv[1:] == ["--observation"]:
        observation()
        return
    if sys.argv[1:] == ["--activation"]:
        activation()
        return
    original = SOURCE.read_bytes()
    source = original.replace(b"\r\n", b"\n")
    controls = {
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
