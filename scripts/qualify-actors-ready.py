#!/usr/bin/env python3
"""Qualify the actual ready queue and its retained Loom oracles."""
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


def run(arguments, expected, diagnostic=None):
    command = BASE + arguments + ["--", "--nocapture"]
    print("STEP ready queue model: " + repr(command), flush=True)
    environment = dict(os.environ, RUSTC_WORKSPACE_WRAPPER=str(
        ROOT / "scripts" / ("loom-rustc.cmd" if os.name == "nt" else "loom-rustc.sh")))
    result = subprocess.run(command, cwd=ROOT, env=environment,
                            capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(output, flush=True)
    if expected == "positive":
        valid = result.returncode == 0 and "6 passed; 0 failed" in output
    else:
        valid = (result.returncode != 0 and "0 passed; 1 failed" in output
                 and arguments[0] in output and diagnostic in output)
    if not valid:
        raise RuntimeError("ready queue model did not establish " + expected
                           + "; recovery: inspect the original command and diagnostic, "
                           "correct source or command, then python3 scripts/qualify-actors-ready.py")


def main():
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
