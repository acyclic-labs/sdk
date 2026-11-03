"""Check production signal/wake/owner models against actual source defects.

Run in a disposable checkout: python3 rust/crates/native-runtime/tests/signal-control.py /tmp/native-signal-control
Select the retained wake-token, final-owner or host model with --wake, --owners or --host.
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys


def main():
    if len(sys.argv) not in (2, 3) or (len(sys.argv) == 3 and sys.argv[2] not in ("--wake", "--owners", "--host")):
        raise RuntimeError("arguments: expected evidence directory and optional model; recovery: pass /tmp/native-signal-control [--wake|--owners|--host]")
    mode = sys.argv[2] if len(sys.argv) == 3 else "--signal"
    root = Path(__file__).resolve().parents[4]
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    signal = root / "rust/crates/native-runtime/src/process_tree/events/signal.rs"
    outcome = signal.with_name("outcome.rs")
    wake = root / "rust/crates/native-runtime/src/wake.rs"
    owners = root / "rust/crates/native-runtime/src/health/owners.rs"
    host = root / "rust/crates/native-runtime/src/testing/host.rs"
    target, sources, selection = {
        "--signal": ("process_signal_loom.rs", (signal, outcome), "--native-signal"),
        "--wake": ("wake_loom.rs", (wake,), "--native-wake"),
        "--owners": ("owner_count_loom.rs", (owners, owners.parent / "owners/model.rs"), "--native-owner"),
        "--host": ("../src/testing/host/models.rs", (host, host.parent / "host/pool.rs"), "--native-host"),
    }[mode]
    oracle = root / "rust/crates/native-runtime/tests" / target
    qualifier = root / "scripts/qualify-mcp-loom.mjs"
    originals = {path: path.read_bytes() for path in (*sources, oracle, qualifier)}
    digest = lambda data: hashlib.sha256(data).hexdigest()
    command = ["node", "scripts/qualify-mcp-loom.mjs", selection]
    report = {"inputs": {str(path.relative_to(root)): digest(data) for path, data in originals.items()}, "steps": []}

    def run(label, failure=None):
        result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        log = output / (label + ".log")
        log.write_bytes(result.stdout)
        text = result.stdout.decode(errors="replace")
        report["steps"].append({"step": label, "argv": command, "exit": result.returncode, "log_sha256": digest(result.stdout)})
        print(f"{label}: exit={result.returncode}; log={log}", flush=True)
        valid = (result.returncode != 0 and "panicked at" in text and failure in text) if failure else result.returncode == 0
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
             b"let parked_driver = state.waiting_start.then(|| state.driver.clone()).flatten();",
             b"let parked_driver = None::<Waker>;",
             "deadlock; threads ="),
        ]
    try:
        for label, path, before, after, failure in mutations:
            original = originals[path]
            if original.count(before) != 1:
                raise RuntimeError(f"{label}: expected exactly one production match; recovery: inspect {path} and update the exact mutation")
            mutant = original.replace(before, after)
            (output / (label + "-mutant.rs")).write_bytes(mutant)
            report["steps"].append({"mutation": label, "path": str(path.relative_to(root)), "sha256": digest(mutant)})
            try:
                path.write_bytes(mutant)
                run(label, failure)
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
