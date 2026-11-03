"""Check the production Signal model and require its unchanged oracle to reject defects.

Run in a disposable checkout: python3 rust/crates/native-runtime/tests/signal-control.py /tmp/native-signal-control
"""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("arguments: expected evidence directory; recovery: pass /tmp/native-signal-control")
    root = Path(__file__).resolve().parents[4]
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    signal = root / "rust/crates/native-runtime/src/process_tree/events/signal.rs"
    outcome = signal.with_name("outcome.rs")
    oracle = root / "rust/crates/native-runtime/tests/process_signal_loom.rs"
    qualifier = root / "scripts/qualify-mcp-loom.mjs"
    originals = {path: path.read_bytes() for path in (signal, outcome, oracle, qualifier)}
    digest = lambda data: hashlib.sha256(data).hexdigest()
    command = ["node", "scripts/qualify-mcp-loom.mjs", "--native-signal"]
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
    try:
        run("positive")
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
