"""Check the real native cleanup oracle against late status publication."""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import sys


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("arguments: expected evidence directory; recovery: python3 rust/crates/native-runtime/tests/exit_status_control.py /tmp/native-exit-status")
    root = Path(__file__).resolve().parents[4]
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = root / "rust/crates/native-runtime/src/process_tree/unix.rs"
    original = source.read_bytes()
    publication = b"                    *retained = observed;\n"
    fence = b"        recorded?;\n        match waited {"
    if original.count(publication) != 1 or original.count(fence) != 1:
        raise RuntimeError("plant: expected one terminal publication and recording fence; recovery: inspect process_tree/unix.rs and correct the exact source mutation")
    mutant = original.replace(publication, b"").replace(
        fence,
        b"        recorded?;\n        if let Ok(status) = waited { *retained = status; }\n        match waited {",
    )
    command = ["cargo", "test", "--locked", "-p", "acyclic-native-runtime", "--all-features", "--lib", "process_tree::tests::cleanup_recording_failure_retains_the_observed_terminal", "--", "--exact", "--nocapture"]
    oracle = root / "rust/crates/native-runtime/src/process_tree/tests.rs"
    oracle_hash = hashlib.sha256(oracle.read_bytes()).hexdigest()
    report = {"source_sha256": hashlib.sha256(original).hexdigest(), "mutant_sha256": hashlib.sha256(mutant).hexdigest(), "oracle_sha256": oracle_hash, "commands": []}
    try:
        for step, contents in [("late-publication-negative", mutant), ("original-positive", original)]:
            source.write_bytes(contents)
            result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            log = output / (step + ".log")
            log.write_text(result.stdout)
            print(result.stdout, flush=True)
            report["commands"].append({"step": step, "argv": command, "exit": result.returncode, "log": str(log)})
            valid = (
                result.returncode != 0
                and "an admitted wait retains its original status before recording failure" in result.stdout
                and "panicked at" in result.stdout
            ) if step.endswith("negative") else result.returncode == 0 and "1 passed;" in result.stdout
            if not valid:
                raise RuntimeError(f"{step}: unchanged semantic oracle not satisfied; recovery: inspect {shlex.quote(str(log))}, correct source/command, then {shlex.join(command)}")
    finally:
        source.write_bytes(original)
        report["source_restored"] = source.read_bytes() == original
        report["oracle_unchanged"] = hashlib.sha256(oracle.read_bytes()).hexdigest() == oracle_hash
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not report["source_restored"] or not report["oracle_unchanged"]:
        raise RuntimeError("restore: source/oracle mismatch; recovery: inspect report.json and restore the original source before further qualification")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-exit-status-control: {error}; recovery: inspect retained evidence and correct the named step before resubmission", file=sys.stderr)
        sys.exit(1)
