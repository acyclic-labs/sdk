"""Qualify the production claim decoder and its unchanged checksum oracle."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    if len(sys.argv) != 2:
        raise RuntimeError("arguments: expected evidence directory; recovery: python3 rust/fuzz/native-claim-control.py /tmp/native-claim-control")
    root = Path(__file__).resolve().parents[2]
    output = Path(sys.argv[1]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    corpus = output / "corpus"
    artifacts = output / "artifacts"
    artifacts.mkdir()
    codec = root / "rust/crates/native-runtime/src/claim/record.rs"
    oracle = root / "rust/fuzz/native_claim_record.rs"
    original = codec.read_bytes()
    oracle_hash = digest(oracle.read_bytes())
    lock_hash = digest((root / "Cargo.lock").read_bytes())
    report = {"codec_sha256": digest(original), "oracle_sha256": oracle_hash, "commands": []}

    def run(step, command, negative=False):
        log = output / (step + ".log")
        with log.open("wb") as stream:
            with subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as result:
                for line in result.stdout:
                    stream.write(line)
                    stream.flush()
                    sys.stdout.buffer.write(line)
                    sys.stdout.buffer.flush()
                result.wait()
        text = log.read_text(errors="replace")
        report["commands"].append({"step": step, "argv": command, "exit": result.returncode, "log": log.name})
        coverage = re.findall(r"cov: (\d+) ft: (\d+)", text)
        if coverage:
            report["commands"][-1]["instrumented_edges_features"] = coverage[-1]
        print(f"{step}: exit={result.returncode}; log={log}", flush=True)
        if negative:
            valid = result.returncode != 0 and "panicked at" in text and "claim checksum bypassed" in text
        else:
            valid = result.returncode == 0
        if not valid:
            raise RuntimeError(f"{step}: deciding oracle failed; recovery: inspect {shlex.quote(str(log))}, correct source or command, then run {shlex.join(command)}")

    try:
        capture = output / ("capture-native-claim.exe" if os.name == "nt" else "capture-native-claim")
        run("compile-capture", ["rustc", "--edition=2024", "rust/fuzz/examples/capture_native_claim.rs", "-o", str(capture)])
        run("capture", [str(capture), str(corpus)])
        report["corpus"] = {p.name: {"bytes": p.stat().st_size, "sha256": digest(p.read_bytes()), "producer": "claim::record::Record::encode"} for p in sorted(corpus.iterdir())}
        command = ["cargo", "+nightly-2026-10-12", "fuzz", "run", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", "native_claim_record", str(corpus), "--", "-runs=10000", "-seed=1", "-max_len=8192", "-artifact_prefix=" + str(artifacts) + os.sep]
        run("positive", command)
        needle = b"if checksum(body) != sum {"
        if original.count(needle) != 1:
            raise RuntimeError("plant-checksum: expected one original checksum guard; recovery: inspect claim/record.rs and update the exact mutation")
        mutant = original.replace(needle, b"if false && checksum(body) != sum {")
        (output / "checksum-mutant.rs").write_bytes(mutant)
        report["mutant_sha256"] = digest(mutant)
        codec.write_bytes(mutant)
        run("checksum-negative", command, negative=True)
    finally:
        codec.write_bytes(original)
        report["source_restored"] = codec.read_bytes() == original
        report["oracle_unchanged"] = digest(oracle.read_bytes()) == oracle_hash
        report["lock_unchanged"] = digest((root / "Cargo.lock").read_bytes()) == lock_hash
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not all(report[key] for key in ("source_restored", "oracle_unchanged", "lock_unchanged")):
        raise RuntimeError("restore: source/oracle/lock mismatch; recovery: inspect report.json and restore the original checkout before further validation")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-claim-control: {error}; recovery: inspect retained logs, correct the cause, then python3 rust/fuzz/native-claim-control.py /tmp/native-claim-control-corrected", file=sys.stderr)
        sys.exit(1)
