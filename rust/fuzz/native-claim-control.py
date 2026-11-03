"""Qualify the production claim decoder and its unchanged checksum oracle."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib


def digest(data):
    return hashlib.sha256(data).hexdigest()


def lock_contents(data):
    contents = tomllib.loads(data.decode())
    contents["package"].sort(key=lambda package: (package["name"], package["version"], package.get("source", "")))
    return contents


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
    lock = root / "Cargo.lock"
    original_lock = lock.read_bytes()
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
        return text

    try:
        capture = output / ("capture-native-claim.exe" if os.name == "nt" else "capture-native-claim")
        run("compile-capture", ["rustc", "--edition=2024", "rust/fuzz/examples/capture_native_claim.rs", "-o", str(capture)])
        run("capture", [str(capture), str(corpus)])
        report["corpus"] = {p.name: {"bytes": p.stat().st_size, "sha256": digest(p.read_bytes()), "producer": "claim::record::Record::encode"} for p in sorted(corpus.iterdir())}
        toolchain = "+nightly-2026-07-01"
        command = ["cargo", toolchain, "fuzz", "run", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", "native_claim_record", str(corpus), "--", "-runs=10000", "-seed=1", "-max_len=8192", "-artifact_prefix=" + str(artifacts) + os.sep]
        run("positive", command)
        target = output / "coverage-build"
        run("coverage", ["cargo", toolchain, "fuzz", "coverage", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", "--target-dir", str(target), "native_claim_record", str(corpus)])
        binaries = list(target.rglob("native_claim_record"))
        if len(binaries) != 1:
            raise RuntimeError("coverage-binary: expected one instrumented target; recovery: inspect the coverage-build directory and correct binary selection")
        sysroot = run("sysroot", ["rustc", toolchain, "--print", "sysroot"]).strip()
        llvm = Path(sysroot) / "lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov"
        profile = root / "rust/fuzz/coverage/native_claim_record/coverage.profdata"
        coverage = json.loads(run("coverage-export", [str(llvm), "export", str(binaries[0]), "-instr-profile=" + str(profile), "-summary-only"]))
        rows = [row for entry in coverage["data"] for row in entry["files"] if row["filename"].replace("\\", "/").endswith("/claim/record.rs")]
        if len(rows) != 1:
            raise RuntimeError("coverage-codec: expected original claim codec row; recovery: inspect coverage-export.log and correct source selection")
        report["codec_coverage"] = rows[0]["summary"]
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
        rewritten_lock = lock.read_bytes()
        lock.write_bytes(original_lock)
        report["source_restored"] = codec.read_bytes() == original
        report["oracle_unchanged"] = digest(oracle.read_bytes()) == oracle_hash
        report["lock_semantics_unchanged"] = lock_contents(rewritten_lock) == lock_contents(original_lock)
        report["lock_reordered"] = rewritten_lock != original_lock
        report["lock_restored"] = lock.read_bytes() == original_lock
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not all(report[key] for key in ("source_restored", "oracle_unchanged", "lock_semantics_unchanged", "lock_restored")):
        raise RuntimeError("restore: source/oracle/lock mismatch; recovery: inspect report.json and restore the original checkout before further validation")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-claim-control: {error}; recovery: inspect retained logs, correct the cause, then python3 rust/fuzz/native-claim-control.py /tmp/native-claim-control-corrected", file=sys.stderr)
        sys.exit(1)
