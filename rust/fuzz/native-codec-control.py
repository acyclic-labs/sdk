"""Qualify native persisted codecs with their original writers and shared oracles."""
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
    if len(sys.argv) not in (3, 4) or sys.argv[1] not in ("claim", "owner-envelope") or (len(sys.argv) == 4 and sys.argv[3] != "--routine"):
        raise RuntimeError("arguments: expected codec, evidence directory and optional --routine; recovery: python3 rust/fuzz/native-codec-control.py <claim|owner-envelope> /tmp/native-codec-control [--routine]")
    owner = sys.argv[1] == "owner-envelope"
    routine = len(sys.argv) == 4
    target_name = "native_owner_record" if owner else "native_claim_record"
    subdirectory = "owner_codec" if owner else "claim_codec"
    failure = "corrupt record decoded" if owner else "claim checksum bypassed"
    features = ["--features", "owner-record"] if owner else []
    root = Path(__file__).resolve().parents[2]
    output = Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    corpus = output / "corpus"
    artifacts = output / "artifacts"
    artifacts.mkdir()
    codec = root / "rust/crates/native-runtime/src" / ("owner_record/mod.rs" if owner else "claim/record.rs")
    oracle = root / "rust/crates/native-runtime/tests" / subdirectory / "oracle.rs"
    original = codec.read_bytes()
    oracle_hash = digest(oracle.read_bytes())
    lock = root / "Cargo.lock"
    original_lock = lock.read_bytes()
    report = {"codec": sys.argv[1], "mode": "routine" if routine else "fuzz", "codec_sha256": digest(original), "oracle_sha256": oracle_hash, "commands": []}

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
            valid = result.returncode != 0 and "panicked at" in text and failure in text
        else:
            valid = result.returncode == 0
        if not valid:
            raise RuntimeError(f"{step}: deciding oracle failed; recovery: inspect {shlex.quote(str(log))}, correct source or command, then run {shlex.join(command)}")
        return text

    ordinary = ["cargo", "test", "--locked", "-p", "acyclic-native-runtime", "--test", target_name, "--", "--nocapture"]
    try:
        if routine:
            corpus = root / "rust/crates/native-runtime/tests" / subdirectory / "corpus"
            command = ordinary
        else:
            if owner:
                corpus = root / "rust/crates/native-runtime/tests/owner_codec/corpus"
                run("capture", ["cargo", "run", "--locked", "-p", "acyclic-native-runtime", "--example", "capture_owner_record", "--", str(corpus)])
            else:
                capture = output / ("capture-native-claim.exe" if os.name == "nt" else "capture-native-claim")
                run("compile-capture", ["rustc", "--edition=2024", "rust/fuzz/examples/capture_native_claim.rs", "-o", str(capture)])
                run("capture", [str(capture), str(corpus)])
            toolchain = "+nightly-2026-07-01"
            command = ["cargo", toolchain, "fuzz", "run", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", *features, target_name, str(corpus), "--", "-runs=" + ("1000" if owner else "10000"), "-seed=1", "-max_len=8192", "-artifact_prefix=" + str(artifacts) + os.sep]
        report["corpus"] = {p.name: {"bytes": p.stat().st_size, "sha256": digest(p.read_bytes()), "producer": "owner_record::encode_with" if owner else "claim::record::Record::encode"} for p in sorted(corpus.iterdir())}
        if owner and not routine:
            for path in sorted(corpus.iterdir()):
                print("OWNER_CORPUS " + json.dumps({"name": path.name, **report["corpus"][path.name], "bytes_hex": path.read_bytes().hex()}), flush=True)
        needle = b"if magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes())) {" if owner else b"if checksum(body) != sum {"
        replacement = b"if false && (magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes()))) {" if owner else b"if false && checksum(body) != sum {"
        if original.count(needle) != 1:
            raise RuntimeError("plant-integrity: expected one original guard; recovery: inspect the selected codec and update the exact mutation")
        mutant = original.replace(needle, replacement)
        (output / "integrity-mutant.rs").write_bytes(mutant)
        report["mutant_sha256"] = digest(mutant)
        codec.write_bytes(mutant)
        run("integrity-negative", command, negative=True)
        codec.write_bytes(original)
        result = run("positive", command)
        if routine and "test result: ok. 1 passed;" not in result:
            raise RuntimeError("routine-count: expected one ordinary corpus test; recovery: inspect positive.log and restore test discovery")
        if not routine:
            if owner:
                result = run("routine-positive", ordinary)
                if "test result: ok. 1 passed;" not in result:
                    raise RuntimeError("routine-count: expected one ordinary corpus test; recovery: inspect routine-positive.log and restore test discovery")
            target = output / "coverage-build"
            run("coverage", ["cargo", toolchain, "fuzz", "coverage", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", *features, "--target-dir", str(target), target_name, str(corpus)])
            binaries = list(target.rglob(target_name))
            if len(binaries) != 1:
                raise RuntimeError("coverage-binary: expected one instrumented target; recovery: inspect the coverage-build directory and correct binary selection")
            sysroot = run("sysroot", ["rustc", toolchain, "--print", "sysroot"]).strip()
            llvm = Path(sysroot) / "lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov"
            profile = root / "rust/fuzz/coverage" / target_name / "coverage.profdata"
            coverage = json.loads(run("coverage-export", [str(llvm), "export", str(binaries[0]), "-instr-profile=" + str(profile), "-summary-only"]))
            suffixes = ("/owner_record/mod.rs", "/owner_record/protection.rs") if owner else ("/claim/record.rs",)
            rows = [row for entry in coverage["data"] for row in entry["files"] if row["filename"].replace("\\", "/").endswith(suffixes)]
            if len(rows) != len(suffixes):
                raise RuntimeError("coverage-codec: expected original codec rows; recovery: inspect coverage-export.log and correct source selection")
            report["codec_coverage"] = {row["filename"]: row["summary"] for row in rows}
            if owner:
                report["coverage_corpus"] = {}
                for path in sorted(corpus.iterdir()):
                    row = {"name": path.name, "sha256": digest(path.read_bytes()), "bytes_hex": path.read_bytes().hex()}
                    report["coverage_corpus"][path.name] = row["sha256"]
                    print("OWNER_COVERAGE_INPUT " + json.dumps(row), flush=True)
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
    print("NATIVE_CODEC_REPORT " + json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-codec-control: {error}; recovery: inspect retained logs, correct the cause, then python3 rust/fuzz/native-codec-control.py <claim|owner-envelope> /tmp/native-codec-control-corrected", file=sys.stderr)
        sys.exit(1)
