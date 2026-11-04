"""Qualify native codecs with their original writers and shared oracles."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
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
    if len(sys.argv) not in (3, 4) or sys.argv[1] not in ("claim", "owner-envelope", "report-port") or (len(sys.argv) == 4 and sys.argv[3] not in ("--routine", "--coverage-only")):
        raise RuntimeError("arguments: expected codec, evidence directory and optional mode; recovery: python3 rust/fuzz/native-codec-control.py <claim|owner-envelope|report-port> /tmp/native-codec-control [--routine|--coverage-only]")
    owner = sys.argv[1] == "owner-envelope"
    report_port = sys.argv[1] == "report-port"
    mode = sys.argv[3] if len(sys.argv) == 4 else "fuzz"
    routine = mode == "--routine"
    coverage_only = mode == "--coverage-only"
    toolchain = "+nightly-2026-07-01"
    target_name, subdirectory, failure, source, feature, suffixes, producer = {
        "claim": ("native_claim_record", "claim_codec", "claim checksum bypassed", "claim/record.rs", None,
                  ("/claim/record.rs",), "claim::record::Record::encode"),
        "owner-envelope": ("native_owner_record", "owner_codec", "corrupt record decoded", "owner_record/mod.rs", "owner-record",
                           ("/owner_record/mod.rs", "/owner_record/protection.rs"), "owner_record::encode_with"),
        "report-port": ("native_report_port", "report_port", "report decoder readmitted its consumed request", "obs/report_port.rs", "report-port",
                        ("/obs/report_port.rs",), "ReportRequest serde writer through original native ReportPort"),
    }[sys.argv[1]]
    features = ["--features", feature] if feature else []
    root = Path(__file__).resolve().parents[2]
    output = Path(sys.argv[2]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    corpus = output / "corpus"
    artifacts = output / "artifacts"
    artifacts.mkdir()
    codec = root / "rust/crates/native-runtime/src" / source
    oracle = root / "rust/crates/native-runtime/tests" / subdirectory / "oracle.rs"
    retained = oracle.parent / "corpus"
    retained_hashes = {path.name: digest(path.read_bytes()) for path in retained.iterdir()}
    original = codec.read_bytes()
    original_oracle = oracle.read_bytes()
    oracle_hash = digest(original_oracle)
    lock = root / "Cargo.lock"
    original_lock = lock.read_bytes()
    report = {"codec": sys.argv[1], "mode": mode.removeprefix("--"), "codec_sha256": digest(original), "oracle_sha256": oracle_hash, "commands": []}

    def run(step, command, negative=False, expected_failure=failure, echo=True, timeout_control=False):
        log = output / (step + ".log")
        with log.open("wb") as stream:
            with subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as result:
                for line in result.stdout:
                    stream.write(line)
                    stream.flush()
                    if echo:
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
            valid = result.returncode != 0 and expected_failure in text and (timeout_control or "panicked at" in text)
        else:
            valid = result.returncode == 0
        if not valid:
            raise RuntimeError(f"{step}: deciding oracle failed; recovery: inspect {shlex.quote(str(log))}, correct source or command, then run {shlex.join(command)}")
        return text

    ordinary = ["cargo", "test", "--locked", "-p", "acyclic-native-runtime", "--test", target_name, "--", "--nocapture"]
    try:
        if routine or coverage_only:
            corpus = retained
            command = ordinary
        else:
            if report_port:
                shutil.copytree(retained, corpus)
                # Derived adversarial input, not another external wire fixture.
                (corpus / "unread-delimiters").write_bytes((retained / "request").read_bytes()[-1:] * 4096)
            elif owner:
                run("capture", ["cargo", "run", "--locked", "-p", "acyclic-native-runtime", "--example", "capture_owner_record", "--", str(corpus)])
            else:
                capture = output / ("capture-native-claim.exe" if os.name == "nt" else "capture-native-claim")
                run("compile-capture", ["rustc", "--edition=2024", "rust/fuzz/examples/capture_native_claim.rs", "-o", str(capture)])
                run("capture", [str(capture), str(corpus)])
            command = ["cargo", toolchain, "fuzz", "run", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", *features, target_name, str(corpus), "--", "-runs=" + ("1000" if owner or report_port else "10000"), "-seed=1", "-max_len=" + ("4096" if report_port else "8192"), "-artifact_prefix=" + str(artifacts) + os.sep]
        report["corpus_path"] = str(corpus)
        report["corpus"] = {p.name: {"bytes": p.stat().st_size, "sha256": digest(p.read_bytes()), "producer": producer} for p in sorted(corpus.iterdir())}
        if report_port and not routine and not coverage_only:
            report["corpus"]["unread-delimiters"]["producer"] = "4096 final delimiters derived from retained ReportRequest writer"
            retirement = b"""    tokio::pin!(write, read);
    tokio::select! {
        // A decoded line consumes the port even when input remains unread.
        // Retire that input producer with the original request observer.
        observed = &mut read => observed,
        () = &mut write => read.await,
    }"""
            if original_oracle.count(retirement) != 1:
                raise RuntimeError("plant-writer-retirement: expected one original observer; recovery: inspect report_port/oracle.rs and update the exact source mutation")
            mutant = original_oracle.replace(retirement, b"    let ((), observed) = tokio::join!(write, read);\n    observed")
            report["writer_retirement_mutant_sha256"] = digest(mutant)
            (output / "writer-retirement-mutant.rs").write_bytes(mutant)
            blocked = [str(corpus / "unread-delimiters") if arg == str(corpus) else arg for arg in command]
            # libFuzzer's timeout detects the planted deadlock; it is never a
            # readiness timer or a success condition for the restored oracle.
            try:
                oracle.write_bytes(mutant)
                run("writer-retirement-negative", [*blocked, "-runs=1", "-timeout=1"], negative=True,
                    expected_failure="ERROR: libFuzzer: timeout", timeout_control=True)
            finally:
                oracle.write_bytes(original_oracle)
        if owner and not routine:
            for path in sorted(corpus.iterdir()):
                print("OWNER_CORPUS " + json.dumps({"name": path.name, **report["corpus"][path.name], "bytes_hex": path.read_bytes().hex()}), flush=True)
        if not coverage_only:
            needle = b"if magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes())) {" if owner else b"if checksum(body) != sum {"
            replacement = b"if false && (magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes()))) {" if owner else b"if false && checksum(body) != sum {"
            mutations = [("integrity", needle, replacement, failure)]
            if report_port:
                mutations = [
                    ("consumption", b"        self.request = RequestState::Finished;\n        let mut line = String::new();",
                     b"        let mut line = String::new();", failure),
                    ("identity", b"if !identity(&request.request_id) || !identity(&request.generation) {",
                     b"if false && (!identity(&request.request_id) || !identity(&request.generation)) {",
                     "report decoder admitted a noncanonical identity"),
                    ("ready", b"        self.request = RequestState::Ready;", b"",
                     "ready report port changed its original request outcome"),
                ]
            report["mutants"] = {}
            for name, needle, replacement, marker in mutations:
                if original.count(needle) != 1:
                    raise RuntimeError(f"plant-{name}: expected one original guard; recovery: inspect the selected codec and update the exact mutation")
                mutant = original.replace(needle, replacement)
                (output / (name + "-mutant.rs")).write_bytes(mutant)
                report["mutants"][name] = digest(mutant)
                codec.write_bytes(mutant)
                run(name + "-negative", command, negative=True, expected_failure=marker)
            codec.write_bytes(original)
            result = run("positive", command)
            if routine and "test result: ok. 1 passed;" not in result:
                raise RuntimeError("routine-count: expected one ordinary corpus test; recovery: inspect positive.log and restore test discovery")
        if not routine:
            if (owner or report_port) and not coverage_only:
                result = run("routine-positive", ordinary)
                if "test result: ok. 1 passed;" not in result:
                    raise RuntimeError("routine-count: expected one ordinary corpus test; recovery: inspect routine-positive.log and restore test discovery")
            target = (root / Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "native-codec-coverage" / target_name).resolve()
            report["coverage_target"] = str(target)
            if owner:
                report["coverage_corpus"] = {}
                for path in sorted(corpus.iterdir()):
                    row = {"name": path.name, "sha256": digest(path.read_bytes()), "bytes_hex": path.read_bytes().hex()}
                    report["coverage_corpus"][path.name] = row["sha256"]
                    print("OWNER_COVERAGE_INPUT " + json.dumps(row), flush=True)
            run("coverage", ["cargo", toolchain, "fuzz", "coverage", "--fuzz-dir", "rust/fuzz", "--dev", "--no-default-features", *features, "--target-dir", str(target), target_name, str(corpus)])
            binaries = list(target.rglob(target_name))
            if len(binaries) != 1:
                raise RuntimeError("coverage-binary: expected one instrumented target; recovery: inspect the coverage-build directory and correct binary selection")
            sysroot = run("sysroot", ["rustc", toolchain, "--print", "sysroot"]).strip()
            llvm = Path(sysroot) / "lib/rustlib/x86_64-unknown-linux-gnu/bin/llvm-cov"
            profile = root / "rust/fuzz/coverage" / target_name / "coverage.profdata"
            coverage = json.loads(run("coverage-export", [str(llvm), "export", str(binaries[0]), "-instr-profile=" + str(profile), "-summary-only"], echo=False))
            rows = [row for entry in coverage["data"] for row in entry["files"] if row["filename"].replace("\\", "/").endswith(suffixes)]
            if len(rows) != len(suffixes):
                raise RuntimeError("coverage-codec: expected original codec rows; recovery: inspect coverage-export.log and correct source selection")
            report["codec_coverage"] = {row["filename"]: row["summary"] for row in rows}
    finally:
        codec.write_bytes(original)
        rewritten_lock = lock.read_bytes()
        lock.write_bytes(original_lock)
        report["source_restored"] = codec.read_bytes() == original
        report["oracle_unchanged"] = digest(oracle.read_bytes()) == oracle_hash
        report["retained_corpus_unchanged"] = retained_hashes == {path.name: digest(path.read_bytes()) for path in retained.iterdir()}
        report["lock_semantics_unchanged"] = lock_contents(rewritten_lock) == lock_contents(original_lock)
        report["lock_reordered"] = rewritten_lock != original_lock
        report["lock_restored"] = lock.read_bytes() == original_lock
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    if not all(report[key] for key in ("source_restored", "oracle_unchanged", "retained_corpus_unchanged", "lock_semantics_unchanged", "lock_restored")):
        raise RuntimeError("restore: source/oracle/corpus/lock mismatch; recovery: inspect report.json and restore the original checkout before further validation")
    print("NATIVE_CODEC_REPORT " + json.dumps(report, sort_keys=True), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"native-codec-control: {error}; recovery: inspect retained logs, correct the cause, then python3 rust/fuzz/native-codec-control.py <claim|owner-envelope|report-port> /tmp/native-codec-control-corrected", file=sys.stderr)
        sys.exit(1)
