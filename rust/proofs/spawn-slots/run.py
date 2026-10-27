#!/usr/bin/env python3
"""Focused bb2 producer. All controls edit production bytes, never the oracle."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(__doc__)
parser.add_argument("mode", choices=["rust", "docs", "finish", "remaining", "kani", "mutants", "type-control", "check"])
parser.add_argument("--evidence", type=Path, required=True)
args = parser.parse_args()
start = time.monotonic()
report = {"mode": args.mode, "steps": [], "status": "RUNNING"}
evidence_created = False
recovery = shlex.join([sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]])


def refuse(step, detail):
    raise RuntimeError(f"step={step} detail={detail!r} recovery={recovery!r}")


def sha(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(label, command, cwd=ROOT, env=None, failure=None, passed=None):
    remaining = 500 - (time.monotonic() - start)
    if remaining < 5:
        refuse(label, "job budget exhausted; inspect completed receipts before dispatching missing steps")
    log = args.evidence / (label + ".log")
    before = time.monotonic()
    with log.open("wb") as output:
        result = subprocess.run(command, cwd=cwd, env=env, stdout=output,
                                stderr=subprocess.STDOUT, timeout=remaining)
    row = {"step": label, "argv": command, "cwd": str(cwd), "exit": result.returncode,
           "seconds": time.monotonic() - before, "log_sha256": sha(log)}
    report["steps"].append(row)
    print(json.dumps(row), flush=True)
    if failure is None:
        if result.returncode != 0:
            refuse(label, f"exit {result.returncode}; read {log} and fix the failing input before a changed-source run")
    elif result.returncode == 0 or failure not in log.read_text(errors="replace"):
        refuse(label, f"control did not fail its unchanged semantic oracle ({failure})")
    if passed is not None:
        # rustdoc reports runnable and compile-fail examples in separate
        # groups; both must contribute to the nonempty test count.
        total = sum(map(int, re.findall(r"test result: ok\. (\d+) passed;", log.read_text(errors="replace"))))
        if total != passed:
            refuse(label, f"expected exactly {passed} nonempty passing tests, observed {total}")
    return log


def build(cwd, label):
    log = run(label, ["cargo", "test", "--locked", "-p", "acyclic-harness-core",
                     "--features", "filesystem", "--lib", "--test", "agent-model",
                     "--no-run", "--message-format=json"], cwd)
    artifacts = {}
    for line in log.read_text(errors="replace").splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if item.get("reason") == "compiler-artifact" and item.get("executable"):
            exe = Path(item["executable"])
            artifacts[item["target"]["name"]] = str(exe)
            report.setdefault("executables", []).append({"step": label, "path": str(exe), "sha256": sha(exe)})
    for name in ["acyclic_harness_core", "agent-model"]:
        if name not in artifacts:
            refuse(label, "missing Cargo executable " + name)
    return artifacts


def variant(label, path, before, after, reuse=False):
    destination = Path(os.environ["TMPDIR"]) / ("verify-1697-" + ("mutations" if reuse else label))
    if not destination.exists():
        destination.mkdir()
        archive = args.evidence / (label + ".tar")
        with archive.open("wb") as output:
            subprocess.run(["git", "archive", "HEAD"], cwd=ROOT, stdout=output, check=True)
        with tarfile.open(archive) as source:
            source.extractall(destination, filter="data")
        archive.unlink()
    target = destination / path
    original = target.read_text()
    if original.count(before) != 1:
        refuse(label, "mutation must have exactly one production match")
    target.write_text(original.replace(before, after))
    differences = [name for name, expected in inputs.items() if sha(destination / name) != expected]
    if differences != [path]:
        refuse(label, "derived source must differ at exactly the declared production file: " + repr(differences))
    report.setdefault("mutations", []).append({"id": label, "path": path,
        "before_sha256": sha(ROOT / path), "after_sha256": sha(target),
        "before": before, "after": after})
    return destination


try:
    if not os.environ.get("BB_JOB_ACTIVE") or not os.environ.get("BB_ADMITTED"):
        refuse("admission", "run only inside canonical bb2 build-slot")
    args.evidence.mkdir(parents=True, exist_ok=False)
    evidence_created = True
    if subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=ROOT).returncode:
        refuse("source", "producer requires unchanged signed tracked bytes")
    report["source"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).split(b"\0")
    inputs = {os.fsdecode(p): sha(ROOT / os.fsdecode(p)) for p in paths if p}
    (args.evidence / "inputs.json").write_text(json.dumps(inputs, sort_keys=True))
    run("toolchain", ["rustc", "--version", "--verbose"])
    env = os.environ.copy()
    env.update(PROPTEST_RNG_SEED="1697", CARGO_INCREMENTAL="0",
               CARGO_PROFILE_TEST_DEBUG="0", CARGO_PROFILE_DEV_DEBUG="0")
    os.environ.update(env)
    if args.mode == "check":
        run("affected-check", ["cargo", "check", "--locked", "--all-features", "-p", "acyclic-harness-core",
                               "-p", "acyclic-stream", "-p", "acyclic-contract-derive"])
    elif args.mode == "rust":
        binaries = build(ROOT, "positive-build")
        for name in ["record_bounds_cover_every_kind_and_json_escape_expansion",
                     "every_record_a_step_writes_fits_its_limit_at_the_configured_maxima",
                     "every_write_family_preserves_digest_admission_and_retry_evidence"]:
            run(name, [binaries["acyclic_harness_core"], name, "--nocapture"], passed=1)
        run("slot-schedules", [binaries["agent-model"], "slot_schedules::", "--nocapture"], passed=2)
        run("explicit-provider-docs", ["cargo", "test", "--locked", "-p", "acyclic-contract-derive",
                                      "--doc", "explicit_provider", "--", "--nocapture"], passed=3)
    elif args.mode in ("docs", "finish", "remaining"):
        if args.mode != "remaining":
            run("explicit-provider-docs", ["cargo", "test", "--locked", "-p", "acyclic-contract-derive",
                                          "--doc", "explicit_provider", "--", "--nocapture"], passed=3)
        if args.mode != "docs":
            run("affected-check", ["cargo", "check", "--locked", "--all-features", "-p", "acyclic-harness-core",
                                   "-p", "acyclic-stream", "-p", "acyclic-contract-derive"])
            changed = variant("missing-method", "rust/crates/harness-core/src/filesystem/attestation.rs",
                "    async fn durability_barrier(&self) -> Result<(), StreamError> {\n        self.inner.durability_barrier().await\n    }\n", "")
            run("missing-method", ["cargo", "check", "--locked", "-p", "acyclic-harness-core", "--features", "filesystem"],
                changed, failure="missing: `durability_barrier`")
    elif args.mode == "kani":
        env.pop("RUSTC", None)
        env.pop("RUSTC_WRAPPER", None)
        env["KANI_HOME"] = "/home/var/.kani"
        env["PATH"] = "/home/var/.kani/kani-0.68.0/bin:" + env["PATH"]
        command = ["cargo", "kani", "--manifest-path", "rust/proofs/spawn-slots/Cargo.toml"]
        run("kani-version", ["cargo", "kani", "--version"], env=env)
        run("kani-positive", command, env=env)
        slot_path = "rust/crates/harness-core/src/agent/slots.rs"
        for label, before, after in [
            ("reservation-control", "MAX_OPEN_CHILDREN.saturating_sub(if turn { reserved } else { 0 })", "MAX_OPEN_CHILDREN"),
            ("vacated-control", "reserved.saturating_add(1).min(MAX_OPEN_CHILDREN as u16)", "reserved"),
        ]:
            changed = variant(label, slot_path, before, after)
            run(label, command, cwd=changed, env=env, failure="VERIFICATION:- FAILED")
    elif args.mode == "mutants":
        for label, path, before, after, target, oracle in [
            ("vacated-control", "rust/crates/harness-core/src/agent/slots.rs",
             "reserved.saturating_add(1).min(MAX_OPEN_CHILDREN as u16)", "reserved", "agent-model",
             "a_child_finishing_during_pause_cannot_strand_a_recorded_replay"),
            ("digest-control", "rust/crates/harness-core/src/filesystem/attestation.rs",
             "digest: digest()?", "digest: [0; 32]", "acyclic_harness_core",
             "every_write_family_preserves_digest_admission_and_retry_evidence"),
            ("admission-control", "rust/crates/harness-core/src/filesystem/attestation.rs",
             "admission: &self.admission", "admission: &super::TurnAdmission::LocalRoot", "acyclic_harness_core",
             "every_write_family_preserves_digest_admission_and_retry_evidence"),
            ("record-control", "rust/crates/harness-core/src/conversation/mod.rs",
             "pub const MAX_PATH_BYTES: usize = 4096;", "pub const MAX_PATH_BYTES: usize = 32768;",
             "acyclic_harness_core", "record_bounds_cover_every_kind_and_json_escape_expansion"),
        ]:
            changed = variant(label, path, before, after, reuse=True)
            binaries = build(changed, label + "-build")
            run(label, [binaries[target], oracle, "--nocapture"], changed, failure="test result: FAILED")
            (changed / path).write_bytes((ROOT / path).read_bytes())
    else:
        changed = variant("missing-method", "rust/crates/harness-core/src/filesystem/attestation.rs",
            "    async fn durability_barrier(&self) -> Result<(), StreamError> {\n        self.inner.durability_barrier().await\n    }\n", "")
        run("missing-method", ["cargo", "check", "--locked", "-p", "acyclic-harness-core", "--features", "filesystem"],
            changed, failure="missing: `durability_barrier`")
    report["status"] = "PASS"
except Exception as error:
    report["status"] = "REFUSED"
    report["error"] = str(error)
    print(f"step={args.mode} detail={str(error)!r} recovery={recovery!r}", file=sys.stderr)
finally:
    report["seconds"] = time.monotonic() - start
    if evidence_created:
        (args.evidence / "report.json").write_text(json.dumps(report, indent=2))
sys.exit(0 if report["status"] == "PASS" else 1)
