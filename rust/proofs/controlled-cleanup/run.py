#!/usr/bin/env python3
"""Qualify the real cleanup owner and its retained scope-loss mutation."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / "rust/crates/harness/src/local/workspace/volumes.rs"
RECOVERY = "git diff -- rust/crates/harness/src/local/workspace/volumes.rs; inspect target/controlled-cleanup/*.log before running python3 rust/proofs/controlled-cleanup/run.py on changed source"
CLAIMS = "local::workspace::volumes::publication_tests::cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head"
TESTS = [
    CLAIMS,
    "local::tests::agents::a_shell_call_stages_its_output_on_the_private_volume",
    "local::owner::publication_tests::private_legacy_owner_migrates_once_without_changing_its_original_identity",
    "local::owner::publication_tests::concurrent_owner_migration_cannot_publish_another_callers_provisional_key",
    "local::owner::publication_tests::explicit_file_custody_reopens_the_original_owner_without_native_fallback",
]


def require(condition, step):
    if not condition:
        raise RuntimeError(f"Failed step: {step}. Recovery: {RECOVERY}")


def run(label, command):
    result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = ROOT / "target/controlled-cleanup"
    output.mkdir(parents=True, exist_ok=True)
    (output / f"{label}.log").write_text(result.stdout, encoding="utf-8")
    print(json.dumps({"step": label, "command": command, "exit": result.returncode}), flush=True)
    print("\n".join(result.stdout.splitlines()[-40:]), flush=True)
    return result


def main():
    original = SOURCE.read_bytes()
    patch = str(HERE / "unscoped-claims.patch")
    cargo = ["--locked", "--manifest-path", "Cargo.toml", "-p", "acyclic-harness", "--features", "filesystem-local,native-execution,test-support,grpc,search-http", "--lib"]
    require(subprocess.run(["git", "apply", "--check", patch], cwd=ROOT).returncode == 0, "production mutation anchors")
    try:
        require(subprocess.run(["git", "apply", patch], cwd=ROOT).returncode == 0, "apply scope-loss mutation")
        negative = run("unscoped", ["cargo", "test", *cargo, CLAIMS, "--", "--exact", "--nocapture"])
        require(negative.returncode == 101 and "physical cleanup escaped its original task scope" in negative.stdout and "0 passed; 1 failed" in negative.stdout, "unchanged cancellation oracle rejects direct submission")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "exact production restoration")
    selected = " | ".join(f"test(={name})" for name in TESTS)
    positive = run("restored", ["cargo", "nextest", "run", *cargo, "-E", selected, "--no-tests=fail"])
    require(positive.returncode == 0 and "5 tests run: 5 passed" in positive.stdout, "five nonempty native controls")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"Failed step: cleanup qualification: {error}. Recovery: {RECOVERY}") from error
