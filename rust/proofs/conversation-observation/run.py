#!/usr/bin/env python3
"""Check native observation cost against the original duplicate-settlement path."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "rust/crates/harness/src/local/agent_conversation.rs"
ORACLE = ROOT / "rust/crates/harness/src/local/tests/agents.rs"
TEST = "local::tests::agents::parent_and_child_context_and_writes_retain_their_own_volumes"
COMMAND = ["cargo", "nextest", "run", "--locked", "-p", "acyclic-harness", "--all-features", "--lib", "-E", f"test(={TEST})", "--profile", "ci", "--no-fail-fast", "--failure-output", "immediate-final", "--run-ignored", "all", "--retries", "0", "--no-tests=fail"]
RECOVERY = "inspect target/conversation-observation/*.log and git diff -- rust/crates/harness/src/local/agent_conversation.rs; fix source before python3 rust/proofs/conversation-observation/run.py"


def require(ok, step):
    if not ok:
        raise RuntimeError(f"Failed step: {step}. Recovery: {RECOVERY}")


def main():
    original = SOURCE.read_bytes()
    oracle = ORACLE.read_bytes()
    before = b"""        let (settled, statuses, _) = self.settlement().await?;
        super::Observer::open(
            &self.connection,
            from.unwrap_or(settled),
            statuses.saturating_sub(1),
        )"""
    after = b"""        let from = match from {
            Some(from) => from,
            None => self.settled().await?,
        };
        let (_, statuses, _) = self.settlement().await?;
        super::Observer::open(&self.connection, from, statuses.saturating_sub(1))"""
    require(original.count(before) == 1, "unique production observation")
    output = ROOT / "target/conversation-observation"
    output.mkdir(parents=True, exist_ok=True)
    phases = []
    try:
        for label, source, code, marker in [
            ("duplicate-settlement", original.replace(before, after), 100, "default observation repeated its settlement query"),
            ("restored", original, 0, "1 test run: 1 passed"),
        ]:
            SOURCE.write_bytes(source)
            require(ORACLE.read_bytes() == oracle, "unchanged native oracle")
            result = subprocess.run(COMMAND, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            (output / f"{label}.log").write_bytes(result.stdout)
            print(result.stdout.decode(errors="replace"), flush=True)
            phase = {"phase": label, "exit": result.returncode, "source_sha256": hashlib.sha256(source).hexdigest(), "oracle_sha256": hashlib.sha256(oracle).hexdigest()}
            phases.append(phase)
            print(json.dumps(phase), flush=True)
            require(result.returncode == code and marker.encode() in result.stdout, f"{label} semantic verdict")
            require(b"Starting 1 test across 1 binary" in result.stdout, "exact native selection")
            require(ORACLE.read_bytes() == oracle, "native oracle retained")
    finally:
        SOURCE.write_bytes(original)
        (output / "phases.json").write_text(json.dumps(phases, indent=2) + "\n")
    require(SOURCE.read_bytes() == original, "exact production restoration")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"Failed step: native observation qualification: {error}. Recovery: {RECOVERY}") from error
