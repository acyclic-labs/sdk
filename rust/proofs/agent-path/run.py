#!/usr/bin/env python3
"""Check direct-child admission against the former parent-copy operation."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "rust/crates/harness-core/src/agent/layout.rs"
TEST = "agent::layout::tests::canonical_subtree_admission_borrows_ancestry_without_parent_copies"
COMMAND = ["cargo", "nextest", "run", "--locked", "-p", "acyclic-harness-core", "--lib", "-E", f"test(={TEST})", "--no-tests=fail"]
RECOVERY = "git diff -- rust/crates/harness-core/src/agent/layout.rs; inspect target/agent-path/*.log before running python3 rust/proofs/agent-path/run.py on changed source"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"Failed step: {step}. Recovery: {RECOVERY}")


def run(label):
    result = subprocess.run(COMMAND, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = ROOT / "target/agent-path"
    output.mkdir(parents=True, exist_ok=True)
    (output / f"{label}.log").write_bytes(result.stdout)
    print(f"{label}: exit={result.returncode}", flush=True)
    print(result.stdout.decode(errors="replace"), flush=True)
    return result


def main():
    original = SOURCE.read_bytes()
    before = b"self.depth == parent.depth + 1 && parent.contains(self)"
    after = b"self.parent().as_ref() == Some(parent)"
    require(original.count(before) == 1, "unique production mutation anchor")
    try:
        SOURCE.write_bytes(original.replace(before, after))
        negative = run("parent-copy")
        require(negative.returncode != 0 and b"child admission copied ancestry" in negative.stdout,
                "unchanged allocation oracle rejects original copying operation")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "exact production restoration")
    positive = run("restored")
    require(positive.returncode == 0 and b"1 test run: 1 passed" in positive.stdout,
            "nonempty restored ancestry and allocation control")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"Failed step: direct-child qualification: {error}. Recovery: {RECOVERY}") from error
