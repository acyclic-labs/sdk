#!/usr/bin/env python3
"""Qualify a production ownership/event oracle and its retained mutation."""
import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


class ClaimParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, f"step=select claim; cause={message}; recovery: python3 rust/proofs/qualify.py --help\n")


parser = ClaimParser(description=__doc__)
parser.add_argument("claim", choices=["retained-credit", "settlement-terminal"])
claim = parser.parse_args().claim
if claim == "retained-credit":
    source = "rust/crates/filesystem/src/kernel/allocation.rs"
    crate = "acyclic-fs"
    test = "kernel::allocation::retained::tests::retained_credit_conserves_full_width_ownership_transitions"
    diagnostic = "scratch retirement accepted retained credit"
    features = []
else:
    source = "rust/crates/harness/src/local/tests/agents.rs"
    crate = "acyclic-harness"
    test = "local::tests::agents::lost_model_producer_closes_local_session_without_settling_input"
    diagnostic = "settlement bypassed the original runtime terminal event"
    features = ["--features", "filesystem-local,native-execution,test-support,grpc,search-http"]
SOURCE = ROOT / source
COMMAND = ["cargo", "test", "--locked", "-p", crate, *features, "--lib", test,
           "--", "--exact", "--nocapture"]
RECOVERY = f"git diff -- {source}; inspect target/{claim}/*.log; correct source or command, then python3 rust/proofs/qualify.py {claim}"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {RECOVERY}")


def run(label):
    environment = os.environ.copy()
    environment.setdefault("PROPTEST_RNG_SEED", "1697")
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True, env=environment)
    output = result.stdout + result.stderr
    directory = ROOT / "target" / claim
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{label}.log").write_text(output, encoding="utf-8")
    print(f"step={label}; command={COMMAND!r}; exit={result.returncode}", flush=True)
    print("\n".join(output.splitlines()[-35:]), flush=True)
    return result.returncode, output


def main():
    original = SOURCE.read_bytes()
    patch = str(ROOT / "rust/proofs" / claim / "mutation.patch")
    require(subprocess.run(["git", "apply", "--check", patch], cwd=ROOT).returncode == 0,
            "check mutation anchors")
    try:
        require(subprocess.run(["git", "apply", patch], cwd=ROOT).returncode == 0,
                "apply original ownership/event mutation")
        code, output = run("mutated")
        require(code == 101 and diagnostic in output and "0 passed; 1 failed" in output,
                "unchanged oracle rejects ownership/event loss")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "restore exact original source")
    code, output = run("restored")
    require(code == 0 and "1 passed; 0 failed" in output,
            "restored original oracle completes")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"step={claim} qualification; cause={error}; recovery: {RECOVERY}") from error
