#!/usr/bin/env python3
"""Qualify the original retained-credit property and its production mutation."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SOURCE = ROOT / "rust/crates/filesystem/src/kernel/allocation.rs"
TEST = "kernel::allocation::retained::tests::retained_credit_conserves_full_width_ownership_transitions"
COMMAND = ["cargo", "test", "--locked", "-p", "acyclic-fs", "--lib", TEST,
           "--", "--exact", "--nocapture"]
RECOVERY = "git diff -- rust/crates/filesystem/src/kernel/allocation.rs; inspect target/retained-credit/*.log; correct source or command, then python3 rust/proofs/retained-credit/run.py"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {RECOVERY}")


def run(label):
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    directory = ROOT / "target/retained-credit"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{label}.log").write_text(output, encoding="utf-8")
    print(f"step={label}; command={COMMAND!r}; exit={result.returncode}", flush=True)
    print("\n".join(output.splitlines()[-35:]), flush=True)
    return result.returncode, output


def main():
    original = SOURCE.read_bytes()
    patch = str(HERE / "scratch-retirement.patch")
    require(subprocess.run(["git", "apply", "--check", patch], cwd=ROOT).returncode == 0,
            "check production mutation anchors")
    try:
        require(subprocess.run(["git", "apply", patch], cwd=ROOT).returncode == 0,
                "apply scratch retirement mutation")
        code, output = run("unfenced")
        require(code == 101 and "scratch retirement accepted retained credit" in output
                and "0 passed; 1 failed" in output,
                "original property rejects retained credit retirement")
    finally:
        SOURCE.write_bytes(original)
    require(SOURCE.read_bytes() == original, "restore exact production source")
    code, output = run("restored")
    require(code == 0 and "1 passed; 0 failed" in output,
            "restored original property completes")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"step=retained credit qualification; cause={error}; recovery: {RECOVERY}") from error
