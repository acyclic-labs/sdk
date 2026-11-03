"""Replay the shipped argv corpus and detect an actual admission defect."""
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
source = root / "rust/crates/harness/src/bin/graphcoder/args.rs"
corpus = "rust/crates/harness-fuzz/corpus/graphcoder_args"
command = [
    "cargo", "+nightly-2026-07-01", "fuzz", "run", "--dev",
    "--fuzz-dir", "rust/crates/harness-fuzz", "graphcoder_args",
]
recovery = "python3 rust/crates/harness-fuzz/graphcoder-args-control.py"


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {recovery}")


def run(seed):
    result = subprocess.run(
        [*command, seed, "--", "-runs=0", "-print_final_stats=1"],
        cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    sys.stdout.buffer.write(result.stdout)
    sys.stdout.flush()
    return result


def main():
    original = source.read_bytes()
    guard = b"if self.input_format == InputFormat::Jsonl && self.operation.is_none() {"
    require(original.count(guard) == 1, "locate original operation admission")
    try:
        source.write_bytes(original.replace(guard, b"if false {"))
        defect = run(f"{corpus}/exec-missing-operation")
        require(
            defect.returncode != 0
            and b"JSONL requires an operation identity" in defect.stdout,
            "unchanged oracle must detect removed production operation admission",
        )
        print("planted defect: original operation admission omission detected", flush=True)
    finally:
        source.write_bytes(original)
    require(source.read_bytes() == original, "restore production source")
    replay = run(corpus)
    require(replay.returncode == 0, "replay production argv corpus")
    print("production argv corpus passed; planted-defect oracle passed", flush=True)


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError) as error:
        print(f"step=qualify argv fuzz oracle: {error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
