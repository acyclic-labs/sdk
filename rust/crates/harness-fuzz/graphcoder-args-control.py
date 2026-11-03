"""Replay one shipped input corpus and detect its production admission defect."""
import argparse
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
recovery = "python3 rust/crates/harness-fuzz/graphcoder-args-control.py"
controls = {
    "graphcoder_args": (
        "args.rs",
        b"if self.input_format == InputFormat::Jsonl && self.operation.is_none() {",
        b"if false {",
        "exec-missing-operation",
        b"JSONL requires an operation identity",
    ),
    "graphcoder_input": (
        "headless/input.rs",
        b"#[serde(untagged, deny_unknown_fields)]",
        b"#[serde(untagged)]",
        "mixed-actions",
        b"a request must select exactly one action without unknown fields",
    ),
}


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {recovery}")


def run(target, seed, witness):
    child = subprocess.Popen(
        [
            "cargo", "+nightly-2026-07-01", "fuzz", "run", "--dev",
            "--fuzz-dir", "rust/crates/harness-fuzz", target, seed,
            "--", "-runs=0", "-print_final_stats=1",
        ],
        cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    found = False
    with child:
        for line in child.stdout:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
            found |= witness in line
    return child.returncode, found


def qualify(target):
    filename, guard, replacement, seed, witness = controls[target]
    source = root / "rust/crates/harness/src/bin/graphcoder" / filename
    corpus = f"rust/crates/harness-fuzz/corpus/{target}"
    original = source.read_bytes()
    require(original.count(guard) == 1, f"locate original {target} admission")
    try:
        source.write_bytes(original.replace(guard, replacement))
        code, found = run(target, f"{corpus}/{seed}", witness)
        require(
            code != 0 and found,
            f"unchanged oracle must detect removed production {target} admission",
        )
        print(f"{target}: planted production admission defect detected", flush=True)
    finally:
        source.write_bytes(original)
    require(source.read_bytes() == original, "restore production source")
    code, _ = run(target, corpus, witness)
    require(code == 0, f"replay production {target} corpus")
    print(f"{target}: production corpus and planted-defect oracle passed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", nargs="?", choices=controls, default="graphcoder_args")
    target = parser.parse_args().target
    recovery = f"{recovery} {target}"
    try:
        qualify(target)
    except (OSError, RuntimeError) as error:
        print(f"step=qualify {target} fuzz oracle: {error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
