"""Replay one shipped input corpus and detect its production admission defect."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
recovery = "python3 rust/crates/harness-fuzz/graphcoder-args-control.py"
controls = {
    "native_model_selection": (
        "rust/crates/harness-core/src/native_settings/model_selection.rs",
        b"Some(provider) => (provider, model),",
        b'Some(_) => ("openai", model),',
        "opaque-name",
        b"provider and opaque model stay paired",
    ),
    "graphcoder_args": (
        "rust/crates/harness/src/bin/graphcoder/args.rs",
        b"if self.input_format == InputFormat::Jsonl && self.operation.is_none() {",
        b"if false {",
        "exec-missing-operation",
        b"JSONL requires an operation identity",
    ),
    "graphcoder_input": (
        "rust/crates/harness/src/bin/graphcoder/headless/input.rs",
        b"#[serde(untagged, deny_unknown_fields)]",
        b"#[serde(untagged)]",
        "mixed-actions",
        b"a request must select exactly one action without unknown fields",
    ),
    "event_projection": (
        "rust/crates/harness-client/src/event_projection.rs",
        b"if self.version != 1 || self.operation != operation {",
        b"if self.version != 1 {",
        "wrong-operation",
        b"cursor admission must retain operation, version and opaque validity",
    ),
    "native_input": (
        "plugin/src/input.rs",
        b"if bytes.len() > 65_536 {",
        b"if false {",
        "null",
        b"native input exceeds its admitted byte bound",
    ),
}


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {recovery}")


def run(target, seed, witness, *, capture=False, runs=0):
    options = {
        "event_projection": ["--no-default-features", "--features", "client"],
        "native_model_selection": ["--no-default-features"],
    }.get(target, [])
    environment = os.environ.copy()
    environment.pop("ACYCLIC_FUZZ_CORPUS", None)
    if capture:
        environment["ACYCLIC_FUZZ_CORPUS"] = str(root / seed)
    child = subprocess.Popen(
        [
            "cargo", "+nightly-2026-07-01", "fuzz", "run", "--dev",
            "--fuzz-dir", "rust/crates/harness-fuzz", *options, target, seed,
            "--", f"-runs={runs}", "-print_final_stats=1",
        ],
        cwd=root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    found = False
    with child:
        for line in child.stdout:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
            found |= witness in line
    return child.returncode, found


def qualify(target, runs=0):
    filename, guard, replacement, seed, witness = controls[target]
    source = root / filename
    corpus = f"rust/crates/harness-fuzz/corpus/{target}"
    original = source.read_bytes()
    require(original.count(guard) == 1, f"locate original {target} admission")
    if target == "event_projection":
        code, _ = run(target, corpus, witness, capture=True)
        require(code == 0, "capture original serialized cursor corpus")
        require((root / corpus / seed).is_file(), "retain original writer seed")
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
    code, _ = run(target, corpus, witness, runs=runs)
    require(code == 0, f"replay production {target} corpus")
    print(f"{target}: production corpus and planted-defect oracle passed", flush=True)


def binding_oracle_regression():
    source = root / "rust/crates/harness-fuzz/fuzz_targets/graphcoder_input.rs"
    original = source.read_bytes()
    corrected = (
        b"                let original: acyclic_harness::interaction::ApprovalBinding =\n"
        b"                    serde_json::from_value(fields[\"binding\"].clone()).unwrap();\n"
        b"                assert_eq!(binding, original);"
    )
    stale = original.replace(
        corrected,
        b"                assert_eq!(fields[\"binding\"], serde_json::to_value(binding).unwrap());",
    )
    # This must be the exact original oracle, not a parallel failing example.
    require(
        hashlib.sha256(stale).hexdigest()
        == "688bd1ba9de86b3ee2b05dfab135c226ee0271a6402d6e23b4856a203dfbb4f2",
        "restore exact original binding oracle for red-first regression",
    )
    try:
        source.write_bytes(stale)
        code, found = run(
            "graphcoder_input",
            "rust/crates/harness-fuzz/corpus/graphcoder_input/approval-alternate-uuid",
            b"panicked at rust/crates/harness-fuzz/fuzz_targets/graphcoder_input.rs:26:",
        )
        require(code != 0 and found, "original oracle must reject a valid alternate UUID")
    finally:
        source.write_bytes(original)
    require(source.read_bytes() == original, "restore corrected binding oracle")
    print("original binding oracle RED reproduced; corrected oracle restored", flush=True)


if __name__ == "__main__":
    class Parser(argparse.ArgumentParser):
        def error(self, message):
            raise RuntimeError(f"admit control selection: {message}")

    parser = Parser(description=__doc__)
    parser.add_argument("targets", nargs="*", choices=controls)
    parser.add_argument("--runs", type=int, default=0, help="finite restored-source campaign units")
    target = "selection"
    try:
        arguments = parser.parse_args()
        require(arguments.runs >= 0, "admit finite nonnegative campaign units")
        targets = arguments.targets or ["graphcoder_args"]
        for target in targets:
            recovery = f"python3 rust/crates/harness-fuzz/graphcoder-args-control.py {target}"
            if target == "graphcoder_input":
                binding_oracle_regression()
            qualify(target, arguments.runs)
    except (OSError, RuntimeError) as error:
        print(f"step=qualify {target} fuzz oracle: {error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
