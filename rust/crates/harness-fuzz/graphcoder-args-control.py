"""Replay one shipped input corpus and detect its production admission defect."""
import argparse
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[3]
recovery = "python3 rust/crates/harness-fuzz/graphcoder-args-control.py"
controls = {
    "native_settings": (
        "rust/crates/harness-core/src/native_settings.rs",
        b"if bytes.len() as u64 > config.maximum_file_bytes {",
        b"if false {",
        "project-server",
        b"valid over-bound settings must be refused",
    ),
    "native_marketplace": (
        "plugin/src/install/marketplace.rs",
        b'if name == "acyclic" && selected.replace(entry).is_some() {',
        b'if name == "acyclic" && { selected = Some(entry); false } {',
        "duplicate-registration",
        b"duplicate native registrations must be refused",
    ),
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
additional_controls = {
    "native_settings": [
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"if config.trust_project_mcp {",
            b"if true {",
            "project-server",
            b"untrusted project declarations must not enter the snapshot",
        ),
    ],
}


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {recovery}")


def run(target, seed, witness, *, capture=False, runs=0):
    options = {
        "native_settings": ["--no-default-features", "--features", "native-settings"],
        "event_projection": ["--no-default-features", "--features", "client"],
        "native_model_selection": ["--no-default-features"],
        "native_marketplace": ["--no-default-features"],
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


def marketplace_corpus():
    """Stage original bytes and explicitly derived mutations outside tracked files."""
    encoded = os.environ.get("ACYCLIC_MARKETPLACE_CAPTURE", "")
    require(bool(encoded), "supply original marketplace stdout as base64 in ACYCLIC_MARKETPLACE_CAPTURE")
    try:
        original = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise RuntimeError("decode original marketplace capture base64") from error
    require(
        len(original) == 314 and hashlib.sha256(original).hexdigest()
        == "1e4e2006ef89ae8a33a2d7c9ab356f0ba64f2273f603d911993daefd486c9a6d",
        "verify unchanged original marketplace stdout",
    )
    corpus = "target/native-marketplace-corpus"
    directory = root / corpus
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "native-list-original").write_bytes(original)
    recorded = json.loads(original)
    entry = recorded["marketplaces"][0]
    mutations = {
        "duplicate-registration": {"marketplaces": [entry, entry]},
        "missing-list": {},
        "null-list": {"marketplaces": None},
        "empty-list": {"marketplaces": []},
    }
    for name, path, replacement in [
        ("missing-name", ["name"], None),
        ("foreign-name", ["name"], "foreign"),
        ("number-name", ["name"], 1),
        ("missing-root", ["root"], None),
        ("number-root", ["root"], 1),
        ("missing-source", ["marketplaceSource"], None),
        ("missing-origin", ["marketplaceSource", "source"], None),
        ("number-origin", ["marketplaceSource", "source"], 1),
        ("missing-type", ["marketplaceSource", "sourceType"], None),
        ("nonlocal-type", ["marketplaceSource", "sourceType"], "remote"),
    ]:
        changed = copy.deepcopy(entry)
        parent = changed if len(path) == 1 else changed[path[0]]
        if replacement is None:
            del parent[path[-1]]
        else:
            parent[path[-1]] = replacement
        mutations[name] = {"marketplaces": [changed]}
    for name, document in mutations.items():
        (directory / name).write_bytes(json.dumps(document).encode("utf-8"))
    # Prefix truncations retain the recorded protocol bytes, not authored output.
    for length in [0, 1, 156, 313]:
        (directory / f"partial-{length}").write_bytes(original[:length])
    print("native_marketplace: original 314-byte capture and derived mutations staged", flush=True)
    return corpus


def qualify(target, runs=0):
    corpus = f"rust/crates/harness-fuzz/corpus/{target}"
    if target == "native_marketplace":
        corpus = marketplace_corpus()
    if target == "event_projection":
        _, _, _, seed, witness = controls[target]
        code, _ = run(target, corpus, witness, capture=True)
        require(code == 0, "capture original serialized cursor corpus")
        require((root / corpus / seed).is_file(), "retain original writer seed")
    for filename, guard, replacement, seed, witness in [
        controls[target], *additional_controls.get(target, []),
    ]:
        source = root / filename
        original = source.read_bytes()
        require(original.count(guard) == 1, f"locate original {target} admission")
        try:
            source.write_bytes(original.replace(guard, replacement))
            code, found = run(target, f"{corpus}/{seed}", witness)
            require(
                code != 0 and found,
                f"unchanged oracle must detect removed production {target} admission: {witness!r}",
            )
            print(f"{target}: planted production admission defect detected: {witness!r}", flush=True)
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


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise RuntimeError(f"admit control selection: {message}")


def selection(argv=None):
    parser = Parser(description=__doc__)
    parser.add_argument("targets", nargs="*", metavar="TARGET")
    parser.add_argument("--runs", type=int, default=0, help="finite restored-source campaign units")
    arguments = parser.parse_args(argv)
    arguments.targets = arguments.targets or ["graphcoder_args"]
    require(all(target in controls for target in arguments.targets), "admit known control targets")
    require(arguments.runs >= 0, "admit finite nonnegative campaign units")
    return arguments


if __name__ == "__main__":
    target = "selection"
    try:
        arguments = selection()
        for target in arguments.targets:
            recovery = f"python3 rust/crates/harness-fuzz/graphcoder-args-control.py {target}"
            if target == "graphcoder_input":
                binding_oracle_regression()
            qualify(target, arguments.runs)
    except (OSError, RuntimeError) as error:
        print(f"step=qualify {target} fuzz oracle: {error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
