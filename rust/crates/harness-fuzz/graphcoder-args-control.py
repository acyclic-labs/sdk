"""Check a shipped input boundary and detect its production admission defect."""
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
    "native_stdin_binary": (
        "plugin/src/input.rs",
        b"input.read_to_end(&mut bytes)?;",
        b"let _ = input.read_to_end(&mut bytes);",
        "null",
        b"native stdin read error must retain its original cause",
    ),
    "command_result": (
        "rust/crates/harness-core/src/contract_errors.rs",
        b"crate::wire::ErrorCode::InteractionCancelled | crate::wire::ErrorCode::Cancelled => {\n                Self::Cancelled\n            }",
        b"crate::wire::ErrorCode::InteractionCancelled | crate::wire::ErrorCode::Cancelled => {\n                Self::Failed\n            }",
        "cancelled",
        b"settled result must retain completion and payload",
    ),
    "native_workspace": (
        "rust/crates/harness-core/src/native_settings/workspace.rs",
        b"if (left_unc, left_rooted, left.is_empty()) != (right_unc, right_rooted, right.is_empty()) {",
        b"if false {",
        "drive-root",
        b"workspace roots must remain distinct",
    ),
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
        b"if maximum.is_some_and(|bound| bytes.len() as u64 > bound) {",
        b"if false {",
        "null",
        b"native input exceeds its admitted byte bound",
    ),
}
additional_controls = {
    "graphcoder_args": [
        (
            "rust/crates/harness/src/bin/graphcoder/args.rs",
            b'long, requires = "host_state_directory", conflicts_with_all',
            b'long, conflicts_with_all',
            "host-missing-state",
            b"native workspace ownership requires explicit state and one owner",
        ),
        (
            "rust/crates/harness/src/bin/graphcoder/args.rs",
            b'#[arg(long, requires = "tls_certificate")]\n    pub tls_key:',
            b'#[arg(long)]\n    pub tls_key:',
            "tls-partial",
            b"serving requires a complete TLS identity or none",
        ),
        (
            "rust/crates/harness/src/bin/graphcoder/args.rs",
            b'#[arg(short = \'p\', long = "prompt", conflicts_with = "input_format")]',
            b'#[arg(short = \'p\', long = "prompt")]',
            "mixed-input",
            b"text prompt and JSONL stdin require distinct input owners",
        ),
    ],
    "native_stdin_binary": [
        (
            "plugin/src/main.rs",
            b"let action = serde_json::from_value(read_json(io::stdin().lock(), None)?)?;",
            b"let action = serde_json::from_value(read_json(io::stdin().lock(), Some(65_536))?)?;",
            "null",
            b"workspace envelope must reach the original workspace client",
        ),
    ],
    "native_marketplace": [
        (
            "plugin/src/install/marketplace.rs",
            b'let name = entry.get("name").and_then(Value::as_str).ok_or_else(|| {\n                format!("step=admit Codex marketplace: native marketplace name absent; {RECOVERY}")\n            })?;',
            b'let name = entry.get("name").and_then(Value::as_str).unwrap_or("foreign");',
            "missing-name",
            b"every native marketplace must retain its name",
        ),
        (
            "plugin/src/install/marketplace.rs",
            b"root.zip(original).filter(|_| local)",
            b"root.zip(original)",
            "nonlocal-type",
            b"native source must have local root and origin",
        ),
        (
            "plugin/src/install/marketplace.rs",
            b'let entries = self\n            .0\n            .get("marketplaces")\n            .and_then(Value::as_array)\n            .ok_or_else(|| {\n                format!("step=admit Codex marketplaces: marketplace list absent; {RECOVERY}")\n            })?;',
            b'let Some(entries) = self.0.get("marketplaces").and_then(Value::as_array) else { return Ok(None); };',
            "missing-list",
            b"native output must name a marketplace list",
        ),
        (
            "plugin/src/install/marketplace.rs",
            b'let root = entry.get("root").and_then(Value::as_str);',
            b'let root = entry.get("root").and_then(Value::as_str).or(Some(""));',
            "missing-root",
            b"native source must have local root and origin",
        ),
        (
            "plugin/src/install/marketplace.rs",
            b'.and_then(Value::as_str);\n        let local = source',
            b'.and_then(Value::as_str).or(Some(""));\n        let local = source',
            "missing-origin",
            b"native source must have local root and origin",
        ),
        (
            "plugin/src/install/marketplace.rs",
            b"let Some(entry) = selected else {",
            b"let Some(entry) = selected.or_else(|| entries.first()) else {",
            "foreign-name",
            b"foreign registrations are not ours",
        ),
    ],
    "event_projection": [
        (
            "rust/crates/harness-client/src/event_projection.rs",
            b"if *previous == cursor {",
            b"if true {",
            "resource-state",
            b"a changed source cursor must be retained",
        ),
        (
            "rust/crates/harness-client/src/event_projection.rs",
            b"if position.input < previous.input",
            b"if false && position.input < previous.input",
            "execution-state",
            b"execution cursor moved backwards",
        ),
    ],
    "native_input": [
        (
            "rust/crates/harness/src/local/host_owner.rs",
            b'#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]',
            b'#[serde(tag = "kind", rename_all = "snake_case")]',
            "null",
            b"native action unknown fields must be refused",
        ),
        (
            "rust/crates/harness/src/local/host_owner/sessions.rs",
            b"    Next {},",
            b"    Next,",
            "null",
            b"native session action unknown fields must be refused",
        ),
        (
            "rust/crates/harness/src/client/commands.rs",
            b"    Theme {},",
            b"    Theme,",
            "null",
            b"command action unknown fields must be refused",
        ),
        (
            "rust/crates/harness-core/src/command_request.rs",
            b"    Quit {},",
            b"    Quit,",
            "null",
            b"command request unknown fields must be refused",
        ),
        (
            "plugin/src/input.rs",
            b"input.read_to_end(&mut bytes)?;",
            b"input.take(1).read_to_end(&mut bytes)?;",
            "null",
            b"native input admission changed its original JSON",
        ),
        (
            "plugin/src/input.rs",
            b"input.read_to_end(&mut bytes)?;",
            b"let _ = input.read_to_end(&mut bytes);",
            "null",
            b"native input read failure must retain its OS error",
        ),
        (
            "rust/crates/harness/src/client/commands.rs",
            b"if spec.arguments != expected {",
            b"if false {",
            "catalog-installed",
            b"catalog argument-shape-",
        ),
    ],
    "command_result": [
        (
            "rust/crates/harness/src/client/command_result.rs",
            b".is_none_or(|n| n > self.maximum)",
            b".is_none_or(|_| false)",
            "escaped",
            b"encoded command result must remain within the admitted byte bound",
        ),
        (
            "rust/crates/harness-core/src/contract_errors.rs",
            b"if matches!(error, Error::GoalBlocked(_)) {",
            b"if false {",
            "goal-blocked",
            b"settled result must retain completion and payload",
        ),
        (
            "rust/crates/harness-core/src/contract_errors.rs",
            b"Self::InteractionRejected(InteractionRejection::Cancelled) => {\n                ErrorCode::InteractionCancelled\n            }",
            b"Self::InteractionRejected(InteractionRejection::Cancelled) => {\n                ErrorCode::Invalid\n            }",
            "cancelled",
            b"settled result must retain completion and payload",
        ),
    ],
    "native_settings": [
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"if !load_mcp {",
            b"if false {",
            "project-server",
            b"inactive MCP sections must not block hooks",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"Entry::Occupied(mut entry) if entry.get().0 <= order => {",
            b"Entry::Occupied(mut entry) if entry.get().0 >= order => {",
            "project-server",
            b"winning scope must replace lower invalid declarations",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"if source.local\n",
            b"if false\n",
            "project-server",
            b"selected workspace entries must retain their winning declaration",
        ),
        (
            "rust/crates/harness-core/src/mcp/config.rs",
            b"#[serde(default, deny_unknown_fields)]",
            b"#[serde(default)]",
            "unknown-server",
            b"invalid selected MCP declaration must be refused",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"if hooks && config.trust_project_hooks {",
            b"if hooks {",
            "project-server",
            b"hook sources must follow their independent trust role",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"if config.trust_project_mcp {",
            b"if true {",
            "project-server",
            b"untrusted project declarations must not enter the snapshot",
        ),
        (
            "rust/crates/harness-core/src/native_settings/model.rs",
            b"if bytes.len() as u64 > self.maximum_file_bytes {",
            b"if false {",
            "model-file",
            b"valid over-bound model settings must be refused",
        ),
        (
            "rust/crates/harness-core/src/native_settings/model.rs",
            b'let path = self.codex_home.join("config.toml");',
            b'let path = self.home.join(".codex/config.toml");',
            "model-file",
            b"model file admission must refine the original parser",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"std::fs::File::open(&path)\n                .map_err(Error::process_io)?\n                .take(config.maximum_file_bytes + 1)\n                .read_to_end(&mut bytes)\n                .map_err(Error::process_io)?;",
            b"let _ = std::fs::File::open(&path).ok().and_then(|file| file.take(config.maximum_file_bytes + 1).read_to_end(&mut bytes).ok());",
            "project-server",
            b"selected file I/O must not be admitted or reclassified",
        ),
        (
            "rust/crates/harness-core/src/native_settings/model.rs",
            b'let file = match std::fs::File::open(&path) {\n            Ok(file) => file,\n            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),\n            Err(error) => return Err(Error::process_io(error)),\n        };\n        if self.maximum_file_bytes == 0 || self.maximum_file_bytes == u64::MAX {\n            return Err(Error::Invalid(\n                "settings require a finite positive byte bound".into(),\n            ));\n        }\n        let mut bytes = Vec::new();\n        file.take(self.maximum_file_bytes + 1)\n            .read_to_end(&mut bytes)\n            .map_err(Error::process_io)?;',
            b'let file = std::fs::File::open(&path).ok();\n        if self.maximum_file_bytes == 0 || self.maximum_file_bytes == u64::MAX {\n            return Err(Error::Invalid(\n                "settings require a finite positive byte bound".into(),\n            ));\n        }\n        let mut bytes = Vec::new();\n        let _ = file.and_then(|file| file.take(self.maximum_file_bytes + 1).read_to_end(&mut bytes).ok());',
            "model-file",
            b"selected file I/O must not be admitted or reclassified",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"let workspace = std::fs::canonicalize(configured_workspace).map_err(Error::process_io)?;",
            b"let workspace = std::fs::canonicalize(configured_workspace).unwrap_or_else(|_| configured_workspace.clone());",
            "project-server",
            b"selected file I/O must not be admitted or reclassified",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"Err(error) => return Err(Error::process_io(error)),",
            b"Err(_) => continue,",
            "project-server",
            b"selected file I/O must not be admitted or reclassified",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"Self::snapshot(config, true, false).await?.mcp",
            b"Self::snapshot(config, true, true).await?.mcp",
            "project-server",
            b"MCP-only startup must ignore hook-only sections and sources",
        ),
        (
            "rust/crates/harness-core/src/native_settings.rs",
            b"Self::snapshot(config, false, true).await?.hooks",
            b"Self::snapshot(config, false, false).await?.hooks",
            "project-server",
            b"hook-only startup must refuse selected invalid hooks",
        ),
    ],
}


def require(condition, step):
    if not condition:
        raise RuntimeError(f"step={step}; recovery: {recovery}")


def run(target, seed, witness, *, capture=False, runs=0):
    options = {
        "native_input": ["--no-default-features", "--features", "native-input"],
        "command_result": ["--no-default-features", "--features", "core"],
        "native_workspace": ["--no-default-features"],
        "native_settings": ["--no-default-features", "--features", "native-settings"],
        "event_projection": ["--no-default-features", "--features", "client"],
        "native_model_selection": ["--no-default-features"],
        "native_marketplace": ["--no-default-features"],
    }.get(target, [])
    environment = os.environ.copy()
    environment.pop("ACYCLIC_FUZZ_CORPUS", None)
    if capture:
        environment["ACYCLIC_FUZZ_CORPUS"] = str(root / seed)
    if target == "native_stdin_binary":
        command = [
            "cargo", "nextest", "run", "--locked", "-p", "acyclic-plugin",
            "--test", "main", "--profile", "ci", "-E",
            "test(native_input::native_stdin_failures_stop_before_workspace_effects)",
        ]
    else:
        command = [
            "cargo", "+nightly-2026-07-01", "fuzz", "run", "--dev",
            "--fuzz-dir", "rust/crates/harness-fuzz", *options, target, seed,
            "--", f"-runs={runs}", "-print_final_stats=1",
        ]
    child = subprocess.Popen(
        command,
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


def qualify(target, runs=0, selected_controls=None):
    corpus = f"rust/crates/harness-fuzz/corpus/{target}"
    if target == "native_marketplace":
        corpus = marketplace_corpus()
    if target in {"event_projection", "command_result", "native_input"}:
        _, _, _, seed, witness = controls[target]
        code, _ = run(target, corpus, witness, capture=True)
        require(code == 0, f"capture original serialized {target} corpus")
        require((root / corpus / seed).is_file(), "retain original writer seed")
    mutations = [controls[target], *additional_controls.get(target, [])]
    if selected_controls is not None:
        mutations = [mutations[index] for index in selected_controls]
    for filename, guard, replacement, seed, witness in mutations:
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
    oracle = "named binary test" if target == "native_stdin_binary" else "production corpus"
    require(code == 0, f"replay {target} {oracle}")
    print(f"{target}: {oracle} and planted-defect oracle passed", flush=True)


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
    parser.add_argument("--control", type=int, action="append", help="zero-based source mutation; default: all")
    arguments = parser.parse_args(argv)
    arguments.targets = arguments.targets or ["graphcoder_args"]
    require(all(target in controls for target in arguments.targets), "admit known control targets")
    require(arguments.runs >= 0, "admit finite nonnegative campaign units")
    if arguments.control is not None:
        require(len(set(arguments.control)) == len(arguments.control), "admit unique mutation indices")
        require(all(
            0 <= index < 1 + len(additional_controls.get(target, []))
            for target in arguments.targets for index in arguments.control
        ), "admit existing mutation indices")
    return arguments


if __name__ == "__main__":
    target = "selection"
    try:
        arguments = selection()
        for target in arguments.targets:
            recovery = f"python3 rust/crates/harness-fuzz/graphcoder-args-control.py {target}"
            if target == "graphcoder_input":
                binding_oracle_regression()
            qualify(target, arguments.runs, arguments.control)
    except (OSError, RuntimeError) as error:
        print(f"step=qualify {target} fuzz oracle: {error}; recovery: {recovery}", file=sys.stderr)
        sys.exit(1)
