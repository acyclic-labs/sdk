#!/usr/bin/env python3
"""Qualify capture registration and dispatcher-preserving spawn boundaries."""
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import tomllib


ROOT = Path(__file__).resolve().parent.parent
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
LOGS = TARGET / "scoped-tracing-qualification"
LOGS.mkdir(parents=True, exist_ok=True)
FEATURES = (
    "acyclic-harness/filesystem-local,acyclic-harness/native-execution,"
    "acyclic-harness/mcp-http,acyclic-harness/test-support,"
    "acyclic-grpc-observability/task-context"
)
WINDOWS_FEATURES = FEATURES + ",acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/mcp-stdio,acyclic-harness/grpc"
PACKAGES = [
    "acyclic-harness", "acyclic-fs", "acyclic-actors",
    "acyclic-grpc-observability", "acyclic-test-support",
]
SELECTION = [arg for package in PACKAGES for arg in ["-p", package]]


def retains_root_policy(root, local):
    for name, value in root.items():
        if name == "disallowed-methods":
            paths = {entry["path"] for entry in local.get(name, [])}
            if not {entry["path"] for entry in value} <= paths:
                return False
        elif local.get(name) != value:
            return False
    return True


SPAWNS = {
    "tokio::spawn", "tokio::task::spawn_blocking", "tokio::task::JoinSet::spawn",
    "tokio::runtime::Handle::spawn", "tokio::runtime::Handle::spawn_blocking",
    "std::thread::spawn", "std::thread::Builder::spawn", "std::thread::Scope::spawn",
}


def retains_spawn_policy(local):
    return SPAWNS <= {entry["path"] for entry in local.get("disallowed-methods", [])}


policy = tomllib.loads((ROOT / "clippy.toml").read_text())
for crate in ["harness", "filesystem", "actors", "grpc-observability"]:
    local = tomllib.loads((ROOT / f"rust/crates/{crate}/clippy.toml").read_text())
    assert retains_spawn_policy(local), f"{crate} permits a raw spawn"
    for path in SPAWNS:
        removed = dict(local, **{"disallowed-methods": [
            entry for entry in local["disallowed-methods"] if entry["path"] != path
        ]})
        assert not retains_spawn_policy(removed)
    assert retains_root_policy(policy, local), f"{crate} shadows a root Clippy policy"
    lost_method = dict(local, **{"disallowed-methods": []})
    assert not retains_root_policy(policy, lost_method)
    extended = dict(policy, **{"capture-policy-control": True})
    assert not retains_root_policy(extended, local)


def run(name, command, env=None, rejected=False):
    start = time.monotonic()
    result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True)
    content = result.stdout + result.stderr
    (LOGS / (name + ".log")).write_text(content)
    print(f"{name}: rc={result.returncode} seconds={time.monotonic()-start:.3f}", flush=True)
    if rejected:
        assert result.returncode and "disallowed method" in content, content[-12000:]
    elif result.returncode:
        raise RuntimeError(content[-16000:])
    return content


def windows_check():
    content = run("windows-gnu", [
        "cargo", "check", "--locked", *SELECTION, "--lib", "--tests",
        "--features", WINDOWS_FEATURES, "--target", "x86_64-pc-windows-gnu",
        "--message-format=json",
    ])
    artifacts = [json.loads(line) for line in content.splitlines() if line.startswith("{")]
    harness = [a for a in artifacts if a.get("reason") == "compiler-artifact"
               and a["target"]["name"] == "acyclic_harness"]
    required = {"terminal", "tui", "mcp-stdio", "grpc"}
    assert harness and all(required <= set(a["features"]) for a in harness)
    assert {"e2e", "projfs_profile"} <= {
        a["target"]["name"] for a in artifacts if a.get("reason") == "compiler-artifact"
    }, "changed diagnostic targets were not compiled"
    for artifact in harness:
        for missing in required:
            assert not required <= (set(artifact["features"]) - {missing}), "omitted entry point escaped"


if "--windows-only" in sys.argv:
    windows_check()
    print("SCOPED_TRACING_WINDOWS_PASS", flush=True)
    sys.exit(0)


# Native spawn helpers must not expand the browser's runtime feature closure.
# The manifest oracle also rejects the original misplaced dependency in memory.
def native_dependency(manifest, name):
    return name not in manifest.get("dependencies", {}) and name in manifest.get(
        "target", {}).get('cfg(not(target_arch = "wasm32"))', {}).get("dependencies", {})


for crate, dependency in [("harness", "acyclic-grpc-observability"), ("grpc-observability", "tokio")]:
    manifest = tomllib.loads((ROOT / f"rust/crates/{crate}/Cargo.toml").read_text())
    assert native_dependency(manifest, dependency)
    misplaced = dict(manifest, dependencies=dict(manifest["dependencies"], **{dependency: {}}))
    assert not native_dependency(misplaced, dependency), "browser dependency mutation escaped"
for package, features, forbidden in [
    ("acyclic-harness", [], "acyclic-grpc-observability"),
    ("acyclic-grpc-observability", ["--no-default-features", "--features", "task-context"], "tokio"),
]:
    tree = run(package + "-browser-tree", [
        "cargo", "tree", "--locked", "-p", package, *features,
        "--target", "wasm32-unknown-unknown", "-e", "normal", "--prefix", "none", "--format", "{p} {f}",
    ])
    assert not re.search(rf"^{forbidden} v", tree, re.M), tree


content = (LOGS / "native-build.log").read_text() if "--reuse" in sys.argv else run("native-build", [
    "cargo", "test", "--locked", *SELECTION, "--features", FEATURES,
    "--lib", "--no-run", "--message-format=json",
])
artifacts = [json.loads(line) for line in content.splitlines() if line.startswith("{")]
artifacts = [a for a in artifacts if a.get("reason") == "compiler-artifact"]
binaries = {a["target"]["name"]: a["executable"] for a in artifacts if a.get("executable")}
for crate, filters in [
    ("acyclic_test_support", ["capture::tests::slow_registration_cannot_overwrite_capture_interest"]),
    ("acyclic_grpc_observability", ["task::tests::", "tests::runtime::"]),
    ("acyclic_actors", ["client_calls_emit_spans_without_credentials", "grpc_tests::"]),
    ("acyclic_fs", ["obs::worker_context_tests::"]),
    ("acyclic_harness", [
        "obs::", "stock_turn_emits_spans_without_prompt_or_tool_fields",
        "concurrent_stock_turn_span_captures_are_isolated",
        "journal_tracing_captures_the_poll_context", "indeterminate_command",
        "start_failures_are_retained_and_only_explicit_failure_policy_blocks", "live::tests::",
    ]),
]:
    for index, name in enumerate(filters):
        output = run(f"{crate}-{index}", [binaries[crate], name, "--nocapture"])
        assert re.search(r"test result: ok\. ([1-9]\d*) passed;", output), output
        print(output[-1800:], flush=True)

# Probe Clippy against the exact compiled dependency artifacts. No second Cargo
# compilation or synthetic runtime is needed to prove the admission policy.
runtime_profiles = [a["profile"] for a in artifacts
                    if a["target"]["name"] == "acyclic_grpc_observability"
                    and "test-support" in a["features"] and not a.get("executable")]
assert len(runtime_profiles) == 1, runtime_profiles


def rlib(crate):
    matches = [a for a in artifacts if a["target"]["name"] == crate
               and (crate != "acyclic_grpc_observability" or "task-context" in a["features"])
               and a["profile"] == runtime_profiles[0]]
    files = [Path(f) for a in matches for f in a["filenames"] if f.endswith(".rlib")]
    assert len(set(files)) == 1, files
    return files[0]


with tempfile.TemporaryDirectory(prefix="spawn-policy-", dir=LOGS) as temporary:
    temporary = Path(temporary)
    env = dict(os.environ, CLIPPY_CONF_DIR=str(ROOT / "rust/crates/harness"))
    dependencies = ["-L", f"dependency={rlib('tokio').parent}"]
    for crate in ["tokio", "acyclic_grpc_observability"]:
        dependencies.extend(["--extern", f"{crate}={rlib(crate)}"])
    samples = [
        "tokio::spawn(async {});",
        "tokio::task::spawn_blocking(|| ());",
        "let mut set = tokio::task::JoinSet::new(); set.spawn(async {});",
        "tokio::runtime::Handle::current().spawn(async {});",
        "tokio::runtime::Handle::current().spawn_blocking(|| ());",
        "std::thread::spawn(|| ());",
        "let _ = std::thread::Builder::new().spawn(|| ());",
        "std::thread::scope(|scope| { scope.spawn(|| ()); });",
    ]
    for index, body in enumerate([
        "acyclic_grpc_observability::task::spawn(async {});", *samples,
    ]):
        source = temporary / f"probe{index}.rs"
        source.write_text(f"pub fn probe() {{ {body} }}\n")
        run(f"spawn-policy-{index}", [
            "clippy-driver", "--edition=2024", "--crate-type=lib", "--emit=metadata",
            "--out-dir", str(temporary), "-Dclippy::disallowed_methods",
            *dependencies, str(source),
        ], env=env, rejected=index != 0)

# Compile the actual filesystem synchronization shim in both dedicated model
# domains, using the already-built dependencies rather than rebuilding the
# workspace under different RUSTFLAGS. Its worker touches real loom state.
# The original regression routed loom's Builder into the std-only helper;
# compiling that mutation must fail the very same type check.
with tempfile.TemporaryDirectory(prefix="spawn-model-", dir=LOGS) as temporary:
    temporary = Path(temporary)
    shim = (ROOT / "rust/crates/filesystem/src/sync.rs").read_text()
    modeled = "thread::Builder::new().spawn(acyclic_grpc_observability::task::blocking(work))"
    assert shim.count(modeled) == 1
    mutant = shim.replace(modeled, "acyclic_grpc_observability::task::named_thread(thread::Builder::new(), work)")
    dependencies = ["-L", f"dependency={rlib('loom').parent}"]
    for crate in ["loom", "acyclic_grpc_observability"]:
        dependencies.extend(["--extern", f"{crate}={rlib(crate)}"])
    for domain in ["acyclic_fs_loom", "acyclic_fs_native_loom"]:
        for broken, source in [(False, shim), (True, mutant)]:
            name = f"{domain}-{'control' if broken else 'shim'}"
            path = temporary / (name + ".rs")
            path.write_text(source)
            binary = temporary / name
            command = [
                "rustc", "--edition=2024", "--test", "--cfg", domain,
                "--cfg", 'feature="native-mount"', *dependencies,
                str(path), "-o", str(binary),
            ]
            if broken:
                result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
                output = result.stdout + result.stderr
                (LOGS / (name + ".log")).write_text(output)
                assert result.returncode and "error[E0308]" in output, output
                assert "loom::thread::Builder" in output and "std::thread::Builder" in output, output
                print(f"{name}: original builder mismatch rejected", flush=True)
            else:
                run(name + "-build", command)
                output = run(name, [str(binary), "modeled_native_spawn_keeps_worker_in_scheduler"])
                assert "test result: ok. 1 passed;" in output, output

run("browser-check", [
    "cargo", "check", "--locked", "-p", "acyclic-harness", "-p", "acyclic-grpc-observability",
    "--lib", "--features", "acyclic-grpc-observability/task-context", "--target", "wasm32-unknown-unknown",
])
windows_check()
print("SCOPED_TRACING_QUALIFICATION_PASS", flush=True)
