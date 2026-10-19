#!/usr/bin/env python3
"""Compile the production catalog module for each constructor's target."""
import json
import pathlib
import subprocess
import tempfile
import time
import tomllib

repository = pathlib.Path(__file__).resolve().parent.parent
toolchain = tomllib.loads((repository / "rust-toolchain.toml").read_text())["toolchain"]["channel"]
source = (repository / "rust/crates/harness-providers/src/registry/catalog_update.rs").read_text()
guard = '#[cfg(not(target_arch = "wasm32"))]'
assert source.count(guard) == 4

def dependencies(target):
    """Use Cargo's real, target-specific dependencies for the included modules."""
    command = ["cargo", f"+{toolchain}", "check", "--locked", "-p",
               "acyclic-harness-providers", "--lib", "--tests", "--features", "browser",
               "--message-format=json"]
    if target:
        command += ["--target", target]
    libraries = {}
    directories = set()
    start = time.monotonic()
    with subprocess.Popen(command, cwd=repository, text=True,
                          stdout=subprocess.PIPE) as process:
        for line in process.stdout:
            message = json.loads(line)
            if message["reason"] == "compiler-message":
                print(message["message"]["rendered"], end="", flush=True)
            if message["reason"] == "compiler-artifact":
                for filename in message["filenames"]:
                    path = pathlib.Path(filename)
                    directories.add(path.parent)
                    if path.suffix == ".rmeta" and not message["profile"]["test"]:
                        libraries[message["target"]["name"]] = path
        assert process.wait() == 0, "catalog qualification dependencies failed to build"
    names = ["acyclic_harness", "acyclic_harness_providers", "serde", "serde_json"]
    if not target:
        names.append("keyring")
    arguments = []
    for name in names:
        arguments += ["--extern", f"{name}={libraries[name]}"]
    for directory in sorted(directories):
        arguments += ["-L", f"dependency={directory}"]
    print("qualification dependency build", target or "native",
          time.monotonic() - start, flush=True)
    return arguments


# Public ModelSpec consumers include harness integration fixtures. Compile every
# native test entry point, not only the provider library that defines the API.
subprocess.run(["cargo", f"+{toolchain}", "check", "--locked", "-p",
                "acyclic-harness", "--all-features", "--tests"],
               cwd=repository, check=True)
# Build time is separate from the bounded compiler proof, as with test suites.
native_dependencies = dependencies(None)
wasm_dependencies = dependencies("wasm32-unknown-unknown")
deadline = time.monotonic() + 8

with tempfile.TemporaryDirectory(prefix="catalog-target-") as directory:
    root = pathlib.Path(directory)
    module = root / "catalog_update.rs"
    module.write_text(source)
    for name in ["selection", "snapshot"]:
        (root / f"{name}.rs").write_text((repository /
            f"rust/crates/harness-providers/src/registry/{name}.rs").read_text())
    credentials = (repository /
        "rust/crates/harness-providers/src/credentials.rs").read_text()
    credentials_module = root / "credentials.rs"
    credentials_module.write_text(credentials)
    fixture = root / "target_contract.rs"
    # Credential selection and its Credentials type are production modules.
    # Only unrelated registry data and the login backend are substituted.
    fixture.write_text('''
use std::{collections::BTreeMap, sync::Arc};
use acyclic_harness::{Error, Result};
mod sync { pub use std::sync::Mutex; }
#[path = "credentials.rs"] mod credentials;
pub use credentials::Credentials;
#[cfg(not(target_arch = "wasm32"))]
struct CodexLogin;
#[cfg(not(target_arch = "wasm32"))]
impl CodexLogin { fn discover() -> Option<Self> { None } }
#[cfg(not(target_arch = "wasm32"))]
#[derive(PartialEq)]
enum Authentication { ApiKeyOrCodexLogin }
struct ProviderSpec {
    base_url: String,
    models: BTreeMap<String, ()>,
    token_counter: Option<()>,
    #[cfg(not(target_arch = "wasm32"))]
    authentication: Authentication,
}
// Endpoint behavior is covered by the provider suite; this fixture proves cfg/API wiring.
mod counting { pub mod wire {
    pub fn endpoint(_: &str, _: &str, _: &()) -> acyclic_harness::Result<String> {
        Ok(String::new())
    }
} }
struct Registry {
    providers: BTreeMap<String, Arc<ProviderSpec>>,
    default_model: Option<String>,
    credentials: Credentials,
    selection: snapshot::Snapshot<selection::Observations>,
    #[cfg(not(target_arch = "wasm32"))]
    codex_login: Option<Arc<CodexLogin>>,
}
fn invalid(message: impl Into<String>) -> Error { Error::Invalid(message.into()) }
fn validate_spec(_: &str, _: &ProviderSpec) -> Result<()> { Ok(()) }
#[cfg(not(target_arch = "wasm32"))]
fn codex_route(_: &ProviderSpec) -> bool { true }
#[path = "selection.rs"] mod selection;
#[path = "snapshot.rs"] mod snapshot;
#[path = "catalog_update.rs"] mod catalog_update;
pub fn exercise() -> Result<()> {
    let mut registry = Registry {
        providers: BTreeMap::new(), default_model: None,
        credentials: Credentials::explicit(BTreeMap::new()),
        selection: snapshot::Snapshot::default(),
        #[cfg(not(target_arch = "wasm32"))]
        codex_login: None,
    };
    registry.update_catalog(catalog_update::Changes::One(
        "fixture".into(), ProviderSpec {
            base_url: "https://fixture.invalid".into(),
            models: BTreeMap::new(), token_counter: Some(()),
            #[cfg(not(target_arch = "wasm32"))]
            authentication: Authentication::ApiKeyOrCodexLogin,
        }), None)?;
    match registry.selected_credential("fixture", &registry.providers["fixture"])? {
        selection::Credential::Key(key) => { assert!(!key.is_empty()); }
        #[cfg(not(target_arch = "wasm32"))]
        selection::Credential::Codex(login) => { assert_eq!(Arc::strong_count(&login), 1); }
        selection::Credential::Missing => {}
    }
    #[cfg(not(target_arch = "wasm32"))]
    registry.update_catalog(catalog_update::Changes::Many(BTreeMap::new()), None)?;
    Ok(())
}
''')

    def compile(label, wasm, error=None, contract=None):
        command = ["rustc", f"+{toolchain}", "--edition=2024", "--crate-type=lib",
                   "--emit=metadata", "-Dwarnings", str(contract or fixture), "--out-dir", str(root)]
        if wasm:
            command += ["--target", "wasm32-unknown-unknown"]
        command += wasm_dependencies if wasm else native_dependencies
        remaining = deadline - time.monotonic()
        assert remaining > 0, "catalog target proof exceeded eight seconds"
        start = time.monotonic()
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=remaining)
        print(label, time.monotonic() - start, result.returncode, result.stdout, flush=True)
        assert (result.returncode == 0) == (error is None)
        if error:
            assert error in result.stdout

    compile("actual-module WASM", True)
    module.write_text(source.replace(guard, ""))
    compile("native constructor control", False)
    compile("unguarded production variant negative", True,
            'variant `Many` is never constructed')
    module.write_text(source)
    assert credentials.count("fn codex_login(") == 1
    credentials_module.write_text(credentials.replace("fn codex_login(", "fn removed_login("))
    compile("production credential API drift negative", False,
            "no method named `codex_login`")
    # Exercise the real public types, including constructors outside this crate.
    model_contract = root / "model_contract.rs"
    model_source = """
pub fn model() -> acyclic_harness_providers::ModelSpec {
    acyclic_harness_providers::ModelSpec {
        max_context_tokens: None,
        capacity: acyclic_harness::context::ModelContextCapacity {
            context_tokens: 4096, output_tokens: Some(1024),
        },
        features: acyclic_harness_providers::ModelFeatures::default(),
    }
}
"""
    model_contract.write_text(model_source)
    compile("public ModelSpec native fixture", False, contract=model_contract)
    compile("public ModelSpec WASM fixture", True, contract=model_contract)
    model_contract.write_text(model_source.replace("Some(1024)", "1024"))
    compile("unverified output-limit fixture negative", False,
            "mismatched types", contract=model_contract)
    model_contract.write_text(model_source.replace("max_context_tokens: None,", ""))
    compile("missing catalog maximum fixture negative", False,
            "missing field `max_context_tokens`", contract=model_contract)
    assert time.monotonic() < deadline, "catalog target proof exceeded eight seconds"
