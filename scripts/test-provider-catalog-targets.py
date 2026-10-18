#!/usr/bin/env python3
"""Compile the production catalog module for each constructor's target."""
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
deadline = time.monotonic() + 8

with tempfile.TemporaryDirectory(prefix="catalog-target-") as directory:
    root = pathlib.Path(directory)
    module = root / "catalog_update.rs"
    module.write_text(source)
    for name in ["selection", "snapshot"]:
        (root / f"{name}.rs").write_text((repository /
            f"rust/crates/harness-providers/src/registry/{name}.rs").read_text())
    fixture = root / "target_contract.rs"
    # Only dependencies are substituted: the enum, iterators, validation and
    # transaction below are compiled directly from the production module.
    fixture.write_text('''
use std::{collections::BTreeMap, sync::Arc};
type Result<T> = std::result::Result<T, String>;
mod sync { pub use std::sync::Mutex; }
mod credentials {
    pub struct KeychainDecode;
    impl KeychainDecode { pub fn error(&self) -> String { "credential".into() } }
}
struct Credentials;
impl Credentials {
    fn observe_key(&self, _: &str) -> std::result::Result<Option<String>, credentials::KeychainDecode> {
        Ok(None)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn allows_native_sources(&self) -> bool { false }
}
#[cfg(not(target_arch = "wasm32"))]
struct CodexLogin;
#[cfg(not(target_arch = "wasm32"))]
impl CodexLogin { fn discover() -> Option<Self> { None } }
#[cfg(not(target_arch = "wasm32"))]
#[derive(PartialEq)]
enum Authentication { ApiKeyOrCodexLogin }
struct ProviderSpec {
    models: BTreeMap<String, ()>,
    token_counter: Option<()>,
    #[cfg(not(target_arch = "wasm32"))]
    authentication: Authentication,
}
struct Registry {
    providers: BTreeMap<String, Arc<ProviderSpec>>,
    default_model: Option<String>,
    credentials: Credentials,
    selection: snapshot::Snapshot<selection::Observations>,
    #[cfg(not(target_arch = "wasm32"))]
    codex_login: Option<Arc<CodexLogin>>,
}
fn invalid(message: &str) -> String { message.into() }
fn validate_spec(_: &str, _: &ProviderSpec) -> Result<()> { Ok(()) }
#[cfg(not(target_arch = "wasm32"))]
fn codex_route(_: &ProviderSpec) -> bool { true }
#[path = "selection.rs"] mod selection;
#[path = "snapshot.rs"] mod snapshot;
#[path = "catalog_update.rs"] mod catalog_update;
pub fn exercise() -> Result<()> {
    let mut registry = Registry {
        providers: BTreeMap::new(), default_model: None,
        credentials: Credentials, selection: snapshot::Snapshot::default(),
        #[cfg(not(target_arch = "wasm32"))]
        codex_login: None,
    };
    registry.update_catalog(catalog_update::Changes::One(
        "fixture".into(), ProviderSpec {
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

    def compile(label, wasm, succeeds):
        command = ["rustc", f"+{toolchain}", "--edition=2024", "--crate-type=lib",
                   "--emit=metadata", "-Dwarnings", str(fixture), "--out-dir", str(root)]
        if wasm:
            command += ["--target", "wasm32-unknown-unknown"]
        remaining = deadline - time.monotonic()
        assert remaining > 0, "catalog target proof exceeded eight seconds"
        start = time.monotonic()
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=remaining)
        print(label, time.monotonic() - start, result.returncode, result.stdout, flush=True)
        assert (result.returncode == 0) == succeeds
        if not succeeds:
            assert 'variant `Many` is never constructed' in result.stdout

    compile("actual-module WASM", True, True)
    module.write_text(source.replace(guard, ""))
    compile("native constructor control", False, True)
    compile("unguarded production variant negative", True, False)
    assert time.monotonic() < deadline, "catalog target proof exceeded eight seconds"
