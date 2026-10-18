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
    fixture = root / "target_contract.rs"
    # Only dependencies are substituted: the enum, iterators, validation and
    # transaction below are compiled directly from the production module.
    fixture.write_text('''
use std::{collections::BTreeMap, sync::Arc};
type Result<T> = std::result::Result<T, String>;
struct ProviderSpec { models: BTreeMap<String, ()> }
struct Registry {
    providers: BTreeMap<String, Arc<ProviderSpec>>,
    default_model: Option<String>,
}
fn invalid(message: &str) -> String { message.into() }
fn validate_spec(_: &str, _: &ProviderSpec) -> Result<()> { Ok(()) }
impl Registry {
    fn counts_input(&self, _: &str, _: &ProviderSpec) -> Result<bool> { Ok(true) }
}
#[path = "catalog_update.rs"] mod catalog_update;
pub fn exercise() -> Result<()> {
    let mut registry = Registry { providers: BTreeMap::new(), default_model: None };
    registry.update_catalog(catalog_update::Changes::One(
        "fixture".into(), ProviderSpec { models: BTreeMap::new() }), None)?;
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
