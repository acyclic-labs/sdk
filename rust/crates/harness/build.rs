//! Select the Loom configuration and register the data-driven integration tests.
use std::{collections::BTreeSet, io::Write as _, path::PathBuf};

#[path = "tests/support/scenario_inventory.rs"]
mod scenario_inventory;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ACYCLIC_LOOM=1 (CONTRIBUTING.md) opts Loom models in; the feature alone never does.
    println!("cargo:rerun-if-env-changed=ACYCLIC_LOOM");
    if std::env::var_os("ACYCLIC_LOOM").is_some()
        && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some()
    {
        println!("cargo:rustc-cfg=loom");
    }
    write_scenario_cases()
}

fn write_scenario_cases() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=tests/support/scenario_inventory.rs");
    println!("cargo:rerun-if-changed=tests/scenarios");
    let manifest = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is missing")?,
    );
    let root = manifest.join("tests/scenarios");
    let mut paths = Vec::new();
    scenario_inventory::discover(&root, &mut paths)?;
    paths.sort();
    if paths.is_empty() {
        return Err("no scenario fixtures discovered".into());
    }
    let out = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("OUT_DIR is missing")?);
    let mut output = std::io::BufWriter::new(std::fs::File::create(out.join("scenario_cases.rs"))?);
    writeln!(
        output,
        "acyclic_test_support::cases! {{\n    module = scenarios,\n    values: &str,\n    runner = super::run_scenario,\n    cases = ["
    )?;
    let mut names = BTreeSet::new();
    for path in paths {
        let relative = path
            .strip_prefix(&root)?
            .iter()
            .map(|component| component.to_str().ok_or("scenario path is not UTF-8"))
            .collect::<Result<Vec<_>, _>>()?
            .join("/");
        let stem = relative
            .strip_suffix(".json")
            .ok_or("scenario path is missing .json")?;
        let name: String = stem
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        if !names.insert(name.clone()) {
            return Err(format!("scenario test name collides for {relative}").into());
        }
        writeln!(output, "        case_{name} => {relative:?},")?;
    }
    writeln!(output, "    ],\n}}")?;
    output.flush()?;
    Ok(())
}
