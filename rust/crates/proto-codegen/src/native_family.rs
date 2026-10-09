//! Native packaging facts from maintained Cargo metadata and Rust-owned package metadata.
use cargo_metadata::{CargoOpt, CrateType, MetadataCommand};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::Path};

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct Napi {
    family: String,
    npm_package: String,
    package_path: String,
    companion_directory: String,
    wasm_package: String,
    wasm_output_name: String,
    wasm_target_directory: Option<String>,
    #[serde(default)]
    wasm_features: Vec<String>,
    #[serde(default)]
    wasm_no_default_features: bool,
    targets: Vec<String>,
    #[serde(default)]
    build_features: Vec<String>,
}

pub fn render(root: &Path, key: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let root = root.canonicalize()?;
    let metadata = MetadataCommand::new()
        .manifest_path(root.join("Cargo.toml"))
        .no_deps()
        .other_options(vec!["--locked".into(), "--offline".into()])
        .exec()?;
    if metadata.workspace_root.as_std_path().canonicalize()? != root {
        return Err("Cargo workspace physical identity differs".into());
    }
    let mut matches = Vec::new();
    for package in &metadata.packages {
        if package.source.is_none() {
            if let Some(value) = package.metadata.get("napi") {
                if value.get("family").and_then(Value::as_str) == Some(key) {
                    matches.push((package, serde_json::from_value::<Napi>(value.clone())?));
                }
            }
        }
    }
    if matches.len() != 1 {
        return Err("native family must have one Rust package owner".into());
    }
    let (package, napi) = matches.pop().ok_or("native family owner is missing")?;
    if napi.family.is_empty()
        || !napi
            .family
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c == b'-')
    {
        return Err("native family must be a lowercase package component".into());
    }
    if napi.companion_directory.is_empty()
        || !napi
            .companion_directory
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c == b'-')
    {
        return Err("companion directory must be a lowercase path component".into());
    }
    let wasm_owners: Vec<_> = metadata
        .packages
        .iter()
        .filter(|p| p.source.is_none() && p.name.as_str() == napi.wasm_package)
        .collect();
    if wasm_owners.len() != 1 || wasm_owners[0].version != package.version {
        return Err("WASM package owner or version differs".into());
    }
    let wasm_owner = wasm_owners[0];
    let wasm_libraries: Vec<_> = wasm_owner
        .targets
        .iter()
        .filter(|t| t.crate_types.contains(&CrateType::CDyLib))
        .collect();
    if wasm_libraries.len() != 1
        || napi
            .wasm_features
            .iter()
            .any(|f| !wasm_owner.features.contains_key(f))
    {
        return Err("WASM owner must declare one cdylib and its selected features".into());
    }
    if napi.wasm_output_name.is_empty()
        || !napi
            .wasm_output_name
            .bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        || napi.wasm_target_directory.as_ref().is_some_and(|s| {
            s.is_empty()
                || !s
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        })
    {
        return Err("WASM output and target directory must be path components".into());
    }
    let targets: BTreeSet<_> = napi.targets.iter().collect();
    if targets.len() != napi.targets.len()
        || targets.is_empty()
        || targets.iter().any(|t| t.is_empty())
    {
        return Err("Rust native target matrix must be nonempty and unique".into());
    }
    if napi
        .build_features
        .iter()
        .any(|f| !package.features.contains_key(f))
    {
        return Err("native binding feature is not declared by the Rust owner".into());
    }
    let libraries: Vec<_> = package
        .targets
        .iter()
        .filter(|t| t.crate_types.contains(&CrateType::CDyLib))
        .collect();
    if libraries.len() != 1 {
        return Err("native Rust owner must have one cdylib target".into());
    }
    let relative = |path: &Path| -> Result<String, Box<dyn std::error::Error>> {
        Ok(path
            .canonicalize()?
            .strip_prefix(&root)?
            .to_str()
            .ok_or("source path is not UTF-8")?
            .replace('\\', "/"))
    };
    let manifest = relative(package.manifest_path.as_std_path())?;
    let package_path = relative(&root.join(&napi.package_path))?;
    if package_path != napi.package_path {
        return Err("npm package path must be canonical and relative".into());
    }
    let npm: Value = serde_json::from_slice(&std::fs::read(
        root.join(&package_path).join("package.json"),
    )?)?;
    if npm.get("name").and_then(Value::as_str) != Some(napi.npm_package.as_str())
        || npm.get("version").and_then(Value::as_str) != Some(package.version.to_string().as_str())
    {
        return Err("Rust and npm package identity/version differ".into());
    }
    // Resolve the selected owner's declared features, not every workspace feature.
    // Cargo owns target-conditioned dependency selection; retain its whole target union.
    let mut roots = BTreeSet::new();
    for (selected, features, no_defaults) in [
        (package, &napi.build_features, false),
        (
            wasm_owner,
            &napi.wasm_features,
            napi.wasm_no_default_features,
        ),
    ] {
        let mut command = MetadataCommand::new();
        command
            .manifest_path(selected.manifest_path.as_std_path())
            .features(CargoOpt::SomeFeatures(features.clone()))
            .other_options(vec!["--locked".into(), "--offline".into()]);
        if no_defaults {
            command.features(CargoOpt::NoDefaultFeatures);
        }
        let resolved = command.exec()?;
        if resolved.workspace_root.as_std_path().canonicalize()? != root {
            return Err("resolved Cargo workspace physical identity differs".into());
        }
        let graph = resolved
            .resolve
            .as_ref()
            .ok_or("Cargo dependency graph is missing")?;
        let mut seen = BTreeSet::new();
        let mut pending = vec![selected.id.clone()];
        while let Some(id) = pending.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let owner = resolved
                .packages
                .iter()
                .find(|p| p.id == id)
                .ok_or("resolved Cargo package is missing")?;
            if owner.source.is_none() {
                roots.insert(relative(
                    owner
                        .manifest_path
                        .parent()
                        .ok_or("manifest parent is missing")?
                        .as_std_path(),
                )?);
            }
            let node = graph
                .nodes
                .iter()
                .find(|n| n.id == id)
                .ok_or("resolved Cargo node is missing")?;
            pending.extend(
                node.deps
                    .iter()
                    .filter(|d| {
                        d.dep_kinds
                            .iter()
                            .any(|k| k.kind != cargo_metadata::DependencyKind::Development)
                    })
                    .map(|d| d.pkg.clone()),
            );
        }
    }
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        ".cargo/config.toml",
        "package.json",
        "bun.lock",
        "tsconfig.base.json",
        "buf.yaml",
        "buf.lock",
        "buf.gen.yaml",
        "rust/crates/proto-codegen",
        "scripts/native-family.mjs",
        "scripts/build-native-family.mjs",
        "scripts/assemble-native-family.mjs",
        "scripts/archive-utils.mjs",
        "scripts/build-wasm.mjs",
        "scripts/generate.mjs",
        "scripts/check-generated.mjs",
        "scripts/generated-bindings.mjs",
        "scripts/ensure-bun.ps1",
        "scripts/ensure-bun.sh",
        "patches/napi-rs-cli-3.10.5.patch",
    ] {
        roots.insert(path.to_owned());
    }
    for path in [
        format!("scripts/build-{key}-native.mjs"),
        format!("scripts/assemble-{key}-native-package.mjs"),
        format!("scripts/generated/native-families/{key}.json"),
        format!("{package_path}/package.json"),
        format!("{package_path}/tsconfig.json"),
        format!("{package_path}/src"),
        format!("{package_path}/generated/proto"),
    ] {
        roots.insert(path);
    }
    Ok(
        json!({"schema":"acyclic.native-family.v1", "key":key, "rustPackageName":package.name.to_string(), "version":package.version.to_string(), "rustTarget":libraries[0].name, "manifestRelative":manifest, "packageRelative":format!("{package_path}/package.json"), "packageDirectory":package_path, "companionDirectory":napi.companion_directory, "wasmPackageName":napi.wasm_package, "wasm":{"artifact":wasm_libraries[0].name, "outName":napi.wasm_output_name, "targetDirectory":napi.wasm_target_directory, "features":napi.wasm_features, "noDefaultFeatures":napi.wasm_no_default_features}, "npmPackage":napi.npm_package, "targets":napi.targets, "features":napi.build_features, "buildScript":format!("scripts/build-{key}-native.mjs"), "sourceRoots":roots.into_iter().collect::<Vec<_>>() }),
    )
}
