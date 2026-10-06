//! Canonical Cargo source identity recipe shared by Rust SDK producers.

use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
pub fn normalized_build_recipe(
    source_root: &Path,
    metadata: &Value,
    build_target: Option<&str>,
) -> Result<Vec<u8>, String> {
    let source_root = source_root
        .canonicalize()
        .map_err(|error| format!("canonicalize source root: {error}"))?;
    let packages = metadata
        .get("packages")
        .and_then(Value::as_array)
        .ok_or("cargo metadata packages are missing")?;
    let package_by_id = packages
        .iter()
        .filter_map(|package| Some((package.get("id")?.as_str()?, package)))
        .collect::<BTreeMap<_, _>>();
    let nodes = metadata
        .pointer("/resolve/nodes")
        .and_then(Value::as_array)
        .ok_or("cargo metadata resolve.nodes are missing")?;
    let mut node_by_id = BTreeMap::new();
    for node in nodes {
        let id = node
            .get("id")
            .and_then(Value::as_str)
            .ok_or("cargo metadata node id is missing")?;
        node_by_id.insert(id, node);
    }
    let root_id = metadata
        .pointer("/resolve/root")
        .and_then(Value::as_str)
        .ok_or("cargo metadata resolve.root is missing")?;
    let root_package = package_by_id
        .get(root_id)
        .ok_or_else(|| format!("cargo metadata package is missing for {root_id}"))?;
    let root_label = format!(
        "{}@{}",
        root_package
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("unknown"),
        root_package
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
    );
    let mut reachable = BTreeSet::new();
    let mut pending = VecDeque::from([root_id]);
    while let Some(id) = pending.pop_front() {
        if !reachable.insert(id) {
            continue;
        }
        let node = node_by_id
            .get(id)
            .ok_or_else(|| format!("cargo metadata node is missing for {id}"))?;
        for dependency in node
            .get("dependencies")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            pending.push_back(dependency);
        }
    }
    let mut normalized = BTreeMap::new();
    for id in reachable {
        let package = package_by_id
            .get(id)
            .ok_or_else(|| format!("cargo metadata package is missing for {id}"))?;
        let node = node_by_id
            .get(id)
            .ok_or_else(|| format!("cargo metadata node is missing for {id}"))?;
        let manifest = package
            .get("manifest_path")
            .and_then(Value::as_str)
            .map(PathBuf::from)
            .and_then(|path| path.canonicalize().ok())
            .and_then(|path| path.strip_prefix(&source_root).ok().map(Path::to_owned))
            .map(|path| path.to_string_lossy().replace('\\', "/"));
        let key = format!(
            "{}@{}",
            package
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
            package
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        );
        let targets = package
            .get("targets")
            .and_then(Value::as_array)
            .map(|targets| {
                let mut targets = targets
                    .iter()
                    .map(|target| {
                        json!({
                            "name": target.get("name"),
                            "kind": target.get("kind"),
                            "crate_types": target.get("crate_types"),
                            "required_features": target.get("required_features"),
                        })
                    })
                    .collect::<Vec<_>>();
                targets.sort_by_key(Value::to_string);
                targets
            })
            .unwrap_or_default();
        normalized.insert(
            format!("{key}:{}", manifest.as_deref().unwrap_or("registry")),
            json!({
                "name": package.get("name"),
                "version": package.get("version"),
                "source": package
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or("local"),
                "manifest": manifest,
                "features": node.get("features"),
                "targets": targets,
                "dependencies": normalized_dependencies(package, &source_root)?,
            }),
        );
    }
    let recipe = json!({
        "schema": "acyclic.sdk.cargo-build-recipe.v1",
        "target": build_target,
        "root": root_label,
        "packages": normalized,
    });
    serde_json::to_vec(&recipe).map_err(|error| format!("encode Cargo build recipe: {error}"))
}

pub fn normalized_dependencies(package: &Value, source_root: &Path) -> Result<Vec<Value>, String> {
    let package_dir = package
        .get("manifest_path")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .and_then(|path| path.parent().map(Path::to_owned))
        .ok_or("cargo metadata package manifest path is missing")?;
    let mut dependencies = package
        .get("dependencies")
        .and_then(Value::as_array)
        .ok_or("cargo metadata package dependencies are missing")?
        .iter()
        .map(|dependency| {
            let path = dependency
                .get("path")
                .and_then(Value::as_str)
                .map(PathBuf::from)
                .map(|path| {
                    let path = if path.is_absolute() {
                        path
                    } else {
                        package_dir.join(path)
                    };
                    let path = path
                        .canonicalize()
                        .map_err(|error| format!("canonicalize dependency path: {error}"))?;
                    path.strip_prefix(source_root)
                        .map(|relative| relative.to_string_lossy().replace('\\', "/"))
                        .map_err(|_| {
                            format!("dependency path escapes source root: {}", path.display())
                        })
                })
                .transpose()?;
            Ok(json!({
                "name": dependency.get("name"),
                "source": dependency.get("source"),
                "req": dependency.get("req"),
                "kind": dependency.get("kind"),
                "rename": dependency.get("rename"),
                "optional": dependency.get("optional"),
                "uses_default_features": dependency.get("uses_default_features"),
                "features": dependency.get("features"),
                "target": dependency.get("target"),
                "registry": dependency.get("registry"),
                "path": path,
            }))
        })
        .collect::<Result<Vec<_>, String>>()?;
    dependencies.sort_by_key(Value::to_string);
    Ok(dependencies)
}

#[cfg(test)]
mod tests {
    use super::normalized_build_recipe;
    use serde_json::json;
    use std::fs;
    use std::path::Path;

    #[test]
    fn recipe_records_target_and_relative_local_dependency_features() {
        let root = std::env::temp_dir().join(format!("sdk-source-identity-{}", std::process::id()));
        let dep = root.join("dep");
        fs::create_dir_all(&dep).expect("create dependency");
        fs::write(
            root.join("Cargo.toml"),
            b"[package]\\nname = \"root\"\\nversion = \"1.0.0\"\\n",
        )
        .expect("write root manifest");
        fs::write(
            dep.join("Cargo.toml"),
            b"[package]\\nname = \"dep\"\\nversion = \"1.0.0\"\\n",
        )
        .expect("write dependency manifest");
        let root_id = "path+file:///snapshot/root#root@1.0.0";
        let dep_id = "path+file:///snapshot/dep#dep@1.0.0";
        let metadata = json!({
            "packages": [
                {"id": root_id, "name": "root", "version": "1.0.0", "manifest_path": root.join("Cargo.toml"), "source": null,
                 "dependencies": [{"name":"dep","source":null,"req":"*","kind":"normal","rename":null,"optional":true,"uses_default_features":false,"features":["grpc","http"],"target":null,"registry":null,"path":dep}],
                 "targets":[{"name":"root","kind":["lib"],"crate_types":["lib"],"required_features":[]}]},
                {"id": dep_id, "name": "dep", "version": "1.0.0", "manifest_path": dep.join("Cargo.toml"), "source": null, "dependencies": [], "targets": []}
            ],
            "resolve": {"root": root_id, "nodes": [{"id":root_id,"dependencies":[dep_id],"features":["http","grpc"]},{"id":dep_id,"dependencies":[],"features":[]}]}
        });
        let bytes = normalized_build_recipe(&root, &metadata, Some("wasm32-unknown-unknown"))
            .expect("recipe");
        let text = String::from_utf8(bytes).expect("utf8 recipe");
        assert!(text.contains("wasm32-unknown-unknown"));
        assert!(text.contains("dep/Cargo.toml"));
        assert!(text.contains("grpc"));
        assert!(!text.contains(dep.to_string_lossy().as_ref()));
        fs::remove_dir_all(root).expect("remove fixture");
    }

    #[test]
    fn recipe_changes_with_explicit_target() {
        let metadata = json!({
            "packages": [{"id":"root","name":"root","version":"1.0.0","manifest_path":"Cargo.toml","source":null,"dependencies":[],"targets":[]}],
            "resolve": {"root":"root","nodes":[{"id":"root","dependencies":[],"features":[]}]}
        });
        let host =
            normalized_build_recipe(Path::new("."), &metadata, Some("x86_64-pc-windows-msvc"))
                .expect("host recipe");
        let wasm =
            normalized_build_recipe(Path::new("."), &metadata, Some("wasm32-unknown-unknown"))
                .expect("wasm recipe");
        assert_ne!(host, wasm);
    }
}
