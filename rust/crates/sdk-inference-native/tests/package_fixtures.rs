//! Verify the platform package fixture shape for the native Inference addon.

use std::fs;
use std::path::Path;

const TARGETS: &[(&str, &str, &str, Option<&str>, &str)] = &[
    ("win32-x64", "win32", "x64", None, "@acyclic-labs/inference-win32-x64"),
    ("win32-arm64", "win32", "arm64", None, "@acyclic-labs/inference-win32-arm64"),
    ("linux-x64-gnu", "linux", "x64", Some("glibc"), "@acyclic-labs/inference-linux-x64-gnu"),
    ("linux-x64-musl", "linux", "x64", Some("musl"), "@acyclic-labs/inference-linux-x64-musl"),
    ("linux-arm64-gnu", "linux", "arm64", Some("glibc"), "@acyclic-labs/inference-linux-arm64-gnu"),
    ("linux-arm64-musl", "linux", "arm64", Some("musl"), "@acyclic-labs/inference-linux-arm64-musl"),
    ("darwin-x64", "darwin", "x64", None, "@acyclic-labs/inference-darwin-x64"),
    ("darwin-arm64", "darwin", "arm64", None, "@acyclic-labs/inference-darwin-arm64"),
];

#[test]
fn every_platform_fixture_is_loadable_shape_with_matching_binary_name() -> std::io::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("npm");
    for (target, os, cpu, libc, package_name) in TARGETS {
        let package = fs::read_to_string(root.join(target).join("package.json"))?;
        assert!(package.contains(&format!("\"name\":\"{package_name}\"")));
        assert!(package.contains("\"version\":\"0.2.0\""));
        assert!(package.contains("\"license\":\"Apache-2.0\""));
        assert!(package.contains(&format!("\"os\":[\"{os}\"]")));
        assert!(package.contains(&format!("\"cpu\":[\"{cpu}\"]")));
        if let Some(libc) = libc {
            assert!(package.contains(&format!("\"libc\":[\"{libc}\"]")));
        }
        assert!(package.contains("\"main\":\"index.js\""));
        assert!(package.contains("\"acyclic_inference_native.node\""));
        let loader = fs::read_to_string(root.join(target).join("index.js"))?;
        assert_eq!(
            loader.trim(),
            "module.exports = require(\"./acyclic_inference_native.node\");"
        );
    }
    Ok(())
}
