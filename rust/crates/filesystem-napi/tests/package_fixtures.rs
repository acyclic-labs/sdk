//! Verify the Rust-owned npm companion matrix for the filesystem addon.

use std::fs;
use std::path::Path;

const TARGETS: &[(&str, &str, &str)] = &[
    ("win32-x64", "win32", "x64"),
    ("win32-arm64", "win32", "arm64"),
    ("linux-x64", "linux", "x64"),
    ("linux-arm64", "linux", "arm64"),
    ("darwin-x64", "darwin", "x64"),
    ("darwin-arm64", "darwin", "arm64"),
];

#[test]
fn every_filesystem_native_companion_fixture_is_installable()
    -> Result<(), Box<dyn std::error::Error>>
{
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../sdk-filesystem-native/npm");
    for (target, platform, architecture) in TARGETS {
        let package_dir = root.join(target);
        let package: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(package_dir.join("package.json"))?)?;
        let expected_name = format!("@acyclic-labs/fs-{target}");
        assert_eq!(
            package.get("name").and_then(serde_json::Value::as_str),
            Some(expected_name.as_str())
        );
        assert_eq!(
            package.get("version").and_then(serde_json::Value::as_str),
            Some("0.2.0")
        );
        assert_eq!(
            package
                .get("os")
                .and_then(serde_json::Value::as_array)
                .and_then(|values| values.first())
                .and_then(serde_json::Value::as_str),
            Some(*platform)
        );
        assert_eq!(
            package
                .get("cpu")
                .and_then(serde_json::Value::as_array)
                .and_then(|values| values.first())
                .and_then(serde_json::Value::as_str),
            Some(*architecture)
        );
        assert_eq!(
            package.get("main").and_then(serde_json::Value::as_str),
            Some("index.js")
        );
        assert!(package
            .get("files")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|files| {
                files.iter().any(|file| file == "index.js")
                    && files.iter().any(|file| file == "acyclic-fs.node")
            }));
        assert_eq!(
            fs::read_to_string(package_dir.join("index.js"))?.trim(),
            "module.exports = require(\"./acyclic-fs.node\");"
        );
    }
    Ok(())
}
