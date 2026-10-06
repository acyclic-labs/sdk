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
fn every_filesystem_native_companion_fixture_is_installable() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../sdk-filesystem-native/npm");
    for (target, platform, architecture) in TARGETS {
        let package_dir = root.join(target);
        let package: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(package_dir.join("package.json"))
                .unwrap_or_else(|error| panic!("missing {target} package fixture: {error}")),
        )
        .unwrap_or_else(|error| panic!("invalid {target} package fixture: {error}"));
        assert_eq!(package["name"], format!("@acyclic-labs/fs-{target}"));
        assert_eq!(package["version"], "0.2.0");
        assert_eq!(package["os"][0], *platform);
        assert_eq!(package["cpu"][0], *architecture);
        assert_eq!(package["main"], "index.js");
        assert!(package["files"].as_array().is_some_and(|files| {
            files.iter().any(|file| file == "index.js")
                && files.iter().any(|file| file == "acyclic-fs.node")
        }));
        assert_eq!(
            fs::read_to_string(package_dir.join("index.js"))
                .unwrap_or_else(|error| panic!("missing {target} loader fixture: {error}"))
                .trim(),
            "module.exports = require(\"./acyclic-fs.node\");"
        );
    }
}
