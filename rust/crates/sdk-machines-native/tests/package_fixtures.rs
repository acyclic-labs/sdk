//! Verify the platform package fixture shape for the native addon.

use std::fs;
use std::path::Path;

const TARGETS: &[&str] = &[
    "win32-x64",
    "win32-arm64",
    "linux-x64-gnu",
    "linux-x64-musl",
    "linux-arm64-gnu",
    "linux-arm64-musl",
    "darwin-x64",
    "darwin-arm64",
];

#[test]
fn every_platform_fixture_uses_the_machines_native_binary_name() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("npm");
    for target in TARGETS {
        let package = fs::read_to_string(root.join(target).join("package.json"))
            .unwrap_or_else(|_| panic!("missing {target} package fixture"));
        assert!(package.contains("\"main\":\"index.js\""));
        assert!(package.contains("\"sdk_machines_native.node\""));
        let loader = fs::read_to_string(root.join(target).join("index.js"))
            .unwrap_or_else(|_| panic!("missing {target} loader fixture"));
        assert_eq!(
            loader.trim(),
            "module.exports = require(\"./sdk_machines_native.node\");"
        );
    }
}
