//! Verify the platform package fixture shape for the native addon.

use std::fs;
use std::path::Path;

const TARGETS: &[&str] = &[
    "win32-x64",
    "win32-arm64",
    "linux-x64-gnu",
    "linux-arm64-gnu",
    "darwin-x64",
    "darwin-arm64",
];

#[test]
fn every_platform_fixture_uses_the_machines_native_binary_name() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("npm");
    for target in TARGETS {
        let package = match fs::read_to_string(root.join(target).join("package.json")) {
            Ok(value) => value,
            Err(_) => {
                assert!(false, "missing {target} package fixture");
                continue;
            }
        };
        assert!(package.contains("\"main\":\"index.js\""));
        assert!(package.contains("\"sdk_machines_native.node\""));
        let loader = match fs::read_to_string(root.join(target).join("index.js")) {
            Ok(value) => value,
            Err(_) => {
                assert!(false, "missing {target} loader fixture");
                continue;
            }
        };
        assert_eq!(
            loader.trim(),
            "module.exports = require(\"./sdk_machines_native.node\");"
        );
    }
}
