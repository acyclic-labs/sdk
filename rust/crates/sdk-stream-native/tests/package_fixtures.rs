use std::fs;
use std::path::Path;

const TARGETS: &[(&str, &str, &str, &str)] = &[
    (
        "win32-x64",
        "win32",
        "x64",
        "@acyclic-labs/stream-win32-x64",
    ),
    (
        "win32-arm64",
        "win32",
        "arm64",
        "@acyclic-labs/stream-win32-arm64",
    ),
    (
        "linux-x64-gnu",
        "linux",
        "x64",
        "@acyclic-labs/stream-linux-x64-gnu",
    ),
    (
        "linux-arm64-gnu",
        "linux",
        "arm64",
        "@acyclic-labs/stream-linux-arm64-gnu",
    ),
    (
        "darwin-x64",
        "darwin",
        "x64",
        "@acyclic-labs/stream-darwin-x64",
    ),
    (
        "darwin-arm64",
        "darwin",
        "arm64",
        "@acyclic-labs/stream-darwin-arm64",
    ),
];

#[test]
fn every_platform_fixture_is_loadable_shape_with_matching_binary_name() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("npm");
    for (target, os, cpu, package_name) in TARGETS {
        let package = match fs::read_to_string(root.join(target).join("package.json")) {
            Ok(value) => value,
            Err(_) => {
                assert!(false, "platform package fixture must be readable");
                continue;
            }
        };
        assert!(package.contains(&format!("\"name\":\"{package_name}\"")));
        assert!(package.contains("\"version\":\"0.2.0\""));
        assert!(package.contains("\"license\":\"Apache-2.0\""));
        assert!(package.contains(&format!("\"os\":[\"{os}\"]")));
        assert!(package.contains(&format!("\"cpu\":[\"{cpu}\"]")));
        assert!(package.contains("\"main\":\"index.js\""));
        assert!(package.contains("\"acyclic_stream_native.node\""));
        let loader = match fs::read_to_string(root.join(target).join("index.js")) {
            Ok(value) => value,
            Err(_) => {
                assert!(false, "platform loader fixture must be readable");
                continue;
            }
        };
        assert_eq!(
            loader.trim(),
            "module.exports = require(\"./acyclic_stream_native.node\");"
        );
    }
}
