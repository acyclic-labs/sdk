use semver::{Version, VersionReq};
use serde_json::Value;
use std::{env, fs};

fn read(name: &str) -> Value {
    let path = env::var(name).expect("Bazel supplies the input path");
    serde_json::from_slice(&fs::read(path).expect("read audit input")).expect("valid audit JSON")
}

fn matches_any(version: &Version, ranges: &Value) -> bool {
    ranges
        .as_array()
        .expect("advisory ranges are an array")
        .iter()
        .any(|range| {
            VersionReq::parse(range.as_str().expect("advisory range is a string"))
                .expect("valid advisory version range")
                .matches(version)
        })
}

#[test]
fn resolved_crates_have_no_unpatched_advisories() {
    let index = read("ADVISORY_INDEX");
    let lock = read("CRATE_LOCK");
    let crates = lock["crates"].as_object().expect("resolved crate map");
    assert!(!crates.is_empty(), "resolved crate map must not be empty");
    let mut failures = Vec::new();
    for entry in crates.values() {
        let Some(url) = entry
            .pointer("/repository/Http/url")
            .and_then(Value::as_str)
        else {
            continue;
        };
        if !url.starts_with("https://static.crates.io/crates/") {
            continue;
        }
        let name = entry["name"].as_str().expect("crate name");
        let version = Version::parse(entry["version"].as_str().expect("crate version"))
            .expect("valid crate version");
        if let Some(advisories) = index.get(name) {
            for advisory in advisories.as_array().expect("advisories are an array") {
                if !matches_any(&version, &advisory["patched"])
                    && !matches_any(&version, &advisory["unaffected"])
                {
                    failures.push(format!(
                        "{} {version}: {}",
                        name,
                        advisory["id"].as_str().expect("advisory id")
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "unpatched RustSec advisories:\n{}",
        failures.join("\n")
    );
}

#[test]
fn version_requirements_follow_rust_semver() {
    let ranges = serde_json::json!([">= 1.8.4, < 1.9.0", ">= 1.13.1"]);
    assert!(matches_any(&Version::parse("1.8.4").unwrap(), &ranges));
    assert!(!matches_any(&Version::parse("1.9.0").unwrap(), &ranges));
    assert!(matches_any(&Version::parse("1.13.1").unwrap(), &ranges));
}
