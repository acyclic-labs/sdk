use serde_json::Value;
use std::collections::BTreeSet;

#[cfg(test)]
#[path = "../tests/certification.rs"]
mod tests;

pub(crate) fn valid(
    receipt: &Value,
    executable_blake3: &str,
    os: &str,
    arch: &str,
    version: &str,
) -> bool {
    const COVERAGE: &[&str] = &[
        "create-read-write",
        "atomic-save",
        "rename-delete",
        "rename-before-hydration",
        "nested-paths",
        "large-directory-paging",
        "concurrent-handles",
        "watchers",
        "crash-detach-recovery",
        "mount-restoration",
        "hard-links",
        "symbolic-links-reparse-points",
        "metadata",
        "case-behavior",
        "escape-attempts",
        "root-checkout-untouched",
        "git-administration-untouched",
    ];
    const CASES: &[&str] = &[
        "real-mount-mutation-matrix",
        "crash-detach-recovery",
        "checkout-and-git-untouched",
    ];
    let (required_kind, provider_process_io_observable) = match os {
        "linux" => ("linux-fuse", true),
        "macos" => ("macos-nfs", true),
        "windows" => ("windows-projfs", false),
        _ => return false,
    };
    let Some(document) = receipt.as_object() else {
        return false;
    };
    let expected_keys = [
        "schema",
        "os",
        "arch",
        "coverage",
        "capability",
        "required_kind",
        "release_version",
        "executable_blake3",
        "passed",
        "cases",
    ];
    if document.len() != expected_keys.len()
        || expected_keys.iter().any(|key| !document.contains_key(*key))
        || receipt.get("schema").and_then(Value::as_str)
            != Some("acyclic-native-mount-qualification-v2")
        || receipt.get("os").and_then(Value::as_str) != Some(os)
        || receipt.get("arch").and_then(Value::as_str) != Some(arch)
        || receipt.get("required_kind").and_then(Value::as_str) != Some(required_kind)
        || receipt.get("release_version").and_then(Value::as_str) != Some(version)
        || receipt.get("executable_blake3").and_then(Value::as_str) != Some(executable_blake3)
        || receipt.get("passed").and_then(Value::as_bool) != Some(true)
    {
        return false;
    }
    let coverage = receipt.get("coverage").and_then(Value::as_array);
    if coverage.is_none_or(|values| {
        let observed = values
            .iter()
            .map(Value::as_str)
            .collect::<Option<BTreeSet<_>>>();
        observed.is_none_or(|observed| {
            observed.len() != values.len()
                || COVERAGE.iter().any(|expected| !observed.contains(expected))
        })
    }) {
        return false;
    }
    let capability = receipt.get("capability");
    if capability
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        != Some(required_kind)
        || capability
            .and_then(|value| value.get("available"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("writable"))
            .and_then(Value::as_bool)
            != Some(true)
        || capability
            .and_then(|value| value.get("provider_process_io_observable"))
            .and_then(Value::as_bool)
            != Some(provider_process_io_observable)
        || capability
            .and_then(|value| value.get("session_isolation"))
            .and_then(Value::as_str)
            != Some("SharedProcess")
        || capability
            .and_then(|value| value.get("unavailable_reason"))
            .is_none_or(|value| !value.is_null())
    {
        return false;
    }
    receipt
        .get("cases")
        .and_then(Value::as_array)
        .is_some_and(|cases| {
            let observed = cases
                .iter()
                .filter_map(|case| case.get("name").and_then(Value::as_str))
                .collect::<BTreeSet<_>>();
            observed.len() == cases.len()
                && CASES.iter().all(|expected| observed.contains(expected))
                && cases.iter().all(|case| {
                    case.get("status").and_then(Value::as_str) == Some("passed")
                        && case.get("reason").is_some_and(Value::is_null)
                })
        })
}
