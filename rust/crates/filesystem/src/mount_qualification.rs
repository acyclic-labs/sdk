//! Shared release-artifact policy for native mount qualification receipts.
//!
//! A receipt is a report, not an authentication credential. Verification binds
//! its complete passing coverage to the caller's intended release artifact.
use io::Read as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

// Preserve the native verifier's existing input bound at the shared ingress.
const MAXIMUM_RECEIPT_BYTES: usize = 65_536;

/// Release identity supplied by the consumer, never inferred from the verifier.
#[derive(Clone, Copy)]
pub struct ReleaseTarget<'a> {
    /// Release operating system: linux, macos or windows.
    pub os: &'a str,
    /// Release architecture: `x86_64` or `aarch64`.
    pub arch: &'a str,
    /// Version of the intended release package.
    pub version: &'a str,
}

/// Mutation families required of every supported native mount backend.
pub const COVERAGE: &[&str] = &[
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

// Typed parsing rejects duplicate known fields before they can be collapsed by
// a generic JSON map. Top-level fields are exactly the emitted report schema;
// per-case timing and future capability metadata survive verified publication.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    schema: String,
    os: String,
    arch: String,
    coverage: Vec<String>,
    capability: Capability,
    required_kind: String,
    release_version: String,
    executable_blake3: String,
    passed: bool,
    cases: Vec<Case>,
}

#[derive(Deserialize, Serialize)]
struct Capability {
    kind: String,
    available: bool,
    writable: bool,
    provider_process_io_observable: bool,
    session_isolation: String,
    unavailable_reason: Value,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Deserialize, Serialize)]
struct Case {
    name: String,
    status: String,
    reason: Value,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

/// A failed receipt never grants native-mount certification.
#[derive(Debug, thiserror::Error)]
pub enum ReceiptError {
    /// The receipt or executable could not be read.
    #[error("read native mount receipt or release executable: {0}")]
    Read(#[from] io::Error),
    /// The receipt exceeds the existing native input bound.
    #[error("native mount receipt exceeds the existing 65536-byte input bound")]
    TooLarge,
    /// Required fields, field types or JSON syntax are invalid.
    #[error("parse native mount receipt: {0}")]
    Parse(#[from] serde_json::Error),
    /// The report does not qualify the supplied release artifact.
    #[error("native mount receipt does not match the release artifact")]
    Mismatch,
}

/// Parse and verify the full policy and hash the supplied executable, including
/// when its target differs from the verifier's platform. Return the verified
/// document so publication cannot accidentally reread a different report.
///
/// # Errors
///
/// Returns an error for unreadable inputs, oversized or malformed reports,
/// incomplete or failed coverage, and release identity or digest mismatches.
pub fn verify(
    receipt: impl io::Read,
    executable: impl io::Read,
    target: ReleaseTarget<'_>,
) -> Result<Value, ReceiptError> {
    let mut bytes = Vec::new();
    receipt
        .take((MAXIMUM_RECEIPT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAXIMUM_RECEIPT_BYTES {
        return Err(ReceiptError::TooLarge);
    }
    let report: Receipt = serde_json::from_slice(&bytes)?;
    let mut hash = blake3::Hasher::new();
    hash.update_reader(executable)?;
    if !valid(&report, &hash.finalize().to_hex(), target) {
        return Err(ReceiptError::Mismatch);
    }
    Ok(serde_json::to_value(report)?)
}

fn valid(report: &Receipt, digest: &str, target: ReleaseTarget<'_>) -> bool {
    let (kind, observable) = match target.os {
        "linux" => ("linux-fuse", true),
        "macos" => ("macos-nfs", true),
        "windows" => ("windows-projfs", false),
        _ => return false,
    };
    if !matches!(target.arch, "x86_64" | "aarch64") || target.version.is_empty() {
        return false;
    }
    let capability = &report.capability;
    let coverage = report
        .coverage
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let cases = report
        .cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<BTreeSet<_>>();
    report.schema == "acyclic-native-mount-qualification-v2"
        && report.os == target.os
        && report.arch == target.arch
        && report.required_kind == kind
        && report.release_version == target.version
        && report.executable_blake3 == digest
        && report.passed
        && coverage.len() == report.coverage.len()
        && COVERAGE.iter().all(|expected| coverage.contains(expected))
        && capability.kind == kind
        && capability.available
        && capability.writable
        && capability.provider_process_io_observable == observable
        && capability.session_isolation == "SharedProcess"
        && capability.unavailable_reason.is_null()
        && cases.len() == report.cases.len()
        && CASES.iter().all(|expected| cases.contains(expected))
        && report
            .cases
            .iter()
            .all(|case| case.status == "passed" && case.reason.is_null())
}
