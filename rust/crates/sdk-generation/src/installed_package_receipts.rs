//! Validation for the installed JavaScript/WASM package consumers.
//!
//! Package qualification is only complete when the executable consumer receipt
//! is consumed by the Rust gate.  This module keeps that check independent of
//! the JavaScript manifest writer so a stale or hand-edited receipt cannot bless
//! a different archive.

use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Read;
use std::path::Path;
use tar::Archive;

pub const INSTALLED_RECEIPT_SCHEMA: &str = "acyclic.sdk.installed-package-qualification.v1";

const REQUIRED_FILESYSTEM_CHECKS: &[&str] = &[
    "npm-install",
    "archive-only-extraction",
    "packaged-wasm-load",
    "memory-workspace-consumer",
    "negative-consumer-entrypoint",
    "negative-public-method-removal",
    "negative-wasm-tamper",
];
const REQUIRED_HARNESS_CHECKS: &[&str] = &[
    "npm-install",
    "archive-only-extraction",
    "packaged-wasm-load",
    "NativeContracts-consumer",
    "client-transport-conformance",
    "negative-consumer-entrypoint",
    "negative-public-method-removal",
    "negative-wasm-tamper",
];

#[derive(Debug, Deserialize)]
struct Receipt {
    schema: String,
    status: String,
    package: String,
    source_revision: String,
    archive: ArchiveEvidence,
    consumer: ConsumerEvidence,
}

#[derive(Debug, Deserialize)]
struct ArchiveEvidence {
    name: String,
    sha256: String,
    wasm_entry: String,
    wasm_sha256: String,
}

#[derive(Debug, Deserialize)]
struct ConsumerEvidence {
    status: String,
    checks: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct InstalledReceiptSummary {
    pub package: String,
    pub receipt: String,
    pub archive: String,
    pub archive_sha256: String,
    pub wasm_entry: String,
    pub wasm_sha256: String,
}

/// Validate both installed JS/WASM receipts against the actual package bytes.
///
/// `package_root` is the release package directory containing `filesystem/`
/// and `harness/`. `source_revision` is the Rust source identity already
/// computed by the generation gate.
pub fn validate_installed_package_receipts(
    package_root: &Path,
    source_revision: &str,
) -> Result<Vec<InstalledReceiptSummary>, String> {
    if !is_git_oid(source_revision) {
        return Err(format!(
            "invalid source revision for installed receipts: {source_revision}"
        ));
    }
    [
        (
            "filesystem",
            "acyclic-fs.tgz",
            "package/generated/wasm/acyclic_fs_wasm_bg.wasm",
            REQUIRED_FILESYSTEM_CHECKS,
        ),
        (
            "harness",
            "acyclic-harness.tgz",
            "package/generated/wasm/acyclic_harness_wasm_bg.wasm",
            REQUIRED_HARNESS_CHECKS,
        ),
    ]
    .into_iter()
    .map(|(family, archive_name, wasm_entry, required_checks)| {
        validate_one(
            package_root,
            source_revision,
            family,
            archive_name,
            wasm_entry,
            required_checks,
        )
    })
    .collect()
}

fn validate_one(
    package_root: &Path,
    source_revision: &str,
    family: &str,
    archive_name: &str,
    wasm_entry: &str,
    required_checks: &[&str],
) -> Result<InstalledReceiptSummary, String> {
    let directory = package_root.join(family);
    let receipt_path = directory.join("installed-consumer-receipt.json");
    let receipt: Receipt = serde_json::from_slice(
        &fs::read(&receipt_path)
            .map_err(|error| format!("read {}: {error}", receipt_path.display()))?,
    )
    .map_err(|error| format!("parse {}: {error}", receipt_path.display()))?;
    if receipt.schema != INSTALLED_RECEIPT_SCHEMA
        || receipt.status != "passed"
        || receipt.package != family
        || receipt.source_revision != source_revision
        || receipt.consumer.status != "passed"
    {
        return Err(format!(
            "installed {family} receipt identity/status is invalid"
        ));
    }
    for required in required_checks {
        if !receipt
            .consumer
            .checks
            .iter()
            .any(|check| check == required)
        {
            return Err(format!(
                "installed {family} receipt is missing check {required}"
            ));
        }
    }
    if receipt.archive.name != archive_name || Path::new(archive_name).file_name().is_none() {
        return Err(format!(
            "installed {family} receipt names an unexpected archive"
        ));
    }
    let archive_path = directory.join(archive_name);
    let archive_bytes = fs::read(&archive_path)
        .map_err(|error| format!("read {}: {error}", archive_path.display()))?;
    let archive_digest = sha256(&archive_bytes);
    if receipt.archive.sha256 != archive_digest {
        return Err(format!(
            "installed {family} receipt archive digest does not match bytes"
        ));
    }
    let wasm = wasm_entry_bytes(&archive_bytes, wasm_entry)
        .ok_or_else(|| format!("installed {family} archive is missing {wasm_entry}"))?;
    let wasm_digest = sha256(&wasm);
    if receipt.archive.wasm_entry != wasm_entry || receipt.archive.wasm_sha256 != wasm_digest {
        return Err(format!(
            "installed {family} receipt WASM evidence does not match bytes"
        ));
    }
    Ok(InstalledReceiptSummary {
        package: family.to_owned(),
        receipt: receipt_path.display().to_string(),
        archive: archive_path.display().to_string(),
        archive_sha256: archive_digest,
        wasm_entry: wasm_entry.to_owned(),
        wasm_sha256: wasm_digest,
    })
}

fn is_git_oid(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn wasm_entry_bytes(archive: &[u8], expected: &str) -> Option<Vec<u8>> {
    let decoder = GzDecoder::new(archive);
    let mut archive = Archive::new(decoder);
    let entries = archive.entries().ok()?;
    for entry in entries {
        let mut entry = entry.ok()?;
        let path = entry.path().ok()?.to_string_lossy().replace('\\', "/");
        if path == expected || path.ends_with(&format!("/{expected}")) {
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).ok()?;
            return Some(bytes);
        }
    }
    None
}
