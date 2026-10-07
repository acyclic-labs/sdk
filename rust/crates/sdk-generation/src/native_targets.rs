//! Rust-owned native target metadata for generated SDK packages.
//!
//! The target list is declared in `actors-napi/Cargo.toml` under Cargo's
//! package metadata. This module only validates and serializes the metadata
//! returned by `cargo metadata`; it deliberately does not maintain a second
//! platform or architecture mapping.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::io;

pub const ACTORS_NAPI_PACKAGE: &str = "acyclic-actors-napi";
pub const ACTORS_NAPI_METADATA_PATH: &str = "rust/crates/actors-napi/Cargo.toml";
pub const NATIVE_TARGETS_SCHEMA: &str = "acyclic.actors.native-targets.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeTargetsArtifact {
    pub schema: String,
    pub package: String,
    pub version: String,
    pub source_path: String,
    pub source_revision: String,
    pub source_sha256: String,
    /// Raw Rust target triples consumed by the maintained NAPI-RS CLI.
    pub targets: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct NapiMetadata {
    targets: Vec<String>,
}

/// Extract the exact raw Rust triples from Cargo package metadata.
///
/// `cargo metadata` exposes package metadata as an arbitrary JSON value. The
/// strongly typed boundary begins at the NAPI metadata object so malformed,
/// empty, or duplicate target declarations fail generation before any native
/// package is built.
pub fn parse_targets(metadata: Option<&Value>) -> io::Result<Vec<String>> {
    let metadata = metadata.ok_or_else(|| {
        io::Error::other(format!(
            "Cargo package {ACTORS_NAPI_PACKAGE} is missing package.metadata"
        ))
    })?;
    let napi = metadata.get("napi").ok_or_else(|| {
        io::Error::other(format!(
            "Cargo package {ACTORS_NAPI_PACKAGE} is missing package.metadata.napi"
        ))
    })?;
    let napi: NapiMetadata = serde_json::from_value(napi.clone()).map_err(|error| {
        io::Error::other(format!(
            "Cargo package {ACTORS_NAPI_PACKAGE} has invalid package.metadata.napi: {error}"
        ))
    })?;
    if napi.targets.is_empty() {
        return Err(io::Error::other(format!(
            "Cargo package {ACTORS_NAPI_PACKAGE} declares no NAPI targets"
        )));
    }

    let mut seen = BTreeSet::new();
    for target in &napi.targets {
        if target.is_empty() || target.trim() != target || target.chars().any(char::is_whitespace) {
            return Err(io::Error::other(format!(
                "Cargo package {ACTORS_NAPI_PACKAGE} contains an invalid Rust target triple: {target:?}"
            )));
        }
        if !seen.insert(target) {
            return Err(io::Error::other(format!(
                "Cargo package {ACTORS_NAPI_PACKAGE} repeats NAPI target {target:?}"
            )));
        }
    }
    Ok(napi.targets)
}

pub fn artifact(
    version: impl Into<String>,
    source_revision: impl Into<String>,
    source_sha256: impl Into<String>,
    targets: Vec<String>,
) -> NativeTargetsArtifact {
    NativeTargetsArtifact {
        schema: NATIVE_TARGETS_SCHEMA.into(),
        package: ACTORS_NAPI_PACKAGE.into(),
        version: version.into(),
        source_path: ACTORS_NAPI_METADATA_PATH.into(),
        source_revision: source_revision.into(),
        source_sha256: source_sha256.into(),
        targets,
    }
}
