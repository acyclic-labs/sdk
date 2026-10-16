//! Historical planning uses released archives and Cargo-owned sources.
use cargo_metadata::{Metadata, TargetKind};
use sdk_docs::historical::{
    ArchiveSource, CapturedSource, OriginalLock, ProducerDependencyArchive, ProducerResolution,
    PublisherVcs, ReleasedPackage, ResolutionLock,
};
use sdk_docs::CommandOutput;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Component, Path};
use std::process::{Command, Stdio};

/// The current SDK registry remains the default qualification scope.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum Scope {
    #[default]
    CurrentSdk,
    HistoricalRelease {
        released_packages: Vec<ReleasedPackage>,
        archive_source_paths: Vec<String>,
    },
    RegistryArchives {
        released_packages: Vec<ReleasedPackage>,
        archive_source_paths: Vec<String>,
        captured_source: CapturedSource,
        source_root: String,
    },
}

impl Scope {
    pub fn is_current(&self) -> bool {
        matches!(self, Self::CurrentSdk)
    }

    pub fn archive_source_paths(&self) -> &[String] {
        match self {
            Self::CurrentSdk => &[],
            Self::HistoricalRelease {
                archive_source_paths,
                ..
            }
            | Self::RegistryArchives {
                archive_source_paths,
                ..
            } => archive_source_paths,
        }
    }
}

/// Verified archive inputs for one immutable historical source/version.
pub struct Plan {
    pub released_packages: Vec<ReleasedPackage>,
    pub source_files: BTreeMap<String, String>,
    pub archives: Vec<(std::path::PathBuf, RegistryVersion)>,
    pub captured_source: Option<CapturedSource>,
    pub owner_metadata: BTreeMap<String, Metadata>,
}

/// Cargo permits crates.io by default or when its registry name is explicit.
/// A private-only publish allowlist cannot establish a crates.io release owner.
fn crates_io_eligible(publish: Option<&[String]>) -> bool {
    publish.is_none_or(|registries| registries.iter().any(|registry| registry == "crates-io"))
}

/// Query every publishable same-version Cargo owner; missing registry versions
/// are excluded, while errors and missing archives for actual releases fail.
pub fn plan(
    metadata: &Metadata,
    root: &Path,
    revision: &str,
    version: &str,
    archive_dir: &Path,
) -> Result<Plan, String> {
    let mut result = Plan {
        released_packages: Vec::new(),
        source_files: BTreeMap::new(),
        archives: Vec::new(),
        captured_source: None,
        owner_metadata: BTreeMap::new(),
    };
    let mut expected = BTreeSet::new();
    for package in metadata.packages.iter().filter(|package| {
        package.version.to_string() == version && crates_io_eligible(package.publish.as_deref())
    }) {
        let Some(release) = registry_version_optional(package.name.as_ref(), version)? else {
            continue;
        };
        let name = format!("{}-{version}.crate", package.name);
        expected.insert(name.clone());
        let archive = archive_dir.join(name);
        let (identity, files) = verify_archive(metadata, root, revision, &release, &archive)?;
        result.source_files.extend(files);
        result.released_packages.push(identity);
        result.archives.push((archive, release));
    }
    if result.released_packages.is_empty() {
        return Err(
            "historical scope has no actual registry releases for this source/version".into(),
        );
    }
    validate_archive_inventory(archive_dir, &expected)?;
    result
        .released_packages
        .sort_by(|a, b| a.package.cmp(&b.package));
    Ok(result)
}

/// The fields read from the authoritative crates.io version endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegistryVersion {
    #[serde(rename = "crate")]
    pub package: String,
    pub num: String,
    pub checksum: String,
    pub yanked: bool,
}

mod archives;
mod corpus;
mod discovery;
mod examples;
mod probe;
mod producer;
mod registry;

pub use archives::*;
pub use corpus::*;
pub use discovery::*;
pub use examples::*;
pub use producer::*;
pub use registry::*;

fn validate_version(version: &str) -> Result<(), String> {
    if version.is_empty()
        || !version
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+'))
    {
        return Err("invalid registry version".into());
    }
    Ok(())
}
fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err("invalid registry package name".into());
    }
    Ok(())
}
fn command_output(command: &mut Command) -> Result<Vec<u8>, String> {
    let output = command.run_output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    Ok(output.stdout)
}

#[cfg(test)]
mod tests;
