//! Immutable announcement documents selected by the retained history's digest.
use super::{Error, Roster, WorkspaceEntry, WorkspaceTable};
use serde::Serialize;
use std::collections::BTreeMap;

/// One canonical document, admitted once and announced only as a new message.
pub trait AnnouncementDocument {
    /// Retained canonical bytes and their version-qualified digest.
    fn canonical(&self) -> (&[u8], [u8; 32]);

    /// Select one appended delta if the selected context lacks this document.
    /// A history window or checkpoint passes `None` when it dropped the file.
    fn announcement(&self, announced: Option<[u8; 32]>) -> Option<(&[u8], [u8; 32])> {
        let (bytes, digest) = self.canonical();
        (announced != Some(digest)).then_some((bytes, digest))
    }
}

impl AnnouncementDocument for Roster {
    fn canonical(&self) -> (&[u8], [u8; 32]) {
        Roster::canonical(self)
    }
}

/// Shared immutable document backing. Reading it never re-encodes or hashes.
#[derive(Clone, Debug)]
pub struct Announcement(std::sync::Arc<Canonical>);

#[derive(Debug)]
struct Canonical {
    bytes: Vec<u8>,
    digest: [u8; 32],
}

impl Announcement {
    /// Canonicalise an admitted document using its format's digest domain.
    pub fn new(document: &impl Serialize, domain: &'static str) -> Result<Self, Error> {
        let bytes = crate::contract::canonical_json_bytes(document)
            .map_err(|_| Error::Record("announcement does not encode"))?;
        let mut hasher = blake3::Hasher::new_derive_key(domain);
        hasher.update(&bytes);
        Ok(Self(std::sync::Arc::new(Canonical {
            bytes,
            digest: *hasher.finalize().as_bytes(),
        })))
    }
}

impl AnnouncementDocument for Announcement {
    fn canonical(&self) -> (&[u8], [u8; 32]) {
        (&self.0.bytes, self.0.digest)
    }
}

/// Actual absolute target paths for one source, supplied by mount admission.
/// An absent target means the workspace is not exposed on that target.
pub struct WorkspaceExposure {
    /// Original table identity, never a lookup by its model-facing name.
    pub source: super::WorkspaceSource,
    /// Absolute path by roster target name.
    pub paths: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct WorkspaceDocument<'a> {
    default: Option<&'a str>,
    workspaces: Vec<AnnouncedWorkspace<'a>>,
}

#[derive(Serialize)]
struct AnnouncedWorkspace<'a> {
    name: &'a str,
    kind: super::WorkspaceKind,
    mode: super::WorkspaceMode,
    inherit: super::WorkspaceInherit,
    paths: &'a BTreeMap<String, String>,
}

/// The version-one workspace announcement; source identities stay out of it.
#[derive(Clone, Debug)]
pub struct WorkspaceAnnouncement(Announcement);

impl WorkspaceAnnouncement {
    /// Admit one actual exposure per table entry. Work depends on workspace
    /// and target counts, never on the size of a tree workspace's subtree.
    pub fn new(
        table: &WorkspaceTable,
        roster: &Roster,
        exposures: Vec<WorkspaceExposure>,
    ) -> Result<Self, Error> {
        let mut by_source = BTreeMap::new();
        for exposure in &exposures {
            if by_source
                .insert(&exposure.source, &exposure.paths)
                .is_some()
            {
                return Err(Error::Record("workspace exposure repeats"));
            }
            for (target, path) in &exposure.paths {
                let target = roster
                    .targets()
                    .iter()
                    .find(|entry| &entry.name == target)
                    .ok_or(Error::Record("workspace target is not in the roster"))?;
                if !absolute(path, target.path_style) {
                    return Err(Error::Record("workspace target path is not absolute"));
                }
            }
        }
        if by_source.len() != table.entries().len() {
            return Err(Error::Record("workspace exposure does not match table"));
        }
        let workspaces = table
            .entries()
            .iter()
            .map(|entry: &WorkspaceEntry| {
                let paths = by_source
                    .get(&entry.source)
                    .ok_or(Error::Record("workspace source is not exposed"))?;
                Ok(AnnouncedWorkspace {
                    name: &entry.name,
                    kind: entry.kind,
                    mode: entry.mode,
                    inherit: entry.inherit,
                    paths,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self(Announcement::new(
            &WorkspaceDocument {
                default: table.entries().first().map(|entry| entry.name.as_str()),
                workspaces,
            },
            "acyclic.harness.workspaces.v1",
        )?))
    }
}

impl AnnouncementDocument for WorkspaceAnnouncement {
    fn canonical(&self) -> (&[u8], [u8; 32]) {
        self.0.canonical()
    }
}

fn absolute(path: &str, style: super::PathStyle) -> bool {
    if path.chars().any(char::is_control) {
        return false;
    }
    match style {
        super::PathStyle::Posix => path.starts_with('/'),
        super::PathStyle::Windows => {
            path.starts_with("\\\\")
                || path.starts_with("//")
                || matches!(path.as_bytes(), [drive, b':', b'/' | b'\\', ..] if drive.is_ascii_alphabetic())
        }
    }
}

#[cfg(test)]
mod tests;
