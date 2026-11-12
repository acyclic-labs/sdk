//! Immutable announcement documents selected by the retained history's digest.
use super::{Error, Roster, WorkspaceEntry, WorkspaceTable};
use serde::{Deserialize, Serialize};
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

/// Independent documents carried by the shared appended-delta mechanism.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum AnnouncementKind {
    /// Execution targets and their home.
    Targets,
    /// Ordered visible workspace entries and their target paths.
    Workspaces,
}

/// Instructions discovered for a newly visible user slot, in discovery order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct WorkspaceInstructions {
    /// Stable slot whose instructions these are.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub entry: super::EntryId,
    /// Identity of the source and root whose instructions were discovered.
    /// Renaming a slot preserves it; replacing its source or root changes it.
    pub source: [u8; 32],
    /// Immutable admitted instruction files.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef[]"))]
    pub files: Vec<crate::conversation::FileRef>,
}

impl WorkspaceInstructions {
    /// Bind discovery to the selected repository, independently of its label.
    pub fn source_for(entry: &WorkspaceEntry) -> Result<[u8; 32], Error> {
        let source = Announcement::new(
            &(&entry.source, &entry.root),
            "acyclic.harness.workspace-instructions.v1",
            &[],
        )?;
        Ok(source.canonical().1)
    }
}

/// One immutable document announced at this conversation position.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct AnnouncedDocument {
    /// Which independently changing document this replaces.
    pub kind: AnnouncementKind,
    /// Semantic identity, including source changes with unchanged visible paths.
    pub digest: [u8; 32],
    /// The canonical document in the journal volume.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub file: crate::conversation::FileRef,
    /// Newly discovered instructions in this same delta.
    pub instructions: Option<AnnouncedInstructions>,
}

/// Fixed-size references to one immutable instruction delta. Neither the
/// provenance list nor its rendered bodies live in records or checkpoints.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct AnnouncedInstructions {
    /// Version-one provenance, used to recover the retained discovery frontier.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub manifest: crate::conversation::FileRef,
    /// Original instruction bodies, in discovery order, labelled by entry.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmAgentFileRef"))]
    pub content: crate::conversation::FileRef,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct InstructionManifest {
    pub(crate) version: u32,
    pub(crate) entries: Vec<WorkspaceInstructions>,
}

impl InstructionManifest {
    pub(crate) fn new(entries: Vec<WorkspaceInstructions>) -> crate::Result<Self> {
        let manifest = Self {
            version: 1,
            entries,
        };
        manifest.check()?;
        Ok(manifest)
    }

    fn check(&self) -> crate::Result<()> {
        if self.version != 1 {
            return Err(crate::Error::Invalid(
                "instruction manifest is not v1".into(),
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        if self.entries.iter().any(|entry| !ids.insert(entry.entry)) {
            return Err(crate::Error::Invalid(
                "workspace instructions repeat a slot".into(),
            ));
        }
        Ok(())
    }
}

impl AnnouncedInstructions {
    /// Load exactly the selected provenance, independently of current bindings.
    pub async fn entries(
        &self,
        reader: &dyn crate::conversation::ContentResidencyVerifier,
    ) -> crate::Result<Vec<WorkspaceInstructions>> {
        let bytes = reader
            .read(
                &self.manifest,
                self.manifest.descriptor().byte_length(),
                None,
                None,
            )?
            .await?;
        let manifest: InstructionManifest = crate::contract::json_from_slice(&bytes)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?;
        manifest.check()?;
        Ok(manifest.entries)
    }
}

impl AnnouncedDocument {
    /// Every exact file needed to replay this delta, without granting a directory.
    pub fn files(&self) -> impl Iterator<Item = &crate::conversation::FileRef> {
        std::iter::once(&self.file).chain(
            self.instructions
                .iter()
                .flat_map(|selection| [&selection.manifest, &selection.content]),
        )
    }
}

/// One appended message for this turn's changed documents and instructions.
pub async fn announcement_message(
    documents: &[AnnouncedDocument],
    reader: &dyn crate::conversation::ContentResidencyVerifier,
    render_bytes: Option<u64>,
) -> crate::Result<crate::model::ModelContent> {
    use crate::model::{FileProjectionPolicy, ModelContent, ModelContentPart};
    let mut parts = Vec::new();
    for document in documents {
        for file in std::iter::once(&document.file).chain(
            document
                .instructions
                .iter()
                .map(|selection| &selection.content),
        ) {
            // Retain the immutable selection's identity without requiring the
            // aggregate body to fit a single file/render allowance.
            parts.push(ModelContentPart::File {
                file: file.clone(),
                policy: FileProjectionPolicy::Reference,
            });
            let bytes = reader
                .read(file, file.descriptor().byte_length(), None, None)?
                .await?;
            let text = std::str::from_utf8(&bytes)
                .map_err(|_| crate::Error::Invalid("announcement is not UTF-8".into()))?;
            parts.extend(
                ModelContent::bounded_text(text, render_bytes)?
                    .parts()
                    .iter()
                    .cloned(),
            );
        }
    }
    if parts.len() == 1 {
        Ok(ModelContent::Part(parts.remove(0)))
    } else {
        Ok(ModelContent::Parts(parts))
    }
}

/// Discover one user entry's instruction files through the shared, pinned
/// volume discovery primitive. Entry roots are ordinary source directories.
pub async fn discover_workspace_instructions(
    entry: &WorkspaceEntry,
    reader: &dyn crate::conversation::ContentResidencyVerifier,
    policy: &crate::context::ContextDiscoveryPolicy,
    work: &mut crate::context::DiscoveryWork,
) -> crate::Result<WorkspaceInstructions> {
    if entry.role != super::WorkspaceRole::User {
        return Err(crate::Error::Invalid(
            "instructions require a user workspace".into(),
        ));
    }
    let root = crate::context::ContextRoot {
        volume: entry
            .volume()
            .map_err(|error| crate::Error::Invalid(error.to_string()))?
            .clone(),
        directory: entry.root.clone(),
    };
    let mut source = crate::context::VolumeDiscovery {
        reader,
        root: &root,
        io: None,
    };
    let files = crate::context::capture_instructions(
        &mut source,
        &root.directory,
        "",
        &policy.instruction_names,
        work,
        None,
    )
    .await?;
    Ok(WorkspaceInstructions {
        entry: entry.id,
        source: WorkspaceInstructions::source_for(entry)
            .map_err(|error| crate::Error::Invalid(error.to_string()))?,
        files: files
            .iter()
            .map(|instruction| (**instruction).clone())
            .collect(),
    })
}

/// A host's admitted workspace document and the instructions newly discovered
/// for this conversation. Storage and history selection belong to the shared host.
pub struct WorkspaceUpdate {
    /// Immutable canonical workspace document.
    pub document: WorkspaceAnnouncement,
    /// Instructions for slots not previously seen in this selected context.
    pub instructions: Vec<WorkspaceInstructions>,
}

impl WorkspaceUpdate {
    /// Shared discovery for a host's actual table and admitted target paths.
    /// Hosts without shell exposure pass null paths (or no targets), while
    /// file-only workspaces still receive the same instruction delta.
    pub async fn discover(
        entries: Vec<WorkspaceEntry>,
        roster: Option<&Roster>,
        exposures: Vec<WorkspaceExposure>,
        seen: &BTreeMap<super::EntryId, [u8; 32]>,
        reader: &dyn crate::conversation::ContentResidencyVerifier,
        policy: &crate::context::ContextDiscoveryPolicy,
        work: &mut crate::context::DiscoveryWork,
    ) -> crate::Result<Self> {
        let table = WorkspaceTable::new(entries).map_err(crate::Error::from)?;
        let document =
            WorkspaceAnnouncement::new(&table, roster, exposures).map_err(crate::Error::from)?;
        let mut instructions = Vec::new();
        for entry in table
            .entries()
            .iter()
            .filter(|entry| entry.role == super::WorkspaceRole::User)
        {
            let source = WorkspaceInstructions::source_for(entry).map_err(crate::Error::from)?;
            if seen.get(&entry.id) != Some(&source) {
                instructions
                    .push(discover_workspace_instructions(entry, reader, policy, work).await?);
            }
        }
        Ok(Self {
            document,
            instructions,
        })
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
    /// A fixed-width state identity distinguishes re-roots whose displayed paths
    /// stay unchanged. Targets pass an empty identity to retain their v1 digest.
    pub fn new(
        document: &impl Serialize,
        domain: &'static str,
        identity: &[u8],
    ) -> Result<Self, Error> {
        let bytes = crate::contract::canonical_json_bytes(document)
            .map_err(|_| Error::Record("announcement does not encode"))?;
        let mut hasher = blake3::Hasher::new_derive_key(domain);
        hasher.update(identity);
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
    pub entry: super::EntryId,
    /// Absolute path by roster target name.
    pub paths: BTreeMap<String, Option<String>>,
}

#[derive(Serialize)]
struct WorkspaceDocument<'a> {
    default: Option<&'a str>,
    workspaces: Vec<AnnouncedWorkspace<'a>>,
}

#[derive(Serialize)]
struct AnnouncedWorkspace<'a> {
    name: &'a str,
    id: super::EntryId,
    role: super::WorkspaceRole,
    mode: super::WorkspaceMode,
    inherit: super::WorkspaceInherit,
    paths: &'a BTreeMap<String, Option<String>>,
}

/// The version-one workspace announcement; source identities stay out of it.
#[derive(Clone, Debug)]
pub struct WorkspaceAnnouncement(Announcement);

impl WorkspaceAnnouncement {
    /// Admit one actual exposure per table entry. Work depends on workspace
    /// and target counts, never on the size of a tree workspace's subtree.
    pub fn new<'a>(
        table: &WorkspaceTable,
        roster: impl Into<Option<&'a Roster>>,
        exposures: Vec<WorkspaceExposure>,
    ) -> Result<Self, Error> {
        let mut by_entry = BTreeMap::new();
        let targets = roster
            .into()
            .into_iter()
            .flat_map(|roster| roster.targets())
            .map(|target| (target.name.as_str(), target.path_style))
            .collect::<BTreeMap<_, _>>();
        for exposure in &exposures {
            if by_entry.insert(exposure.entry, &exposure.paths).is_some() {
                return Err(Error::Record("workspace exposure repeats"));
            }
            for (target, path) in &exposure.paths {
                let style = targets
                    .get(target.as_str())
                    .ok_or(Error::Record("workspace target is not in the roster"))?;
                if path.as_ref().is_some_and(|path| !absolute(path, *style)) {
                    return Err(Error::Record("workspace target path is not absolute"));
                }
            }
        }
        let visible = table
            .entries()
            .iter()
            .filter(|entry| entry.role != super::WorkspaceRole::Internal);
        if by_entry.len() != visible.clone().count() {
            return Err(Error::Record("workspace exposure does not match table"));
        }
        let workspaces = visible
            .map(|entry: &WorkspaceEntry| {
                let paths = by_entry
                    .get(&entry.id)
                    .ok_or(Error::Record("workspace source is not exposed"))?;
                Ok(AnnouncedWorkspace {
                    name: &entry.name,
                    id: entry.id,
                    role: entry.role,
                    mode: entry.mode,
                    inherit: entry.inherit,
                    paths,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let table_state = Announcement::new(table, "acyclic.harness.workspace-table.v1", &[])?;
        Ok(Self(Announcement::new(
            &WorkspaceDocument {
                default: table.primary().map(|entry| entry.name.as_str()),
                workspaces,
            },
            "acyclic.harness.workspaces.v1",
            &table_state.canonical().1,
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
