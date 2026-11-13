//! Conditional publication and live snapshots of an agent's client-visible files.
use crate::{
    conversation::{FileDescriptor, FileRef},
    resources::GenerationRef,
    supervision::Position,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub mod trace;

/// Versioned live source selected by `status/session-directory.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionDirectory {
    /// Must be one.
    pub version: u32,
    /// Original directory stream; access still requires its owner credential.
    pub source: acyclic_stream::StreamPath,
}

/// Original placement-directory records, committed with root creation.
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionDirectoryEntry {
    /// The directory exists, independently of its current root count.
    Created {
        /// Must be one.
        version: u32,
    },
    /// An independently created session root.
    Root {
        /// Must be one.
        version: u32,
        /// Original root identity.
        agent: crate::agent::AgentPath,
    },
}

/// A file root selected through the same authenticated local or remote client.
/// Private/home roots are the agent's original bindings, never caller-provided
/// volume identities. A UI session selects its own shared file volume.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileRoot {
    /// The agent's private files, including status and controls.
    Private {},
    /// The agent's writable home layer, including goals and configuration.
    Home {},
    /// A client's separately authored UI-session files.
    ClientSession {
        /// Stable session identity scoped to the selected agent.
        session: crate::OperationId,
    },
}

/// Stable volume identity of a client session on an admitted agent tree.
/// Constructing an identity grants no access to it.
pub fn volume(
    provider: crate::resources::ProviderRef,
    agent: &crate::agent::AgentPath,
    session: crate::OperationId,
) -> crate::Result<crate::conversation::VolumeRef> {
    use crate::conversation::{VolumeClass, VolumeOwner, VolumeRef};
    let agent = crate::AgentId::from_bytes(agent.id_bytes());
    VolumeRef::new(
        provider,
        format!("client-{agent}-{session}"),
        VolumeClass::SessionShared,
        VolumeOwner::Session(format!("{agent}:{session}")),
        &mut None,
    )
}

/// One immutable file selected by a session query.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientFile {
    /// Original authored file or immutable input to a computed view.
    pub source: FileSource,
    /// UTF-8 contents at the snapshot's selected cut.
    pub text: String,
}

/// Provenance of a file. A query never invents a filesystem generation or an
/// authored replacement precondition; creating an override uses `None`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileSource {
    /// An accepted original filesystem publication.
    Authored {
        /// Descriptor used unchanged as a replacement precondition.
        file: FileRef,
    },
    /// Pure projection of an immutable original stream record.
    Query {
        /// Versioned view implementation identity.
        view: String,
        /// Exact original input, never an observer timestamp.
        input: Position,
        /// Integrity of the selected projected bytes.
        descriptor: FileDescriptor,
    },
}

impl ClientFile {
    /// The authored replacement precondition, absent for a default view.
    pub fn original(&self) -> Option<&FileRef> {
        match &self.source {
            FileSource::Authored { file } => Some(file),
            FileSource::Query { .. } => None,
        }
    }

    /// Integrity descriptor shared by authored and computed values.
    pub fn descriptor(&self) -> &FileDescriptor {
        match &self.source {
            FileSource::Authored { file } => file.descriptor(),
            FileSource::Query { descriptor, .. } => descriptor,
        }
    }
}

/// Original input vector for an event-driven file view. Its size grows only
/// with the sources of explicitly requested files, never with agent count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileCut {
    /// Must be one.
    pub version: u32,
    /// Exact authored filesystem generation.
    pub generation: GenerationRef,
    /// Original authority minimum; reconnects must not regress behind it.
    pub revision: acyclic_stream::RevisionCut,
    /// Exclusive committed cuts for the requested query sources. JSON values
    /// are canonical decimal strings, preserving all 64 bits in JavaScript.
    #[serde(with = "acyclic_stream::serde_u64::map")]
    pub queries: BTreeMap<acyclic_stream::StreamPath, u64>,
}

impl FileCut {
    /// Whether the visible input vector is unchanged. A newer unrelated
    /// authority revision alone must not wake a file-view subscription.
    pub fn same_inputs(&self, other: &Self) -> bool {
        self.version == other.version
            && self.generation == other.generation
            && self.queries == other.queries
    }
}

/// One pure, demand-selected file value and its immutable input.
pub struct QueryFile {
    /// Requested relative path.
    pub path: String,
    /// Exact record used to derive these bytes.
    pub input: Position,
    /// UTF-8 projection; never a second authored publication.
    pub text: String,
    /// Media type of the projected UTF-8 value.
    pub media_type: String,
}

/// Shared file-view source. Construction does no IO and owns no subscription.
/// Hosts bind views once; work grows only with explicitly requested files.
pub trait FileView<P: acyclic_stream::StreamProvider + ?Sized>:
    acyclic_stream::ProviderPlatform
{
    /// Stable implementation identity, including its format version.
    fn identity(&self) -> &'static str;

    /// Select an original source for a requested file, without IO. `None`
    /// leaves the path entirely in the authored namespace.
    fn source(
        &self,
        agent: &crate::agent::AgentPath,
        root: FileRoot,
        path: &str,
    ) -> crate::Result<Option<acyclic_stream::StreamPath>>;

    /// Compute only these requested files from records strictly before the
    /// captured exclusive source cut. Known absence omits the requested path.
    fn read<'a>(
        &'a self,
        provider: &'a P,
        agent: &'a crate::agent::AgentPath,
        paths: &'a [&'a str],
        source: &'a acyclic_stream::StreamPath,
        through: u64,
        at_least: acyclic_stream::RevisionCut,
    ) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<Vec<QueryFile>>>;
}

/// Requested authored/default files at an immutable input cut; `None` is absent.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientFiles {
    /// File root selected by the request and resolved by the admitted host.
    pub root: FileRoot,
    /// Original bound volume; every returned descriptor belongs to it.
    pub volume: crate::conversation::VolumeRef,
    /// Exact snapshot cut, also the next live query's wait condition.
    pub cut: FileCut,
    /// Only the explicitly requested paths, including absent paths.
    pub files: BTreeMap<String, Option<ClientFile>>,
}

/// Bounded UTF-8 previews at one immutable generation. Previews are not
/// replacement preconditions and cannot be mistaken for complete files.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilePreviews {
    /// Exact snapshot cut and next live query's wait condition.
    pub generation: GenerationRef,
    /// Requested paths; absent files are distinct from empty prefixes.
    pub files: BTreeMap<String, Option<String>>,
}

/// One conditional UTF-8 replacement or deletion in an atomic publication.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientFileChange {
    /// Relative path under the selected file root.
    pub path: String,
    /// Original descriptor, or `None` to create an absent file.
    pub expected: Option<FileRef>,
    /// Complete replacement contents, or null to delete the expected file.
    /// Deletion requires an original descriptor; absent-file deletion is invalid.
    pub text: Option<String>,
}
