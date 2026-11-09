//! Conditional publication and live snapshots of a client's session files.
use crate::{conversation::FileRef, resources::GenerationRef};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
    /// Original descriptor used unchanged as a replacement precondition.
    pub file: FileRef,
    /// UTF-8 contents at the snapshot's generation.
    pub text: String,
}

/// Requested files at one immutable volume generation; `None` means absent.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientFiles {
    /// Exact snapshot cut, also the next live query's wait condition.
    pub generation: GenerationRef,
    /// Only the explicitly requested paths, including absent paths.
    pub files: BTreeMap<String, Option<ClientFile>>,
}

/// One conditional UTF-8 replacement in an atomic session publication.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientFileChange {
    /// Relative path in the client's session volume.
    pub path: String,
    /// Original descriptor, or `None` to create an absent file.
    pub expected: Option<FileRef>,
    /// Complete replacement contents.
    pub text: String,
}
