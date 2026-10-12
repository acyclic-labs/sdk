//! Typed capture and publication of a child environment.

use crate::conversation::{
    FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef, decode_attachment_manifest,
};
use crate::core::Authority;
use crate::resources::{
    ArtifactRef, CheckpointRef, ContextRef, GenerationRef, ProviderRef, ResourceRef, StreamRef,
};
use crate::{AgentId, Capabilities, Error, OperationId, Result};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Largest portable count representable by the contract.
pub const MAX_FORK_AGENTS: usize = crate::conversation::MAX_PORTABLE_COUNT;
/// Maximum selected resources in one fork request or seed.
pub const MAX_FORK_RESOURCES: usize = crate::conversation::MAX_PORTABLE_COUNT;
/// Maximum references or grants in one fork.
pub const MAX_FORK_REFERENCES: usize = u32::MAX as usize;
/// Maximum aggregate attachment manifest bytes in one fork seed.
pub const MAX_FORK_ATTACHMENT_MANIFEST_BYTES: u64 = u64::MAX;
/// Maximum inherited context bytes in one fork preparation.
pub const MAX_FORK_INHERITED_BYTES: u64 = u64::MAX;
/// Maximum aggregate referenced file bytes in one fork seed.
pub const MAX_FORK_REFERENCE_BYTES: u64 = u64::MAX;
/// Largest sequence representable by the contract.
pub const MAX_FORK_INHERITED_MESSAGES: u64 = u64::MAX;

mod capture;
mod content;
mod grants;
mod history;
mod provider;
mod quota;
mod report;
mod request;
mod seed;
mod selection;
mod summary;
#[cfg(test)]
mod tests;
mod verification;
pub use summary::*;

use crate::store::history::{HistoryContentFuture, HistoryIo};
pub use capture::{Capture, CapturedResource, ForkOmission, ReferenceGrant, SharedGrant};
pub use content::StreamHistoryForkVerifier;
pub(in crate::fork) use grants::{decode_fork_manifest, reserve_reference_bytes};
pub use history::{InheritedConversationPrefix, PreparedForkHistory};
pub use provider::{ForkPublicationGuard, ForkSeedVerifier};
pub(crate) use provider::{
    ObservedForkPublicationGuard, admit_fork_future, boundary_error, release_fork_guard_with_work,
};
pub(in crate::fork) use provider::{
    acquire_fork_fence, frame_bytes, read_fork_manifest, release_frame,
};
pub(crate) use quota::CodecQuota;
pub use report::ForkReport;
pub use request::ForkRequest;
pub use seed::ForkSeed;
pub(crate) use seed::copy_seed;
pub(in crate::fork) use selection::validate_child_result_generation;
pub use selection::{
    AttestedBoundary, ForkCaptureProvider, ForkHistoryPolicy, ForkPreparation, ForkPreparer,
    ForkSelection, ResourceRevision,
};
pub use verification::CompositeForkVerifier;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::fork) use verification::admit_verifier_allocation;
