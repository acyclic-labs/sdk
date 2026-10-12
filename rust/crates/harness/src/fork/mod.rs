//! Typed capture and publication of a child environment.

use crate::conversation::{
    FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef,
    decode_attachment_manifest,
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

mod summary;
mod history;
mod provider;
mod verification;
mod grants;
mod content;
mod selection;
mod request;
mod capture;
mod report;
mod seed;
mod quota;
#[cfg(test)]
mod tests;
pub use summary::*;

use crate::store::history::{HistoryIo, HistoryContentFuture};
pub use history::{InheritedConversationPrefix, PreparedForkHistory};
pub use provider::{ForkSeedVerifier, ForkPublicationGuard};
pub use verification::{CompositeForkVerifier};
pub use content::StreamHistoryForkVerifier;
pub use selection::{ResourceRevision, ForkSelection, AttestedBoundary, ForkHistoryPolicy, ForkPreparation, ForkPreparer, ForkCaptureProvider};
pub use request::{ForkRequest};
pub use capture::{Capture, CapturedResource, ForkOmission, SharedGrant, ReferenceGrant};
pub use report::{ForkReport};
pub use seed::{ForkSeed};
pub(in crate::fork) use grants::{reserve_reference_bytes, decode_fork_manifest};
pub(in crate::fork) use provider::{frame_bytes, release_frame, acquire_fork_fence, read_fork_manifest};
pub(in crate::fork) use verification::{admit_native_verifier_share};
pub(in crate::fork) use selection::{validate_child_result_volume, validate_child_result_generation};
pub(crate) use seed::copy_seed;
pub(crate) use provider::{admit_fork_future, release_fork_guard_with_work, boundary_error, ObservedForkPublicationGuard};
pub(crate) use quota::CodecQuota;
