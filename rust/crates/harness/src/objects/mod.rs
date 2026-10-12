#![doc = include_str!("../../docs/objects.md")]
#![cfg_attr(
    test,
    allow(
        clippy::too_many_lines,
        reason = "one ordered scenario keeps each step next to the state it checks"
    )
)]

use crate::{
    Error, OperationId, Result,
    conversation::{
        Attachment, ContentGrant, ContentPublisher, ContentResidencyVerifier, FileDescriptor,
        FileRef, VolumeOperation, VolumeRef, validate_content_path,
    },
    core::{AuthorityVerifier, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier,
        ForkSelection, ResourceRevision,
    },
    resources::{ArtifactRef, ProviderRef},
    runtime::ContentBindings,
};
use acyclic_objects::v1::{ObjectsProvider, wire};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

mod store;
pub use store::ObjectContentStore;
mod mount;
pub use mount::ObjectContentMountResolver;
use store::{content_identity, storage};
#[cfg(test)]
mod tests;
