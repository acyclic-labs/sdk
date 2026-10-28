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
        ContentGrant, ContentPublisher, ContentResidencyVerifier, FileDescriptor, FileRef,
        VolumeOperation, VolumeRef, validate_content_path,
    },
    core::{AuthorityVerifier, Scope},
    resources::{ArtifactRef, ProviderRef},
    runtime::ContentBindings,
};
use acyclic_objects::{Blobs, ByteRange, Digest};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::collections::BTreeMap;
use std::sync::Arc;

mod store;
pub use store::ObjectContentStore;
mod mount;
pub use mount::ObjectContentMountResolver;
#[cfg(test)]
mod tests;
