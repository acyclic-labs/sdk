//! Source-backed sparse workspaces.

use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceNode, SourceNodeKind, SourceReference,
    SourceVersion,
};
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NamespacePath,
};
use crate::model::VolumeConfig;
use crate::path::PortablePath;
use crate::record_store::MAXIMUM_CAS_ATTEMPTS;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, FileId,
    ForkOptions, Fs, IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters,
    Workspace, WorkspaceError, WorkspaceId, WorkspaceStat,
};
#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::NativeAllocation>,
}

impl<A, O, D, S> Clone for LazyWorkspace<A, O, D, S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            source: Arc::clone(&self.source),
            store: self.store.clone(),
            #[cfg(not(target_arch = "wasm32"))]
            allocation: None,
        }
    }
}

impl<A, O, D, S> LazyWorkspace<A, O, D, S> {
    /// Funds this shared handle before allocating its control and payload.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_arc(
        mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Arc<Self>, LazyWorkspaceError> {
        let layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<Self>())
            .map_err(|_| crate::WorkError::Overflow)?
            .0;
        let allocation = accounting.claim_owned(layout.pad_to_align().size() as u64, 1)?;
        if let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) =
            allocation
        {
            self.allocation = Some(allocation);
        }
        Ok(Arc::new(self))
    }
}

mod model;
use model::*;
pub use model::LAZY_SNAPSHOT_DOMAIN;
mod hash;
pub(crate) use hash::hash_lazy_value;
pub(crate) use model::LAZY_STATE_SCHEMA;
mod fork_state;
mod prepared_fork;
pub use fork_state::{CompletedSelectedImport, LazyForkOrigin, LazyForkPhase};
mod store;
use store::*;
mod store_node;
use store_node::copy_bytes;
pub(crate) use store_node::{MemoryLazyNode, clone_lazy_node};
mod treap;
use treap::*;
mod lookup;
pub(crate) use lookup::InspectBasis;
mod read;
mod seek;
mod source_stream;
use source_stream::*;
mod cursor;
use cursor::*;
mod binding;
mod directory;
mod error;
mod exactify;
mod materialize;
mod overlay;
mod path;
mod promotion;
mod publication_codec;
mod qualification;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use publication_codec::native_publication_evidence;
mod selected_apply;
mod selected_commit;
mod selected_plan;
pub(crate) use selected_apply::SelectedImportEffects;
pub use selected_plan::SelectedImportPlan;
mod snapshot;
pub use qualification::NativePublicationSnapshot;
pub(crate) use qualification::NativeSnapshotContext;
mod tombstones;
mod transaction;
mod work;
mod write;
pub use cursor::LazyDirectoryCursor;
pub use directory::{LazyDirectoryEntry, LazyDirectoryPage, LazySeekTarget};
pub use error::LazyWorkspaceError;
pub use lookup::{LazyLookup, LazyStat};
pub use model::{
    LazyOverlay, LazyOverlayChange, LazyOverlayId, LazyShadow, LazyShadowId, LazySnapshotId,
    LazySnapshotRef, LazyWorkspaceState, PendingLazyRemove, PendingLazyRemoveKind,
};
pub use store::{LazyWorkspaceStore, MemoryLazyWorkspaceStore};
pub(crate) use work::retained_directory_page_bytes;
use work::*;

#[cfg(test)]
mod tests;
