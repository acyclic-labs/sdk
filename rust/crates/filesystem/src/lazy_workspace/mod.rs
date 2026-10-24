//! Source-backed sparse workspaces.

use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceNode, SourceNodeKind, SourceReference,
    SourceVersion,
};
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NamespacePath,
};
use crate::kernel::{KernelAccounting, KernelOwnedAllocation, Owned};
use crate::model::VolumeConfig;
use crate::path::PortablePath;
use crate::record_store::MAXIMUM_CAS_ATTEMPTS;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, FileId,
    ForkOptions, Fs, IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters,
    Workspace, WorkspaceError, WorkspaceId,
};
#[cfg(test)]
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use thiserror::Error;

macro_rules! accounted {
    ($ctx:expr, |$work:ident, $cancel:ident| $call:expr) => {{
        (async {
            let $cancel = cancellation($ctx);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some($work) = $ctx.native_work() {
                return crate::heap_future::in_heap_claimed($work, || async { $call.await })?
                    .await
                    .map_err(LazyWorkspaceError::from);
            }
            // Each expansion awaits the call twice; boxed, neither reserves
            // its future in the enclosing poll frame. The frame is paid for on
            // the ledger first and stays live while the receipt is accepted,
            // so the receipt's peak includes it.
            let (measured, reservation) =
                crate::heap_future::in_heap_budgeted(&mut *$ctx, |$work| $call)?.await;
            let accepted = accept_receipt(measured, $ctx);
            crate::heap_future::FrameLedger::release_frame(&mut *$ctx, reservation)?;
            accepted
        })
        .await
    }};
}

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: Option<crate::NativeAllocation>,
}

impl<A, O, D, S: Clone> LazyWorkspace<A, O, D, S> {
    /// Funds the shared handle before allocating it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn share(&self, ctx: &mut KernelAccounting<'_>) -> Result<Arc<Self>, LazyWorkspaceError> {
        let layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<Self>())
            .map_err(|_| crate::WorkError::Overflow)?
            .0;
        let allocation = match ctx.claim_owned(layout.pad_to_align().size() as u64, 1)? {
            KernelOwnedAllocation::NativeOriginal(guard) => Some(guard),
            KernelOwnedAllocation::Standalone => None,
        };
        Ok(Arc::new(Self {
            workspace: self.workspace.clone(),
            source: Arc::clone(&self.source),
            store: self.store.clone(),
            _allocation: allocation,
        }))
    }
}

mod model;
pub use model::LAZY_SNAPSHOT_DOMAIN;
pub(crate) use model::LAZY_STATE_SCHEMA;
pub use model::{LazyForkOrigin, LazyForkPhase};
mod store_node;
pub(crate) use store_node::MemoryLazyNode;
mod hash;
pub(crate) use hash::hash_lazy_value;
mod lookup;
mod store;
mod treap;
pub(crate) use lookup::InspectBasis;
mod read;
mod seek;
mod source_stream;
use source_stream::DemandBlobSource;
mod directory;
mod error;
mod fork;
mod publication_codec;
mod qualification;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use publication_codec::publication_evidence;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
mod selected_apply;
mod selected_commit;
mod selected_plan;
#[cfg(not(target_arch = "wasm32"))]
mod snapshot_write;
pub use qualification::NativePublicationSnapshot;
pub(crate) use qualification::NativeSnapshotContext;
pub use selected_plan::SelectedImportPlan;
mod binding;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
mod name_changes;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub use name_changes::{NativeNameChanges, NativeNameDelta};
mod exactify;
mod materialize;
mod overlay;
mod path;
mod promotion;
mod tombstones;
mod transaction;
mod work;
mod write;
pub use directory::{LazyDirectoryCursor, LazyDirectoryEntry, LazyDirectoryPage};
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
