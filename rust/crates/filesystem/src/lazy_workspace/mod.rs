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

macro_rules! accounted {
    ($ctx:expr, |$work:ident, $cancel:ident| $call:expr) => {{
        (async {
            let $cancel = cancellation($ctx);
            'sdk: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some($work) = $ctx.native_work() {
                    break 'sdk $call.await.map_err(LazyWorkspaceError::from);
                }
                let $work = $ctx.remaining()?;
                accept_receipt($call.await, $ctx)
            }
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
    /// Funds the shared handle before allocating it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_arc(
        mut self,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Arc<Self>, LazyWorkspaceError> {
        let layout = std::alloc::Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
            .extend(std::alloc::Layout::new::<Self>())
            .map_err(|_| crate::WorkError::Overflow)?
            .0;
        if let KernelOwnedAllocation::NativeOriginal(guard) =
            ctx.claim_owned(layout.pad_to_align().size() as u64, 1)?
        {
            self.allocation = Some(guard);
        }
        Ok(Arc::new(self))
    }
}

mod model;
pub(crate) use model::LAZY_STATE_SCHEMA;
use model::*;
pub use model::{LazyForkOrigin, LazyForkPhase};
mod store_node;
use store_node::copy_bytes;
pub(crate) use store_node::{MemoryLazyNode, clone_lazy_node};
mod hash;
pub(crate) use hash::hash_lazy_value;
mod store;
use store::*;
mod treap;
use treap::*;
mod lookup;
pub(crate) use lookup::InspectBasis;
use lookup::authored_stat;
mod read;
mod seek;
use read::*;
mod source_stream;
use source_stream::DemandBlobSource;
mod directory;
use directory::*;
mod error;
use error::*;
mod fork;
mod publication_codec;
mod qualification;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use publication_codec::publication_evidence;
mod selected_apply;
mod selected_commit;
mod selected_plan;
#[cfg(not(target_arch = "wasm32"))]
mod snapshot_write;
pub use qualification::NativePublicationSnapshot;
pub(crate) use qualification::NativeSnapshotContext;
pub(crate) use selected_apply::SelectedImportEffects;
pub use selected_plan::SelectedImportPlan;
mod binding;
use binding::*;
mod materialize;
use materialize::*;
mod transaction;
use transaction::*;
mod promotion;
mod tombstones;
mod write;
use promotion::*;
mod exactify;
use exactify::*;
mod overlay;
use overlay::*;
mod path;
use path::*;
mod work;
pub use directory::{LazyDirectoryCursor, LazyDirectoryEntry, LazyDirectoryPage, LazySeekTarget};
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
