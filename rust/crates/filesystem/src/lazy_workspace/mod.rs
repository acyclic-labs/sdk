//! Source-backed sparse workspaces.
//!
//! A lazy workspace keeps unresolved host state outside the authenticated
//! authored checkout. Forking copies only one immutable overlay identifier;
//! paths are observed individually through [`DemandSource`].

use crate::demand::{
    DemandError, DemandSource, SourceCursor, SourceDirectoryEntry, SourceNode, SourceNodeKind,
    SourceReference, SourceVersion,
};
use crate::heap_future::in_heap;
use crate::kernel::{
    FileKind, FileMetadata, FilePayload, FileRecord, LogicalName, MetadataField, NamespacePath,
};
use crate::model::VolumeConfig;
use crate::path::PortablePath;
use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, stored_revision};
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, CancellationToken, FileId,
    ForkOptions, Fs, IdempotencyKey, OperationReceipt, TransactionCommit, WorkBudget, WorkCounters,
    Workspace, WorkspaceDirectoryEntry, WorkspaceError, WorkspaceExtentKind, WorkspaceId,
    WorkspaceStat,
};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// A source-backed workspace whose unresolved paths remain outside its authored generation.
pub struct LazyWorkspace<A, O, D, S> {
    workspace: Workspace<A, O>,
    source: Arc<D>,
    store: S,
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
        }
    }
}


mod model;
use model::*;
mod store;
use store::*;
mod treap;
use treap::*;
mod lookup;
use lookup::*;
mod read;
use read::*;
mod directory;
use directory::*;
mod error;
use error::*;
mod snapshot;
use snapshot::*;
mod binding;
use binding::*;
mod materialize;
use materialize::*;
mod transaction;
use transaction::*;
mod promotion;
use promotion::*;
mod exactify;
use exactify::*;
mod overlay;
use overlay::*;
mod path;
use path::*;
mod work;
use work::*;
pub use model::{LazySnapshotId, LazySnapshotRef, LazyOverlayId, LazyWorkspaceState, LazyShadowId, LazyShadow, PendingLazyRemove, PendingLazyRemoveKind, LazyOverlay, LazyOverlayChange};
pub use store::{LazyWorkspaceStore, MemoryLazyWorkspaceStore};
pub use lookup::{LazyStat, LazyLookup};
pub use directory::{LazySeekTarget, LazyDirectoryEntry, LazyDirectoryCursor, LazyDirectoryPage};
pub use error::{LazyWorkspaceError};

#[cfg(test)]
mod tests;
