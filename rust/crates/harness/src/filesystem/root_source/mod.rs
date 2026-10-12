//! Authenticated same-graph source selection over the original Filesystem.

use super::FilesystemHost;
use crate::contract::{JsonBounds, canonical_json_bytes_bounded};
use crate::conversation::{VolumeClass, VolumeRef};
use crate::core::Authority;
use crate::source_authorization::{
    ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
};
use crate::{Error, Result, TaskId};
use acyclic_fs::demand::DemandSource;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore,
    WorkBudget, WorkCounters,
};
use serde::Deserialize;
use std::sync::Arc;

mod lazy;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
mod verify;
pub use lazy::FsRootNativeSource;
/// Creator-only project selection within one explicitly configured service graph.
pub struct FsRootProjectSourceProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    graph_root: TaskId,
    bootstrap: Option<Arc<super::fork_preparer::FilesystemRootBootstrapAllocation<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FsRootProjectSourceProvider<A, O>
{
    /// Binds the service owner and graph root with optional metadata accounting.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        owner: &Authority,
        graph_root: TaskId,
        io: Option<&super::PreparationIo>,
    ) -> Result<Self> {
        let mut observer = io.map(super::PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        host.provider.validate(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io
            .map(super::PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        let owner = match work.as_deref_mut() {
            Some(work) => work.clone_authority(owner)?,
            None => owner.clone(),
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _native_custody =
            match io.filter(|io| io.original_native_work().is_some()) {
                Some(io) => {
                    io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                    Some(io.take_native_custody(
                        io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            super::fork_preparer_io::failure(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "root source custody underflows",
                            )
                        })?,
                    )?)
                }
                None => None,
            };
        Ok(Self {
            host,
            owner,
            graph_root,
            bootstrap: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
        })
    }

    /// Shares the paid policy; stdlib Arc backing is not SDK payload.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            ._native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root source policy belongs to another native ledger",
            });
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(Error::from)?;
        Ok(Arc::new(self))
    }

    /// Transfers only an already admitted Arc handle; all original record and Project payload remains guarded by the authentic creator token.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_bootstrap_source_with_native_work(
        mut self,
        proof: Arc<super::fork_preparer::FilesystemRootBootstrapAllocation<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        work.remaining()?;
        if proof.owner() != &self.owner
            || proof.witness.binding().original_task() != self.graph_root
            || proof.request.volume.class() != VolumeClass::Project
            || !Arc::ptr_eq(&proof.host, &self.host)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root bootstrap proof differs from this configured service",
            });
        }
        if !self
            ._native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_allocation_context(work))
            || !proof.matches_native_work(work)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Root bootstrap proof requires its original configured native ledger",
            });
        }
        self.bootstrap = Some(proof);
        Ok(self)
    }

    fn native_source(&self, project: &VolumeRef) -> Option<&FsRootNativeSource<A, O>> {
        self.bootstrap
            .as_ref()
            .filter(|proof| &proof.request.volume == project)
            .and_then(|proof| proof.native_source.as_ref())
    }
}
