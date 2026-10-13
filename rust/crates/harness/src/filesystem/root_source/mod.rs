//! Authenticated same-graph source selection over the original Filesystem.
use super::{FilesystemHost, PreparationIo, fork_preparer::FilesystemRootBootstrapAllocation};
use crate::{
    Error, Result, TaskId,
    conversation::{VolumeClass, VolumeRef},
    core::Authority,
    executor::json::JsonBounds,
    executor::with_history_io,
    source_authorization::{
        ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
    },
};
use acyclic_fs::demand::DemandSource;
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore,
    WorkBudget, WorkCounters,
};
use std::sync::Arc;

mod lazy;
mod verify;
pub use lazy::FsRootNativeSource;

/// Creator-only project selection within one configured service graph.
pub struct FsRootProjectSourceProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    graph_root: TaskId,
    bootstrap: Option<Arc<FilesystemRootBootstrapAllocation<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FsRootProjectSourceProvider<A, O>
{
    /// Binds and shares the service policy with its optional original creator proof.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        owner: &Authority,
        graph_root: TaskId,
        bootstrap: Option<Arc<FilesystemRootBootstrapAllocation<A, O>>>,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        with_history_io(io, |work| host.provider.validate(work))?;
        if let Some(proof) = &bootstrap {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let work = io.and_then(PreparationIo::original_native_work).ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "Root bootstrap proof requires its original configured native ledger",
                })?;
                work.remaining()?;
                if proof.owner() != owner
                    || proof.witness.0.original_task() != graph_root
                    || proof.request.volume.class() != VolumeClass::Project
                    || !Arc::ptr_eq(&proof.host, &host)
                {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "Root bootstrap proof differs from this configured service",
                    });
                }
                if !proof.matches_native_work(work) {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "Root bootstrap proof requires its original configured native ledger",
                    });
                }
            }
            #[cfg(target_arch = "wasm32")]
            return Err(Error::Unsupported(
                "Root bootstrap proof requires original native work".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let owner = with_history_io(io, |work| match work.as_deref_mut() {
            Some(work) => work.clone_authority(owner),
            None => Ok(owner.clone()),
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let _native_custody = match io.filter(|io| io.original_native_work().is_some()) {
            Some(io) => {
                io.retain_owned(std::mem::size_of::<Self>() as u64)?;
                let bytes = io
                    .owned_bytes()?
                    .checked_sub(before)
                    .ok_or_else(|| io.invalid("root source custody underflows"))?;
                let custody = io.take_native_custody(bytes)?;
                custody.work().charge(WorkCounters {
                    allocation_operations: 1,
                    ..Default::default()
                })?;
                Some(custody)
            }
            None => None,
        };
        Ok(Arc::new(Self {
            host,
            owner,
            graph_root,
            bootstrap,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
        }))
    }
    fn native_source(&self, project: &VolumeRef) -> Option<&Arc<FsRootNativeSource<A, O>>> {
        self.bootstrap
            .as_ref()
            .filter(|proof| &proof.request.volume == project)
            .and_then(|proof| proof.native_source.as_ref())
    }
}
