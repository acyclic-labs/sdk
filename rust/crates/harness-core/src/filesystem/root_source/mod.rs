//! Authenticated same-graph source selection over the original Filesystem.
use super::{FilesystemHost, PreparationIo};
use crate::{
    Error, Result, TaskId,
    conversation::VolumeClass,
    core::Authority,
    executor::with_history_io,
    source_authorization::{
        ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
    },
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::WorkCounters;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, LazySnapshotRef, WorkBudget};
use std::sync::Arc;

mod verify;

/// Creator-only project selection within one configured service graph.
pub struct FsRootProjectSourceProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    graph_root: TaskId,
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
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        with_history_io(io, |work| host.provider.validate(work))?;
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
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
        }))
    }
}
