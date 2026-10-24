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
        with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let owner = crate::core::copy_authority(owner, &mut quota)?;
            #[cfg(not(target_arch = "wasm32"))]
            if quota
                .work()
                .as_deref()
                .is_some_and(|io| io.original_native_work().is_some())
            {
                quota.retain(std::mem::size_of::<Self>() as u64)?;
                quota
                    .observer()?
                    .allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            }
            let _native_custody = quota.into_custody()?;
            Ok(Arc::new(Self {
                host,
                owner,
                graph_root,
                #[cfg(not(target_arch = "wasm32"))]
                _native_custody,
            }))
        })
    }
}
