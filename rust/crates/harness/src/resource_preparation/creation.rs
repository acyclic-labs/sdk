use super::*;
use crate::scheduler::LeaseFence;
#[cfg(feature = "filesystem")]
use acyclic_stream::{ProviderPlatform, ProviderTask, StreamProvider};
#[cfg(feature = "filesystem")]
use std::future::Future;

/// Original provenance retained by the allocation provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCreationOrigin {
    /// Actual coordinator owner.
    pub owner: Authority,
    /// Shared preparation and child parent.
    pub parent_task: TaskId,
    /// Admitted task owning the allocation.
    pub preparation_task: TaskId,
    /// Intended child operation.
    pub intended_child: OperationId,
    /// Complete retained admission digest.
    pub admission_digest: [u8; 32],
    /// Authenticated donor grants.
    pub source_grants: Capabilities,
    /// Complete request and child-scope digest.
    pub request_digest: [u8; 32],
    /// Original allocation fence.
    pub creation_fence: LeaseFence,
}

/// Owner-authenticated preparation request and lease.
#[cfg(feature = "filesystem")]
pub struct TaskResourcePreparation {
    pub(crate) owner: std::sync::Arc<dyn ResourcePreparationLease>,
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) origin: ResourceCreationOrigin,
    pub(crate) project_source: Option<crate::source_authorization::ProjectSourceReadHandle>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}
#[cfg(feature = "filesystem")]
impl TaskResourcePreparation {
    /// Immutable admitted input.
    pub const fn request(&self) -> &ResourcePreparationRequest {
        &self.request
    }
    /// Original allocation provenance.
    pub const fn origin(&self) -> &ResourceCreationOrigin {
        &self.origin
    }
    /// Explicit Project source authenticated by the owner.
    pub fn project_source(&self) -> Option<&crate::source_authorization::ProjectSourceReadHandle> {
        self.project_source.as_ref()
    }
    /// Live owner invocation cancellation token.
    pub fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.owner.cancellation()
    }
    /// Verifies the original lease; settlement permits observing cancelled work.
    pub fn verify<'a>(
        &'a self,
        settlement: bool,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<impl Future<Output = Result<()>> + ProviderTask + 'a> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            if self
                .native_custody
                .as_ref()
                .is_none_or(|guard| !guard.work().shares_allocation_context(work))
            {
                return Err(Error::NativeBoundary {
                    code: Unauthorized,
                    reason: "preparation lacks original native custody",
                });
            }
        }
        self.owner.verify_lease(settlement, io)
    }
}
#[cfg(feature = "filesystem")]
pub(crate) trait ResourcePreparationLease: ProviderPlatform {
    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken>;
    fn verify_lease<'a>(
        &'a self,
        settlement: bool,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
}
#[cfg(feature = "filesystem")]
impl<P: StreamProvider> ResourcePreparationLease for crate::durable_host::TaskJournalOwner<P> {
    fn cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        self.invocation_cancellation()
    }
    fn verify_lease<'a>(
        &'a self,
        settlement: bool,
        io: Option<&'a crate::filesystem::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(|io| io.original_native_work()) {
            if !self.native_shared_work_matches(work) {
                return Err(Error::NativeBoundary {
                    code: Unauthorized,
                    reason: "preparation lease uses another native ledger",
                });
            }
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
            let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
            self.verify(settlement, &mut work).await
        })
    }
}

pub(crate) trait CreatedTaskLease: acyclic_stream::ProviderPlatform {
    fn fence(&self) -> Result<crate::scheduler::LeaseFence>;
    fn verify<'a>(
        &'a self,
        io: Option<&'a crate::executor::PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
}
