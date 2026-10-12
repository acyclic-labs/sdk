use super::{RuntimeScope, TaskContext};
use crate::{
    Error, Result,
    conversation::{VolumeOperation, VolumeRef},
    durable_host::TaskJournalOwner,
};
use acyclic_fs::native_mount::{
    NativeMountCallbackFuture, NativeMountCallbackGuard, NativeMountError,
};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_stream::StreamProvider;
use std::sync::Arc;

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unauthorized,
        reason,
    }
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

/// Original dispatched task authority for canonical native volume preparation.
pub struct NativeVolumeCallbackGuard<P: StreamProvider> {
    original: Arc<(Arc<TaskJournalOwner<P>>, RuntimeScope, NativeAllocation)>,
}

struct VolumeCallback<P: StreamProvider> {
    original: Arc<(Arc<TaskJournalOwner<P>>, RuntimeScope, NativeAllocation)>,
    volume: VolumeRef,
    writable: bool,
    custody: NativeAllocation,
}

/// Revalidates the original native volume before publication.
pub trait NativeVolumePublicationGuard: Send + Sync {
    /// Checks the original owner and volume on its native work.
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}

impl TaskContext {
    /// Authenticates the actual SDK-issued fresh-dispatch context before copying its immutable scope.
    pub async fn volume_callback_guard_with_native_work<P: StreamProvider + 'static>(
        &self,
        owner: &Arc<TaskJournalOwner<P>>,
        work: &NativeMaterializationWork,
    ) -> Result<NativeVolumeCallbackGuard<P>> {
        let context = self;
        context.require_original_native_dispatch(work)?;
        if !self.native_metadata_matches(work) || !owner.native_custody.as_ref().is_some_and(|custody| custody.work().shares_allocation_context(work)) {
            return Err(denied(
                "native volume callback lacks original shared metadata custody",
            ));
        }
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        if owner.task_binding() != (task, fence) {
            return Err(denied(
                "native volume callback differs from the original task and full fence",
            ));
        }
        owner.require_task_context_binding(
            context,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        )?;
        owner.require_mail_scope(context.scope(), &mut Some(&mut crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)))?;
        owner
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;

        let bytes = (std::mem::size_of::<NativeVolumeCallbackGuard<P>>()
            + std::mem::size_of::<(Arc<TaskJournalOwner<P>>, RuntimeScope, NativeAllocation)>()
            - std::mem::size_of::<RuntimeScope>()) as u64;
        let mut custody = work
            .claim(bytes, 0, acyclic_fs::WorkCounters::default())
            .map_err(native_error)?;
        let (scope, mut scope_custody) = context.scope().clone_with_native_work(work)?;
        if let Err(cause) = custody.absorb(&mut scope_custody) {
            drop(scope);
            return Err(native_error(cause));
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(native_error)?;
        Ok(NativeVolumeCallbackGuard {
            original: Arc::new((Arc::clone(owner), scope, custody)),
        })
    }
}

impl<P: StreamProvider + 'static> NativeVolumeCallbackGuard<P> {
    /// Selects one exact canonical preparation volume before any physical IO.
    pub fn for_volume(
        &self,
        owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Arc<dyn NativeMountCallbackGuard>,
        Arc<dyn NativeVolumePublicationGuard>,
    )> {
        self.require_volume_with_native_work(owner, volume, writable, work)?;
        let mut custody = work
            .claim(
                std::mem::size_of::<VolumeCallback<P>>() as u64,
                0,
                acyclic_fs::WorkCounters::default(),
            )
            .map_err(native_error)?;
        let mut observer = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        let volume = volume.copy(&mut Some(&mut observer))?;
        let mut volume_custody = observer.into_custody()?;
        if let Err(cause) = custody.absorb(&mut volume_custody) {
            drop(volume);
            return Err(native_error(cause));
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(native_error)?;
        let callback = Arc::new(VolumeCallback {
            original: Arc::clone(&self.original),
            volume,
            writable,
            custody,
        });
        Ok((callback.clone(), callback))
    }

    /// Borrows the same declaration checks without allocating a callback that preparation would immediately discard.
    pub(crate) fn require_volume_with_native_work(
        &self,
        owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default()).map_err(native_error)?;
        if !std::ptr::eq(owner, self.original.0.as_ref())
            || owner.task_binding() != self.original.0.task_binding()
            || !self.original.2.work().shares_ledger(work)
        {
            return Err(denied(
                "native preparation differs from its original callback owner or work family",
            ));
        }
        require_volume(&self.original, volume, writable, work)
    }
}

fn require_volume<P: StreamProvider>(
    original: &(Arc<TaskJournalOwner<P>>, RuntimeScope, NativeAllocation),
    volume: &VolumeRef,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    if !io.contains_capability(original.1.grants(), volume, "volume:read:")?
        || (writable && !io.contains_capability(original.1.grants(), volume, "volume:write:")?)
    {
        return Err(denied(
            "native callback volume policy differs from its original declared scope",
        ));
    }
    original.0.require_volume_grant(
        volume,
        VolumeOperation::Read,
        &mut Some(&mut crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        )),
    )?;
    if writable {
        original.0.require_volume_grant(
            volume,
            VolumeOperation::Write,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )?;
    }
    Ok(())
}

impl<P: StreamProvider + 'static> VolumeCallback<P> {
    async fn verify_with_native_work(
        &self,
        work: &NativeMaterializationWork,
        require_write: bool,
    ) -> Result<()> {
        if require_write && !self.writable {
            return Err(denied(
                "read-only native callback cannot authorize authored writes",
            ));
        }
        work.charge(WorkCounters::default()).map_err(native_error)?;
        if !self.custody.work().shares_allocation_context(work) {
            return Err(denied(
                "native callback differs from its original per-volume work view",
            ));
        }
        require_volume(&self.original, &self.volume, self.writable, work)?;
        self.original
            .0
            .verify(
                false,
                &mut Some(&mut crate::store::HistoryWork::new(
                    crate::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;
        require_volume(&self.original, &self.volume, self.writable, work)
    }

    fn sdk_check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
        require_write: bool,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, move || async move {
            self.verify_with_native_work(work, require_write).await
        })
    }
}

impl<P: StreamProvider + 'static> NativeVolumePublicationGuard for VolumeCallback<P> {
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || self.verify_with_native_work(work, false))
    }
}

impl<P: StreamProvider + 'static> NativeMountCallbackGuard for VolumeCallback<P> {
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.sdk_check(work, false)
    }

    fn check_write<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.sdk_check(work, true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_host::tests::created_resource_access::CreationFixture;

    fn work(cancellation: &acyclic_fs::CancellationToken) -> NativeMaterializationWork {
        let units = 16_384;
        let bytes = 16 * 1_024 * 1_024;
        NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(units, bytes),
            cancellation,
        )
        .expect("original finite fixture ledger")
    }

    #[tokio::test]
    async fn genuine_read_metadata_cannot_issue_preparation_callbacks() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation);
        let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
        let owner = fixture
            .host
            .journal_owner_with_native_work(fixture.root, &fence, &original)
            .await?
            .into_native_shared(&original)?;
        let (context, custody) = fixture
            .harness
            .durable_context_with_native_work(
                fixture.root,
                fixture.root_lease.operation.operation_id,
                &original,
            )
            .await?;
        let context = context.into_native_shared(custody, &original)?;
        let before = original.receipt();
        assert!(matches!(
            context
                .volume_callback_guard_with_native_work(&owner, &original)
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
        assert_eq!(
            original.receipt(),
            before,
            "READ rejection precedes copies and owner IO"
        );

        let foreign = work(&acyclic_fs::CancellationToken::new());
        let before = foreign.receipt();
        assert!(matches!(
            context
                .volume_callback_guard_with_native_work(&owner, &foreign)
                .await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));
        assert_eq!(
            foreign.receipt(),
            before,
            "a funded foreign family is not an issued source"
        );

        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(
            context
                .volume_callback_guard_with_native_work(&owner, &original)
                .await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        assert_eq!(
            original.receipt(),
            before,
            "the original cancellation precedes owner IO"
        );
        Ok(())
    }
}
