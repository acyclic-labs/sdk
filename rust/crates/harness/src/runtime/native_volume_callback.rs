use super::TaskContext;
use crate::{
    Error, Result,
    conversation::{VolumeOperation, VolumeRef},
    durable_host::TaskJournalOwner,
    executor::PreparationIo,
};
use acyclic_fs::{
    NativeAllocation, NativeMaterializationWork, WorkCounters,
    native_mount::{NativeMountCallbackFuture, NativeMountCallbackGuard, NativeMountError},
};
use acyclic_stream::StreamProvider;
use std::sync::Arc;

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Unauthorized,
        reason,
    }
}

struct VolumeCallback<P: StreamProvider> {
    context: Arc<TaskContext>,
    owner: Arc<TaskJournalOwner<P>>,
    volume: VolumeRef,
    writable: bool,
    custody: NativeAllocation,
}

impl TaskContext {
    /// Authenticates and retains the original issued dispatch context.
    pub async fn verify_volume_owner<P: StreamProvider + 'static>(
        self: &Arc<Self>,
        owner: &Arc<TaskJournalOwner<P>>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_volume_owner(owner, work)?;
        let io = PreparationIo::from_native_work(work)?;
        owner.verify(false, &mut Some(&mut io.history_io())).await?;
        Ok(())
    }

    fn require_volume_owner<P: StreamProvider + 'static>(
        &self,
        owner: &TaskJournalOwner<P>,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.require_original_native_dispatch(work)?;
        if !self.native_metadata_matches(work) {
            return Err(denied(
                "native callback lacks original shared metadata custody",
            ));
        }
        let (task, fence) = self.owned_task_binding_with_native_work(work)?;
        if !owner.matches_task_binding(task, fence) {
            return Err(denied(
                "native callback differs from the original task and full fence",
            ));
        }
        Ok(())
    }

    /// Selects the exact volume and retains its callback on the caller ledger.
    pub fn volume_callback<P: StreamProvider + 'static>(
        self: &Arc<Self>,
        owner: &Arc<TaskJournalOwner<P>>,
        volume: &VolumeRef,
        writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<dyn NativeMountCallbackGuard>> {
        self.require_volume(owner, volume, writable, work)?;
        let io = PreparationIo::from_native_work(work)?;
        let mut custody = work.claim(
            std::mem::size_of::<VolumeCallback<P>>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        let volume = volume.copy(&mut Some(&mut io.history_io()))?;
        let retained = io.owned_bytes()?;
        let mut volume_custody = io.into_native_custody(retained)?;
        custody.absorb(&mut volume_custody)?;
        Ok(Arc::new(VolumeCallback {
            context: Arc::clone(self),
            owner: Arc::clone(owner),
            volume,
            writable,
            custody,
        }))
    }

    /// Checks the original task's declared volume authority without allocating a callback.
    pub(crate) fn require_volume<P: StreamProvider + 'static>(
        &self,
        owner: &TaskJournalOwner<P>,
        volume: &VolumeRef,
        writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default())?;
        if !self
            .native_custody
            .as_ref()
            .is_some_and(|custody| custody.work().shares_ledger(work))
        {
            return Err(denied(
                "native preparation differs from its original owner or work family",
            ));
        }
        self.require_volume_owner(owner, self.native_work()?)?;
        let io = PreparationIo::from_native_work(work)?;
        for operation in [VolumeOperation::Read]
            .into_iter()
            .chain(writable.then_some(VolumeOperation::Write))
        {
            let capability = volume.capability(operation, &mut Some(&mut io.history_io()))?;
            if !self.scope().grants().contains(&capability) {
                return Err(denied(
                    "native callback volume differs from its original scope",
                ));
            }
            owner.require_volume_grant(volume, operation, &mut Some(&mut io.history_io()))?;
        }
        Ok(())
    }
}

impl<P: StreamProvider + 'static> VolumeCallback<P> {
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
        require_write: bool,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, || async move {
            if require_write && !self.writable {
                return Err(denied("read-only callback cannot authorize writes"));
            }
            work.charge(WorkCounters::default())?;
            if !self.custody.work().shares_allocation_context(work) {
                return Err(denied(
                    "native callback differs from its original per-volume work",
                ));
            }
            self.context
                .require_volume(&self.owner, &self.volume, self.writable, work)?;
            let io = PreparationIo::from_native_work(work)?;
            self.owner
                .verify(false, &mut Some(&mut io.history_io()))
                .await?;
            self.context
                .require_volume(&self.owner, &self.volume, self.writable, work)
        })
    }
}

impl<P: StreamProvider + 'static> NativeMountCallbackGuard for VolumeCallback<P> {
    fn check<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.check(work, false)
    }
    fn check_write<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        self.check(work, true)
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
            .journal_owner(fixture.root, &fence, Some(&crate::executor::PreparationIo::from_native_work(&original)?))
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
        assert!(matches!(
            context.verify_volume_owner(&owner, &original).await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));

        let foreign = work(&acyclic_fs::CancellationToken::new());
        assert!(matches!(
            context.verify_volume_owner(&owner, &foreign).await,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                ..
            })
        ));

        cancellation.cancel();
        assert!(matches!(
            context.verify_volume_owner(&owner, &original).await,
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            })
        ));
        Ok(())
    }
}
