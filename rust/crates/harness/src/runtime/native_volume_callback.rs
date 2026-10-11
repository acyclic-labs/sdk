use super::{NativeTaskContext, RuntimeScope};
use crate::{Error, Result, conversation::{VolumeOperation, VolumeRef}, durable_host::TaskJournalOwner};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use acyclic_fs::native_mount::{NativeMountCallbackFuture, NativeMountCallbackGuard,
    NativeMountError, NativeMountMeterWork};
use acyclic_stream::StreamProvider;
use std::sync::Arc;

fn denied(reason: &'static str) -> Error {
    Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason }
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

/// Original dispatched task authority for canonical native volume preparation.
///
/// The independent protected owner and immutable scope never retain a task
/// context, directly or through its working-view attachment. Sharing aliases
/// existing paid owner metadata; it does not reallocate that metadata or grant
/// another capability. Logical Self/deep capacity excludes opaque Arc backing.
pub struct NativeVolumeCallbackGuard<P: StreamProvider> {
    original: Arc<OriginalVolumeAuthority<P>>,
}

struct OriginalVolumeAuthority<P: StreamProvider> {
    owner: Arc<TaskJournalOwner<P>>,
    scope: RuntimeScope,
    // Both owned metadata and references drop before their original custody.
    custody: NativeAllocation,
}

struct VolumeCallback<P: StreamProvider> {
    original: Arc<OriginalVolumeAuthority<P>>,
    volume: VolumeRef,
    writable: bool,
    work: NativeMaterializationWork,
    custody: NativeAllocation,
}

impl NativeTaskContext {
    /// Authenticates the actual SDK-issued fresh-dispatch context before copying
    /// its immutable scope. READ and reconciliation contexts cannot issue this
    /// preparation authority, even if they retain an original owner and budget.
    pub async fn volume_callback_guard_with_native_work<P: StreamProvider + 'static>(
        &self, owner: &Arc<TaskJournalOwner<P>>, work: &NativeMaterializationWork,
    ) -> Result<NativeVolumeCallbackGuard<P>> {
        let context = self.context();
        context.require_original_native_dispatch(work)?;
        if !self.native_metadata_matches(work) || !owner.native_shared_work_matches(work) {
            return Err(denied("native volume callback lacks original shared metadata custody"));
        }
        let (task, fence) = context.owned_task_binding_with_native_work(work)?;
        if owner.task_binding() != (task, fence) {
            return Err(denied("native volume callback differs from the original task and full fence"));
        }
        owner.require_task_context_binding_with_native_work(context, work)?;
        owner.require_mail_scope_with_native_work(context.scope(), work)?;
        owner.verify_with_native_work(false, work).await?;

        // The canonical Scope copier owns its complete inline header and deep
        // copies. Only the enclosing known values remain to be funded here.
        let bytes = (std::mem::size_of::<NativeVolumeCallbackGuard<P>>()
            + std::mem::size_of::<OriginalVolumeAuthority<P>>()
            - std::mem::size_of::<RuntimeScope>()) as u64;
        let mut custody = work.retain_owned(bytes).map_err(native_error)?;
        let (scope, mut scope_custody) = context.scope().clone_with_native_work(work)?;
        if let Err(cause) = custody.absorb(&mut scope_custody) {
            drop(scope);
            return Err(native_error(cause));
        }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
            .map_err(native_error)?;
        Ok(NativeVolumeCallbackGuard { original: Arc::new(OriginalVolumeAuthority {
            owner: Arc::clone(owner), scope, custody,
        }) })
    }
}

impl<P: StreamProvider + 'static> NativeVolumeCallbackGuard<P> {
    /// Selects one exact canonical preparation volume before any physical IO.
    /// Per-volume resource views may differ from the metadata allocation view;
    /// their original family must not. The returned callback retains that exact
    /// selected view rather than manufacturing a fresh meter or root allowance.
    pub fn for_volume(
        &self, owner: &TaskJournalOwner<P>, volume: &VolumeRef, writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<dyn NativeMountCallbackGuard>> {
        self.require_volume_with_native_work(owner, volume, writable, work)?;
        // Includes every inline reference, selected Work and volume header.
        // The canonical reference copier separately admits its deep capacities.
        let mut custody = work.retain_owned(std::mem::size_of::<VolumeCallback<P>>() as u64)
            .map_err(native_error)?;
        let (volume, mut volume_custody) = volume.clone_with_native_work(work)?;
        if let Err(cause) = custody.absorb(&mut volume_custody) {
            drop(volume);
            return Err(native_error(cause));
        }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })
            .map_err(native_error)?;
        Ok(Arc::new(VolumeCallback {
            original: Arc::clone(&self.original), volume, writable, work: work.clone(), custody,
        }))
    }

    /// Borrows the same declaration checks without allocating a callback that
    /// preparation would immediately discard. Physical attachment constructs
    /// the single paid callback with `for_volume` on its actual selected work.
    pub(crate) fn require_volume_with_native_work(
        &self, owner: &TaskJournalOwner<P>, volume: &VolumeRef, writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default()).map_err(native_error)?;
        if !std::ptr::eq(owner, self.original.owner.as_ref())
            || owner.task_binding() != self.original.owner.task_binding()
            || !self.original.custody.work().shares_ledger(work)
        {
            return Err(denied("native preparation differs from its original callback owner or work family"));
        }
        require_volume(&self.original, volume, writable, work)
    }
}

fn require_volume<P: StreamProvider>(
    original: &OriginalVolumeAuthority<P>, volume: &VolumeRef, writable: bool,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let io = crate::filesystem::PreparationIo::from_native_work(work)?;
    if !io.contains_capability(original.scope.grants(), volume, "volume:read:")?
        || (writable && !io.contains_capability(original.scope.grants(), volume, "volume:write:")?)
    {
        return Err(denied("native callback volume policy differs from its original declared scope"));
    }
    original.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Read, work)?;
    if writable {
        original.owner.require_volume_grant_with_native_work(volume, VolumeOperation::Write, work)?;
    }
    Ok(())
}

impl<P: StreamProvider + 'static> NativeMountCallbackGuard for VolumeCallback<P> {
    fn check<'a>(
        &'a self, work: &'a NativeMountMeterWork,
    ) -> std::result::Result<NativeMountCallbackFuture<'a>, NativeMountError> {
        // The SDK prepays the concrete future and typed original Error slot
        // before building or polling it. No formatter or replacement carrier.
        NativeMountCallbackFuture::new_guarded::<Error, _, _>(work, || async move {
            let work = work.native_materialization_work();
            work.charge(WorkCounters::default()).map_err(native_error)?;
            if !self.work.shares_allocation_context(&work) || !self.custody.matches_work(&work) {
                return Err(denied("native callback differs from its original per-volume work view"));
            }
            // Exact grants precede even owner-history reads. Current full-fence
            // authorization is checked anew before every native SDK action.
            require_volume(&self.original, &self.volume, self.writable, &work)?;
            self.original.owner.verify_with_native_work(false, &work).await?;
            require_volume(&self.original, &self.volume, self.writable, &work)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_host::tests::created_resource_access::CreationFixture;

    fn work(cancellation: &acyclic_fs::CancellationToken) -> NativeMaterializationWork {
        let units = 16_384;
        let bytes = 16 * 1_024 * 1_024;
        NativeMaterializationWork::new(acyclic_fs::WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
            bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
            source_path_components: units, source_entries_visited: units, output_bytes: bytes,
            items_examined: units, items_returned: units, allocation_operations: units,
            peak_allocation_bytes: bytes, materializations: units,
        }, cancellation).expect("original finite fixture ledger")
    }

    #[tokio::test]
    async fn genuine_read_metadata_cannot_issue_preparation_callbacks() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation);
        let fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
        let owner = fixture.host.journal_owner_with_native_work(
            fixture.root, &fence, &original,
        ).await?.into_native_shared(&original)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let context = context.into_native_shared(custody, &original)?;
        let users = Arc::strong_count(&context);
        let before = original.receipt();
        assert!(matches!(context.volume_callback_guard_with_native_work(&owner, &original).await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt(), before, "READ rejection precedes copies and owner IO");
        assert_eq!(Arc::strong_count(&context), users);

        let foreign = work(&acyclic_fs::CancellationToken::new());
        let before = foreign.receipt();
        assert!(matches!(context.volume_callback_guard_with_native_work(&owner, &foreign).await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(foreign.receipt(), before, "a funded foreign family is not an issued source");

        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(context.volume_callback_guard_with_native_work(&owner, &original).await,
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before, "the original cancellation precedes owner IO");
        Ok(())
    }
}
