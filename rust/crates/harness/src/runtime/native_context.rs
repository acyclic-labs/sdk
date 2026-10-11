use super::{ContentResidencyVerifier, TaskContext, ToolContext, ToolTaskContext};
use crate::{Error, FileRef, NativeFutureBundle, Result, durable_host::TaskJournalOwner};
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use std::sync::Arc;

// Only the SDK's observed context producer can issue this receipt. It carries
// identity and coverage, not authority or a second allocation ledger.
pub(super) struct NativeMetadataSource {
    work: NativeMaterializationWork,
    bytes: u64,
}

fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn same_work(left: &NativeMaterializationWork, right: &NativeMaterializationWork) -> bool {
    std::ptr::eq(left.runtime_meter(), right.runtime_meter())
}

/// The SDK-issued task context shared without cloning its sealed metadata.
/// Its original metadata custody outlives every use of the context payload.
/// Shared-slot accounting covers the known SDK value, not allocator overhead,
/// RSS, or the standard-library Arc backing retained by weak references.
pub struct NativeTaskContext {
    context: TaskContext,
    custody: NativeAllocation,
}

impl TaskContext {
    /// Called only after the SDK context producer has completed every observed
    /// metadata allocation and retained the corresponding original guards.
    pub(crate) fn mark_native_metadata_source(
        &mut self,
        work: &NativeMaterializationWork,
        bytes: u64,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.native_metadata_source = Some(NativeMetadataSource { work: work.clone(), bytes });
        Ok(())
    }

    /// Consumes the actual SDK-issued context and its observed metadata custody.
    /// An unrelated allocation, or an ordinary cloned context, is not a source
    /// receipt. The known SDK value slot and its allocation attempt are admitted
    /// before `Arc::new`; this does not account private Arc backing internals.
    pub fn into_native_shared(
        self,
        custody: NativeAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeTaskContext>> {
        let mut payload = NativeTaskContext { context: self, custody };
        let Some(source) = payload.context.native_metadata_source.as_ref() else {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "task context has no original native metadata source"));
        };
        if !same_work(&source.work, work) || !same_work(payload.custody.work(), work)
            || payload.custody.bytes() < source.bytes
        {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "task context metadata custody differs from its original source"));
        }
        let mut slot = match work.claim(std::mem::size_of::<NativeTaskContext>() as u64) {
            Ok(slot) => slot,
            Err(cause) => {
                let NativeTaskContext { context, custody } = payload;
                drop(context);
                return Err(Error::NativeWork { cause, custody: Some(custody) });
            }
        };
        payload.custody.absorb(&mut slot)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(payload))
    }
}

impl NativeTaskContext {
    /// Borrows the same issued context used by the native command and provider.
    #[must_use]
    pub const fn context(&self) -> &TaskContext { &self.context }

    /// Pure membership check for the original metadata ledger, not authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        same_work(self.custody.work(), work)
    }

    /// Builds an original-owner READ facade over this same shared context.
    /// Neither the owner nor the context is reconstructed or metadata-cloned.
    pub fn content_residency_verifier_with_native_work(
        self: &Arc<Self>,
        owner: Arc<TaskJournalOwner>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<dyn ContentResidencyVerifier>> {
        let (task, fence) = owner.task_binding();
        if !self.context.original_task_binding_matches(task, fence) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ owner differs from the issued task context"));
        }
        if !same_work(self.custody.work(), work) || !owner.native_shared_work_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ facade lacks original shared metadata custody"));
        }
        let custody = work.claim(std::mem::size_of::<NativeContextVerifier>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Arc::new(NativeContextVerifier {
            context: Arc::clone(self), owner, custody,
        }))
    }
}

impl ToolContext {
    /// Binds native tool metadata to the same SDK-issued shared task context.
    /// Allocation membership does not authenticate an invocation; the admitted
    /// definition, operation and call identity must still pass the tool gate.
    pub fn new_with_native_work(
        task: Arc<NativeTaskContext>,
        operation_id: crate::OperationId,
        call_id: &str,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !task.native_metadata_matches(work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "tool context differs from its original native metadata ledger"));
        }
        crate::tool::ToolInvocation::validate_call_id_with_native_work(call_id, work)?;
        // This stack value has no allocator attempt of its own. Its known
        // logical metadata is admitted before the actual SDK String producer.
        let mut slot = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut output = work.display(call_id)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        output.1.absorb(&mut slot)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(Self {
            task: ToolTaskContext::Native(task),
            operation_id,
            call_id: output.0,
            native_custody: Some(output.1),
        })
    }

    /// Borrows the original shared task without reconstructing its owner.
    /// Callers must separately check this tool's genuine metadata and admission.
    pub(crate) fn native_task(&self) -> Option<&Arc<NativeTaskContext>> {
        match &self.task {
            ToolTaskContext::Native(task) => Some(task),
            ToolTaskContext::Ordinary(_) => None,
        }
    }

    /// Checks genuine SDK tool metadata funding without granting tool authority.
    pub(crate) fn native_metadata_matches(&self, work: &NativeMaterializationWork) -> bool {
        let ToolTaskContext::Native(task) = &self.task else { return false; };
        let Some(custody) = &self.native_custody else { return false; };
        task.native_metadata_matches(work) && same_work(custody.work(), work)
            && (std::mem::size_of::<Self>() as u64)
                .checked_add(self.call_id.capacity() as u64)
                .is_some_and(|bytes| custody.bytes() >= bytes)
    }
}

struct NativeContextVerifier {
    context: Arc<NativeTaskContext>,
    owner: Arc<TaskJournalOwner>,
    custody: NativeAllocation,
}

impl NativeContextVerifier {
    fn original_binding(&self) -> Result<()> {
        let (task, fence) = self.owner.task_binding();
        if !self.context.context.original_task_binding_matches(task, fence) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ owner differs from the issued task context"));
        }
        Ok(())
    }

    async fn authorize_native(
        &self, reference: &FileRef, maximum_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.charge(WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.original_binding()?;
        if !same_work(self.custody.work(), work) {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "content READ callback differs from its original native ledger"));
        }
        if reference.descriptor().byte_length() > maximum_bytes {
            return Err(boundary(crate::wire::ErrorCode::Invalid,
                "content exceeds the native callback byte bound"));
        }
        self.owner.validate_input_file_with_native_work(reference, work)?;
        self.owner.verify_read_access_with_native_work(work).await
    }

    async fn authorize(&self, reference: &FileRef) -> Result<()> {
        self.original_binding()?;
        self.owner.validate_input_file(reference)?;
        self.owner.verify_read_access().await
    }
}

impl ContentResidencyVerifier for NativeContextVerifier {
    fn verify<'a>(&'a self, reference: &'a FileRef) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            self.authorize(reference).await?;
            self.context.context.verify_bound_file(reference).await?;
            self.authorize(reference).await
        })
    }

    fn read<'a>(&'a self, reference: &'a FileRef) -> acyclic_stream::BoxProviderFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.authorize(reference).await?;
            let bytes = self.context.context.read_file(reference).await?;
            self.authorize(reference).await?;
            Ok(bytes)
        })
    }

    fn verify_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, ()>> {
        NativeFutureBundle::new(work, || async move {
            self.authorize_native(reference, maximum_bytes, work).await?;
            // A typed native failure returns immediately, before the second
            // owner observation can replace its first cause or custody.
            self.context.context.verify_bound_file_with_native_work(reference, work).await?;
            self.authorize_native(reference, maximum_bytes, work).await
        })
    }

    fn read_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, (Vec<u8>, NativeAllocation)>> {
        NativeFutureBundle::new(work, || async move {
            self.authorize_native(reference, maximum_bytes, work).await?;
            let output = self.context.context.read_file_with_native_work(reference, work).await?;
            self.authorize_native(reference, maximum_bytes, work).await?;
            Ok(output)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durable_host::tests::created_resource_access::CreationFixture;

    fn budget() -> acyclic_fs::WorkBudget {
        let units = 16_384;
        let bytes = 16 * 1_024 * 1_024;
        acyclic_fs::WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes, bytes_hashed: bytes,
            bytes_copied: bytes, bytes_encoded: bytes, source_bytes_read: bytes,
            source_path_components: units, source_entries_visited: units, output_bytes: bytes,
            items_examined: units, items_returned: units, allocation_operations: units,
            peak_allocation_bytes: bytes, materializations: units,
        }
    }

    fn work(cancellation: &acyclic_fs::CancellationToken) -> Result<NativeMaterializationWork> {
        NativeMaterializationWork::new(budget(), cancellation)
            .map_err(|failure| Error::NativeWork { cause: failure.error, custody: None })
    }

    #[tokio::test]
    async fn ordinary_context_cannot_be_qualified_by_an_opaque_allocation() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let context = fixture.harness.durable_context(
            fixture.root, fixture.root_lease.operation.operation_id,
        ).await?;
        let custody = original.claim(65_536)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        assert_eq!(original.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn native_metadata_receipt_rejects_clone_foreign_and_short_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        assert!(custody.bytes() > 0);
        let clone = context.clone();
        let opaque = original.claim(custody.bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let cloned = clone.into_native_shared(opaque, &original);
        assert!(matches!(cloned, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        drop(context);
        drop(custody);

        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let foreign = work(&cancellation)?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &foreign);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let (context, mut custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        // The real producer, not a test marker, issued the source amount.
        let source_bytes = context.native_metadata_source.as_ref()
            .expect("genuine native metadata source").bytes;
        assert!(source_bytes > 0);
        let removed = custody.bytes() - (source_bytes - 1);
        let released = custody.split_off(removed)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);
        drop(released);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_shared_context_preserves_original_cause_and_metadata_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let bytes = custody.bytes();
        cancellation.cancel();
        let before = original.receipt();
        let rejected = context.into_native_shared(custody, &original);
        match rejected {
            Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                custody: Some(custody),
            }) => {
                assert_eq!(custody.bytes(), bytes);
                assert!(same_work(custody.work(), &original));
                drop(custody);
            }
            _ => panic!("cancelled shared allocation must preserve the original cause and custody"),
        }
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn shared_native_content_uses_original_owner_and_keeps_real_output_custody() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let created = fixture.create_filesystem_child().await?;
        let task = crate::TaskId::from_bytes([84; 16]);
        assert!(matches!(fixture.admit_child(created.request.child.clone()).await?,
            crate::Admission::Accepted(admitted) if admitted == task));
        let observation = fixture.host.observe_task(task).await?;
        let reservation = observation.reservation
            .ok_or_else(|| Error::NotFound("created native context lease".into()))?;
        let fence = crate::scheduler::LeaseFence::from(&reservation);
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            task, created.request.child.operation_id, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        let owner = fixture.host.journal_owner_with_native_work(task, &fence, &original).await?
            .into_native_shared(&original)?;
        let shared_users = Arc::strong_count(&shared);
        let reader = shared.content_residency_verifier_with_native_work(
            Arc::clone(&owner), &original,
        )?;
        assert_eq!(Arc::strong_count(&shared), shared_users + 1);
        let staged = shared.context().stage_file_once_with_native_work(
            crate::OperationId::from_bytes([118; 16]), "native/shared.txt",
            b"same issued context", "text/plain", "shared.txt", &original,
        ).await?;
        let before = original.live_allocation_bytes();
        let (bytes, custody) = reader.read_with_native_work(&staged.0, 65_536, &original)?.await?;
        assert_eq!(bytes, b"same issued context");
        assert!(same_work(custody.work(), &original));
        assert!(custody.bytes() >= bytes.capacity() as u64);
        assert_eq!(original.live_allocation_bytes(), before + custody.bytes());
        drop(bytes);
        drop(custody);
        assert_eq!(original.live_allocation_bytes(), before);
        reader.verify_with_native_work(&staged.0, 65_536, &original)?.await?;

        let root_fence = crate::scheduler::LeaseFence::from(&fixture.root_lease.reservation);
        let root_owner = fixture.host.journal_owner_with_native_work(
            fixture.root, &root_fence, &original,
        ).await?.into_native_shared(&original)?;
        let before = original.receipt();
        let wrong_owner = shared.content_residency_verifier_with_native_work(
            Arc::clone(&root_owner), &original,
        );
        assert!(matches!(wrong_owner, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let unmeasured = Arc::new(fixture.host.journal_owner(task, fence.clone()).await?);
        let before = original.receipt();
        let rejected = shared.content_residency_verifier_with_native_work(unmeasured, &original);
        assert!(matches!(rejected, Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized, ..
        })));
        assert_eq!(original.receipt(), before);

        let (root, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let root = root.into_native_shared(custody, &original)?;
        let root_reader = root.content_residency_verifier_with_native_work(root_owner, &original)?;
        let reads = original.receipt().backend_read_operations;
        assert!(matches!(root_reader.read_with_native_work(&staged.0, 65_536, &original)?.await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(original.receipt().backend_read_operations, reads);

        // Fill the remaining logical allowance with an actual owned SDK-sized
        // buffer. The callback must deny its future slot before owner/provider IO.
        let available = budget().peak_allocation_bytes - original.live_allocation_bytes();
        let quota = original.claim(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
            .expect("finite fixture allocation");
        let blocker = (buffer, quota);
        let reads = original.receipt().backend_read_operations;
        let denied = reader.read_with_native_work(&staged.0, 65_536, &original);
        assert!(matches!(denied, Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }),
            ..
        })));
        assert_eq!(original.receipt().backend_read_operations, reads);
        drop(blocker);
        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(reader.read_with_native_work(&staged.0, 65_536, &original),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before);
        Ok(())
    }

    #[tokio::test]
    async fn native_tool_context_borrows_the_issued_context_and_invalidates_clone_funding() -> Result<()> {
        let fixture = CreationFixture::new().await?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = work(&cancellation)?;
        let (context, custody) = fixture.harness.durable_context_with_native_work(
            fixture.root, fixture.root_lease.operation.operation_id, &original,
        ).await?;
        let shared = context.into_native_shared(custody, &original)?;
        let operation = crate::OperationId::from_bytes([119; 16]);
        let call_id = operation.to_string();
        let baseline = original.live_allocation_bytes();
        let before = original.receipt();
        let users = Arc::strong_count(&shared);
        let tool = ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &original,
        )?;
        assert!(std::ptr::eq(tool.task(), shared.context()));
        assert!(Arc::ptr_eq(tool.native_task().expect("original native task"), &shared));
        assert!(std::ptr::eq(&*tool, shared.context()));
        assert_eq!(tool.operation_id(), operation);
        assert_eq!(tool.call_id(), call_id);
        assert!(tool.native_metadata_matches(&original));
        assert_eq!(Arc::strong_count(&shared), users + 1);
        assert!(original.receipt().bytes_copied >= before.bytes_copied + call_id.len() as u64);
        let held = tool.native_custody.as_ref().expect("genuine SDK tool metadata").bytes();
        assert!(held >= std::mem::size_of::<ToolContext>() as u64 + tool.call_id.capacity() as u64);
        assert_eq!(original.live_allocation_bytes(), baseline + held);

        let before = original.receipt();
        let clone = tool.clone();
        assert!(std::ptr::eq(clone.task(), shared.context()));
        assert!(Arc::ptr_eq(clone.native_task().expect("same shared task"), &shared));
        assert!(!clone.native_metadata_matches(&original));
        assert_eq!(clone.call_id(), tool.call_id());
        assert_eq!(original.receipt(), before);
        drop(tool);
        assert_eq!(original.live_allocation_bytes(), baseline);
        drop(clone);

        let foreign = work(&cancellation)?;
        let before = foreign.receipt();
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &foreign,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(foreign.receipt(), before);
        let before = original.receipt().allocation_operations;
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, "bad/call", &original,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(original.receipt().allocation_operations, before);

        let available = budget().peak_allocation_bytes - original.live_allocation_bytes();
        let quota = original.claim(available)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(usize::try_from(available).expect("fixture byte bound"))
            .expect("finite fixture allocation");
        let blocker = (buffer, quota);
        let allocations = original.receipt().allocation_operations;
        assert!(matches!(ToolContext::new_with_native_work(
            Arc::clone(&shared), operation, &call_id, &original,
        ), Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Work(
                acyclic_fs::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }), ..
        })));
        assert_eq!(original.receipt().allocation_operations, allocations);
        drop(blocker);
        cancellation.cancel();
        let before = original.receipt();
        assert!(matches!(ToolContext::new_with_native_work(
            shared, operation, &call_id, &original,
        ), Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. })));
        assert_eq!(original.receipt(), before);
        Ok(())
    }
}
