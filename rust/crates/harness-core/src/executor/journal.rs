use super::*;

pub(super) fn validate_journal(
    journal: &dyn ExecutionJournal,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(PreparationIo::original_native_work)
        && !journal.supports_native_execution(work)?
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "native execution journal is unavailable",
        ));
    }
    let _ = (journal, io);
    Ok(())
}

/// Bounded selection from one complete authenticated execution step.
pub struct ExecutionRecordQuery<'a> {
    /// Exact operation whose authenticated step is selected.
    pub operation: OperationId,
    /// Step ordinal within that operation.
    pub step: u32,
    /// Includes the operation's initial binding in the selection.
    pub include_started: bool,
    /// Bound on all observations admitted for this step, including summary work.
    pub maximum_step_records: usize,
    /// Bound on the selected output records.
    pub maximum: usize,
    /// Selection applied only after authenticating each indexed observation.
    pub select: &'a (dyn Fn(&ExecutionEvent) -> bool + Send + Sync),
}

/// Selected observations and the complete journal tail at their authenticated cut.
pub type IndexedExecutionRecords = (u64, Vec<ExecutionRecord>);
/// Optional indexed result; a missing result requires ordinary journal replay.
pub type IndexedExecutionFuture<'a> =
    crate::conversation::BoundedContentFuture<'a, Option<IndexedExecutionRecords>>;

impl ExecutionRecordQuery<'_> {
    pub(crate) fn step_bound(limits: Limits) -> usize {
        limits
            .model_events_per_step
            .saturating_mul(2)
            .saturating_add(limits.tool_calls_per_step.saturating_mul(3))
            .saturating_add(7)
    }
}

/// Opaque identity of an immutable journal binding, independent of its address.
/// `Default` creates a fresh identity; copying it preserves the same binding.
/// This partitions caches and never grants authority to journal contents.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionJournalCacheId(OperationId);

/// Durable host services available to an executor; policy remains executor-owned.
pub trait ExecutionJournal: acyclic_stream::ProviderPlatform {
    /// Authenticates private working cuts issued by this journal binding.
    /// Without this capability the executor retains the ordinary replay path.
    fn working_checkpoint_authority(&self) -> Option<&crate::core::AuthorityVerifier> {
        None
    }

    /// Stable identity of this immutable journal binding for execution caches.
    /// Create a fresh identity with `Default` for every new or rebound journal.
    /// Returning `None` disables memoization without changing replay semantics.
    fn cache_identity(&self) -> Option<ExecutionJournalCacheId> {
        None
    }

    /// All stores touched by a write phase can seal their accepted prefixes.
    fn supports_deferred_durability(&self) -> bool {
        false
    }

    /// Admits completion before the phase accepts deferred writes. Polling
    /// seals its accepted prefix without rechecking caller cancellation.
    fn durability_barrier<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "execution journal has no phase durability barrier",
        ))
    }

    /// Maximum canonical model-event bytes accepted inline; zero uses files.
    /// Inline payloads must retain the journal's original private-volume grants.
    fn inline_model_event_bytes(&self) -> u64 {
        0
    }

    /// Excludes live attempts before replay can mistake them for crashed work.
    /// All aliases of an operation must use the same ownership registry.
    fn claim_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Result<ExecutionClaim<'a>>;

    /// Follows an authorized cancellation while awaiting a model, approval or
    /// tool invocation whose executor guarantees cancellation on future drop.
    /// The default journal has no filesystem cancellation binding.
    fn model_cancellation(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(std::future::pending())
    }

    /// Implementations must scope sequences and retry keys by `operation_id`.
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>>;

    /// Selects a complete authenticated step at a current journal cut.
    /// `None` requires ordinary replay; an index must never omit an unindexed tail.
    fn indexed_records<'a>(
        &'a self,
        _query: ExecutionRecordQuery<'a>,
        _io: Option<&'a PreparationIo>,
    ) -> Result<Option<IndexedExecutionFuture<'a>>> {
        Ok(None)
    }

    /// Appends one reconstructable observation on the original optional ledger.
    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;

    /// Appends at the exact operation-scoped journal tail.
    fn append_if_tail<'a>(
        &'a self,
        _operation_id: OperationId,
        _expected_tail: u64,
        _claim_id: &'a str,
        _event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "atomic execution journal append is unavailable",
        ))
    }

    /// Stages immutable private bytes before any referring observation is appended.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>>;

    /// Stages an admitted request with an authenticated incremental prefix.
    /// Providers without this capability return `None` before allocating.
    fn stage_prepared_model_request<'a>(
        &'a self,
        _operation: OperationId,
        _key: &'a str,
        _prepared: &'a crate::model::PreparedModelRequest,
        _previous: Option<&'a FileRef>,
        _io: Option<&'a PreparationIo>,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, FileRef>>> {
        Ok(None)
    }

    /// Observes an already staged immutable payload by its original operation and retry key.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn staged_ref_with_native_work<'a>(
        &'a self,
        _operation_id: OperationId,
        _idempotency_key: &'a str,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining()?;
        Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native original staged-payload observation is unavailable",
        })
    }

    /// Reads an exact version under the journal owner's grant and verifies its descriptor.
    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>;

    /// Verifies a user file under the original owner and optional work.
    fn verify_input_file<'a>(
        &'a self,
        _reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "turn input file verification is unavailable",
        ))
    }

    /// Non-file content has no independent journal policy beyond its immutable
    /// shape and the executor's limits. Opting in permits a single executor to
    /// retain that admission under this journal's stable cache identity. Files,
    /// native configurations and summary provenance are still checked on every
    /// dispatch. Rebinding this contract requires a fresh cache identity.
    fn has_file_bound_model_verification(&self) -> bool {
        false
    }

    /// Verifies data and original option bindings without granting fresh execution.
    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        context: Option<&'a crate::runtime::TaskContext>,
        _summary_files: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() || (content.native_configurations().next().is_some() && context.is_none()) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original native option verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if content.native_configurations().next().is_some()
                && let Some(context) = context
            {
                context
                    .verify_model_contents(std::iter::once(content))
                    .await?;
            }
            for file in content.file_refs() {
                self.verify_input_file(file, io)?.await?;
            }
            Ok(())
        })
    }

    /// Queries the private provenance file for this exact immutable summary.
    fn authenticate_summary<'a>(
        &'a self,
        _file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async { Ok(false) })
    }

    /// Authenticates selected files and their projection against the committed
    /// conversation under the original authority and work.
    fn verify_selected_context<'a>(
        &'a self,
        _operation: OperationId,
        _selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "canonical model context verification is unavailable",
        ))
    }

    /// Opens one typed durable interaction exactly once on its optional original IO.
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;

    /// Reads the durable outcome while its optional IO retains returned metadata.
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>>;

    /// Pure original-work capability check, before a native frame or provider callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }
}
