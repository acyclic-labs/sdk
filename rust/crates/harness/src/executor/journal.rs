use super::*;

/// Durable host services available to an executor; policy remains executor-owned.
pub trait ExecutionJournal: acyclic_stream::ProviderPlatform {
    /// Authenticated canonical aggregate whose selections this journal publishes.
    fn canonical_authority(&self) -> Option<&crate::core::Authority> {
        None
    }

    /// Implementations must scope sequences and retry keys by `operation_id`.
    fn replay<'a>(
        &'a self,
        operation_id: OperationId,
        after: u64,
        maximum: u32,
    ) -> BoxFuture<'a, Result<Vec<ExecutionRecord>>>;

    /// Optional finite replay of the same authenticated journal engine.
    #[cfg(feature = "filesystem")]
    fn replay_measured<'a>(
        &'a self,
        _operation: OperationId,
        _after: u64,
        _maximum: u32,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<
        'a,
        acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<ExecutionRecord>>, Error>,
    > {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(
                Error::Unsupported("finite execution replay is unavailable".into()),
            ))
        })
    }

    /// Appends one reconstructable executor observation.
    fn append<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>>;

    /// Atomically appends a dispatch or terminal observation at an exact journal tail.
    fn append_if_tail<'a>(
        &'a self,
        _operation_id: OperationId,
        _expected_tail: u64,
        _claim_id: String,
        _event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "atomic execution journal append is unavailable".into(),
            ))
        })
    }

    /// Stages immutable private bytes before any referring observation is appended.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>>;

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
    ) -> BoxFuture<'a, Result<acyclic_stream::OwnedBytes>>;

    /// Optional finite-work load under the same original journal-owner grant.
    #[cfg(feature = "filesystem")]
    fn load_measured<'a>(
        &'a self,
        _reference: &'a FileRef,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<
        'a,
        acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>,
    > {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(
                Error::Unsupported("finite journal payload resolution is unavailable".into()),
            ))
        })
    }

    /// Verifies a user-supplied file under its owner-mediated grant before turn admission.
    fn verify_input_file<'a>(&'a self, _reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "turn input file verification is unavailable".into(),
            ))
        })
    }

    #[cfg(feature = "filesystem")]
    fn verify_input_file_measured<'a>(
        &'a self,
        _reference: &'a FileRef,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(
                Error::Unsupported("finite turn input verification is unavailable".into()),
            ))
        })
    }

    /// Verifies data and native option bindings without granting fresh execution.
    fn verify_model_content<'a>(&'a self, content: &'a ModelContent) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if content.native_configurations().next().is_some() {
                return Err(Error::Unsupported(
                    "original native option verification is unavailable".into(),
                ));
            }
            for file in content.file_refs() {
                self.verify_input_file(file).await?;
            }
            Ok(())
        })
    }

    #[cfg(feature = "filesystem")]
    fn verify_model_content_measured<'a>(
        &'a self,
        _content: &'a ModelContent,
        _budget: acyclic_fs::WorkBudget,
        _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(
                Error::Unsupported("finite model content verification is unavailable".into()),
            ))
        })
    }

    /// Verifies the selected context against the committed conversation.
    fn verify_selected_context<'a>(
        &'a self,
        _operation_id: OperationId,
        _selected: &'a SelectedModelContext,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "canonical model context verification is unavailable".into(),
            ))
        })
    }

    /// Opens one typed durable interaction exactly once.
    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>>;

    /// Reads the canonical durable outcome without treating refusal as an answer.
    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>>;

    /// Pure original-work capability check, before a native frame or provider callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining()?;
        Ok(false)
    }

    /// Replays the same journal while retaining its known records under original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn replay_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _after: u64,
        _maximum: u32,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Vec<ExecutionRecord>, acyclic_fs::NativeAllocation)>>
    {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution replay is unavailable",
        })
    }

    /// Appends borrowed paid metadata; a custom journal must explicitly audit this producer.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn append_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _key: &'a str,
        _event: &'a ExecutionEvent,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution append is unavailable",
        })
    }

    /// Claims exactly the same journal tail; false preserves lost-claim semantics.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn append_if_tail_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _tail: u64,
        _key: &'a str,
        _event: &'a ExecutionEvent,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, bool>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native atomic execution append is unavailable",
        })
    }

    /// Stages borrowed immutable bytes before copying keys, metadata or provider input.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn stage_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _key: &'a str,
        _bytes: &'a [u8],
        _media: &'static str,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution staging is unavailable",
        })
    }

    /// Resolves exact journal-owner bytes without substituting a remaining budget.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_with_native_work<'a>(
        &'a self,
        _reference: &'a FileRef,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution payload resolution is unavailable",
        })
    }

    /// Verifies the original user file authority before native turn admission.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_input_file_with_native_work<'a>(
        &'a self,
        _reference: &'a FileRef,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native turn file verification is unavailable",
        })
    }

    /// Verifies data and original option bindings through the audited native engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_model_content_with_native_work<'a>(
        &'a self,
        _content: &'a ModelContent,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native model content verification is unavailable",
        })
    }

    /// Verifies the previously committed canonical selection under its original owner.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_selected_context_with_native_work<'a>(
        &'a self,
        _operation: OperationId,
        _selected: &'a SelectedModelContext,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native selected context verification is unavailable",
        })
    }

    /// Opens the same admitted durable interaction without reconstructing authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn open_interaction_with_native_work<'a>(
        &'a self,
        _id: InteractionId,
        _interaction: &'a Interaction,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution interaction opening is unavailable",
        })
    }

    /// Reads the original outcome with custody through its actual last use.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn interaction_outcome_with_native_work<'a>(
        &'a self,
        _id: InteractionId,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (Option<InteractionOutcome>, acyclic_fs::NativeAllocation)>,
    > {
        Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution interaction outcome is unavailable",
        })
    }
}
