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
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>>;

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

    /// Verifies data and original option bindings without granting fresh execution.
    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        if io.is_some() || content.native_configurations().next().is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "original native option verification is unavailable",
            ));
        }
        crate::conversation::BoundedContentFuture::new(io, || async move {
            for file in content.file_refs() {
                self.verify_input_file(file, io)?.await?;
            }
            Ok(())
        })
    }

    /// Verifies the selection against the committed conversation and original work.
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
