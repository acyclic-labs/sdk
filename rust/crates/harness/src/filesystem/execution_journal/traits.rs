use super::*;

impl<P, A, O> ToolApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_tool_approval_with_io(id, operation, digest, None))
    }

    fn verify_measured<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> BoxFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = crate::filesystem::fork_preparer_io::PreparationIo::from_budget(
                budget,
                cancellation,
            )
            .map_err(|error| acyclic_fs::OperationFailure::new(error, Default::default()))?;
            let result = self
                .verify_tool_approval_with_io(id, operation, digest, Some(&io))
                .await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt {
                    value,
                    work: io.receipt(),
                }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.verify_tool_approval_with_io(id, operation, digest, Some(&io))
                .await
        })
    }
}

impl<P, A, O> ExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn canonical_authority(&self) -> Option<&crate::core::Authority> {
        Some(self.verifier.audience())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining().map_err(Error::from)?;
        Ok(true)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn staged_ref_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(Error::from)?;
        let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "original staged-payload observation requires a task-owned journal",
        })?;
        owner.require_volume_grant(
            &self.volume,
            VolumeOperation::Read,
            &mut Some(&mut crate::store::HistoryWork::new(
                crate::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = {
                owner
                    .verify_read_access(&mut Some(&mut io.history_io()))
                    .await?;
                let _identity_hold;
                let before = io.owned_bytes()?;
                let (key, path) =
                    self.stage_identity_with_io(operation_id, idempotency_key, Some(&io))?;
                let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(
                        Some(&io),
                        crate::wire::ErrorCode::Invalid,
                        "staging identity ownership underflow",
                    )
                })?;
                _identity_hold = io.guard_owned(retained)?;
                let file = self
                    .host
                    .original_internal_content_for_operation_with_io(
                        &self.volume,
                        &self.verifier,
                        &self.scope,
                        &key,
                        &path,
                        self.maximum_payload_bytes,
                        InternalContentClass::Execution,
                        &io,
                    )
                    .await?
                    .ok_or_else(|| {
                        canonical_boundary(
                            Some(&io),
                            crate::wire::ErrorCode::NotFound,
                            "original staged payload was not published",
                        )
                    })?;
                owner
                    .verify_read_access(&mut Some(&mut io.history_io()))
                    .await?;
                file
            };
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let records = self.records(operation, after, maximum, io).await?;
            if let Some(io) = io {
                io.remaining()?;
            }
            Ok(records)
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || self.load_content(reference, io))
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let local = io
                .and_then(PreparationIo::original_native_work)
                .map(PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let io = local.as_ref().or(io);
            self.append_validated(operation, None, key, event, io)
                .await
                .map(|_| ())
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let local = io
                .and_then(PreparationIo::original_native_work)
                .map(PreparationIo::from_native_work)
                .transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let io = local.as_ref().or(io);
            self.append_validated(operation, Some(expected_tail), key, event, io)
                .await
        })
    }

    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        #[cfg(not(target_arch = "wasm32"))]
        let native = match io.and_then(PreparationIo::original_native_work) {
            Some(work) => {
                work.remaining()?;
                let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "native staging requires an original dispatch owner",
                })?;
                owner.require_volume_grant(
                    &self.volume,
                    VolumeOperation::Write,
                    &mut Some(&mut io.expect("native ledger").history_io()),
                )?;
                Some((owner, work))
            }
            None => None,
        };
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let local = native
                .map(|(_, work)| PreparationIo::from_native_work(work))
                .transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let stage_io = local.as_ref().or(io);
            #[cfg(target_arch = "wasm32")]
            let stage_io = io;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some((owner, _)) = native {
                owner
                    .verify(
                        false,
                        &mut Some(&mut stage_io.expect("native ledger").history_io()),
                    )
                    .await?;
            }
            let file = self
                .stage_with_io(operation, key, bytes, media, stage_io)
                .await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some((owner, _)) = native {
                owner
                    .verify(
                        false,
                        &mut Some(&mut stage_io.expect("native ledger").history_io()),
                    )
                    .await?;
                io.expect("native ledger").absorb_native_custody(
                    local
                        .expect("native ledger")
                        .into_native_custody(file.logical_owned_bytes()?)?,
                )?;
            }
            Ok(file)
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_input_file_with_io(reference, io)
        })
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_model_content_with_io(content, io)
        })
    }

    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_selected_context_with_io(operation, selected, io)
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: Interaction,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.open_interaction_with_io(id, &interaction, None).await })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn open_interaction_with_native_work<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            self.open_interaction_with_io(id, interaction, Some(&io))
                .await
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
    ) -> BoxFuture<'a, Result<Option<InteractionOutcome>>> {
        Box::pin(self.interaction_outcome_with_io(id, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn interaction_outcome_with_native_work<'a>(
        &'a self,
        id: InteractionId,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<'a, (Option<InteractionOutcome>, acyclic_fs::NativeAllocation)>,
    > {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let outcome = self.interaction_outcome_with_io(id, Some(&io)).await?;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((outcome, custody))
        })
    }
}

use crate::executor::stock_boundary;
