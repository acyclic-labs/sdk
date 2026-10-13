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
            &mut Some(&mut crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?),
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = {
                owner
                    .verify_read_access(&mut Some(&mut crate::filesystem::PreparationIo::from_native_work(
                        work,
                    ).map(crate::store::HistoryWork::new)?))
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
                    .verify_read_access(&mut Some(&mut crate::filesystem::PreparationIo::from_native_work(
                        work,
                    ).map(crate::store::HistoryWork::new)?))
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
            if io.is_none() {
                let span = obs_span!("acyclic.harness.journal.replay", rev = after, items = crate::obs::Empty);
                return traced(span, async move {
                    let records = self.records(operation, after, maximum, io).await?;
                    crate::obs::obs_record!("items" = records.len());
                    Ok(records)
                }).await;
            }
            let records = self.records(operation, after, maximum, io).await?;
            io.expect("bounded branch").remaining()?;
            Ok(records)
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if io.is_none() {
                let span = obs_span!("acyclic.harness.journal.load", bytes = crate::obs::Empty);
                return traced(span, async move {
                    let bytes = self.load_content(reference, io).await?;
                    crate::obs::obs_record!("bytes" = bytes.len());
                    Ok(bytes)
                }).await;
            }
            self.load_content(reference, io).await
        })
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
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let local = PreparationIo::from_native_work(work)?;
                let result = self.append_validated(operation, None, key, event, Some(&local)).await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                return result.map(|_| ());
            }
            let span = obs_span!("acyclic.harness.journal.append");
            traced(span, async move {
                self.append_validated(operation, None, key, event, io).await.map(|_| ())
            }).await
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
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                let local = PreparationIo::from_native_work(work)?;
                let result = self.append_validated(operation, Some(expected_tail), key, event, Some(&local)).await;
                let retained = local.owned_bytes()?;
                drop(local.into_native_custody(retained)?);
                return result;
            }
            let span = obs_span!("acyclic.harness.journal.append_if_tail", rev = expected_tail);
            traced(span, self.append_validated(operation, Some(expected_tail), key, event, io)).await
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
                owner.require_volume_grant(&self.volume, VolumeOperation::Write,
                    &mut Some(&mut io.expect("native ledger").history_io()))?;
                Some((owner, work))
            }
            None => None,
        };
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some((owner, work)) = native {
                let local = PreparationIo::from_native_work(work)?;
                owner.verify(false, &mut Some(&mut local.history_io())).await?;
                let file = self.stage_with_io(operation, key, bytes, media, Some(&local)).await?;
                owner.verify(false, &mut Some(&mut local.history_io())).await?;
                let retained = file.logical_owned_bytes()?;
                io.expect("native ledger").absorb_native_custody(local.into_native_custody(retained)?)?;
                return Ok(file);
            }
            let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
            traced(span, self.stage_with_io(operation, key, bytes, media, io)).await
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || self.verify_input_file_with_io(reference, io))
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || self.verify_model_content_with_io(content, io))
    }

    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || self.verify_selected_context_with_io(operation, selected, io))
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let temporary = io.and_then(PreparationIo::original_native_work).map(PreparationIo::from_native_work).transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let active = temporary.as_ref().or(io);
            #[cfg(target_arch = "wasm32")]
            let active = io;
            self.open_interaction_with_io(id, interaction, active).await
        })
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let temporary = io.and_then(PreparationIo::original_native_work).map(PreparationIo::from_native_work).transpose()?;
            #[cfg(not(target_arch = "wasm32"))]
            let active = temporary.as_ref().or(io);
            #[cfg(target_arch = "wasm32")]
            let active = io;
            let outcome = self.interaction_outcome_with_io(id, active).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(temporary) = temporary {
                let retained = temporary.owned_bytes()?;
                io.expect("native parent IO").absorb_native_custody(temporary.into_native_custody(retained)?)?;
            }
            Ok(outcome)
        })
    }

}

use crate::executor::{stock_boundary};
