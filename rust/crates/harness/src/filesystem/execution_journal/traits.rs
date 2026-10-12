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
    fn stage_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        bytes: &'a [u8],
        media_type: &'static str,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        work.remaining().map_err(Error::from)?;
        let (owner, _) = self.owner.as_ref().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native staging requires an original dispatch owner",
        })?;
        owner.require_volume_grant(
            &self.volume,
            VolumeOperation::Write,
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )
                .await?;
            let file = self
                .stage_with_io(operation_id, idempotency_key, bytes, media_type, Some(&io))
                .await?;
            owner
                .verify(
                    false,
                    &mut Some(&mut crate::store::history::native_work_io(work)?),
                )
                .await?;
            let retained = file.logical_owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            Ok((file, custody))
        })
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
            &mut Some(&mut crate::store::history::native_work_io(work)?),
        )?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let file = {
                owner
                    .verify_read_access(&mut Some(&mut crate::store::history::native_work_io(
                        work,
                    )?))
                    .await?;
                let _identity_hold;
                let before = io.owned_bytes()?;
                let (key, path) =
                    self.stage_identity_with_io(operation_id, idempotency_key, Some(&io))?;
                let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    crate::filesystem::fork_preparer_io::failure(
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
                    .verify_read_access(&mut Some(&mut crate::store::history::native_work_io(
                        work,
                    )?))
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
        operation_id: OperationId,
        idempotency_key: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<()>> {
        let span = obs_span!("acyclic.harness.journal.append");
        traced(span, async move {
            self.append_validated(operation_id, None, &idempotency_key, &event, None)
                .await
                .map(|_| ())
        })
    }

    fn append_if_tail<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        claim_id: String,
        event: ExecutionEvent,
    ) -> BoxFuture<'a, Result<bool>> {
        let span = obs_span!(
            "acyclic.harness.journal.append_if_tail",
            rev = expected_tail
        );
        traced(span, async move {
            self.append_validated(operation_id, Some(expected_tail), &claim_id, &event, None)
                .await
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn append_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self
                .append_validated(operation_id, None, idempotency_key, event, Some(&io))
                .await;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(custody);
            result.map(|_| ())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn append_if_tail_with_native_work<'a>(
        &'a self,
        operation_id: OperationId,
        expected_tail: u64,
        idempotency_key: &'a str,
        event: &'a ExecutionEvent,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, bool>> {
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let result = self
                .append_validated(
                    operation_id,
                    Some(expected_tail),
                    idempotency_key,
                    event,
                    Some(&io),
                )
                .await;
            let retained = io.owned_bytes()?;
            let custody = io.into_native_custody(retained)?;
            drop(custody);
            result
        })
    }

    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        idempotency_key: String,
        bytes: Vec<u8>,
        media_type: &'static str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        let span = obs_span!("acyclic.harness.journal.stage", bytes = bytes.len());
        traced(span, async move {
            self.stage_with_io(operation_id, &idempotency_key, &bytes, media_type, None)
                .await
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
