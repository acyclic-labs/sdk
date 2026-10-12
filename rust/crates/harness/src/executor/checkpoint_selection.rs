use super::*;

/// Finds the canonical base published by this execution's first response step.
pub async fn canonical_checkpoint_for_operation(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Option<FileRef>> {
    #[cfg(feature = "filesystem")]
    let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _records_hold;
    let (_, records) = replay_execution(
        journal,
        operation_id,
        1,
        |event| matches!(event, ExecutionEvent::ContextCompacted { step: 0, .. }),
        io,
    )
    .await?;
    #[cfg(feature = "filesystem")]
    {
        _records_hold = io
            .map(|io| io.guard_owned(io.owned_bytes()? - before))
            .transpose()?;
    }
    let Some(record) = records.first() else {
        return Ok(None);
    };
    let ExecutionEvent::ContextCompacted {
        projection,
        compaction,
        ..
    } = &record.event
    else {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "canonical publication event is invalid",
        ));
    };
    validate_checkpoint_json_reference(projection, limits, io)?;
    let projection = load_json::<ResponseProjection>(journal, projection, io).await?;
    crate::context::validate_projected_context(&projection.0.context, limits, io)?;
    let Some(reference) = projection.0.checkpoint.as_ref() else {
        if projection.0.canonical.is_some() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "canonical publication has no checkpoint",
            ));
        }
        return Ok(None);
    };
    validate_checkpoint_json_reference(&reference, limits, io)?;
    let envelope =
        load_json::<crate::context::CanonicalContextCheckpoint>(journal, reference, io).await?;
    envelope.0.validate(limits, io)?;
    if envelope.0.operation_id != operation_id
        || projection.0.canonical.as_ref() != Some(&envelope.0.source)
        || &envelope.0.compaction != compaction
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "checkpoint differs from its committed publication",
        ));
    }
    Ok(Some(crate::model::clone_file(reference, io)?))
}

/// Imports a checkpoint and returns its owning selection's consumed history bytes.
pub async fn load_canonical_checkpoint_at<P: acyclic_stream::StreamProvider>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    scope: &RuntimeScope,
    history: &crate::store::HistoryReader<P>,
    cursor: &crate::store::HistoryCursor,
    maximum_history_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<(
    crate::context::CanonicalContextCheckpoint,
    crate::context::Context,
    u64,
)> {
    let limits = crate::context::restrict_context_limits(limits, scope.limits(), io)?;
    validate_checkpoint_json_reference(reference, limits, io)?;
    if maximum_history_bytes == 0 || cursor.after_revision > cursor.through_revision {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "checkpoint history bounds are invalid",
        ));
    }
    let owner = journal.canonical_authority().ok_or_else(|| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Unsupported,
            "checkpoint journal has no bound canonical owner",
        )
    })?;
    if owner != &cursor.authority {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "checkpoint journal belongs to another owner",
        ));
    }
    #[cfg(feature = "filesystem")]
    let _committed_hold;
    let committed = match io {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(io) if io.original_native_work().is_some() => {
            let before = io.owned_bytes()?;
            let mut observer = crate::store::HistoryWork::new(PreparationIo::from_native_work(
                io.original_native_work()
                    .expect("native branch retains original work"),
            )?);
            let cursor = history.pin(0, Some(&mut observer)).await?;
            observer.return_staged_output()?;
            io.absorb_native_custody(observer.into_custody()?)?;
            _committed_hold = Some(io.guard_owned(io.owned_bytes()? - before)?);
            cursor
        }
        #[cfg(feature = "filesystem")]
        Some(io) => {
            let mut read_work = crate::store::HistoryWork::new(PreparationIo::from_budget(
                io.remaining()?,
                io.cancellation(),
            )?);
            let result = async {
                read_work.preflight()?;
                history.pin(0, Some(&mut read_work)).await
            }
            .await;
            let receipt = read_work.finish(result);
            let held = receipt
                .as_ref()
                .map_or(0, |receipt| receipt.work.peak_allocation_bytes);
            let value = io.receive_content(receipt)?;
            io.retain_owned(held)?;
            _committed_hold = Some(io.guard_owned(held)?);
            value
        }
        _ => {
            #[cfg(feature = "filesystem")]
            {
                _committed_hold = None;
            }
            history.pin(0, None).await?
        }
    };
    if cursor.authority != committed.authority {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "checkpoint history belongs to another owner",
        ));
    }
    if cursor.through_revision > committed.through_revision {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "checkpoint history cut is not committed",
        ));
    }
    let (envelope, retained) =
        load_canonical_checkpoint(journal, reference, limits, Some(scope), io).await?;
    #[cfg(feature = "filesystem")]
    let _selection_hold;
    let (selection, consumed) = match io {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        Some(io) if io.original_native_work().is_some() => {
            let before = io.owned_bytes()?;
            let mut observer =
                crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(
                    io.original_native_work()
                        .expect("native branch retains original work"),
                )?);
            let selection = history
                .operation_event(
                    envelope.operation_id,
                    maximum_history_bytes,
                    &mut Some(&mut observer),
                )
                .await?;
            observer.return_staged_output()?;
            io.absorb_native_custody(observer.into_custody()?)?;
            _selection_hold = Some(io.guard_owned(io.owned_bytes()? - before)?);
            selection
        }
        #[cfg(feature = "filesystem")]
        Some(io) => {
            let mut read_work = crate::store::HistoryWork::new(PreparationIo::from_budget(
                io.remaining()?,
                io.cancellation(),
            )?);
            let result = async {
                read_work.preflight()?;
                history
                    .operation_event(
                        envelope.operation_id,
                        maximum_history_bytes,
                        &mut Some(&mut read_work),
                    )
                    .await
            }
            .await;
            let receipt = read_work.finish(result);
            let held = receipt
                .as_ref()
                .map_or(0, |receipt| receipt.work.peak_allocation_bytes);
            let value = io.receive_content(receipt)?;
            io.retain_owned(held)?;
            _selection_hold = Some(io.guard_owned(held)?);
            value
        }
        _ => {
            #[cfg(feature = "filesystem")]
            {
                _selection_hold = None;
            }
            history
                .operation_event(envelope.operation_id, maximum_history_bytes, &mut None)
                .await?
        }
    };
    let selection = selection.ok_or_else(|| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "checkpoint has no owning canonical selection",
        )
    })?;
    if selection.revision <= cursor.after_revision
        || selection.revision > cursor.through_revision
        || !matches!(&selection.payload,
            crate::core::EventPayload::ModelContextSelected { selection }
                if selection == &envelope.selection)
    {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "checkpoint selection differs from its history cut",
        ));
    }
    Ok((envelope, retained, consumed))
}

pub(super) fn validate_checkpoint_json_reference(
    reference: &FileRef,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<()> {
    with_history_io(io, |work| limits.validate_file(reference, work))?;
    if reference.descriptor().byte_length() > limits.render_bytes {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Invalid,
            "checkpoint JSON exceeds effective render limit",
        ));
    }
    Ok(())
}
