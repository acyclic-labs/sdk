use super::*;
use crate::wire::ErrorCode::{Invalid, Storage};

/// Select one authenticated step, falling back to replay for unindexed journals.
pub(crate) async fn selected_step_records<'a>(
    journal: &dyn ExecutionJournal,
    query: ExecutionRecordQuery<'_>,
    io: Option<&'a PreparationIo>,
) -> Result<(
    u64,
    Vec<ExecutionRecord>,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    validate_journal(journal, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let before = io
        .filter(|io| io.original_native_work().is_some())
        .map(PreparationIo::owned_bytes)
        .transpose()?
        .unwrap_or(0);
    let operation = query.operation;
    let step = query.step;
    let include_started = query.include_started;
    let maximum = query.maximum;
    let select = query.select;
    let indexed = match journal.indexed_records(query, io)? {
        Some(future) => future.await?,
        None => None,
    };
    if let Some((tail, records)) = indexed {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let hold = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| {
                let retained = io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    stock_boundary(Some(io), Storage, "indexed selection ownership underflows")
                })?;
                io.guard_owned(retained)
            })
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let hold = None;
        Ok((tail, records, hold))
    } else {
        replay_execution(
            journal,
            operation,
            maximum,
            |event| {
                (event.step() == Some(step) || (include_started && event.step().is_none()))
                    && select(event)
            },
            io,
        )
        .await
    }
}

/// Finds the canonical base published by this execution's first response step.
pub async fn canonical_checkpoint_for_operation(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Option<FileRef>> {
    let records = selected_step_records(
        journal,
        ExecutionRecordQuery {
            operation: operation_id,
            step: 0,
            include_started: false,
            maximum_step_records: ExecutionRecordQuery::step_bound(limits),
            maximum: 1,
            select: &|event| matches!(event, ExecutionEvent::ContextCompacted { step: 0, .. }),
        },
        io,
    )
    .await?;
    let Some(record) = records.1.first() else {
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
            Storage,
            "canonical publication event is invalid",
        ));
    };
    validate_checkpoint_json_reference(projection, limits, io)?;
    let projection = load_response_projection(journal, projection, limits, io).await?;
    crate::context::validate_projected_context(&projection.0.context, limits, io)?;
    let Some(reference) = projection.0.checkpoint.as_ref() else {
        if projection.0.canonical.is_some() {
            return Err(stock_boundary(
                io,
                Storage,
                "canonical publication has no checkpoint",
            ));
        }
        return Ok(None);
    };
    validate_checkpoint_json_reference(reference, limits, io)?;
    let envelope =
        load_json::<crate::context::CanonicalContextCheckpoint>(journal, reference, io).await?;
    envelope.0.validate(limits, io)?;
    if envelope.0.operation_id != operation_id
        || projection.0.canonical.as_ref() != Some(&envelope.0.source)
        || &envelope.0.compaction != compaction
    {
        return Err(stock_boundary(
            io,
            Storage,
            "checkpoint differs from its committed publication",
        ));
    }
    Ok(Some(crate::model::clone_file(reference, io)?))
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
            Invalid,
            "checkpoint JSON exceeds effective render limit",
        ));
    }
    Ok(())
}
