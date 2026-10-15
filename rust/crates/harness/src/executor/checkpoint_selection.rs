use super::*;
use crate::wire::ErrorCode::{Conflict, Invalid, Storage, Unauthorized, Unsupported};

/// Finds the canonical base published by this execution's first response step.
pub async fn canonical_checkpoint_for_operation(
    journal: &dyn ExecutionJournal,
    operation_id: OperationId,
    limits: Limits,
    io: Option<&PreparationIo>,
) -> Result<Option<FileRef>> {
    let records = replay_execution(
        journal,
        operation_id,
        1,
        |event| matches!(event, ExecutionEvent::ContextCompacted { step: 0, .. }),
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
    let projection = load_json::<ResponseProjection>(journal, projection, io).await?;
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

/// Imports a checkpoint and returns its owning selection's consumed history bytes.
pub async fn load_canonical_checkpoint_at<P: acyclic_stream::StreamProvider>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    scope: &RuntimeScope,
    history: (
        &crate::store::HistoryReader<P>,
        &crate::store::HistoryCursor,
    ),
    maximum_history_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<(
    crate::context::CanonicalContextCheckpoint,
    crate::context::Context,
    u64,
)> {
    let (history, cursor) = history;
    let limits = crate::context::restrict_context_limits(limits, scope.limits(), io)?;
    validate_checkpoint_json_reference(reference, limits, io)?;
    if maximum_history_bytes == 0 || cursor.after_revision > cursor.through_revision {
        return Err(stock_boundary(
            io,
            Invalid,
            "checkpoint history bounds are invalid",
        ));
    }
    let owner = journal.canonical_authority().ok_or_else(|| {
        stock_boundary(
            io,
            Unsupported,
            "checkpoint journal has no bound canonical owner",
        )
    })?;
    if owner != &cursor.authority {
        return Err(stock_boundary(
            io,
            Unauthorized,
            "checkpoint journal belongs to another owner",
        ));
    }
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(PreparationIo::history_io);
    validate_checkpoint_cut(history, cursor, io).await?;
    let (envelope, retained) =
        load_canonical_checkpoint(journal, reference, limits, Some(scope), io).await?;
    #[cfg(feature = "filesystem")]
    let mut work = observer
        .as_mut()
        .map(|work| work as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work = None;
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _selection_hold;
    let (selection, consumed) = history
        .operation_event(envelope.operation_id, maximum_history_bytes, &mut work)
        .await?;
    #[cfg(feature = "filesystem")]
    {
        _selection_hold = io
            .map(|io| io.guard_owned(io.owned_bytes()? - before))
            .transpose()?;
    }
    let selection = selection.ok_or_else(|| {
        stock_boundary(io, Conflict, "checkpoint has no owning canonical selection")
    })?;
    if selection.revision <= cursor.after_revision
        || selection.revision > cursor.through_revision
        || !matches!(&selection.payload,
            crate::core::EventPayload::ModelContextSelected { selection }
            if selection == &envelope.selection)
    {
        return Err(stock_boundary(
            io,
            Conflict,
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
            Invalid,
            "checkpoint JSON exceeds effective render limit",
        ));
    }
    Ok(())
}

async fn validate_checkpoint_cut<P: acyclic_stream::StreamProvider>(
    history: &crate::store::HistoryReader<P>,
    cursor: &crate::store::HistoryCursor,
    io: Option<&PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(PreparationIo::history_io);

    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _pin_hold;
    #[cfg(feature = "filesystem")]
    let work = observer
        .as_mut()
        .map(|work| work as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let work = None;
    let committed = history.pin(0, work).await?;
    #[cfg(feature = "filesystem")]
    {
        _pin_hold = io
            .map(|io| io.guard_owned(io.owned_bytes()? - before))
            .transpose()?;
    }
    if cursor.authority != committed.authority {
        return Err(stock_boundary(
            io,
            Unauthorized,
            "checkpoint history belongs to another owner",
        ));
    }
    if cursor.through_revision > committed.through_revision {
        return Err(stock_boundary(
            io,
            Invalid,
            "checkpoint history cut is not committed",
        ));
    }
    Ok(())
}
