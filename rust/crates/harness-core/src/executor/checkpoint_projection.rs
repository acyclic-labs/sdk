use super::*;
use crate::wire::ErrorCode::{Conflict, Storage, Unauthorized};

/// Loads a published checkpoint under the optional admission's grants and bounds.
pub async fn load_canonical_checkpoint(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    scope: Option<&RuntimeScope>,
    io: Option<&PreparationIo>,
) -> Result<(
    crate::context::CanonicalContextCheckpoint,
    crate::context::Context,
)> {
    let limits = match scope {
        Some(scope) => crate::context::restrict_context_limits(limits, scope.limits(), io)?,
        None => limits,
    };
    validate_checkpoint_json_reference(reference, limits, io)?;
    let envelope =
        load_json::<crate::context::CanonicalContextCheckpoint>(journal, reference, io).await?;
    envelope.0.validate(limits, io)?;
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _publication_hold;
    let publication =
        canonical_checkpoint_for_operation(journal, envelope.0.operation_id, limits, io).await?;
    #[cfg(feature = "filesystem")]
    {
        _publication_hold = io
            .map(|io| io.guard_owned(io.owned_bytes()? - before))
            .transpose()?;
    }
    if publication.as_ref() != Some(reference) {
        return Err(stock_boundary(
            io,
            Conflict,
            "checkpoint is not published by its owning execution",
        ));
    }
    for file in [
        &envelope.0.source,
        &envelope.0.retained,
        &envelope.0.compaction,
    ] {
        validate_checkpoint_json_reference(file, limits, io)?;
    }
    let source = load_json::<crate::context::Context>(journal, &envelope.0.source, io).await?;
    let retained = load_json::<crate::context::Context>(journal, &envelope.0.retained, io).await?;
    let proof =
        load_json::<crate::context::CompactionReference>(journal, &envelope.0.compaction, io)
            .await?;
    envelope
        .0
        .validate_projection(&source.0, &retained.0, &proof.0, limits, io)?;
    for file in retained.0.metadata.values() {
        with_history_io(io, |work| limits.validate_file(file, work))?;
        if let Some(scope) = scope
            && !with_history_io(io, |work| {
                crate::runtime::read_granted_with_work(scope.grants(), file, work)
            })?
        {
            return Err(stock_boundary(
                io,
                Unauthorized,
                "attenuated task cannot read checkpoint metadata",
            ));
        }
    }
    verify_model_contents_scoped(
        journal,
        retained.0.messages.iter().map(|message| &message.content),
        limits,
        scope,
        io,
        None,
    )
    .await?;
    for file in retained.0.metadata.values() {
        journal.verify_input_file(file, io)?.await?;
    }
    if proof
        .0
        .summary
        .as_ref()
        .is_none_or(|summary| summary.step != 0)
    {
        return Err(stock_boundary(
            io,
            Storage,
            "canonical checkpoint summary belongs to another step",
        ));
    }
    Ok((retain_preparation(envelope)?, retain_preparation(retained)?))
}
