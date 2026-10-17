use super::*;
use crate::{executor::stock_boundary, wire::ErrorCode};

pub(crate) fn plan_compaction_budget(
    policy: &ThresholdCompaction,
    context: &Context,
    count: &ModelTokenCount,
    maximum_messages: usize,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(usize, usize)> {
    // Weights select the retained suffix; they cannot reject or admit a request.
    // The completed compacted request is counted before response dispatch.
    crate::executor::with_history_io(io, |work| {
        policy.projection_budget(context, count, maximum_messages, work)
    })
}

pub(crate) fn validate_compaction(
    reference: &CompactionReference,
    source: &Context,
    compacted: &Context,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let invalid = |reason| stock_boundary(io, ErrorCode::Invalid, reason);
    crate::executor::with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let summary = match &reference.summary {
            Some(summary) => Some(ContextSummary {
                operation_id: summary.operation_id,
                step: summary.step,
                source_messages: summary.source_messages,
                source_digest: summary.source_digest,
                output: summary.output.clone_in_quota(&mut quota)?,
            }),
            None => None,
        };
        #[cfg(feature = "filesystem")]
        let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let (reconstructed, proof) = DurableContextProvider::compact(
            source,
            reference.maximum_messages as usize,
            summary,
            &reference.retention,
            io,
        )?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            quota.adopt(io.owned_bytes()? - before)?;
        }
        if reference.source_digest != proof.source_digest || reconstructed != *compacted {
            return Err(invalid(
                "compaction reference does not bind the source and retained context",
            ));
        }
        Ok(())
    })
}

pub(crate) fn validate_projected_context(
    context: &Context,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let invalid = |reason| stock_boundary(io, ErrorCode::Invalid, reason);
    crate::executor::with_history_io(io, |work| {
        limits.validate(work)?;
        context.validate_current_input(io)?;
        if context.messages.len() > limits.context_messages
            || context.metadata.len() > limits.attachments
        {
            return Err(invalid("context projection exceeded declared bounds"));
        }
        for message in &context.messages {
            message.content.validate_limits(limits, work)?;
        }
        for file in context.metadata.values() {
            limits.validate_file(file, work)?;
        }
        crate::executor::json::json_byte_length(context, limits.render_bytes, work).map(|_| ())
    })
}

pub(crate) async fn verify_file(
    file: &FileRef,
    verifier: &dyn ContentResidencyVerifier,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    if !verifier.supports_read(file.volume(), io)? {
        return Err(stock_boundary(
            io,
            ErrorCode::Unsupported,
            "context verifier has no bounded capability",
        ));
    }
    verifier
        .verify(file, file.descriptor().byte_length(), io)?
        .await
}

pub(super) async fn validate_context_refs(
    context: &Context,
    verifier: &dyn ContentResidencyVerifier,
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let invalid = |reason| stock_boundary(io, ErrorCode::Invalid, reason);
    validate_projected_context(context, limits, io)?;
    for message in &context.messages {
        for file in message.content.file_refs() {
            verify_file(file, verifier, io).await?;
        }
    }
    for (name, file) in &context.metadata {
        if !name.contains('.')
            || name.len() as u64 > crate::COMPONENT_LABEL_MAX_BYTES as u64
            || name.chars().any(char::is_control)
        {
            return Err(invalid("durable context metadata key is invalid"));
        }
        verify_file(file, verifier, io).await?;
    }
    Ok(())
}

/// A source contributes messages only; its unrelated metadata is not imported.
pub(super) fn validate_source_messages(
    messages: &[ModelMessage],
    limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::executor::with_history_io(io, |work| limits.validate(work))?;
    BorrowedContext {
        messages: ContextMessages {
            first: messages,
            user: None,
            second: &[],
        },
        metadata: &BTreeMap::new(),
        current_input_index: None,
    }
    .validate_bounds(limits, io)
}

/// A pinned provider allowance can narrow, never widen, the admitted request.
pub(crate) fn restrict_context_limits(
    pinned: Limits,
    admitted: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<Limits> {
    crate::executor::with_history_io(io, |work| {
        pinned.validate(work)?;
        admitted.validate(work)
    })?;
    Ok(Limits {
        file_bytes: pinned.file_bytes.min(admitted.file_bytes),
        path_bytes: pinned.path_bytes.min(admitted.path_bytes),
        attachments: pinned.attachments.min(admitted.attachments),
        render_bytes: pinned.render_bytes.min(admitted.render_bytes),
        model_steps: pinned.model_steps.min(admitted.model_steps),
        model_events_per_step: pinned
            .model_events_per_step
            .min(admitted.model_events_per_step),
        tool_calls_per_step: pinned.tool_calls_per_step.min(admitted.tool_calls_per_step),
        context_messages: pinned.context_messages.min(admitted.context_messages),
    })
}
