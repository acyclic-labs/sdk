use crate::context::{
    BorrowedContext, CompactionReference, Context, ContextMessages, ContextSummary,
};
use crate::{Result, executor::stock_boundary, wire::ErrorCode};

/// Compacts a context on the optional original preparation ledger.
pub fn compact_projection(
    context: &Context,
    max_messages: usize,
    summary: Option<ContextSummary>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Context, CompactionReference)> {
    let invalid = |reason| stock_boundary(io, ErrorCode::Invalid, reason);
    context.validate_current_input(io)?;
    if max_messages == 0 {
        return Err(invalid("compaction max_messages must be positive"));
    }
    let maximum_messages = u32::try_from(max_messages)
        .map_err(|_| invalid("compaction budget exceeds portable count"))?;
    let source_digest = crate::executor::preparation_digest(context, io)?;
    if let Some(summary) = &summary {
        let covered = context
            .messages
            .get(..summary.source_messages as usize)
            .ok_or_else(|| invalid("summary extent exceeds source projection"))?;
        let summarized = BorrowedContext {
            messages: ContextMessages {
                first: covered,
                user: None,
                second: &[],
            },
            metadata: &context.metadata,
            current_input_index: context
                .current_input_index
                .filter(|index| *index < summary.source_messages),
        };
        if summary.source_messages == 0
            || summary.source_digest != crate::executor::preparation_digest(&summarized, io)?
            || summary.output.descriptor().media_type() != "text/plain"
        {
            return Err(invalid("summary does not bind its source prefix"));
        }
    }
    let (compacted, _) = crate::executor::with_history_io(io, |work| {
        if context.messages.len() > max_messages
            || (summary.is_some() && context.messages.len() == max_messages)
        {
            let summary = summary.as_ref().ok_or_else(|| {
                invalid("compaction requires a durable summary; context cannot be silently dropped")
            })?;
            super::compact_context(context, max_messages, summary, work)
        } else {
            super::clone_context(context, None, work)
        }
    })?;
    Ok((
        compacted,
        CompactionReference {
            source_digest,
            maximum_messages,
            summary,
        },
    ))
}
