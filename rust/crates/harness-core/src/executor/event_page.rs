//! Bounded projections of the original authenticated execution journal.
use super::*;

/// Eight maximum-sized source records leave space for the reply envelope.
/// This is a page size, never an execution or history limit.
pub const EXECUTION_EVENT_PAGE_RECORDS: u32 = 8;

/// A private payload field selected from one authenticated original observation.
/// Callers select the observation, never an arbitrary private file reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum ExecutionPayloadField {
    /// Original conversation envelope.
    Envelope,
    /// Prepared or compacted model context, or a completed tool projection.
    Projection,
    /// Proof of the compacted context.
    Compaction,
    /// Exact admitted model request.
    Request,
    /// Original model stream observation stored outside its journal frame.
    Event,
    /// Original terminal model-call accounting.
    Usage,
    /// Exact admitted tool invocation.
    Invocation,
    /// Original validated tool result.
    Result,
}

impl ExecutionPayloadField {
    /// The finite payload fields of one observation, in stable export order.
    /// This walks eight field kinds, independent of payload or history size.
    pub fn fields(event: &ExecutionEvent) -> impl Iterator<Item = (Self, &FileRef)> {
        [
            Self::Envelope,
            Self::Projection,
            Self::Compaction,
            Self::Request,
            Self::Event,
            Self::Usage,
            Self::Invocation,
            Self::Result,
        ]
        .into_iter()
        .filter_map(|field| field.select(event).map(|reference| (field, reference)))
    }

    /// Selects a reference already owned by this observation. An absent field
    /// is a refusal; it never resolves another record or current workspace path.
    pub fn select(self, event: &ExecutionEvent) -> Option<&FileRef> {
        match (self, event) {
            (Self::Envelope, ExecutionEvent::ConversationItem { envelope, .. }) => Some(envelope),
            (
                Self::Projection,
                ExecutionEvent::ContextPrepared { projection, .. }
                | ExecutionEvent::ContextCompacted { projection, .. }
                | ExecutionEvent::ToolCompleted { projection, .. },
            ) => Some(projection),
            (Self::Compaction, ExecutionEvent::ContextCompacted { compaction, .. }) => {
                Some(compaction)
            }
            (Self::Request, ExecutionEvent::ModelStarted { request, .. }) => Some(request),
            (Self::Event, ExecutionEvent::Model { event, .. }) => Some(event),
            (
                Self::Usage,
                ExecutionEvent::Model { usage, .. } | ExecutionEvent::ModelInline { usage, .. },
            ) => usage.as_ref(),
            (Self::Invocation, ExecutionEvent::ToolStarted { invocation, .. }) => Some(invocation),
            (Self::Result, ExecutionEvent::ToolCompleted { result, .. }) => Some(result),
            _ => None,
        }
    }
}

/// Original observations at an exclusive journal continuation. Referenced
/// payloads keep their immutable file identities and the agent's read policy.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[serde(deny_unknown_fields)]
pub struct ExecutionEventPage {
    /// Original turn identity; absent only for an input that performs no turn.
    pub operation: Option<OperationId>,
    /// Observations in original commit order, with one-based source sequences.
    pub items: Vec<ExecutionRecord>,
    /// Exclusive original journal position after this page.
    pub end: u64,
    /// Continuation when this page filled. None describes the current tail;
    /// only the actor's consumed-input fence can establish finality.
    pub next: Option<u64>,
}

#[cfg(feature = "filesystem")]
impl ExecutionEventPage {
    pub(crate) fn empty(after: u64) -> Self {
        Self {
            operation: None,
            items: Vec::new(),
            end: after,
            next: None,
        }
    }
}

/// Selects only the requested page through ordinary journal authentication.
/// The result retains payload references, without materializing an export or
/// reconstructing prior turns in an auxiliary event store.
pub async fn event_page(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    after: u64,
) -> Result<ExecutionEventPage> {
    let items = journal
        .replay(operation, after, EXECUTION_EVENT_PAGE_RECORDS, None)?
        .await?;
    let end = after
        .checked_add(items.len() as u64)
        .ok_or_else(|| Error::Invalid("event journal continuation overflows".into()))?;
    let next = (items.len() == EXECUTION_EVENT_PAGE_RECORDS as usize).then_some(end);
    Ok(ExecutionEventPage {
        operation: Some(operation),
        items,
        end,
        next,
    })
}
