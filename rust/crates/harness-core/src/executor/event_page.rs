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
    /// Stored model request: its exact bytes, or for a later step the
    /// segments it shares with the steps before it.
    Request,
    /// Exact canonical bytes of the admitted model request, reconstructed
    /// from [`Self::Request`] and verified against its digest. Derived, so
    /// [`Self::fields`] never lists it.
    AdmittedRequest,
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
    /// Reconstructs the original admitted request and verifies its binding to
    /// this observation. The stored request may be a segment manifest; hashing
    /// that manifest would identify its storage layout rather than its content.
    pub async fn admitted_request(
        journal: &dyn ExecutionJournal,
        event: &ExecutionEvent,
    ) -> Result<crate::model::PreparedModelRequest> {
        let reference = Self::AdmittedRequest.select(event).ok_or_else(|| {
            Error::Invalid("admitted request selection failed: model admission required; recovery: select the original ModelStarted observation".into())
        })?;
        let request = load_model_request(journal, reference, Limits::default(), None).await?;
        if !matches!(event, ExecutionEvent::ModelStarted { request_digest, .. }
            if *request_digest == request.request_digest())
        {
            return Err(Error::Conflict("admitted request binding failed: digest differs; recovery: select the original immutable request and its admission".into()));
        }
        Ok(request)
    }

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
            (
                Self::Request | Self::AdmittedRequest,
                ExecutionEvent::ModelStarted { request, .. },
            ) => Some(request),
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
    original_page(journal, operation, after, EXECUTION_EVENT_PAGE_RECORDS).await
}

// Shared cursor semantics for interactive reads and offline projections. A short
// page describes only the current tail; no consumer can infer settlement here.
pub(super) async fn original_page(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    after: u64,
    maximum: u32,
) -> Result<ExecutionEventPage> {
    let items = journal.replay(operation, after, maximum, None)?.await?;
    let end = after
        .checked_add(items.len() as u64)
        .ok_or_else(|| Error::Invalid("journal page cursor overflows; recovery: resume event_page with the original representable source cursor".into()))?;
    let next = (items.len() == maximum as usize).then_some(end);
    Ok(ExecutionEventPage {
        operation: Some(operation),
        items,
        end,
        next,
    })
}
