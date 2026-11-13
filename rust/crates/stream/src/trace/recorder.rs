//! Explicit producer admission; no collector, background task or second store.
use super::{Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};
use crate::{AppendOutcome, IdempotencyKey, RetainedRecord, Stream, StreamError, StreamProvider};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use web_time::Instant;

/// One selected producer incarnation and its original monotonic clock.
/// Hosts retain this only for an admitted trace subscription. Constructing or
/// cloning an ordinary Stream never creates a recorder or reads a clock.
pub struct Recorder<P: ?Sized> {
    stream: Stream<P>,
    process: [u8; 16],
    origin: Instant,
    next: AtomicU64,
}

#[derive(Debug, thiserror::Error)]
enum RecorderFailure {
    #[error("trace producer exhausted its span identities")]
    IdentityExhausted,
    #[error("trace producer monotonic timestamp exceeds its representation")]
    ClockOverflow,
    #[error("unconditional trace append returned a tail conflict at {0}")]
    UnexpectedConflict(u64),
}

impl<P: StreamProvider + ?Sized> Recorder<P> {
    /// The unfollowed path: no clock read, allocation, serialization or IO.
    /// Callers obtain the optional recorder from their host's admitted interest,
    /// rather than constructing one per request or discovering interest by IO.
    pub async fn start_optional(
        recorder: Option<&Arc<Self>>,
        name: &'static str,
        mechanism: Mechanism,
        parent: Option<SpanId>,
        guarantee: Option<&'static str>,
    ) -> Result<Option<Recording<P>>, StreamError> {
        match recorder {
            Some(recorder) => recorder
                .start(name, mechanism, parent, guarantee)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    /// Bind an already authorized owner's ordinary trace stream. `process`
    /// identifies this origin and must never be reused for a new recorder.
    /// Neither this constructor nor an opaque identity grants stream access.
    pub fn new(stream: Stream<P>, process: [u8; 16]) -> Self {
        Self {
            stream,
            process,
            origin: Instant::now(),
            next: AtomicU64::new(0),
        }
    }

    fn now(&self) -> Result<u64, StreamError> {
        u64::try_from(self.origin.elapsed().as_nanos())
            .map_err(|_| StreamError::InvalidResponse.with_source(RecorderFailure::ClockOverflow))
    }

    async fn publish(&self, event: SpanEvent) -> Result<RetainedRecord, StreamError> {
        let bytes = serde_json::to_vec(&TraceRecord::V1(event))
            .map_err(|source| StreamError::InvalidResponse.with_source(source))?;
        match self.stream.append(bytes, IdempotencyKey::unique()).await? {
            AppendOutcome::Committed(receipt) => Ok(RetainedRecord {
                commit_id: receipt.commit_id,
                path: self.stream.path().clone(),
                sequence: receipt.start,
            }),
            AppendOutcome::TailConflict { actual_tail } => Err(StreamError::InvalidResponse
                .with_source(RecorderFailure::UnexpectedConflict(actual_tail))),
        }
    }

    /// Persist admission before starting the observed work. Time includes
    /// admission publication; it is not an inferred remote dispatch clock.
    /// Dropping the returned handle leaves the original start unfinished.
    pub async fn start(
        self: &Arc<Self>,
        name: &'static str,
        mechanism: Mechanism,
        parent: Option<SpanId>,
        guarantee: Option<&'static str>,
    ) -> Result<Recording<P>, StreamError> {
        let sequence = self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .map_err(|_| {
                StreamError::InvalidResponse.with_source(RecorderFailure::IdentityExhausted)
            })?;
        let id = SpanId {
            process: self.process,
            sequence,
        };
        let start = self
            .publish(SpanEvent::Started {
                id,
                parent,
                name: name.to_owned(),
                mechanism,
                at_ns: self.now()?,
                guarantee: guarantee.map(str::to_owned),
            })
            .await?;
        Ok(Recording {
            recorder: self.clone(),
            id,
            start,
        })
    }
}

/// One original admission. Only consuming it can publish its terminal delta;
/// descendants may retain its identity without retaining completion ownership.
#[must_use = "finish observed settlement, or drop to retain an unfinished start"]
pub struct Recording<P: ?Sized> {
    recorder: Arc<Recorder<P>>,
    id: SpanId,
    start: RetainedRecord,
}

impl<P: StreamProvider + ?Sized> Recording<P> {
    /// Original identity to pass to a child admission.
    pub fn id(&self) -> SpanId {
        self.id
    }

    /// Original retained publication, usable even if no terminal is published.
    pub fn start(&self) -> &RetainedRecord {
        &self.start
    }

    /// Publish the caller's actual settlement observation exactly once.
    /// A failure requires its original retained cause through Outcome's type.
    pub async fn finish(
        self,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<RetainedRecord, StreamError> {
        self.recorder
            .publish(SpanEvent::Finished {
                id: self.id,
                at_ns: self.recorder.now()?,
                bytes,
                result,
            })
            .await
    }
}
