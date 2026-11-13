//! Explicit producer admission; no collector, background task or second store.
use super::{Comparison, ComparisonKind, Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};
use crate::{AppendOutcome, IdempotencyKey, RetainedRecord, Stream, StreamError, StreamProvider};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use web_time::Instant;

/// Static request contract, without payloads, credentials or resource labels.
pub struct Request {
    /// Stable operation name.
    pub name: &'static str,
    /// Logical mechanism, independent of backend placement.
    pub mechanism: Mechanism,
    /// Original causal parent, if known.
    pub parent: Option<SpanId>,
    /// Public guarantee the request buys.
    pub guarantee: Option<&'static str>,
}

/// Original operation output and independent observation-publication result.
/// A diagnostic failure never replaces a successful read or its native cause.
#[must_use]
pub struct Observed<T, E, P: ?Sized> {
    /// The original operation result, moved without wrapping its error.
    pub result: Result<T, E>,
    /// Observation state; publication failures retain their original sources.
    pub observation: Result<Settlement<P>, StreamError>,
}

/// What the observer actually retained, distinct from the operation's result.
pub enum Settlement<P: ?Sized> {
    /// No host-admitted interest; no observation work was performed.
    Disabled,
    /// A successful operation has an original terminal publication.
    Finished(RetainedRecord),
    /// Completion belongs to the caller. Failures need their original retained
    /// cause before finishing as Failed; dropping preserves incomplete coverage.
    Pending(Recording<P>),
}

/// One selected producer incarnation and its original monotonic clock.
/// Hosts retain this only for an admitted trace subscription. Constructing or
/// cloning an ordinary Stream never creates a recorder or reads a clock.
pub struct Recorder<P: ?Sized> {
    pub(super) stream: Stream<P>,
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
    /// Observe polling of the original future, retaining its output even when
    /// trace publication fails. Disabled interest never evaluates `bytes`.
    /// A failed operation cannot acquire an invented persisted cause: its
    /// pending handle requires original evidence through Outcome::Failed.
    pub async fn observe<T, E>(
        recorder: Option<&Arc<Self>>,
        request: Request,
        work: impl std::future::Future<Output = Result<T, E>>,
        bytes: impl FnOnce(&T) -> Option<u64>,
    ) -> Observed<T, E, P> {
        let Observed {
            result,
            observation,
        } = Self::handoff(recorder, request, work).await;
        let measured = match (&result, &observation) {
            (Ok(value), Ok(Settlement::Pending(_))) => Some(bytes(value)),
            _ => None,
        };
        let observation = match (observation, measured) {
            (Ok(Settlement::Pending(recording)), Some(bytes)) => recording
                .finish(bytes, Outcome::Succeeded)
                .await
                .map(Settlement::Finished),
            (observation, _) => observation,
        };
        Observed {
            result,
            observation,
        }
    }

    /// Return an original dispatch claim without suspending after its work.
    /// Completion must be published outside that irreversible handoff. Dropping
    /// its recording leaves an explicitly unfinished start, never a false finish.
    pub async fn handoff<T, E>(
        recorder: Option<&Arc<Self>>,
        request: Request,
        work: impl std::future::Future<Output = Result<T, E>>,
    ) -> Observed<T, E, P> {
        let admission = Self::start_optional(
            recorder,
            request.name,
            request.mechanism,
            request.parent,
            request.guarantee,
        )
        .await;
        let result = match &admission {
            Ok(Some(recording)) => super::context::within(recording.id(), work).await,
            _ => work.await,
        };
        let observation = match admission {
            Err(error) => Err(error),
            Ok(None) => Ok(Settlement::Disabled),
            Ok(Some(recording)) => Ok(Settlement::Pending(recording)),
        };
        Observed {
            result,
            observation,
        }
    }

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

    fn encode(event: SpanEvent) -> Result<(Vec<u8>, IdempotencyKey), StreamError> {
        let (id, phase) = match &event {
            SpanEvent::Started { id, .. } => (id, 0),
            SpanEvent::Finished { id, .. } => (id, 1),
            SpanEvent::Compared { id, comparison } => (
                id,
                match comparison.kind {
                    ComparisonKind::Request => 2,
                    ComparisonKind::Output => 3,
                    ComparisonKind::CachePrefix => 4,
                },
            ),
        };
        let mut key = b"acyclic.trace.v1".to_vec();
        key.extend_from_slice(&id.process);
        key.extend_from_slice(&id.sequence.to_be_bytes());
        key.push(phase);
        let key = IdempotencyKey::new(key)?;
        let bytes = serde_json::to_vec(&TraceRecord::V1(event))
            .map_err(|source| StreamError::InvalidResponse.with_source(source))?;
        Ok((bytes, key))
    }

    async fn publish(&self, event: SpanEvent) -> Result<RetainedRecord, StreamError> {
        let (bytes, key) = Self::encode(event)?;
        match self.stream.append(bytes, key).await? {
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
                parent: parent.or_else(super::context::current),
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

    /// Publish a comparison made from the request path's original integrity
    /// descriptors. At most one fact per kind belongs to a span; replaying the
    /// same fact uses its original publication key. No history is scanned.
    pub async fn compare(&self, comparison: Comparison) -> Result<RetainedRecord, StreamError> {
        self.recorder
            .publish(SpanEvent::Compared {
                id: self.id,
                comparison,
            })
            .await
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

    /// Submit the terminal without a suspension point after irreversible work.
    /// The shared append producer retains the exact bytes and retry identity
    /// independently of this recording or any completion observer.
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub fn finish_submitted(
        self,
        bytes: Option<u64>,
        result: Outcome,
    ) -> Result<crate::AppendPending, StreamError> {
        let (bytes, idempotency_key) = Recorder::<P>::encode(SpanEvent::Finished {
            id: self.id,
            at_ns: self.recorder.now()?,
            bytes,
            result,
        })?;
        Ok(self.recorder.stream.submit_append(crate::AppendRequest {
            path: self.recorder.stream.path().clone(),
            records: vec![bytes.into()],
            if_tail: None,
            idempotency_key,
        }))
    }
}
