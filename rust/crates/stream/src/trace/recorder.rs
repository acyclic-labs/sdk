//! Explicit producer admission through original owned appends, without a second store.
use super::{Comparison, ComparisonKind, Mechanism, Outcome, SpanEvent, SpanId, TraceRecord};
use crate::{AppendOutcome, IdempotencyKey, RetainedRecord, Stream, StreamError, StreamProvider};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
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
    demand: Option<AtomicUsize>,
    publications: Arc<tokio::sync::Semaphore>,
    suppressed: AtomicU64,
    #[cfg(not(target_arch = "wasm32"))]
    executor: std::sync::OnceLock<tokio::runtime::Handle>,
}

#[derive(Debug, thiserror::Error)]
enum RecorderFailure {
    #[error("trace producer exhausted its span identities")]
    IdentityExhausted,
    #[error("trace publication credit is occupied; coverage is incomplete")]
    PublicationCredit,
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    #[error("trace publication has no original executor")]
    ExecutorUnavailable,
    #[error("trace producer monotonic timestamp exceeds its representation")]
    ClockOverflow,
    #[error("unconditional trace append returned a tail conflict at {0}")]
    UnexpectedConflict(u64),
}

impl<P: ?Sized> Recorder<P> {
    /// Number of spans omitted because their original publication credit was
    /// occupied. A nonzero value means this producer's coverage is incomplete.
    pub fn suppressed_spans(&self) -> u64 {
        self.suppressed.load(Ordering::Acquire)
    }

    /// Configure simultaneous inherited observation publications. Each credit
    /// covers one start and its terminal until publication completes; saturation
    /// suppresses diagnostics rather than delaying original work.
    pub fn with_publication_credit(
        mut self,
        credit: std::num::NonZeroUsize,
    ) -> Result<Self, StreamError> {
        if credit.get() > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(
                StreamError::InvalidArgument.with_source(RecorderFailure::PublicationCredit)
            );
        }
        self.publications = Arc::new(tokio::sync::Semaphore::new(credit.get()));
        Ok(self)
    }
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn reserve_publication(&self) -> Result<tokio::sync::OwnedSemaphorePermit, StreamError> {
        self.publications.clone().try_acquire_owned().map_err(|_| {
            let _ = self
                .suppressed
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    Some(count.saturating_add(1))
                });
            StreamError::Capacity.with_source(RecorderFailure::PublicationCredit)
        })
    }
    pub(super) fn with_demand(mut self) -> Self {
        self.demand = Some(AtomicUsize::new(0));
        self
    }

    pub(super) fn subscribe_demand(&self) {
        if let Some(demand) = &self.demand {
            // The registry increments its checked total first; this owner count
            // cannot exceed that total, including concurrent subscribe/drop.
            demand.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(super) fn release_demand(&self) {
        if let Some(demand) = &self.demand {
            demand.fetch_sub(1, Ordering::AcqRel);
        }
    }

    pub(super) fn terminal_available(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            if self.executor.get().is_none()
                && let Ok(handle) = tokio::runtime::Handle::try_current()
            {
                let _ = self.executor.set(handle);
            }
            self.executor.get().is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            cfg!(feature = "wasm")
        }
    }
    pub(super) fn has_demand(&self) -> bool {
        self.demand
            .as_ref()
            .is_none_or(|demand| demand.load(Ordering::Acquire) != 0)
    }
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
            Ok(Some(recording)) => {
                super::context::within(recording.id(), recording.recorder.clone(), work).await
            }
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
            demand: None,
            publications: Arc::new(tokio::sync::Semaphore::new(64)),
            suppressed: AtomicU64::new(0),
            #[cfg(not(target_arch = "wasm32"))]
            executor: std::sync::OnceLock::new(),
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
        match super::context::disabled(self.stream.append(bytes, key)).await? {
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
        let (id, event) = self.prepare_start(name, mechanism, parent, guarantee)?;
        let start = self.publish(event).await?;
        Ok(Recording {
            recorder: self.clone(),
            id,
            start: Admission::Retained(start),
        })
    }

    fn prepare_start(
        &self,
        name: &'static str,
        mechanism: Mechanism,
        parent: Option<SpanId>,
        guarantee: Option<&'static str>,
    ) -> Result<(SpanId, SpanEvent), StreamError> {
        let _ = self.terminal_available();
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
        Ok((
            id,
            SpanEvent::Started {
                id,
                parent: parent.or_else(super::context::current),
                name: name.to_owned(),
                mechanism,
                at_ns: self.now()?,
                guarantee: guarantee.map(str::to_owned),
            },
        ))
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub(super) fn submit_start(
        self: &Arc<Self>,
        request: Request,
    ) -> Result<Recording<P>, StreamError> {
        if !self.terminal_available() {
            return Err(StreamError::Unavailable.with_source(RecorderFailure::ExecutorUnavailable));
        }
        let credit = self.reserve_publication()?;
        let (id, event) = self.prepare_start(
            request.name,
            request.mechanism,
            request.parent,
            request.guarantee,
        )?;
        let start = self.publish_submitted(event, None, None)?;
        Ok(Recording {
            recorder: self.clone(),
            id,
            start: Admission::Submitted(start, credit),
        })
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    fn publish_submitted(
        &self,
        event: SpanEvent,
        before: Option<crate::AppendPending>,
        credit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<crate::AppendPending, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        let _executor = self.executor.get().map(tokio::runtime::Handle::enter);
        let (bytes, idempotency_key) = Self::encode(event)?;
        let request = crate::AppendRequest {
            path: self.stream.path().clone(),
            records: vec![bytes.into()],
            if_tail: None,
            idempotency_key,
        };
        let command = request.clone();
        let provider = self.stream.provider.clone();
        Ok(crate::AppendPending::submit_task(
            request,
            super::context::disabled(async move {
                let _credit = credit;
                if let Some(before) = before {
                    retained_append(&before).await?;
                }
                provider.append(command).await
            }),
            &crate::runtime::PlatformExecutor,
        ))
    }
}

enum Admission {
    Retained(RetainedRecord),
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    Submitted(crate::AppendPending, tokio::sync::OwnedSemaphorePermit),
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
async fn retained_append(pending: &crate::AppendPending) -> Result<RetainedRecord, StreamError> {
    match pending.result().await? {
        Ok(AppendOutcome::Committed(receipt)) => Ok(RetainedRecord {
            commit_id: receipt.commit_id,
            path: pending.request().path.clone(),
            sequence: receipt.start,
        }),
        Ok(AppendOutcome::TailConflict { actual_tail }) => Err(StreamError::InvalidResponse
            .with_source(RecorderFailure::UnexpectedConflict(*actual_tail))),
        Err(error) => Err(error.clone()),
    }
}
/// One original admission. Only consuming it can publish its terminal delta;
/// descendants may retain its identity without retaining completion ownership.
#[must_use = "finish observed settlement, or drop to retain an unfinished start"]
pub struct Recording<P: ?Sized> {
    pub(super) recorder: Arc<Recorder<P>>,
    id: SpanId,
    start: Admission,
}

impl<P: StreamProvider + ?Sized> Recording<P> {
    /// Original identity to pass to a child admission.
    pub fn id(&self) -> SpanId {
        self.id
    }

    /// Await the original start receipt, including a previously submitted start.
    /// No retained-record identity is exposed before publication succeeds.
    pub async fn start(&self) -> Result<RetainedRecord, StreamError> {
        match &self.start {
            Admission::Retained(start) => Ok(start.clone()),
            #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
            Admission::Submitted(pending, _) => retained_append(pending).await,
        }
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
        let at_ns = self.recorder.now()?;
        self.start().await?;
        self.recorder
            .publish(SpanEvent::Finished {
                id: self.id,
                at_ns,
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
        let event = SpanEvent::Finished {
            id: self.id,
            at_ns: self.recorder.now()?,
            bytes,
            result,
        };
        let (before, credit) = match self.start {
            Admission::Retained(_) => (None, self.recorder.reserve_publication()?),
            Admission::Submitted(pending, credit) => (Some(pending), credit),
        };
        self.recorder.publish_submitted(event, before, Some(credit))
    }
}
