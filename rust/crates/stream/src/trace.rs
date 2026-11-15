//! Typed request-path facts stored as ordinary records in the owner's stream subtree.
//!
//! Producers emit deltas: a start names the work and its guarantee once; a finish
//! names its outcome once. A missing finish remains observable after a crash.
//! This schema creates no collector, clock, task, buffer, index or storage path.

use crate::RetainedRecord;

#[cfg(feature = "serde")]
mod body;
#[cfg(feature = "serde")]
mod context;
#[cfg(feature = "serde")]
pub use body::RecordedStream;
#[cfg(all(feature = "serde", not(target_arch = "wasm32")))]
pub(crate) use context::inherit_stream;
#[cfg(feature = "serde")]
pub use context::{accompany, accompany_sync};
#[cfg(feature = "serde")]
pub(crate) use context::{inherit, is_source, source_matches};
#[cfg(feature = "serde")]
mod guard;
#[cfg(feature = "serde")]
mod interest;
#[cfg(feature = "serde")]
mod owned;
#[cfg(feature = "serde")]
pub use guard::{ObservedGuard, observe_guard};
#[cfg(feature = "serde")]
mod recorder;
#[cfg(feature = "serde")]
pub use interest::{Lease, Registry};
#[cfg(feature = "serde")]
pub use owned::OwnedRecording;
#[cfg(feature = "serde")]
pub use recorder::{Observed, Recorder, Recording, Settlement};

/// Static request contract, without payloads, credentials or resource labels.
pub struct Request {
    /// Stable operation name. A [`Mechanism::Wait`] reports under this name.
    pub name: &'static str,
    /// Logical mechanism, independent of backend placement.
    pub mechanism: Mechanism,
    /// Original causal parent, if known.
    pub parent: Option<SpanId>,
    /// Public guarantee the request buys.
    pub guarantee: Option<&'static str>,
    /// Opaque operation identity a live wait reports beside its name. Never a
    /// prompt, credential or resource label.
    pub key: Option<Key>,
}

/// Opaque identity of one waiting operation, formatted only when a
/// wait-report subscriber enables the wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Key {
    /// A process-local sequence, such as a session channel id.
    Sequence(u64),
    /// A 16-byte operation identity, reported in its canonical UUID form.
    Identity([u8; 16]),
}

impl std::fmt::Display for Key {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sequence(sequence) => sequence.fmt(formatter),
            Self::Identity(bytes) => uuid::Uuid::from_bytes(*bytes).fmt(formatter),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct WaitKey(Option<Key>);

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Display for WaitKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.map_or(Ok(()), |key| key.fmt(formatter))
    }
}

/// Every [`Mechanism::Wait`] observation is a live wait in the process wait
/// report while it is pending, whether or not a trace admitted it and in every
/// build. Without a wait-report subscriber this is the tracing callsite check
/// only: no allocation, and the key is never formatted.
fn live<F: std::future::Future>(
    request: &Request,
    work: F,
) -> impl std::future::Future<Output = F::Output> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        if request.mechanism == Mechanism::Wait {
            futures::future::Either::Left(acyclic_native_runtime::obs::waiting(
                request.name,
                WaitKey(request.key),
                work,
            ))
        } else {
            futures::future::Either::Right(work)
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = request;
        work
    }
}

/// Invoke the original work under the active owner's child span. Without an
/// admitted owner this performs one local context check, with no allocation,
/// clock, subscription lookup or diagnostic IO. Publication errors never replace
/// the original result; unretained failures remain explicitly unfinished.
pub fn observe<T, E>(
    request: Request,
    work: impl std::future::Future<Output = Result<T, E>>,
    bytes: impl FnOnce(&T) -> Option<u64>,
) -> impl std::future::Future<Output = Result<T, E>> {
    observe_outcome(request, work, |result| {
        result
            .as_ref()
            .ok()
            .map(|value| (bytes(value), Outcome::Succeeded))
    })
}

/// Classify the original result only for an admitted span. Failed classification
/// still requires the original retained cause through Outcome's type. No
/// classifier or terminal publication is evaluated on an unobserved request.
pub fn observe_outcome<T, E>(
    request: Request,
    work: impl std::future::Future<Output = Result<T, E>>,
    outcome: impl FnOnce(&Result<T, E>) -> Option<(Option<u64>, Outcome)>,
) -> impl std::future::Future<Output = Result<T, E>> {
    observe_value(request, work, outcome)
}

/// Observe an original result or value, including a borrowed lock guard, without
/// wrapping an infallible mechanism in a fabricated error type. The classifier
/// runs only for an admitted span and no terminal publication delays return.
/// The observed future holds the work once: observation adds a constant size
/// and one type level, however deeply observations nest. Without `serde` no
/// trace is recorded; a [`Mechanism::Wait`] is still a live reported wait.
pub fn observe_value<T>(
    request: Request,
    work: impl std::future::Future<Output = T>,
    outcome: impl FnOnce(&T) -> Option<(Option<u64>, Outcome)>,
) -> impl std::future::Future<Output = T> {
    let work = live(&request, work);
    #[cfg(feature = "serde")]
    {
        context::record_value(request, work, outcome)
    }
    #[cfg(not(feature = "serde"))]
    {
        let _ = outcome;
        work
    }
}

/// Observe original synchronous work with optional original comparison evidence.
/// The classifier runs only after admission. Terminal and comparison share one
/// bounded append; neither waits for diagnostic IO before returning the value.
/// A [`Mechanism::Wait`] is a live reported wait while the closure runs.
pub fn observe_sync<T>(
    request: Request,
    work: impl FnOnce() -> T,
    outcome: impl FnOnce(&T) -> Option<(Option<u64>, Outcome, Option<Comparison>)>,
) -> T {
    #[cfg(not(target_arch = "wasm32"))]
    let work = {
        let wait = (request.mechanism == Mechanism::Wait).then_some((request.name, request.key));
        move || match wait {
            Some((name, key)) => {
                acyclic_native_runtime::obs::waiting_sync(name, WaitKey(key), work)
            }
            None => work(),
        }
    };
    #[cfg(feature = "serde")]
    {
        context::record_sync(request, work, outcome)
    }
    #[cfg(not(feature = "serde"))]
    {
        let _ = (request, outcome);
        work()
    }
}

/// Every blocked delivery of an opened cursor is a live reported wait, and an
/// admitted owner also records it. The cursor is wrapped only when a wait
/// reporter is installed, so an unreported cursor keeps its original box.
pub(crate) fn records(records: crate::RecordStream) -> crate::RecordStream {
    #[cfg(not(target_arch = "wasm32"))]
    let records = if acyclic_native_runtime::obs::waits_reported() {
        records.map_cursor(|cursor| {
            Box::pin(futures::stream::unfold(cursor, |mut cursor| async move {
                use futures::StreamExt as _;
                let next = live(
                    &Request {
                        name: "stream.record_delivery",
                        mechanism: Mechanism::Wait,
                        parent: None,
                        guarantee: Some("ordered_cursor_delivery"),
                        key: None,
                    },
                    cursor.next(),
                )
                .await;
                next.map(|value| (value, cursor))
            }))
        })
    } else {
        records
    };
    #[cfg(feature = "serde")]
    {
        context::records(records)
    }
    #[cfg(not(feature = "serde"))]
    {
        records
    }
}

#[cfg(feature = "serde")]
mod query;
#[cfg(feature = "serde")]
pub use query::{
    ActiveSpan, ComparisonTotals, MechanismTotals, Observation, QueryError, QuerySnapshot,
    QueryViolation, TraceQuery,
};

/// Identity assigned by one process, also identifying its monotonic clock domain.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SpanId {
    /// Unique producer incarnation; never reused after a process restart.
    pub process: [u8; 16],
    /// Producer's unique span sequence within that incarnation.
    pub sequence: u64,
}

/// Mechanism whose request-path cost is being accounted for.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Mechanism {
    /// Remote request and response.
    RoundTrip,
    /// Durable media confirmation.
    Fsync,
    /// Mutual exclusion acquisition or ownership.
    Lock,
    /// Authority verification.
    AuthCheck,
    /// Byte copying.
    Copy,
    /// Model provider request.
    ProviderCall,
    /// Tool invocation.
    ToolCall,
    /// Event-driven readiness or settlement wait.
    Wait,
    /// Queued admission.
    Queue,
    /// Atomic publication.
    Commit,
    /// Storage or device operation.
    Io,
    /// Actor message delivery.
    ActorDelivery,
}

/// An explicit terminal observation. Failure always references original evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "outcome", rename_all = "snake_case"))]
pub enum Outcome {
    /// The named work settled successfully.
    Succeeded,
    /// The named work failed; the referenced record retains its original cause.
    Failed {
        /// Original publication and position, not a repeated error message.
        cause: RetainedRecord,
    },
    /// Cancellation was observed by the owner.
    Cancelled,
    /// The operation returned or was dropped without observing settlement.
    Unsettled,
}

/// What two original integrity digests describe. Producers use the canonical
/// request/output descriptors already owned by the request path, never secrets
/// or a second serialization of model content.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ComparisonKind {
    /// Complete request identity, including its operation and bound inputs.
    Request,
    /// Complete output identity in the same comparison domain.
    Output,
    /// Expected reusable prefix versus the actually selected prefix.
    CachePrefix,
}

/// Original comparison inputs, sufficient to compute equality without keeping
/// another completed-call index. The preceding publication may be outside the
/// selected query window; its identity is explicit rather than reconstructed.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Comparison {
    /// Typed comparison domain; unlike digests from unlike domains never join.
    pub kind: ComparisonKind,
    /// Original publication owning the preceding descriptor or expected prefix.
    pub previous: RetainedRecord,
    /// Integrity digest from that original descriptor.
    pub previous_digest: [u8; 32],
    /// Integrity digest from the current original descriptor.
    pub current_digest: [u8; 32],
}

/// One span delta. Monotonic timestamps are relative to `id.process`'s origin;
/// timestamps in distinct process domains do not establish elapsed time or order.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "event", rename_all = "snake_case"))]
pub enum SpanEvent {
    /// Coverage lost to publication backpressure, observed by a later terminal.
    Coverage {
        /// Reporting span and producer incarnation.
        id: SpanId,
        /// Cumulative suppressed starts in this producer; never sum repeated reports.
        suppressed: u64,
    },
    /// Original request-path comparison; it neither starts nor settles work.
    Compared {
        /// Owning span, without reconstructing its parent or clock.
        id: SpanId,
        /// Original descriptor evidence and comparison domain.
        comparison: Comparison,
    },
    /// Original admission of named work.
    Started {
        /// Identity and monotonic clock domain.
        id: SpanId,
        /// Immediate causal parent, including a parent in another process.
        #[cfg_attr(feature = "serde", serde(deserialize_with = "required_option"))]
        parent: Option<SpanId>,
        /// Stable operation name; excludes payloads, paths and secrets.
        name: String,
        /// The mechanism doing the work.
        mechanism: Mechanism,
        /// Nanoseconds from the producer's monotonic origin.
        at_ns: u64,
        /// Public guarantee this work buys. `None` explicitly records unaccounted work.
        #[cfg_attr(feature = "serde", serde(deserialize_with = "required_option"))]
        guarantee: Option<String>,
    },
    /// Original terminal observation; contains no repeated start context.
    Finished {
        /// Identity joining the original start.
        id: SpanId,
        /// Nanoseconds from the same producer's monotonic origin.
        at_ns: u64,
        /// Bytes processed by this work, when the mechanism observes them.
        #[cfg_attr(feature = "serde", serde(deserialize_with = "required_option"))]
        bytes: Option<u64>,
        /// Original settlement observation and, on failure, its evidence.
        result: Outcome,
    },
}

// Explicit null is an observation; an absent key is malformed persisted input.
#[cfg(feature = "serde")]
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    serde::Deserialize::deserialize(deserializer)
}

/// Versioned payload of an ordinary trace stream record. Unknown versions fail
/// decoding instead of being interpreted as a current span or successful outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "version", content = "span"))]
pub enum TraceRecord {
    /// Initial delta format.
    #[cfg_attr(feature = "serde", serde(rename = "1"))]
    V1(SpanEvent),
}

#[cfg(all(test, feature = "serde", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::{
        AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath,
        StreamProvider, StreamProviderExt as _,
    };
    use futures::StreamExt as _;

    /// Observation holds the observed work once, beside a constant-size
    /// admission and recording. Nesting adds that constant per layer rather
    /// than multiplying the work, so traced futures stay small on any stack.
    #[test]
    fn observation_adds_a_constant_size_however_deeply_it_nests() {
        const LAYER_BYTES: usize = 512;
        fn work() -> impl std::future::Future<Output = Result<u8, ()>> {
            async {
                let held = std::hint::black_box([7u8; 4096]);
                std::future::ready(()).await;
                Ok(held[4095])
            }
        }
        let request = |name| Request {
            name,
            mechanism: Mechanism::Wait,
            parent: None,
            guarantee: None,
            key: None,
        };
        let inner = std::mem::size_of_val(&work());
        let once = std::mem::size_of_val(&observe(request("once"), work(), |_| None));
        let twice = std::mem::size_of_val(&observe(
            request("outer"),
            observe(request("inner"), work(), |_| None),
            |_| None,
        ));
        let guard = std::mem::size_of_val(&observe_guard(request("guard"), work()));
        // A caller's admission may borrow and carry its own error type.
        let borrowed = 7u8;
        let admission: crate::BoxProviderFuture<'_, Result<OwnedRecording, u8>> =
            Box::pin(async { Err(borrowed) });
        let accompanied =
            std::mem::size_of_val(&OwnedRecording::accompany(Some(admission), work()));
        assert!(
            accompanied <= inner + LAYER_BYTES,
            "{accompanied} > {inner} + {LAYER_BYTES}"
        );
        assert!(
            once <= inner + LAYER_BYTES,
            "{once} > {inner} + {LAYER_BYTES}"
        );
        assert!(
            twice <= inner + 2 * LAYER_BYTES,
            "{twice} > {inner} + 2 * {LAYER_BYTES}"
        );
        assert!(
            guard <= inner + LAYER_BYTES,
            "{guard} > {inner} + {LAYER_BYTES}"
        );
    }

    #[tokio::test]
    async fn span_deltas_page_and_retain_original_failure_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = std::sync::Arc::new(MemoryStream::default());
        let path = StreamPath::new("agents/worker/trace")?;
        let evidence = StreamPath::new("agents/worker/errors")?;
        let AppendOutcome::Committed(publication) = provider
            .append(AppendRequest {
                path: evidence.clone(),
                records: vec![crate::OwnedBytes::from(b"original failure".to_vec()).into()],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?
        else {
            panic!("fresh failure publication conflicted")
        };
        let cause = RetainedRecord {
            commit_id: publication.commit_id,
            path: evidence,
            sequence: 0,
        };
        let recorder = std::sync::Arc::new(Recorder::new(
            crate::Stream::new(provider.clone(), path.as_str())?,
            [1; 16],
        ));
        let admitted = recorder
            .start(
                "stream.read",
                Mechanism::Io,
                None,
                Some("read_committed_prefix"),
            )
            .await?;
        let id = admitted.id();
        let mut events = Vec::new();
        // The real producer publishes its admission before any settlement exists.
        let mut page = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = page.next().await.ok_or("missing original start")??;
        assert_eq!(record.commit_id, admitted.start().await?.commit_id);
        events.push(serde_json::from_slice::<TraceRecord>(
            record.value.as_slice(),
        )?);
        assert!(matches!(&events[0], TraceRecord::V1(SpanEvent::Started {
            id: observed, parent: None, mechanism: Mechanism::Io, guarantee: Some(guarantee), ..
        }) if *observed == id && guarantee == "read_committed_prefix"));
        assert!(page.next().await.is_none());
        let terminal = admitted
            .finish(
                Some(64),
                Outcome::Failed {
                    cause: cause.clone(),
                },
            )
            .await?;
        let mut page = provider
            .read(ReadRequest {
                key: None,
                path: path.clone(),
                from: 1,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = page.next().await.ok_or("missing original finish")??;
        assert_eq!(record.commit_id, terminal.commit_id);
        events.push(serde_json::from_slice::<TraceRecord>(
            record.value.as_slice(),
        )?);
        assert!(matches!(&events[1], TraceRecord::V1(SpanEvent::Finished {
            id: observed, bytes: Some(64), result: Outcome::Failed { cause: original }, ..
        }) if *observed == id && original == &cause));
        assert!(page.next().await.is_none());

        let mut original = provider
            .read(ReadRequest {
                key: None,
                path: cause.path,
                from: cause.sequence,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        let record = original.next().await.ok_or("missing original failure")??;
        assert_eq!(record.commit_id, cause.commit_id);
        assert_eq!(record.value.as_slice(), b"original failure");

        // Malformed persisted input cannot fabricate a causeless failed span.
        let mut malformed = serde_json::to_value(&events[1])?;
        malformed["span"]["result"]
            .as_object_mut()
            .unwrap()
            .remove("cause");
        assert!(serde_json::from_value::<TraceRecord>(malformed).is_err());
        for (event, key) in [
            (&events[0], "parent"),
            (&events[0], "guarantee"),
            (&events[1], "bytes"),
        ] {
            let mut malformed = serde_json::to_value(event)?;
            malformed["span"].as_object_mut().unwrap().remove(key);
            assert!(
                serde_json::from_value::<TraceRecord>(malformed).is_err(),
                "missing {key}"
            );
            let mut unknown = serde_json::to_value(event)?;
            unknown["span"][key] = serde_json::Value::Null;
            assert!(
                serde_json::from_value::<TraceRecord>(unknown).is_ok(),
                "explicit null {key}"
            );
        }
        Ok(())
    }
}

/// Every [`Mechanism::Wait`] is a live reported wait whether or not a trace
/// owner admits it, so one observation reaches both the hang report and an
/// admitted trace. Other mechanisms never enter the wait report.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod wait_tests {
    use super::*;
    use futures::FutureExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    fn request(mechanism: Mechanism) -> Request {
        Request {
            name: "test.wait",
            mechanism,
            parent: None,
            guarantee: None,
            key: Some(Key::Sequence(7)),
        }
    }

    #[test]
    fn an_unadmitted_wait_is_a_live_reported_wait() {
        let waits = acyclic_native_runtime::obs::WaitReport::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(waits.clone()));
        let reported = || {
            waits
                .snapshot()
                .into_iter()
                .map(|wait| (wait.primitive, wait.key))
                .collect::<Vec<_>>()
        };
        let expected = vec![("test.wait".to_owned(), "7".to_owned())];

        let mut pending = Box::pin(observe_value(
            request(Mechanism::Wait),
            std::future::pending::<()>(),
            |()| None,
        ));
        assert!(pending.as_mut().now_or_never().is_none());
        assert_eq!(reported(), expected);
        drop(pending);
        assert!(reported().is_empty(), "a dropped wait retires its report");

        let within = observe_sync(request(Mechanism::Wait), reported, |_| None);
        assert_eq!(within, expected);
        assert!(reported().is_empty(), "a returned wait retires its report");

        let mut io = Box::pin(observe_value(
            request(Mechanism::Io),
            std::future::pending::<()>(),
            |()| None,
        ));
        assert!(io.as_mut().now_or_never().is_none());
        assert!(reported().is_empty(), "only a wait is a reported wait");
    }
}
