//! Typed request-path facts stored as ordinary records in the owner's stream subtree.
//!
//! Producers emit deltas: a start names the work and its guarantee once; a finish
//! names its outcome once. A missing finish remains observable after a crash.
//! This schema creates no collector, clock, task, buffer, index or storage path.

use crate::RetainedRecord;

#[cfg(feature = "serde")]
mod context;
#[cfg(feature = "serde")]
pub(crate) use context::{is_source, records};
#[cfg(feature = "serde")]
pub use context::{observe, observe_outcome, observe_value};
#[cfg(feature = "serde")]
mod interest;
#[cfg(feature = "serde")]
mod owned;
#[cfg(feature = "serde")]
mod recorder;
#[cfg(feature = "serde")]
pub use interest::{Lease, Registry};
#[cfg(feature = "serde")]
pub use owned::OwnedRecording;
#[cfg(feature = "serde")]
pub use recorder::{Observed, Recorder, Recording, Request, Settlement};

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
