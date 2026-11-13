//! Typed request-path facts stored as ordinary records in the owner's stream subtree.
//!
//! Producers emit deltas: a start names the work and its guarantee once; a finish
//! names its outcome once. A missing finish remains observable after a crash.
//! This schema creates no collector, clock, task, buffer, index or storage path.

use crate::RetainedRecord;

#[cfg(feature = "serde")]
mod query;
#[cfg(feature = "serde")]
pub use query::{ActiveSpan, MechanismTotals, Observation, QueryError, QueryViolation, TraceQuery};

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

/// One span delta. Monotonic timestamps are relative to `id.process`'s origin;
/// timestamps in distinct process domains do not establish elapsed time or order.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(tag = "event", rename_all = "snake_case"))]
pub enum SpanEvent {
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
        let provider = MemoryStream::default();
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
        let id = SpanId {
            process: [1; 16],
            sequence: 0,
        };
        let events = [
            TraceRecord::V1(SpanEvent::Started {
                id,
                parent: None,
                name: "stream.read".into(),
                mechanism: Mechanism::Io,
                at_ns: 10,
                guarantee: Some("read_committed_prefix".into()),
            }),
            TraceRecord::V1(SpanEvent::Finished {
                id,
                at_ns: 20,
                bytes: Some(64),
                result: Outcome::Failed {
                    cause: cause.clone(),
                },
            }),
        ];
        for (at, event) in events.iter().enumerate() {
            // An unfinished start is independently readable before its finish exists.
            let outcome = provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![crate::OwnedBytes::from(serde_json::to_vec(event)?).into()],
                    if_tail: Some(at as u64),
                    idempotency_key: IdempotencyKey::unique(),
                })
                .await?;
            assert!(matches!(outcome, AppendOutcome::Committed(_)));
            let mut page = provider
                .read(ReadRequest {
                    key: None,
                    path: path.clone(),
                    from: at as u64,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await?;
            let record = page.next().await.ok_or("missing span delta")??;
            assert_eq!(
                serde_json::from_slice::<TraceRecord>(record.value.as_slice())?,
                *event
            );
            assert!(page.next().await.is_none());
        }
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
