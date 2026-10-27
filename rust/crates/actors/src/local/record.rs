//! The one encoding of an actor state record: protobuf, so the checkpoint's
//! artifact bytes are a raw bytes field rather than a JSON number array, and
//! a record's size is its fields' sizes plus a few bytes of framing.
use super::{Alarm, Error, State, Subscription};
use crate::guest::{Checkpoint, CodecIdentity};
use acyclic_stream::StreamPath;
use prost::Message as _;
use std::collections::BTreeMap;

/// Framing bytes one record adds beyond its variable fields, at most: tags,
/// lengths and the fixed integers and flags.
pub const STATE_RECORD_OVERHEAD: usize = 96;
/// Framing bytes each subscription adds beyond its id and path, at most.
pub const SUBSCRIPTION_OVERHEAD: usize = 32;

#[derive(Clone, PartialEq, prost::Message)]
struct Record {
    #[prost(uint64, tag = "1")]
    epoch: u64,
    #[prost(message, optional, tag = "2")]
    checkpoint: Option<CheckpointRecord>,
    #[prost(btree_map = "string, message", tag = "3")]
    subscriptions: BTreeMap<String, SubscriptionRecord>,
    #[prost(uint64, tag = "4")]
    alarm_generation: u64,
    #[prost(uint64, optional, tag = "5")]
    alarm_deadline_unix_ms: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    alarm_delivered_generation: Option<u64>,
    #[prost(bool, tag = "7")]
    paused: bool,
}

#[derive(Clone, PartialEq, prost::Message)]
struct CheckpointRecord {
    #[prost(string, tag = "1")]
    codec: String,
    #[prost(string, tag = "2")]
    version: String,
    #[prost(bytes = "bytes", tag = "3")]
    bytes: bytes::Bytes,
}

#[derive(Clone, PartialEq, prost::Message)]
struct SubscriptionRecord {
    #[prost(string, tag = "1")]
    path: String,
    #[prost(uint64, tag = "2")]
    cursor: u64,
}

impl State {
    /// This state as its durable record.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        Record {
            epoch: self.epoch,
            checkpoint: self.checkpoint.as_ref().map(|checkpoint| CheckpointRecord {
                codec: checkpoint.codec.name.clone(),
                version: checkpoint.codec.version.clone(),
                bytes: checkpoint.bytes.clone(),
            }),
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(id, subscription)| {
                    (
                        id.clone(),
                        SubscriptionRecord {
                            path: subscription.path.as_str().to_owned(),
                            cursor: subscription.cursor,
                        },
                    )
                })
                .collect(),
            alarm_generation: self.alarm.generation,
            alarm_deadline_unix_ms: self.alarm.deadline_unix_ms,
            alarm_delivered_generation: self.alarm.delivered_generation,
            paused: self.paused,
        }
        .encode_to_vec()
    }

    /// The state a durable record holds.
    pub fn decode(record: &[u8]) -> Result<Self, Error> {
        let record =
            Record::decode(record).map_err(|_| Error::Invalid("malformed actor state record"))?;
        let mut subscriptions = BTreeMap::new();
        for (id, subscription) in record.subscriptions {
            let path = StreamPath::new(subscription.path)
                .map_err(|_| Error::Invalid("actor subscription path is not a stream path"))?;
            subscriptions.insert(
                id,
                Subscription {
                    path,
                    cursor: subscription.cursor,
                },
            );
        }
        Ok(Self {
            epoch: record.epoch,
            checkpoint: record.checkpoint.map(|checkpoint| Checkpoint {
                codec: CodecIdentity {
                    name: checkpoint.codec,
                    version: checkpoint.version,
                },
                bytes: checkpoint.bytes,
            }),
            subscriptions,
            alarm: Alarm {
                generation: record.alarm_generation,
                deadline_unix_ms: record.alarm_deadline_unix_ms,
                delivered_generation: record.alarm_delivered_generation,
            },
            paused: record.paused,
        })
    }

    /// The largest record a state with these variable parts encodes to:
    /// `checkpoint` codec, version and artifact bytes, and each subscription's
    /// id and path. Every state record is at most this.
    #[must_use]
    pub const fn record_bound(checkpoint_bytes: usize, subscription_bytes: usize) -> usize {
        STATE_RECORD_OVERHEAD + checkpoint_bytes + subscription_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_record_round_trips_with_raw_checkpoint_bytes_within_its_bound() {
        let path = StreamPath::new("harness/agents/a/inbox").expect("path");
        let bytes = bytes::Bytes::from(vec![0xff; 10_000]);
        let state = State {
            epoch: u64::MAX,
            checkpoint: Some(Checkpoint {
                codec: CodecIdentity {
                    name: "acyclic.harness.agent".into(),
                    version: "1".into(),
                },
                bytes: bytes.clone(),
            }),
            subscriptions: BTreeMap::from([(
                "inbox".to_owned(),
                Subscription {
                    path: path.clone(),
                    cursor: u64::MAX,
                },
            )]),
            alarm: Alarm {
                generation: u64::MAX,
                deadline_unix_ms: Some(u64::MAX),
                delivered_generation: Some(u64::MAX),
            },
            paused: true,
        };
        let record = state.encode();
        assert_eq!(State::decode(&record).expect("decodes"), state);
        let variable = "acyclic.harness.agent".len() + "1".len() + bytes.len();
        let subscription = "inbox".len() + path.as_str().len() + SUBSCRIPTION_OVERHEAD;
        assert!(record.len() <= State::record_bound(variable, subscription));
        // Raw: the record is the artifact bytes plus framing, not ~3.8x them.
        assert!(record.len() < bytes.len() + 200);
    }
}
