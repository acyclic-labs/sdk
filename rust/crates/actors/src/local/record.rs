//! The one encoding of an actor state record: protobuf, so the checkpoint's
//! artifact bytes are a raw bytes field rather than a JSON number array, and
//! a record's size is its fields' sizes plus a few bytes of framing.
use super::{Alarm, Error, State, Subscription};
use crate::guest::{Checkpoint, CodecIdentity};
use acyclic_stream::StreamPath;
use prost::Message as _;
use std::collections::BTreeMap;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Work {
    pub(super) entries: usize,
    pub(super) copied_bytes: usize,
    pub(super) encoded_bytes: usize,
}

#[cfg(test)]
std::thread_local! {
    static WORK: std::cell::Cell<Work> = const { std::cell::Cell::new(Work { entries: 0, copied_bytes: 0, encoded_bytes: 0 }) };
}

#[cfg(test)]
pub(super) fn work() -> Work {
    WORK.with(std::cell::Cell::get)
}

fn copy_string(value: &str) -> String {
    #[cfg(test)]
    WORK.with(|work| {
        let mut current = work.get();
        current.copied_bytes += value.len();
        work.set(current);
    });
    value.to_owned()
}

fn visit_entry() {
    #[cfg(test)]
    WORK.with(|work| {
        let mut current = work.get();
        current.entries += 1;
        work.set(current);
    });
}

// The original clone boundary is counted, not a reconstruction of its size in
// a test. Bytes and StreamPath retain backing; owned strings physically copy.
impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            epoch: self.epoch,
            placement_branch: self.placement_branch,
            escrow: self.escrow,
            checkpoint: self.checkpoint.as_ref().map(|checkpoint| Checkpoint {
                codec: CodecIdentity {
                    name: copy_string(&checkpoint.codec.name),
                    version: copy_string(&checkpoint.codec.version),
                },
                bytes: checkpoint.bytes.clone(),
            }),
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(id, subscription)| {
                    visit_entry();
                    (copy_string(id), subscription.clone())
                })
                .collect(),
            alarm: self.alarm.clone(),
            paused: self.paused,
            metadata: self
                .metadata
                .iter()
                .map(|(key, value)| {
                    visit_entry();
                    (copy_string(key), value.clone())
                })
                .collect(),
            journal_tail: self.journal_tail,
        }
    }
}

/// Framing bytes one record adds beyond its variable fields, at most: tags,
/// lengths and the fixed integers and flags.
pub const STATE_RECORD_OVERHEAD: usize =
    130 + STATE_FORMAT_V2.len() + 2 + crate::escrow::MAX_RECORD_LEN;
// The first byte has forbidden protobuf wire type 7: an older unversioned
// decoder MUST fail rather than discard metadata and subsequently rewrite it.
// Version 2 keeps funding in its host-owned field. Guest metadata from version
// 1 cannot be promoted into funding authority by the state reader.
const STATE_FORMAT_V2: &[u8] = b"\xffAST\x02";
/// Framing bytes each subscription adds beyond its id and path, at most.
pub const SUBSCRIPTION_OVERHEAD: usize = 32;
/// Framing bytes each metadata entry adds beyond its key and value, at most.
pub const METADATA_OVERHEAD: usize = 32;

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
    #[prost(btree_map = "string, bytes", tag = "8")]
    metadata: BTreeMap<String, bytes::Bytes>,
    #[prost(uint64, tag = "9")]
    journal_tail: u64,
    #[prost(bytes = "vec", optional, tag = "10")]
    placement_branch: Option<Vec<u8>>,
    #[prost(bytes = "bytes", optional, tag = "11")]
    escrow: Option<bytes::Bytes>,
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
        let payload = Record {
            epoch: self.epoch,
            escrow: self.escrow.map(crate::escrow::Escrow::encode),
            checkpoint: self.checkpoint.as_ref().map(|checkpoint| CheckpointRecord {
                codec: copy_string(&checkpoint.codec.name),
                version: copy_string(&checkpoint.codec.version),
                bytes: checkpoint.bytes.clone(),
            }),
            subscriptions: self
                .subscriptions
                .iter()
                .map(|(id, subscription)| {
                    visit_entry();
                    (
                        copy_string(id),
                        SubscriptionRecord {
                            path: copy_string(subscription.path.as_str()),
                            cursor: subscription.cursor,
                        },
                    )
                })
                .collect(),
            alarm_generation: self.alarm.generation,
            alarm_deadline_unix_ms: self.alarm.deadline_unix_ms,
            alarm_delivered_generation: self.alarm.delivered_generation,
            paused: self.paused,
            metadata: self
                .metadata
                .iter()
                .map(|(key, value)| {
                    visit_entry();
                    (copy_string(key), value.clone())
                })
                .collect(),
            journal_tail: self.journal_tail,
            placement_branch: self
                .placement_branch
                .map(|commit| commit.as_bytes().to_vec()),
        };
        let mut record = Vec::with_capacity(STATE_FORMAT_V2.len() + payload.encoded_len());
        record.extend_from_slice(STATE_FORMAT_V2);
        payload.encode_raw(&mut record);
        #[cfg(test)]
        WORK.with(|work| {
            let mut current = work.get();
            current.encoded_bytes += record.len();
            work.set(current);
        });
        record
    }

    /// The state a durable record holds.
    pub fn decode(record: &[u8]) -> Result<Self, Error> {
        let record = acyclic_stream::persistence::header(
            record,
            &STATE_FORMAT_V2[..4],
            &STATE_FORMAT_V2[4..],
        )?;
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
            escrow: record
                .escrow
                .as_ref()
                .map(|bytes| crate::escrow::Escrow::decode(bytes))
                .transpose()?,
            placement_branch: record
                .placement_branch
                .map(|bytes| {
                    bytes
                        .try_into()
                        .map(acyclic_stream::CommitId::from_bytes)
                        .map_err(|_| Error::Invalid("invalid placement branch identity"))
                })
                .transpose()?,
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
            metadata: record.metadata,
            journal_tail: record.journal_tail,
        })
    }

    /// The largest record a state with these variable parts encodes to:
    /// `checkpoint` codec, version and artifact bytes, and each subscription's
    /// id and path, and metadata key/value bytes including METADATA_OVERHEAD
    /// for each entry. Every state record is at most this.
    #[must_use]
    pub const fn record_bound(
        checkpoint_bytes: usize,
        subscription_bytes: usize,
        metadata_bytes: usize,
    ) -> usize {
        STATE_RECORD_OVERHEAD + checkpoint_bytes + subscription_bytes + metadata_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full-state export necessarily encodes all selected state. Keep this as
    /// the positive byte-counter control for delta-publication scaling proofs;
    /// a successful export test is not a claim that publication is incremental.
    #[test]
    fn full_export_exposes_original_copied_and_encoded_state_bytes() -> Result<(), Error> {
        fn measure(population: usize) -> Result<Work, Error> {
            let mut state = State {
                epoch: 1,
                placement_branch: None,
                escrow: None,
                checkpoint: None,
                subscriptions: BTreeMap::new(),
                alarm: Alarm::default(),
                paused: false,
                metadata: BTreeMap::new(),
                journal_tail: 0,
            };
            for index in 0..population {
                let id = format!("entry-{index:08x}");
                state
                    .metadata
                    .insert(id.clone(), bytes::Bytes::from_static(b"retained-value"));
                state.subscriptions.insert(
                    id,
                    Subscription {
                        path: StreamPath::new(format!("input/{index:08x}"))?,
                        cursor: 0,
                    },
                );
            }
            let before = work();
            let copied = state.clone();
            let encoded = copied.encode();
            let after = work();
            let measured = Work {
                entries: after.entries - before.entries,
                copied_bytes: after.copied_bytes - before.copied_bytes,
                encoded_bytes: after.encoded_bytes - before.encoded_bytes,
            };
            assert_eq!(
                measured.encoded_bytes,
                encoded.len(),
                "count the original encoded output"
            );
            assert_eq!(
                measured.entries,
                4 * population,
                "clone and encode each visit both original maps"
            );
            assert_eq!(State::decode(&encoded)?, state);
            let key = "entry-00000000";
            assert_eq!(
                copied.metadata[key].as_ptr(),
                state.metadata[key].as_ptr(),
                "Bytes clone must not be charged as a payload copy"
            );
            Ok(measured)
        }
        let small = measure(256)?;
        let large = measure(4_096)?;
        assert!(small.copied_bytes > 0 && small.encoded_bytes > 0);
        assert_eq!(large.entries, 16 * small.entries);
        assert_eq!(large.copied_bytes, 16 * small.copied_bytes);
        assert!(large.encoded_bytes > 10 * small.encoded_bytes);
        Ok(())
    }

    #[test]
    fn a_state_record_round_trips_with_raw_checkpoint_bytes_within_its_bound() {
        let path = StreamPath::new("harness/agents/a/inbox").expect("path");
        let bytes = bytes::Bytes::from(vec![0xff; 10_000]);
        let state = State {
            placement_branch: Some(acyclic_stream::CommitId::from_bytes([0x7f; 32])),
            escrow: Some(
                crate::escrow::Escrow::new(Some(u64::MAX))
                    .in_branch(Some(acyclic_stream::CommitId::from_bytes([0x7f; 32]))),
            ),
            metadata: BTreeMap::from([("durable".into(), bytes::Bytes::from_static(b"value"))]),
            journal_tail: u64::MAX,
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
        // Neither an old writer ignoring new fields nor an unversioned input
        // can silently replace this actor's committed metadata and journal cut.
        assert!(Record::decode(record.as_slice()).is_err());
        assert!(State::decode(&record[STATE_FORMAT_V2.len()..]).is_err());
        for version in [0, 1, u8::MAX] {
            let mut obsolete = record.clone();
            obsolete[STATE_FORMAT_V2.len() - 1] = version;
            assert!(matches!(
                State::decode(&obsolete),
                Err(Error::Stream(acyclic_stream::StreamError::Unsupported))
            ));
        }
        let variable = "acyclic.harness.agent".len() + "1".len() + bytes.len();
        let subscription = "inbox".len() + path.as_str().len() + SUBSCRIPTION_OVERHEAD;
        assert!(
            record.len()
                <= State::record_bound(
                    variable,
                    subscription,
                    "durable".len() + "value".len() + METADATA_OVERHEAD
                )
        );
    }
}
