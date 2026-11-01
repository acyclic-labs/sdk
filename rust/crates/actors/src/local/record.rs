//! The one encoding of an actor state record: protobuf, so the checkpoint's
//! artifact bytes are a raw bytes field rather than a JSON number array, and
//! a record's size is its fields' sizes plus a few bytes of framing.
use super::{Alarm, Error, State, Subscription};
use crate::guest::{Checkpoint, CodecIdentity};
use acyclic_stream::StreamPath;
use prost::Message as _;
use prost::encoding::{
    WireType, btree_map, bytes as wire_bytes, encode_key, encode_varint, encoded_len_varint,
    key_len, string, uint64,
};
use std::collections::BTreeMap;

/// Framing bytes one record adds beyond its variable fields, at most: tags,
/// lengths and the fixed integers and flags.
pub const STATE_RECORD_OVERHEAD: usize = 96 + STATE_FORMAT_V1.len();
// The first byte has forbidden protobuf wire type 7: an older unversioned
// decoder MUST fail rather than discard metadata and subsequently rewrite it.
const STATE_FORMAT_V1: &[u8] = b"\xffAST\x01";
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
        // The generated scalar writer has no owned variable fields. Encode
        // checkpoint and subscriptions below from their original backing.
        let payload = Record {
            epoch: 0,
            checkpoint: None,
            subscriptions: BTreeMap::new(),
            alarm_generation: self.alarm.generation,
            alarm_deadline_unix_ms: self.alarm.deadline_unix_ms,
            alarm_delivered_generation: self.alarm.delivered_generation,
            paused: self.paused,
            metadata: BTreeMap::new(),
            journal_tail: 0,
        };
        let checkpoint_len = self.checkpoint.as_ref().map(checkpoint_len);
        let subscriptions_len = self
            .subscriptions
            .iter()
            .map(|(id, subscription)| framed_len(3, subscription_entry_len(id, subscription)))
            .sum::<usize>();
        let metadata_len = btree_map::encoded_len(
            string::encoded_len,
            wire_bytes::encoded_len,
            8,
            &self.metadata,
        );
        let journal_len = if self.journal_tail == 0 {
            0
        } else {
            uint64::encoded_len(9, &self.journal_tail)
        };
        let mut record = Vec::with_capacity(
            STATE_FORMAT_V1.len()
                + nonzero_len(1, self.epoch)
                + checkpoint_len.map_or(0, |len| framed_len(2, len))
                + subscriptions_len
                + payload.encoded_len()
                + metadata_len
                + journal_len,
        );
        record.extend_from_slice(STATE_FORMAT_V1);
        encode_nonzero(1, self.epoch, &mut record);
        if let Some(checkpoint) = &self.checkpoint {
            encode_frame(2, checkpoint_len.expect("present checkpoint"), &mut record);
            encode_blob(1, checkpoint.codec.name.as_bytes(), &mut record);
            encode_blob(2, checkpoint.codec.version.as_bytes(), &mut record);
            encode_blob(3, &checkpoint.bytes, &mut record);
        }
        for (id, subscription) in &self.subscriptions {
            encode_frame(3, subscription_entry_len(id, subscription), &mut record);
            encode_blob(1, id.as_bytes(), &mut record);
            let len = subscription_len(subscription);
            if len != 0 {
                encode_frame(2, len, &mut record);
                encode_blob(1, subscription.path.as_str().as_bytes(), &mut record);
                encode_nonzero(2, subscription.cursor, &mut record);
            }
        }
        payload.encode_raw(&mut record);
        btree_map::encode(
            string::encode,
            string::encoded_len,
            wire_bytes::encode,
            wire_bytes::encoded_len,
            8,
            &self.metadata,
            &mut record,
        );
        if self.journal_tail != 0 {
            uint64::encode(9, &self.journal_tail, &mut record);
        }
        record
    }

    /// The state a durable record holds.
    pub fn decode(record: &[u8]) -> Result<Self, Error> {
        let record = acyclic_stream::persistence::header(record, b"\xffAST", b"\x01")?;
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

// Use the original protobuf primitives and default elision. No temporary
// checkpoint strings, subscription map, ids or paths are constructed.
fn framed_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

fn blob_len(tag: u32, bytes: &[u8]) -> usize {
    if bytes.is_empty() {
        0
    } else {
        framed_len(tag, bytes.len())
    }
}

fn nonzero_len(tag: u32, value: u64) -> usize {
    if value == 0 {
        0
    } else {
        uint64::encoded_len(tag, &value)
    }
}

fn checkpoint_len(checkpoint: &Checkpoint) -> usize {
    blob_len(1, checkpoint.codec.name.as_bytes())
        + blob_len(2, checkpoint.codec.version.as_bytes())
        + blob_len(3, &checkpoint.bytes)
}

fn subscription_len(subscription: &Subscription) -> usize {
    blob_len(1, subscription.path.as_str().as_bytes()) + nonzero_len(2, subscription.cursor)
}

fn subscription_entry_len(id: &str, subscription: &Subscription) -> usize {
    let len = subscription_len(subscription);
    blob_len(1, id.as_bytes()) + if len == 0 { 0 } else { framed_len(2, len) }
}

fn encode_frame(tag: u32, len: usize, output: &mut Vec<u8>) {
    encode_key(tag, WireType::LengthDelimited, output);
    encode_varint(len as u64, output);
}

fn encode_blob(tag: u32, bytes: &[u8], output: &mut Vec<u8>) {
    if !bytes.is_empty() {
        encode_frame(tag, bytes.len(), output);
        output.extend_from_slice(bytes);
    }
}

fn encode_nonzero(tag: u32, value: u64, output: &mut Vec<u8>) {
    if value != 0 {
        uint64::encode(tag, &value, output);
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
        let mut encoded = None;
        let allocations = acyclic_test_support::measure(|| encoded = Some(state.encode()));
        let record = encoded.expect("state encoded");
        assert_eq!(allocations.count_total, 1, "only the output buffer");
        assert_eq!(allocations.count_current, 1);
        assert_eq!(allocations.bytes_current, record.capacity() as i64);
        let original = Record::decode(&record[STATE_FORMAT_V1.len()..]).expect("v1 payload");
        // Borrowed fields preserve the original v1 encoding and default
        // elision, including map entries and the journal-tail position.
        assert_eq!(original.encode_to_vec(), record[STATE_FORMAT_V1.len()..]);
        // The former owned intermediate still produces the same wire bytes,
        // but the allocator observes its copied fields and map nodes.
        let mut owned = None;
        let copied = acyclic_test_support::measure(|| {
            owned = Some(original.clone().encode_to_vec());
        });
        assert_eq!(
            owned.expect("owned encoding"),
            record[STATE_FORMAT_V1.len()..]
        );
        assert!(copied.count_total > allocations.count_total);
        assert_eq!(State::decode(&record).expect("decodes"), state);
        // Neither an old writer ignoring new fields nor an unversioned input
        // can silently replace this actor's committed metadata and journal cut.
        assert!(Record::decode(record.as_slice()).is_err());
        assert!(State::decode(&record[STATE_FORMAT_V1.len()..]).is_err());
        for version in [0, 2, u8::MAX] {
            let mut obsolete = STATE_FORMAT_V1.to_vec();
            *obsolete.last_mut().expect("version") = version;
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
        // Raw: the record is the artifact bytes plus framing, not ~3.8x them.
        assert!(record.len() < bytes.len() + 200);
        let mut defaults = state;
        defaults.journal_tail = 0;
        defaults.epoch = 0;
        defaults.checkpoint = Some(Checkpoint {
            codec: CodecIdentity {
                name: String::new(),
                version: String::new(),
            },
            bytes: bytes::Bytes::new(),
        });
        defaults
            .subscriptions
            .insert(String::new(), Subscription { path, cursor: 0 });
        defaults.metadata.insert(String::new(), bytes::Bytes::new());
        let record = defaults.encode();
        let original = Record::decode(&record[STATE_FORMAT_V1.len()..]).expect("default fields");
        assert_eq!(original.encode_to_vec(), record[STATE_FORMAT_V1.len()..]);
        assert_eq!(
            State::decode(&record).expect("default fields decode"),
            defaults
        );
        for len in [0, 127, 128, 16_384] {
            let mut boundary = defaults.clone();
            let text = "λ".repeat(len);
            boundary.checkpoint = Some(Checkpoint {
                codec: CodecIdentity {
                    name: text.clone(),
                    version: String::new(),
                },
                bytes: bytes::Bytes::from(vec![0x80; len]),
            });
            boundary.subscriptions.insert(
                text,
                Subscription {
                    path: StreamPath::new("inputs/text").expect("canonical path"),
                    cursor: len as u64,
                },
            );
            let record = boundary.encode();
            let original =
                Record::decode(&record[STATE_FORMAT_V1.len()..]).expect("nested wire lengths");
            assert_eq!(original.encode_to_vec(), record[STATE_FORMAT_V1.len()..]);
            assert_eq!(
                State::decode(&record).expect("nested fields decode"),
                boundary
            );
        }
    }
}
