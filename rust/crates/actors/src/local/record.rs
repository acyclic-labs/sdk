//! The one encoding of an actor state record: protobuf, so the checkpoint's
//! artifact bytes are a raw bytes field rather than a JSON number array, and
//! a record's size is its fields' sizes plus a few bytes of framing.
use super::{Alarm, Error, State, Subscription};
use crate::guest::{Checkpoint, CodecIdentity};
use acyclic_stream::StreamPath;
use prost::Message as _;
use std::{collections::BTreeMap, sync::Arc};

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
    #[prost(bool, tag = "12")]
    custody: bool,
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

/// The alarm fields of a state record; every other field is skipped unread.
#[derive(Clone, PartialEq, prost::Message)]
struct AlarmRecord {
    #[prost(uint64, tag = "4")]
    generation: u64,
    #[prost(uint64, optional, tag = "5")]
    deadline_unix_ms: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    delivered_generation: Option<u64>,
}

/// The authority-side alarm rule: a state record that marks its alarm
/// delivered is accepted only once the authority's own commit time reaches
/// that alarm's deadline. State records are self-identifying by their format
/// header, so one rule serves every actor root on a ledger; a later record
/// still carrying that delivered alarm is always past its deadline, since the
/// authority's commit time is monotonic. Install it with
/// install_commit_admission on the provider that decides actor commits. It is
/// one process-wide rule, so every opener of a shared ledger installs it
/// idempotently.
#[must_use]
pub fn alarm_admission() -> Arc<dyn acyclic_stream::CommitAdmission> {
    struct Alarms;
    impl acyclic_stream::CommitAdmission for Alarms {
        fn not_before(&self, request: &acyclic_stream::CommitRequest) -> Option<u64> {
            request
                .mutations()
                .flat_map(|mutation| match mutation {
                    acyclic_stream::CommitMutation::Append { records, .. }
                    | acyclic_stream::CommitMutation::Fork { records, .. } => records,
                })
                .filter_map(|record| delivered_deadline(&record.value))
                .max()
        }
    }
    static ALARMS: std::sync::LazyLock<Arc<dyn acyclic_stream::CommitAdmission>> =
        std::sync::LazyLock::new(|| Arc::new(Alarms));
    Arc::clone(&ALARMS)
}

fn delivered_deadline(record: &[u8]) -> Option<u64> {
    let alarm = AlarmRecord::decode(record.strip_prefix(STATE_FORMAT_V2)?).ok()?;
    alarm
        .deadline_unix_ms
        .filter(|_| alarm.delivered_generation == Some(alarm.generation))
}

impl State {
    /// This state as its durable record.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload = Record {
            epoch: self.epoch,
            escrow: self.escrow.map(crate::escrow::Escrow::encode),
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
            metadata: self
                .metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            journal_tail: self.journal_tail,
            custody: self.custody,
            placement_branch: self
                .placement_branch
                .map(|commit| commit.as_bytes().to_vec()),
        };
        let mut record = Vec::with_capacity(STATE_FORMAT_V2.len() + payload.encoded_len());
        record.extend_from_slice(STATE_FORMAT_V2);
        payload.encode_raw(&mut record);
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
            custody: record.custody,
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

// Custody intents live on `<actor state>/custody`, never in state records,
// under their own magic.
const CUSTODY_FORMAT_V1: &[u8] = b"\xffAPC\x01";

#[derive(Clone, PartialEq, prost::Message)]
struct IntentRecord {
    #[prost(string, tag = "1")]
    kind: String,
    #[prost(bytes = "bytes", tag = "2")]
    plan: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")]
    key: bytes::Bytes,
    #[prost(string, optional, tag = "4")]
    delivered_path: Option<String>,
    #[prost(uint64, optional, tag = "5")]
    delivered_sequence: Option<u64>,
}

/// A composed publication's durable participant custody. It is appended by
/// that publication itself, so it exists exactly when the publication applied,
/// under the same actor head and epoch fence; recovery needs no other phase.
/// The publication's retained envelope supplies everything else: the
/// receipt, the next checkpoint, and the state record it replaced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Intent {
    /// The participant request the composer opened.
    pub(super) participant: crate::guest::Participant,
    /// The publication's retry identity, whose retained outcome is its receipt.
    pub(super) key: acyclic_stream::IdempotencyKey,
    /// The delivered input record, by stream and sequence.
    pub(super) delivered: Option<(StreamPath, u64)>,
}

impl Intent {
    pub(super) fn encode(&self) -> Vec<u8> {
        let payload = IntentRecord {
            kind: self.participant.kind.clone(),
            plan: self.participant.plan.clone(),
            key: bytes::Bytes::copy_from_slice(self.key.as_bytes()),
            delivered_path: self
                .delivered
                .as_ref()
                .map(|(path, _)| path.as_str().to_owned()),
            delivered_sequence: self.delivered.as_ref().map(|(_, sequence)| *sequence),
        };
        let mut record = Vec::with_capacity(CUSTODY_FORMAT_V1.len() + payload.encoded_len());
        record.extend_from_slice(CUSTODY_FORMAT_V1);
        payload.encode_raw(&mut record);
        record
    }

    pub(super) fn decode(record: &[u8]) -> Result<Self, Error> {
        let record = acyclic_stream::persistence::header(
            record,
            &CUSTODY_FORMAT_V1[..4],
            &CUSTODY_FORMAT_V1[4..],
        )?;
        let record = IntentRecord::decode(record)
            .map_err(|_| Error::Invalid("malformed actor custody record"))?;
        let delivered = match (record.delivered_path, record.delivered_sequence) {
            (Some(path), Some(sequence)) => Some((
                StreamPath::new(path)
                    .map_err(|_| Error::Invalid("custody input is not a stream path"))?,
                sequence,
            )),
            (None, None) => None,
            _ => return Err(Error::Invalid("malformed actor custody input")),
        };
        Ok(Self {
            participant: crate::guest::Participant {
                kind: record.kind,
                plan: record.plan,
            },
            key: acyclic_stream::IdempotencyKey::new(record.key)?,
            delivered,
        })
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
            placement_branch: Some(acyclic_stream::CommitId::from_bytes([0x7f; 32])),
            escrow: Some(
                crate::escrow::Escrow::new(Some(u64::MAX))
                    .in_branch(Some(acyclic_stream::CommitId::from_bytes([0x7f; 32]))),
            ),
            metadata: BTreeMap::from([("durable".into(), bytes::Bytes::from_static(b"value"))]),
            journal_tail: u64::MAX,
            custody: true,
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
