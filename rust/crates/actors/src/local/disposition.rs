//! Failed inputs and their terminal dispositions are ordinary Stream records.
use super::{Error, PreparedPublish, Snapshot, Store};
use crate::guest::{Effect, GuestError, Trigger};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, IdempotencyKey, Record, RecordHeaders,
    StreamPath, StreamProvider, wire_codec,
};
use prost::Message as _;

// A forbidden protobuf wire type prevents an unversioned decoder accepting it.
const FORMAT_V1: &[u8] = b"\xffAFD\x01";
const DIRECTORY: &str = ".failures";

pub(super) fn fork_exclusions() -> Result<Vec<acyclic_stream::RelativePath>, Error> {
    Ok(vec![acyclic_stream::RelativePath::new(DIRECTORY)?])
}

pub(super) fn is_fork_exclusion(exclude: &[acyclic_stream::RelativePath]) -> bool {
    matches!(exclude, [path] if path.as_str() == DIRECTORY)
}

fn status_path(path: &StreamPath) -> Result<StreamPath, Error> {
    Ok(StreamPath::new(format!("{path}/status"))?)
}

#[derive(Clone, PartialEq, prost::Message)]
struct EntryV1 {
    #[prost(message, optional, tag = "1")]
    failed: Option<InputV1>,
    // 0 = failed input, 1 = replayed, 2 = purged. No implicit default action.
    #[prost(uint32, tag = "2")]
    resolution: u32,
}

#[derive(Clone, PartialEq, prost::Message)]
struct InputV1 {
    #[prost(string, tag = "1")]
    state: String,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(string, tag = "3")]
    subscription: String,
    #[prost(string, tag = "4")]
    source: String,
    #[prost(message, optional, tag = "5")]
    input: Option<acyclic_stream::wire::Record>,
    #[prost(bytes = "vec", tag = "6")]
    cause: Vec<u8>,
}

fn encode(value: &EntryV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FORMAT_V1.len() + value.encoded_len());
    bytes.extend_from_slice(FORMAT_V1);
    value.encode_raw(&mut bytes);
    bytes
}

fn captured_path(path: &StreamPath) -> Result<StreamPath, Error> {
    Ok(StreamPath::new(format!("{path}/input"))?)
}

fn location(state: &StreamPath, revision: u64) -> Result<StreamPath, Error> {
    Ok(StreamPath::new(format!(
        "{state}/{DIRECTORY}/{revision:020}"
    ))?)
}

/// One terminal choice for a failed input. This is an explicit new intent;
/// failures are never automatically retried or discarded by this API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureResolution {
    /// Append the original value and headers as a new delivery, and wake its owner.
    /// Accepted invocation identities cannot be replayed this way.
    Replay,
    /// Retire the visible failure. Physical byte retention remains Streams-owned.
    Purge,
}

/// Explicit disposition intent. Replay borrows the retained input selected
/// from [`FailedDelivery::input_path`], never a replacement live input.
#[derive(Clone, Copy, Debug)]
pub enum FailureDisposition<'a> {
    /// Requeue the original captured record under a new delivery sequence.
    Replay(&'a Record),
    /// Retire the failure without requeueing its input.
    Purge,
}

/// A selected immutable failed input, including the original source identity.
/// Constructed only by decoding its committed Stream record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailedDelivery {
    path: StreamPath,
    commit: CommitId,
    state: StreamPath,
    revision: u64,
    subscription: String,
    source: StreamPath,
    input: Record,
    cause: GuestError,
}

impl FailedDelivery {
    /// The original bound subscription, before its cursor was consumed.
    pub fn subscription(&self) -> &str {
        &self.subscription
    }
    /// The original bound input path; it may differ from a forked record's origin.
    pub fn source(&self) -> &StreamPath {
        &self.source
    }
    /// Read sequence zero here only when the original input is needed. The
    /// captured value/headers retain their own original frame, without embedding
    /// or re-encoding them inside the failure metadata.
    pub fn input_path(&self) -> Result<StreamPath, Error> {
        captured_path(&self.path)
    }
    /// Bind the selected retained body to the SAME failure publication, then
    /// recover its original source identity without rereading mutable resources.
    pub fn input(&self, captured: &Record) -> Result<Record, Error> {
        if captured.origin != self.input_path()?
            || captured.sequence != 0
            || captured.commit_id != self.commit
        {
            return Err(Error::Invalid(
                "failed-input body changes its original publication",
            ));
        }
        Ok(Record {
            value: captured.value.clone(),
            headers: captured.headers.clone(),
            ..self.input.clone()
        })
    }
    /// Original typed guest failure; provider and cancellation errors are not reclassified.
    pub fn cause(&self) -> &GuestError {
        &self.cause
    }
}

/// The tail projection of one failure stream. Use the ordinary `children_page`
/// over [`Store::failures_path`] to page entries, and `follow` for updates.
/// Resolved entries are explicit terminals, never fabricated empty completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureEntry {
    /// Unresolved original failure at sequence zero.
    Failed(FailedDelivery),
    /// The one committed terminal at sequence one.
    Resolved(FailureResolution),
}

impl FailureEntry {
    /// Decode only version one, using the shared Streams record/header codec.
    /// A failure copied to another path cannot authorize its original disposition.
    pub fn decode(path: &StreamPath, record: &Record) -> Result<Self, Error> {
        if status_path(path)? != record.origin {
            return Err(Error::Invalid(
                "failed input was inherited from another path",
            ));
        }
        let bytes = record
            .value
            .strip_prefix(FORMAT_V1)
            .ok_or(Error::Invalid("unsupported failed-input format"))?;
        let entry =
            EntryV1::decode(bytes).map_err(|_| Error::Invalid("invalid failed-input record"))?;
        if encode(&entry).as_slice() != record.value.as_ref() {
            return Err(Error::Invalid("non-canonical failed-input record"));
        }
        match (entry.failed, entry.resolution, record.sequence) {
            (Some(failed), 0, 0) => {
                let state = StreamPath::new(failed.state)?;
                if failed.revision == 0
                    || failed.subscription.is_empty()
                    || location(&state, failed.revision)? != *path
                {
                    return Err(Error::Invalid("failed input changes its owner or revision"));
                }
                let source = StreamPath::new(failed.source)?;
                let input = wire_codec::record(
                    failed
                        .input
                        .ok_or(Error::Invalid("failed input is missing"))?,
                    None,
                )?;
                if !input.value.is_empty() || input.headers != RecordHeaders::default() {
                    return Err(Error::Invalid("failed-input metadata embeds a body"));
                }
                let cause: GuestError = serde_json::from_slice(&failed.cause)
                    .map_err(|_| Error::Invalid("invalid failed-input cause"))?;
                if serde_json::to_vec(&cause)
                    .map_err(|_| Error::Invalid("invalid failed-input cause"))?
                    != failed.cause
                {
                    return Err(Error::Invalid("non-canonical failed-input cause"));
                }
                Ok(Self::Failed(FailedDelivery {
                    path: path.clone(),
                    commit: record.commit_id,
                    state,
                    revision: failed.revision,
                    subscription: failed.subscription,
                    source,
                    input,
                    cause,
                }))
            }
            (None, 1, 1) => Ok(Self::Resolved(FailureResolution::Replay)),
            (None, 2, 1) => Ok(Self::Resolved(FailureResolution::Purge)),
            _ => Err(Error::Invalid("invalid failed-input disposition")),
        }
    }
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// Ordinary child streams, one per original state revision. Store forks exclude
    /// this host-owned subtree, so a branch cannot inherit another owner's failures.
    /// `children_page` with `tail_stream = Some("status")` projects each failure's
    /// current terminal. Follow the projected record's origin for updates.
    pub fn failures_path(
        &self,
        actor: &crate::addressing::ActorReference,
    ) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/{DIRECTORY}",
            self.path(actor)?
        ))?)
    }

    /// Explicitly skip one original failed record, recording its exact bytes and
    /// cause in the SAME conditional commit as the cursor advance. This does no
    /// provider IO. The caller retains failure policy and must not pass an
    /// indeterminate external operation, cancellation or producer-loss error.
    /// Submit through `submit` to retain the original unknown-acknowledgement custody.
    pub fn prepare_failed_input(
        &self,
        previous: &Snapshot,
        epoch: u64,
        trigger: &Trigger,
        input: &Record,
        cause: &GuestError,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        if matches!(cause, GuestError::PreconditionChanged) {
            return Err(Error::PreconditionChanged);
        }
        let original = super::owned(previous, epoch)?;
        let Trigger::Record {
            subscription,
            sequence,
        } = trigger
        else {
            return Err(Error::Invalid("failed input is not a record delivery"));
        };
        if *sequence != input.sequence {
            return Err(Error::Invalid("failed input differs from its trigger"));
        }
        let source = original
            .subscriptions
            .get(subscription)
            .ok_or(Error::Invalid("undeclared failed-input subscription"))?;
        // Validate the selected Store before constructing any recovery bytes.
        self.actor(&previous.path)?;
        let mut state = original.clone();
        super::consume(&mut state, &original.alarm, trigger)?;
        let path = location(&previous.path, previous.tail)?;
        let status = status_path(&path)?;
        let input_path = captured_path(&path)?;
        let bytes = encode(&EntryV1 {
            failed: Some(InputV1 {
                state: previous.path.to_string(),
                revision: previous.tail,
                subscription: subscription.clone(),
                source: source.path.to_string(),
                input: Some(wire_codec::record_wire(
                    Record {
                        sequence: input.sequence,
                        commit_id: input.commit_id,
                        committed_at_micros: input.committed_at_micros,
                        origin: input.origin.clone(),
                        value: bytes::Bytes::new().into(),
                        headers: RecordHeaders::default(),
                    },
                    None,
                )),
                cause: serde_json::to_vec(cause)
                    .map_err(|_| Error::Invalid("failed-input cause does not encode"))?,
            }),
            resolution: 0,
        });
        self.prepare(
            previous,
            state,
            vec![
                Effect {
                    conditions: vec![CommitCondition::Absent {
                        path: status.clone(),
                    }],
                    mutation: CommitMutation::Append {
                        path: status,
                        records: vec![bytes.into()],
                    },
                },
                Effect {
                    conditions: vec![CommitCondition::Absent {
                        path: input_path.clone(),
                    }],
                    mutation: CommitMutation::Append {
                        path: input_path,
                        records: vec![AppendRecord {
                            value: input.value.clone(),
                            headers: input.headers.clone(),
                        }],
                    },
                },
            ],
            key,
        )
    }

    /// Resolve an exact selected failure for the supplied Actor once, without
    /// rereading heads or live resources. Replay appends the original value/headers
    /// and owner activation in the SAME commit as its terminal. A stale competing
    /// disposition conflicts.
    /// Retry the sealed publication after an unknown acknowledgement; never create
    /// a fresh intent to recover the original attempt.
    pub fn prepare_disposition(
        &self,
        actor: &crate::addressing::ActorReference,
        failed: &FailedDelivery,
        disposition: FailureDisposition<'_>,
        key: IdempotencyKey,
    ) -> Result<PreparedPublish, Error> {
        if self.path(actor)? != failed.state
            || location(&failed.state, failed.revision)? != failed.path
        {
            return Err(Error::Invalid("foreign failed-input path"));
        }
        let (resolution, replay) = match disposition {
            FailureDisposition::Purge => (FailureResolution::Purge, None),
            FailureDisposition::Replay(captured) => {
                let input = failed.input(captured)?;
                if crate::invocation::Invocation::decode_bound(&input)?.is_some() {
                    return Err(Error::Invalid("replay requires a new invocation identity"));
                }
                (FailureResolution::Replay, Some(input))
            }
        };
        let mut prepared = PreparedPublish::foreign(key);
        let status = status_path(&failed.path)?;
        prepared.join(Effect {
            conditions: vec![CommitCondition::Head {
                path: status.clone(),
                expected: 1,
                commit_id: failed.commit,
            }],
            mutation: CommitMutation::Append {
                path: status,
                records: vec![
                    encode(&EntryV1 {
                        failed: None,
                        resolution: match resolution {
                            FailureResolution::Replay => 1,
                            FailureResolution::Purge => 2,
                        },
                    })
                    .into(),
                ],
            },
        })?;
        if let Some(input) = replay {
            prepared.join(Effect {
                conditions: Vec::new(),
                mutation: CommitMutation::Append {
                    path: failed.source.clone(),
                    records: vec![AppendRecord {
                        value: input.value,
                        headers: input.headers,
                    }],
                },
            })?;
            prepared.join(self.activation(actor))?;
        }
        Ok(prepared)
    }
}
