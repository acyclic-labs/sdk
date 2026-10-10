//! Derived exact-operation locations, committed with their canonical event.

use crate::{
    Error, OperationId, Result,
    core::{Authority, AuthorityVerifier, Event},
};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, CommittedEnvelope, CommittedMutation,
    IdempotencyKey, Record, StreamClient, StreamPath, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

pub(super) fn validate_canonical_json_with_work(
    bytes: &[u8],
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    if work.is_none() {
        let _: serde_json::Value = crate::executor::decode_json(bytes)?;
        return Ok(());
    }
    let mut items = 0u64;
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["Value", "sequence", "map", "string", "number", "boolean", "null"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let parsed = {
        let observer = work.as_deref_mut().expect("observed canonical validation");
        crate::contract::json_from_slice_bounded_trusted::<serde_json::Value, _>(
            bytes,
            crate::contract::JsonBounds {
                maximum_bytes,
                maximum_items: maximum_bytes,
                maximum_depth: 256,
            },
            bytes.len() as u64,
            &mut |charge| {
                let charged = match charge {
                crate::contract::JsonCodecCharge::Work { items: visited, .. } => {
                    items = items.checked_add(visited)
                        .ok_or_else(|| Error::Invalid("index JSON item count overflows".into()))?;
                    observer.codec_charge(charge)
                }
                crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } => {
                    // Value owns string payloads and logical sequence/map slots.
                    // The lexical count admits minimum-eight/geometric vectors;
                    // opaque map allocator overhead is outside this contract.
                    let slot = std::mem::size_of::<serde_json::Value>()
                        .max(std::mem::size_of::<(String, serde_json::Value)>()) as u64;
                    reservation = items.checked_mul(10).and_then(|count| count.checked_mul(slot))
                        .and_then(|slots| slots.checked_add(maximum_owned_bytes))
                        .and_then(|owned| owned.checked_add(diagnostic_bytes))
                        .ok_or_else(|| Error::Invalid("index JSON reservation overflows".into()))?;
                    observer.retain_owned(reservation)
                }
                _ => observer.codec_charge(charge),
                };
                if charged.is_err() { observed_failure = true; }
                charged
            },
        )
    };
    let parsed = parsed.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal JSON is invalid: {message}")),
                other => other,
            }
        }
    });
    let result = match parsed {
        Ok(parsed) => {
            let encoded = super::history::canonical_bytes_with_work(&parsed, maximum_bytes, work);
            drop(parsed);
            match encoded {
                Ok(encoded) => {
                    let identical = encoded == bytes;
                    let capacity = encoded.capacity() as u64;
                    drop(encoded);
                    if let Some(observer) = work.as_deref_mut() {
                        observer.release_owned(capacity)?;
                    }
                    if identical { Ok(()) } else {
                        Err(Error::Storage("execution journal JSON is not canonical".into()))
                    }
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    };
    if let Some(observer) = work.as_deref_mut() {
        observer.release_owned(reservation)?;
    }
    result
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OperationLocation {
    pub(super) authority: Authority,
    pub(super) operation_id: OperationId,
    pub(super) revision: u64,
    pub(super) intent_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) effect_position: Option<u64>,
}

pub(super) fn decode_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<OperationLocation> {
    if work.is_some() {
        validate_canonical_json_with_work(bytes, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    }
    let Some(observer) = work.as_deref_mut() else {
        return crate::executor::decode_json(bytes);
    };
    // This named-field schema owns only Authority.id; every other field is
    // fixed-width. Decoded UTF-8 string storage cannot exceed lexical input.
    let mut reservation = 0;
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["OperationLocation", "Authority", "AggregateKind", "authority", "operation_id",
         "revision", "intent_digest", "effect_position", "kind", "id", "agent",
         "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut observed_failure = false;
    let decoded = crate::contract::json_from_slice_bounded_trusted::<OperationLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } = charge {
                reservation = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("operation locator reservation overflows".into()))?;
                observer.retain_owned(reservation)
            } else {
                observer.codec_charge(charge)
            };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    observer.release_owned(reservation)?;
    let location = decoded?;
    observer.retain_owned(location.authority.id.capacity() as u64)?;
    observer.copy_bytes(location.authority.id.len() as u64)?;
    Ok(location)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageLocation {
    location: OperationLocation,
    message_id: uuid::Uuid,
    sequence: u64,
}

fn decode_message_location_with_work(
    bytes: &[u8],
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<MessageLocation> {
    if work.is_some() {
        validate_canonical_json_with_work(bytes, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
    }
    let Some(observer) = work.as_deref_mut() else { return crate::executor::decode_json(bytes); };
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        bytes.len() as u64,
        ["MessageLocation", "OperationLocation", "Authority", "AggregateKind", "location",
         "message_id", "sequence", "authority", "operation_id", "revision", "intent_digest",
         "effect_position", "kind", "id", "agent", "conversation", "session", "turn", "task"]
            .iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let mut reservation = 0;
    let mut observed_failure = false;
    let decoded = crate::contract::json_from_slice_bounded_trusted::<MessageLocation, _>(
        bytes,
        crate::contract::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_depth: 256,
        },
        bytes.len() as u64,
        &mut |charge| {
            let charged = if let crate::contract::JsonCodecCharge::Admission { maximum_owned_bytes } = charge {
                reservation = maximum_owned_bytes.checked_add(diagnostic_bytes)
                    .ok_or_else(|| Error::Invalid("message locator reservation overflows".into()))?;
                observer.retain_owned(reservation)
            } else { observer.codec_charge(charge) };
            if charged.is_err() { observed_failure = true; }
            charged
        },
    );
    let decoded = decoded.map_err(|error| {
        if observed_failure { error } else {
            match error {
                Error::Invalid(message) => Error::Storage(format!("execution journal content is invalid: {message}")),
                other => other,
            }
        }
    });
    observer.release_owned(reservation)?;
    let location = decoded?;
    observer.retain_owned(location.location.authority.id.capacity() as u64)?;
    observer.copy_bytes(location.location.authority.id.len() as u64)?;
    Ok(location)
}

fn message_path(authority: &Authority, message: uuid::Uuid) -> Result<StreamPath> {
    if authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message index requires a conversation".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message index authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-messages/{suffix}/{message}"
    ))?)
}

fn message_head_path(authority: &Authority) -> Result<StreamPath> {
    if authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message head requires a conversation".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message head authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-heads/{suffix}"
    ))?)
}

fn message_sequence_path(authority: &Authority, sequence: u64) -> Result<StreamPath> {
    if sequence == 0 || authority.kind != crate::core::AggregateKind::Conversation {
        return Err(Error::Invalid(
            "message sequence index requires a positive conversation sequence".into(),
        ));
    }
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("message sequence authority path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/conversation-sequences/{suffix}/{sequence}"
    ))?)
}

pub(crate) fn operation_path(authority: &Authority, operation: OperationId) -> Result<StreamPath> {
    let path = authority.stream_path()?;
    let suffix = path
        .strip_prefix("harness/v2/")
        .ok_or_else(|| Error::Invalid("aggregate operation path is invalid".into()))?;
    Ok(StreamPath::new(format!(
        "harness/v2/aggregate-operations/{suffix}/{operation}"
    ))?)
}

/// The existing event and a ref-only location, without another admission lifecycle.
pub(crate) struct IndexedPublication {
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: Bytes,
    indexes: Vec<(StreamPath, u64, Bytes)>,
}

fn reserve_indexes(
    indexes: &mut Vec<(StreamPath, u64, Bytes)>,
    additional: usize,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else { return Ok(()); };
    let needed = indexes.len().checked_add(additional)
        .ok_or_else(|| Error::Invalid("publication index count overflows".into()))?;
    if needed <= indexes.capacity() { return Ok(()); }
    let element_bytes = std::mem::size_of::<(StreamPath, u64, Bytes)>() as u64;
    let old_capacity = indexes.capacity();
    observer.admit_owned(needed as u64 * element_bytes)?;
    observer.copy_bytes(indexes.len() as u64 * element_bytes)?;
    indexes.try_reserve_exact(additional)
        .map_err(|_| Error::Invalid("publication index allocation failed".into()))?;
    observer.retain_owned(indexes.capacity() as u64 * element_bytes)?;
    observer.release_owned(old_capacity as u64 * element_bytes)?;
    Ok(())
}

impl IndexedPublication {
    pub(crate) fn new(authority: &Authority, event: &Event, bytes: Vec<u8>) -> Result<Self> {
        Self::new_with_work(authority, event, bytes, &mut None)
    }

    pub(crate) fn new_with_work(
        authority: &Authority,
        event: &Event,
        bytes: Vec<u8>,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let expected_tail = event
            .revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("indexed event revision must be positive".into()))?;
        let location = OperationLocation {
            authority: match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(authority)?,
                None => authority.clone(),
            },
            operation_id: event.operation_id,
            revision: event.revision,
            intent_digest: event.intent_digest,
            effect_position: None,
        };
        let location_capacity = location.authority.id.capacity() as u64;
        let mut indexes = Vec::new();
        let initial_indexes = match event.payload {
            crate::core::EventPayload::ConversationBound { .. } => 2,
            crate::core::EventPayload::ConversationMessageAppended { .. }
                | crate::core::EventPayload::ProjectMergePublished { .. } => 4,
            _ => 1,
        };
        reserve_indexes(&mut indexes, initial_indexes, work)?;
        indexes.push((
            operation_path(authority, event.operation_id)?,
            0,
            Bytes::from(super::history::canonical_bytes_with_work(&location, acyclic_stream::MAX_RECORD_BYTES as u64, work)?),
        ));
        if matches!(
            event.payload,
            crate::core::EventPayload::ConversationBound { .. }
        ) {
            let binding_location = MessageLocation {
                location: OperationLocation {
                    authority: match work.as_deref_mut() {
                        Some(observer) => observer.clone_authority(&location.authority)?,
                        None => location.authority.clone(),
                    },
                    operation_id: location.operation_id,
                    revision: location.revision,
                    intent_digest: location.intent_digest,
                    effect_position: location.effect_position,
                },
                message_id: uuid::Uuid::nil(),
                sequence: 0,
            };
            let binding_bytes = super::history::canonical_bytes_with_work(
                &binding_location, acyclic_stream::MAX_RECORD_BYTES as u64, work,
            )?;
            let binding_capacity = binding_location.location.authority.id.capacity() as u64;
            drop(binding_location);
            if let Some(observer) = work.as_deref_mut() {
                observer.release_owned(binding_capacity)?;
            }
            indexes.push((message_head_path(authority)?, 0, Bytes::from(binding_bytes)));
        }
        let message = match &event.payload {
            crate::core::EventPayload::ConversationMessageAppended { message } => {
                Some(message.as_ref())
            }
            crate::core::EventPayload::ProjectMergePublished { receipt } => Some(&receipt.notice),
            _ => None,
        };
        if let Some(message) = message {
            let message_location =
                Bytes::from(super::history::canonical_bytes_with_work(&MessageLocation {
                    location,
                    message_id: message.id,
                    sequence: message.sequence,
                }, acyclic_stream::MAX_RECORD_BYTES as u64, work)?);
            indexes.push((
                message_path(authority, message.id)?,
                0,
                message_location.clone(),
            ));
            indexes.push((
                message_sequence_path(authority, message.sequence)?,
                0,
                message_location.clone(),
            ));
            // One locator per logical message, in the same atomic commit as
            // the canonical event and immutable ID/sequence indexes. Its tail
            // is one plus the logical message count, including the initial
            // binding marker, independent of subsequent non-message events.
            indexes.push((
                message_head_path(authority)?,
                message.sequence,
                message_location,
            ));
        } else {
            drop(location);
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(location_capacity)?;
        }
        Ok(Self {
            operation_id: event.operation_id,
            path: StreamPath::new(authority.stream_path()?)?,
            expected_tail,
            bytes: Bytes::from(bytes),
            indexes,
        })
    }

    pub(super) fn set_effect_position_with_work(
        &mut self,
        position: u64,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let (_, _, bytes) = self
            .indexes
            .first_mut()
            .ok_or_else(|| Error::Storage("publication operation index is absent".into()))?;
        let mut location = decode_location_with_work(bytes, work)?;
        location.effect_position = Some(position);
        let encoded = super::history::canonical_bytes_with_work(&location, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(location.authority.id.capacity() as u64)?;
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.release_owned(bytes.len() as u64)?;
        }
        *bytes = Bytes::from(encoded);
        Ok(())
    }

    pub(super) fn add_derived_index_with_work(
        &mut self,
        path: StreamPath,
        tail: u64,
        bytes: Bytes,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<()> {
        reserve_indexes(&mut self.indexes, 1, work)?;
        self.indexes.push((path, tail, bytes));
        Ok(())
    }

    pub(crate) fn add_index(&self, request: &mut CommitRequest) {
        for (path, tail, bytes) in &self.indexes {
            request.conditions.push(if *tail == 0 {
                CommitCondition::Absent { path: path.clone() }
            } else {
                CommitCondition::Tail {
                    path: path.clone(),
                    expected: *tail,
                }
            });
            request.mutations.push(CommitMutation::Append {
                path: path.clone(),
                records: vec![bytes.clone()],
            });
        }
    }

    pub(crate) async fn request<P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        key: IdempotencyKey,
    ) -> Result<CommitRequest> {
        self.request_with_work(client, key, &mut None).await
    }

    pub(crate) async fn request_with_work<P: StreamProvider>(
        &self,
        client: &StreamClient<P>,
        key: IdempotencyKey,
        work: &mut Option<&mut dyn super::history::HistoryIo>,
    ) -> Result<CommitRequest> {
        if let Some(observer) = work.as_deref_mut() {
            observer.begin_call()?;
        }
        let target = match client.bounds(self.path.as_str()).await {
            Ok(_) => CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.expected_tail,
            },
            Err(acyclic_stream::StreamError::NotFound) if self.expected_tail == 0 => {
                CommitCondition::Absent {
                    path: self.path.clone(),
                }
            }
            Err(error) => return Err(error.into()),
        };
        let mut request = CommitRequest {
            conditions: vec![target],
            mutations: vec![CommitMutation::Append {
                path: self.path.clone(),
                records: vec![self.bytes.clone()],
            }],
            idempotency_key: key,
        };
        self.add_index(&mut request);
        Ok(request)
    }

    pub(crate) fn verify(&self, envelope: &CommittedEnvelope) -> Result<()> {
        if envelope.mutations.len() != 1 + self.indexes.len() {
            return Err(Error::Storage(
                "indexed publication has invalid mutation count".into(),
            ));
        }
        for (path, tail, bytes) in std::iter::once((&self.path, self.expected_tail, &self.bytes))
            .chain(
                self.indexes
                    .iter()
                    .map(|(path, tail, bytes)| (path, *tail, bytes)),
            )
        {
            let append = envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    CommittedMutation::Append(append) if &append.path == path => Some(append),
                    _ => None,
                })
                .ok_or_else(|| Error::Storage("indexed publication is missing an append".into()))?;
            let end = tail
                .checked_add(1)
                .ok_or_else(|| Error::Storage("indexed publication tail overflows".into()))?;
            if append.start != tail
                || append.end != end
                || append.tail != end
                || append.records.len() != 1
                || append.records.first().is_none_or(|record| {
                    record.sequence != tail
                        || record.value != bytes
                        || record.commit_id != envelope.commit_id
                })
            {
                return Err(Error::Storage(
                    "indexed publication append is invalid".into(),
                ));
            }
        }
        Ok(())
    }
}

pub(super) async fn one_record<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
) -> Result<Option<Record>> {
    one_record_with_work(client, path, from, &mut None).await
}

/// The original one-record lookup with receipts at each visible SDK boundary.
pub(crate) async fn one_record_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<Option<Record>> {
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    let mut records = match client.stream(path.as_str())?.read(from, 1).await {
        Ok(records) => records,
        Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    let first = records.try_next().await?;
    if let Some(record) = &first
        && let Some(work) = work.as_deref_mut()
    {
        work.receive_record(record.value.len() as u64)?;
    }
    if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
    if let Some(extra) = records.try_next().await? {
        if let Some(work) = work.as_deref_mut() {
            work.receive_record(extra.value.len() as u64)?;
        }
        return Err(Error::Storage(
            "operation lookup exceeded its one-record bound".into(),
        ));
    }
    if let Some(record) = &first
        && (record.sequence != from || record.value.len() > acyclic_stream::MAX_RECORD_BYTES)
    {
        return Err(Error::Storage(
            "operation location record is invalid".into(),
        ));
    }
    Ok(first)
}

pub(crate) async fn find_operation<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
) -> Result<Option<Event>> {
    find_operation_bounded(
        client,
        authority,
        verifier,
        operation,
        2 * acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .await
    .map(|(event, _)| event)
}

/// Counts the atomic locator and canonical record against one caller-owned budget.
pub(crate) async fn find_operation_bounded<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
) -> Result<(Option<Event>, u64)> {
    find_operation_bounded_with_work(client, authority, verifier, operation, maximum_bytes, &mut None).await
}

pub(crate) async fn find_operation_bounded_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    operation: OperationId,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<Event>, u64)> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "operation lookup byte bound must be positive".into(),
        ));
    }
    let path = operation_path(authority, operation)?;
    let Some(index) = one_record_with_work(client, &path, 0, work).await? else {
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        return match client.stream(path.as_str())?.tail().await {
            Err(acyclic_stream::StreamError::NotFound) => Ok((None, 0)),
            Ok(_) => Err(Error::Storage(
                "operation location is missing before its committed tail".into(),
            )),
            Err(error) => Err(error.into()),
        };
    };
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(Error::Invalid("operation lookup exceeds byte bound".into()));
    }
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    if client.stream(path.as_str())?.tail().await? != 1 {
        return Err(Error::Storage(
            "operation location is not a single immutable record".into(),
        ));
    }
    let location: OperationLocation = crate::executor::decode_json(&index.value)?;
    if &location.authority != authority
        || location.operation_id != operation
        || location.revision == 0
    {
        return Err(Error::Storage(
            "operation location identity is invalid".into(),
        ));
    }
    let (event, bytes) = verify_operation_locator_with_work(
        client, authority, verifier, &index, &location, maximum_bytes, work,
    ).await?;
    if let Some(work) = work.as_deref_mut() {
        work.stage_output(bytes - index_bytes, 1)?;
        work.release_record(index_bytes)?;
    }
    Ok((Some(event), bytes))
}

pub(super) async fn verify_operation_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
) -> Result<(Event, u64)> {
    verify_operation_locator_with_work(client, authority, verifier, index, location, maximum_bytes, &mut None).await
}

pub(crate) async fn verify_operation_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: &Record,
    location: &OperationLocation,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Event, u64)> {
    verifier.verify_audience(authority)?;
    let index_bytes = index.value.len() as u64;
    if index_bytes >= maximum_bytes {
        return Err(Error::Invalid("operation lookup exceeds byte bound".into()));
    }
    if &location.authority != authority || location.revision == 0 {
        return Err(Error::Storage(
            "operation location identity is invalid".into(),
        ));
    }
    let canonical = StreamPath::new(authority.stream_path()?)?;
    let sequence = location.revision - 1;
    let record = one_record_with_work(client, &canonical, sequence, work)
        .await?
        .ok_or_else(|| Error::Storage("indexed canonical event is missing".into()))?;
    let consumed_bytes = index_bytes
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("operation lookup exceeds byte bound".into()))?;
    let event = super::history::verify_history_record_with_work(verifier, authority, sequence, &record, work)?;
    verify_operation_binding(authority, index, location, &record, &event)?;
    if let Some(work) = work.as_deref_mut() {
        work.release_record(record.value.len() as u64)?;
    }
    Ok((event, consumed_bytes))
}

pub(super) fn verify_operation_binding(
    authority: &Authority,
    index: &Record,
    location: &OperationLocation,
    canonical: &Record,
    event: &Event,
) -> Result<()> {
    if &location.authority != authority
        || location.revision != event.revision
        || canonical.sequence.checked_add(1) != Some(event.revision)
        || event.operation_id != location.operation_id
        || event.intent_digest != location.intent_digest
        || canonical.commit_id != index.commit_id
    {
        return Err(Error::Storage(
            "operation location differs from its atomic canonical event".into(),
        ));
    }
    Ok(())
}

/// Loads one exact archived message without reconstructing a conversation projection.
/// Both the derived location and canonical record must belong to the same commit.
pub(crate) async fn find_message<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<Option<crate::conversation::ConversationMessage>> {
    find_message_counted(
        client,
        authority,
        verifier,
        message_id,
        through_revision,
        maximum_bytes,
    )
    .await
    .map(|(message, _)| message)
}

pub(crate) async fn find_message_counted<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    message_id: uuid::Uuid,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let path = message_path(authority, message_id)?;
    find_message_at_locator(
        client,
        authority,
        verifier,
        path,
        Some(message_id),
        None,
        through_revision,
        maximum_bytes,
    )
    .await
}

pub(crate) async fn find_message_sequence<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    find_message_sequence_with_work(client, authority, verifier, sequence, through_revision, maximum_bytes, &mut None).await
}

pub(crate) async fn find_message_sequence_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let path = message_sequence_path(authority, sequence)?;
    find_message_at_locator_with_work(
        client,
        authority,
        verifier,
        path,
        None,
        Some(sequence),
        through_revision,
        maximum_bytes,
        work,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "one atomic locator verifier binds its exact ID or sequence and pinned byte/read boundary"
)]
async fn find_message_at_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    find_message_at_locator_with_work(client, authority, verifier, path, expected_id,
        expected_sequence, through_revision, maximum_bytes, &mut None).await
}

#[allow(clippy::too_many_arguments, reason = "same exact indexed locator and pinned cut with optional SDK receipts")]
async fn find_message_at_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    path: StreamPath,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let Some(index) = one_record_with_work(client, &path, 0, work).await? else {
        if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
        return match client.stream(path.as_str())?.tail().await {
            Err(acyclic_stream::StreamError::NotFound) => Ok((None, 0)),
            Ok(_) => Err(Error::Storage(
                "message location is missing before its committed tail".into(),
            )),
            Err(error) => Err(error.into()),
        };
    };
    if index.value.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("message lookup exceeds byte bound".into()));
    }
    if let Some(work) = work.as_deref_mut() { work.begin_call()?; }
    if client.stream(path.as_str())?.tail().await? != 1 {
        return Err(Error::Storage("message location is not immutable".into()));
    }
    verify_message_locator_with_work(
        client,
        authority,
        verifier,
        index,
        expected_id,
        expected_sequence,
        through_revision,
        maximum_bytes,
        work,
    )
    .await
}

#[allow(
    clippy::too_many_arguments,
    reason = "shared verification binds one atomic locator to its exact identity and read boundary"
)]
async fn verify_message_locator<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    verify_message_locator_with_work(client, authority, verifier, index, expected_id,
        expected_sequence, through_revision, maximum_bytes, &mut None).await
}

#[allow(clippy::too_many_arguments, reason = "shared original atomic locator verification plus optional receipts")]
async fn verify_message_locator_with_work<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    index: Record,
    expected_id: Option<uuid::Uuid>,
    expected_sequence: Option<u64>,
    through_revision: u64,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn super::history::HistoryIo>,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let location: MessageLocation = crate::executor::decode_json(&index.value)?;
    if &location.location.authority != authority
        || expected_id.is_some_and(|id| location.message_id != id)
        || expected_sequence.is_some_and(|sequence| location.sequence != sequence)
        || location.location.revision == 0
        || (location.sequence == 0 && expected_sequence != Some(0))
    {
        return Err(Error::Storage(
            "message location identity is invalid".into(),
        ));
    }
    let (event, consumed_bytes) = verify_operation_locator_with_work(
        client,
        authority,
        verifier,
        &index,
        &location.location,
        maximum_bytes,
        work,
    )
    .await?;
    let index_bytes = index.value.len() as u64;
    if let Some(work) = work.as_deref_mut() { work.release_record(index_bytes)?; }
    if location.sequence == 0 {
        return if location.message_id.is_nil()
            && matches!(
                event.payload,
                crate::core::EventPayload::ConversationBound { .. }
            ) {
            Ok((None, consumed_bytes))
        } else {
            Err(Error::Storage(
                "empty message head differs from its canonical binding".into(),
            ))
        };
    }
    let message = match event.payload {
        crate::core::EventPayload::ConversationMessageAppended { message } => *message,
        crate::core::EventPayload::ProjectMergePublished { receipt } => receipt.notice,
        _ => {
            return Err(Error::Storage(
                "message location points to another event kind".into(),
            ));
        }
    };
    if message.id != location.message_id || message.sequence != location.sequence {
        return Err(Error::Storage(
            "message location differs from its canonical message".into(),
        ));
    }
    if event.revision <= through_revision
        && let Some(work) = work.as_deref_mut()
    {
        work.stage_output(consumed_bytes - index_bytes, 1)?;
    }
    Ok((
        (event.revision <= through_revision).then_some(message),
        consumed_bytes,
    ))
}

/// Captures one logical message head without a reducer or retained-history scan.
/// The head locator and canonical record share the existing publication commit.
pub(crate) async fn latest_message<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    maximum_bytes: u64,
) -> Result<Option<crate::conversation::ConversationMessage>> {
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let path = message_head_path(authority)?;
    let through = match client.stream(path.as_str())?.tail().await {
        Ok(0) => return Err(Error::Storage("message head is empty".into())),
        Ok(through) => through,
        Err(acyclic_stream::StreamError::NotFound) => {
            return match client.stream(authority.stream_path()?)?.tail().await {
                Ok(0) | Err(acyclic_stream::StreamError::NotFound) => Ok(None),
                Ok(_) => Err(Error::Storage(
                    "message head is absent for a nonempty aggregate".into(),
                )),
                Err(error) => Err(error.into()),
            };
        }
        Err(error) => return Err(error.into()),
    };
    let index = one_record(client, &path, through - 1)
        .await?
        .ok_or_else(|| {
            Error::Storage("message head is missing before its committed tail".into())
        })?;
    if index.value.len() as u64 > maximum_bytes {
        return Err(Error::Invalid("message lookup exceeds byte bound".into()));
    }
    verify_message_locator(
        client,
        authority,
        verifier,
        index,
        None,
        Some(through - 1),
        u64::MAX,
        maximum_bytes,
    )
    .await
    .map(|(message, _)| message)
}
