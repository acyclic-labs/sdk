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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationLocation {
    authority: Authority,
    operation_id: OperationId,
    revision: u64,
    intent_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageLocation {
    location: OperationLocation,
    message_id: uuid::Uuid,
    sequence: u64,
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
    indexes: Vec<(StreamPath, Bytes)>,
}

impl IndexedPublication {
    pub(crate) fn new(authority: &Authority, event: &Event, bytes: Vec<u8>) -> Result<Self> {
        let expected_tail = event
            .revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("indexed event revision must be positive".into()))?;
        let location = OperationLocation {
            authority: authority.clone(),
            operation_id: event.operation_id,
            revision: event.revision,
            intent_digest: event.intent_digest,
        };
        let mut indexes = vec![(
            operation_path(authority, event.operation_id)?,
            Bytes::from(crate::contract::canonical_json_bytes(&location)?),
        )];
        if let crate::core::EventPayload::ConversationMessageAppended { message } = &event.payload {
            let message_location =
                Bytes::from(crate::contract::canonical_json_bytes(&MessageLocation {
                    location,
                    message_id: message.id,
                    sequence: message.sequence,
                })?);
            indexes.push((
                message_path(authority, message.id)?,
                message_location.clone(),
            ));
            indexes.push((
                message_sequence_path(authority, message.sequence)?,
                message_location,
            ));
        }
        Ok(Self {
            operation_id: event.operation_id,
            path: StreamPath::new(authority.stream_path()?)?,
            expected_tail,
            bytes: Bytes::from(bytes),
            indexes,
        })
    }

    pub(crate) fn add_index(&self, request: &mut CommitRequest) {
        for (path, bytes) in &self.indexes {
            request
                .conditions
                .push(CommitCondition::Absent { path: path.clone() });
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
            .chain(self.indexes.iter().map(|(path, bytes)| (path, 0, bytes)))
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

async fn one_record<P: StreamProvider>(
    client: &StreamClient<P>,
    path: &StreamPath,
    from: u64,
) -> Result<Option<Record>> {
    let mut records = match client.stream(path.as_str())?.read(from, 1).await {
        Ok(records) => records,
        Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let first = records.try_next().await?;
    if records.try_next().await?.is_some() {
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
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "operation lookup byte bound must be positive".into(),
        ));
    }
    let path = operation_path(authority, operation)?;
    let Some(index) = one_record(client, &path, 0).await? else {
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
    let canonical = StreamPath::new(authority.stream_path()?)?;
    let sequence = location.revision - 1;
    let record = one_record(client, &canonical, sequence)
        .await?
        .ok_or_else(|| Error::Storage("indexed canonical event is missing".into()))?;
    let consumed_bytes = index_bytes
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("operation lookup exceeds byte bound".into()))?;
    let event = super::history::verify_history_record(verifier, authority, sequence, &record)?;
    if event.operation_id != operation
        || event.intent_digest != location.intent_digest
        || record.commit_id != index.commit_id
    {
        return Err(Error::Storage(
            "operation location differs from its atomic canonical event".into(),
        ));
    }
    Ok((Some(event), consumed_bytes))
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
    let path = message_path(authority, message_id)?;
    let (message, _) = find_message_at_locator(
        client,
        authority,
        verifier,
        path,
        Some(message_id),
        None,
        through_revision,
        maximum_bytes,
    )
    .await?;
    Ok(message)
}

pub(crate) async fn find_message_sequence<P: StreamProvider>(
    client: &StreamClient<P>,
    authority: &Authority,
    verifier: &AuthorityVerifier,
    sequence: u64,
    through_revision: u64,
    maximum_bytes: u64,
) -> Result<(Option<crate::conversation::ConversationMessage>, u64)> {
    let path = message_sequence_path(authority, sequence)?;
    find_message_at_locator(
        client,
        authority,
        verifier,
        path,
        None,
        Some(sequence),
        through_revision,
        maximum_bytes,
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
    verifier.verify_audience(authority)?;
    if maximum_bytes == 0 {
        return Err(Error::Invalid(
            "message lookup byte bound must be positive".into(),
        ));
    }
    let Some(index) = one_record(client, &path, 0).await? else {
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
    if client.stream(path.as_str())?.tail().await? != 1 {
        return Err(Error::Storage("message location is not immutable".into()));
    }
    let location: MessageLocation = crate::executor::decode_json(&index.value)?;
    if &location.location.authority != authority
        || expected_id.is_some_and(|id| location.message_id != id)
        || expected_sequence.is_some_and(|sequence| location.sequence != sequence)
        || location.location.revision == 0
        || location.sequence == 0
    {
        return Err(Error::Storage(
            "message location identity is invalid".into(),
        ));
    }
    let canonical = StreamPath::new(authority.stream_path()?)?;
    let sequence = location.location.revision - 1;
    let record = one_record(client, &canonical, sequence)
        .await?
        .ok_or_else(|| Error::Storage("indexed canonical message is missing".into()))?;
    let consumed_bytes = (index.value.len() as u64)
        .checked_add(record.value.len() as u64)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or_else(|| Error::Invalid("message lookup exceeds byte bound".into()))?;
    let event = super::history::verify_history_record(verifier, authority, sequence, &record)?;
    if event.operation_id != location.location.operation_id
        || event.intent_digest != location.location.intent_digest
        || record.commit_id != index.commit_id
    {
        return Err(Error::Storage(
            "message location differs from its atomic canonical event".into(),
        ));
    }
    let crate::core::EventPayload::ConversationMessageAppended { message } = event.payload else {
        return Err(Error::Storage(
            "message location points to another event kind".into(),
        ));
    };
    if message.id != location.message_id || message.sequence != location.sequence {
        return Err(Error::Storage(
            "message location differs from its canonical message".into(),
        ));
    }
    Ok((
        (event.revision <= through_revision).then_some(*message),
        consumed_bytes,
    ))
}
