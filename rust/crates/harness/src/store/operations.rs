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
    index_path: StreamPath,
    index_bytes: Bytes,
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
        Ok(Self {
            operation_id: event.operation_id,
            path: StreamPath::new(authority.stream_path()?)?,
            expected_tail,
            bytes: Bytes::from(bytes),
            index_path: operation_path(authority, event.operation_id)?,
            index_bytes: Bytes::from(crate::contract::canonical_json_bytes(&location)?),
        })
    }

    pub(crate) fn add_index(&self, request: &mut CommitRequest) {
        request.conditions.push(CommitCondition::Absent {
            path: self.index_path.clone(),
        });
        request.mutations.push(CommitMutation::Append {
            path: self.index_path.clone(),
            records: vec![self.index_bytes.clone()],
        });
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
        if envelope.mutations.len() != 2 {
            return Err(Error::Storage(
                "indexed publication has invalid mutation count".into(),
            ));
        }
        for (path, tail, bytes) in [
            (&self.path, self.expected_tail, &self.bytes),
            (&self.index_path, 0, &self.index_bytes),
        ] {
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
    verifier.verify_audience(authority)?;
    let path = operation_path(authority, operation)?;
    let Some(index) = one_record(client, &path, 0).await? else {
        return match client.stream(path.as_str())?.tail().await {
            Err(acyclic_stream::StreamError::NotFound) => Ok(None),
            Ok(_) => Err(Error::Storage(
                "operation location is missing before its committed tail".into(),
            )),
            Err(error) => Err(error.into()),
        };
    };
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
    let event = super::history::verify_history_record(verifier, authority, sequence, &record)?;
    if event.operation_id != operation
        || event.intent_digest != location.intent_digest
        || record.commit_id != index.commit_id
    {
        return Err(Error::Storage(
            "operation location differs from its atomic canonical event".into(),
        ));
    }
    Ok(Some(event))
}
