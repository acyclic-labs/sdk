//! Filesystem storage bindings over the canonical public service providers.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence,
    authority_commit_digest,
};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters};
use crate::storage::{
    AppendOutcome as FsAppendOutcome, AuthorityReceipt, AuthorityResult, AuthorityStoreError,
    CreateAuthorityOutcome, FenceOutcome, ObjectId, ObjectRead, ObjectReadRequest,
    ObjectReadRetention, ObjectReceipt, ObjectResult, ObjectStoreError, ReplayLimit, object_digest,
};
use crate::streams_record::StreamsDurableRecord;
use acyclic_objects::{
    Condition, GetRequest, ObjectsError, ObjectsProvider, PutRequest, ReadTarget, wire,
};
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;

const STREAM_RECORD_LIMIT: u64 = acyclic_stream::MAX_RECORD_BYTES as u64;
const GENESIS_DOMAIN: &[u8] = b"acyclic-fs-stream-genesis-v1\0";
const EPOCH_DOMAIN: &[u8] = b"acyclic-fs-stream-epoch-v1\0";

/// Filesystem authority over native hierarchical Streams.
///
/// Records, writer epochs, and operation identities are separate child paths
/// changed through one atomic Stream commit. The adapter owns no side database.
#[derive(Clone)]
pub struct StreamAuthorityStore<P> {
    provider: Arc<P>,
}

impl<P, Q> crate::Fs<StreamAuthorityStore<P>, ProviderObjectStore<Q>> {
    /// Composes the canonical filesystem engine directly over authenticated
    /// public providers and one exact account bucket.
    #[must_use]
    pub fn from_public_primitives(
        stream: Arc<P>,
        objects: Arc<Q>,
        bucket: wire::BucketRef,
        capabilities: crate::EmbeddedCapabilities,
    ) -> Self {
        crate::Fs::new(
            StreamAuthorityStore::new(stream),
            ProviderObjectStore::new(objects, bucket),
            capabilities,
        )
    }
}

impl<P> StreamAuthorityStore<P> {
    /// Binds one authenticated public Stream provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

#[derive(Clone, Copy)]
struct AuthoritySnapshot {
    head: Head,
    record_tail: u64,
    epoch_tail: u64,
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    async fn snapshot(
        &self,
        authority_id: AuthorityId,
    ) -> Result<AuthoritySnapshot, AuthorityStoreError> {
        let records = records_path(authority_id)?;
        let epochs = epochs_path(authority_id)?;
        let record_tail = self
            .provider
            .tail(records.clone())
            .await
            .map_err(map_stream_error)?;
        let epoch_tail = self
            .provider
            .tail(epochs.clone())
            .await
            .map_err(map_stream_error)?;
        if record_tail == 0 || epoch_tail == 0 {
            return Err(AuthorityStoreError::Corrupt(
                "authority genesis paths are empty".to_owned(),
            ));
        }
        let epoch_record = read_one(self.provider.as_ref(), epochs, epoch_tail - 1).await?;
        let epoch = decode_epoch(&epoch_record.value, EPOCH_DOMAIN)?;
        let head = if record_tail == 1 {
            let genesis = read_one(self.provider.as_ref(), records, 0).await?;
            let genesis_epoch = decode_epoch(&genesis.value, GENESIS_DOMAIN)?;
            if genesis_epoch != epoch {
                return Err(AuthorityStoreError::Corrupt(
                    "authority genesis and epoch paths disagree".to_owned(),
                ));
            }
            Head::genesis(epoch)
        } else {
            let record = read_one(self.provider.as_ref(), records, record_tail - 1).await?;
            let durable = decode_durable(authority_id, &record.value)?;
            if durable.sequence.get().saturating_add(1) != record_tail {
                return Err(AuthorityStoreError::Corrupt(
                    "Stream sequence does not match filesystem authority sequence".to_owned(),
                ));
            }
            Head {
                epoch,
                sequence: durable.sequence,
                digest: durable.digest,
            }
        };
        Ok(AuthoritySnapshot {
            head,
            record_tail,
            epoch_tail,
        })
    }

    async fn operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
    ) -> Result<Option<DurableCommit>, AuthorityStoreError> {
        let path = operation_path(authority_id, operation_id)?;
        match self.provider.tail(path.clone()).await {
            Ok(1) => {
                let record = read_one(self.provider.as_ref(), path, 0).await?;
                Ok(Some(decode_durable(authority_id, &record.value)?))
            }
            Ok(_) => Err(AuthorityStoreError::Corrupt(
                "operation path contains an invalid record count".to_owned(),
            )),
            Err(acyclic_stream::StreamError::NotFound) => Ok(None),
            Err(error) => Err(map_stream_error(error)),
        }
    }
}

impl<P: acyclic_stream::StreamProvider> AsyncAuthorityStore for StreamAuthorityStore<P> {
    async fn create_authority(
        &self,
        authority_id: AuthorityId,
        genesis_epoch: Epoch,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<CreateAuthorityOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        match self.snapshot(authority_id).await {
            Ok(snapshot) => {
                return authority_success(
                    CreateAuthorityOutcome::Existing(snapshot.head),
                    authority_read_work(2),
                    budget,
                );
            }
            Err(AuthorityStoreError::Missing) => {}
            Err(error) => return Err(OperationFailure::before_work(error)),
        }
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Absent {
                    path: records.clone(),
                },
                acyclic_stream::CommitCondition::Absent {
                    path: epochs.clone(),
                },
            ],
            mutations: vec![
                acyclic_stream::CommitMutation::Append {
                    path: records,
                    records: vec![encode_epoch(GENESIS_DOMAIN, genesis_epoch)],
                },
                acyclic_stream::CommitMutation::Append {
                    path: epochs,
                    records: vec![encode_epoch(EPOCH_DOMAIN, genesis_epoch)],
                },
            ],
            idempotency_key: stream_key(b"create", &authority_id.into_bytes())
                .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(2, 0);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                CreateAuthorityOutcome::Created(Head::genesis(genesis_epoch)),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let snapshot = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?;
                authority_success(
                    CreateAuthorityOutcome::Existing(snapshot.head),
                    work,
                    budget,
                )
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn head(
        &self,
        authority_id: AuthorityId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Head> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        authority_success(snapshot.head, authority_read_work(2), budget)
    }

    async fn compare_and_append(
        &self,
        authority_id: AuthorityId,
        epoch: Epoch,
        expected: Head,
        commit: ProposedCommit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FsAppendOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        if let Some(existing) = self
            .operation(authority_id, commit.operation_id)
            .await
            .map_err(OperationFailure::before_work)?
        {
            let value = if existing.fingerprint == commit.fingerprint {
                FsAppendOutcome::AlreadyCommitted(existing)
            } else {
                FsAppendOutcome::IdempotencyConflict {
                    committed_fingerprint: existing.fingerprint,
                }
            };
            return authority_success(value, authority_read_work(1), budget);
        }
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if snapshot.head.epoch != epoch {
            return authority_success(
                FsAppendOutcome::Fenced {
                    actual_epoch: snapshot.head.epoch,
                },
                authority_read_work(2),
                budget,
            );
        }
        if snapshot.head != expected {
            return authority_success(
                FsAppendOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(2),
                budget,
            );
        }
        let sequence = expected
            .sequence
            .checked_next()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::SequenceExhausted))?;
        let durable = DurableCommit {
            epoch,
            sequence,
            operation_id: commit.operation_id,
            fingerprint: commit.fingerprint,
            previous_digest: expected.digest,
            digest: authority_commit_digest(
                authority_id,
                epoch,
                sequence,
                commit.operation_id,
                commit.fingerprint,
                expected.digest,
                &commit.payload,
            ),
            payload: commit.payload,
        };
        let encoded =
            StreamsDurableRecord::encode(&durable, STREAM_RECORD_LIMIT).map_err(|error| {
                OperationFailure::before_work(AuthorityStoreError::Rejected(error.to_string()))
            })?;
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let operation = operation_path(authority_id, durable.operation_id)
            .map_err(OperationFailure::before_work)?;
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Tail {
                    path: records.clone(),
                    expected: snapshot.record_tail,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: epochs,
                    expected: snapshot.epoch_tail,
                },
                acyclic_stream::CommitCondition::Absent {
                    path: operation.clone(),
                },
            ],
            mutations: vec![
                acyclic_stream::CommitMutation::Append {
                    path: records,
                    records: vec![encoded.clone()],
                },
                acyclic_stream::CommitMutation::Append {
                    path: operation,
                    records: vec![encoded],
                },
            ],
            idempotency_key: stream_key(b"operation", &durable.operation_id.into_bytes())
                .map_err(OperationFailure::before_work)?,
        };
        let work =
            authority_write_work(2, u64::try_from(durable.payload.len()).unwrap_or(u64::MAX));
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => {
                authority_success(FsAppendOutcome::Committed(durable), work, budget)
            }
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                if let Some(existing) = self
                    .operation(authority_id, durable.operation_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                {
                    let value = if existing.fingerprint == durable.fingerprint {
                        FsAppendOutcome::AlreadyCommitted(existing)
                    } else {
                        FsAppendOutcome::IdempotencyConflict {
                            committed_fingerprint: existing.fingerprint,
                        }
                    };
                    return authority_success(value, work, budget);
                }
                let actual = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .head;
                let value = if actual.epoch != epoch {
                    FsAppendOutcome::Fenced {
                        actual_epoch: actual.epoch,
                    }
                } else {
                    FsAppendOutcome::Conflict { actual }
                };
                authority_success(value, work, budget)
            }
            Err(acyclic_stream::StreamError::IdempotencyMismatch) => {
                let existing = self
                    .operation(authority_id, durable.operation_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .ok_or_else(|| {
                        OperationFailure::new(
                            AuthorityStoreError::Corrupt(
                                "Stream idempotency mismatch has no operation record".to_owned(),
                            ),
                            work,
                        )
                    })?;
                authority_success(
                    FsAppendOutcome::IdempotencyConflict {
                        committed_fingerprint: existing.fingerprint,
                    },
                    work,
                    budget,
                )
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn replay(
        &self,
        authority_id: AuthorityId,
        after: Sequence,
        limit: ReplayLimit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Vec<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        if limit.records == 0 || limit.payload_bytes == 0 {
            return Err(OperationFailure::before_work(
                AuthorityStoreError::InvalidReplayLimit,
            ));
        }
        let path = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let mut stream = self
            .provider
            .read(acyclic_stream::ReadRequest {
                path,
                from: after.get().saturating_add(1),
                limit: limit.records,
            })
            .await
            .map_err(|error| OperationFailure::before_work(map_stream_error(error)))?;
        let mut commits = Vec::new();
        let mut payload_bytes = 0_u64;
        while let Some(record) = stream.next().await {
            let record =
                record.map_err(|error| OperationFailure::before_work(map_stream_error(error)))?;
            let durable = decode_durable(authority_id, &record.value)
                .map_err(OperationFailure::before_work)?;
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(durable.payload.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    OperationFailure::before_work(AuthorityStoreError::ReplayRecordTooLarge {
                        observed: u64::MAX,
                        maximum: limit.payload_bytes,
                    })
                })?;
            if payload_bytes > limit.payload_bytes {
                if commits.is_empty() {
                    return Err(OperationFailure::before_work(
                        AuthorityStoreError::ReplayRecordTooLarge {
                            observed: payload_bytes,
                            maximum: limit.payload_bytes,
                        },
                    ));
                }
                break;
            }
            commits.push(durable);
        }
        let work = WorkCounters {
            authority_records_read: u64::try_from(commits.len()).unwrap_or(u64::MAX),
            authority_bytes_read: payload_bytes,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        authority_success(commits, work, budget)
    }

    async fn fence(
        &self,
        authority_id: AuthorityId,
        expected: Head,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<FenceOutcome> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let snapshot = self
            .snapshot(authority_id)
            .await
            .map_err(OperationFailure::before_work)?;
        if snapshot.head != expected {
            return authority_success(
                FenceOutcome::Conflict {
                    actual: snapshot.head,
                },
                authority_read_work(2),
                budget,
            );
        }
        let next =
            expected.epoch.get().checked_add(1).ok_or_else(|| {
                OperationFailure::before_work(AuthorityStoreError::EpochExhausted)
            })?;
        let epoch = Epoch::new(next)
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::EpochExhausted))?;
        let records = records_path(authority_id).map_err(OperationFailure::before_work)?;
        let epochs = epochs_path(authority_id).map_err(OperationFailure::before_work)?;
        let mut fence_identity = Vec::with_capacity(64);
        fence_identity.extend_from_slice(&authority_id.into_bytes());
        fence_identity.extend_from_slice(&expected.epoch.get().to_le_bytes());
        fence_identity.extend_from_slice(&expected.sequence.get().to_le_bytes());
        fence_identity.extend_from_slice(expected.digest.as_bytes());
        let request = acyclic_stream::CommitRequest {
            conditions: vec![
                acyclic_stream::CommitCondition::Tail {
                    path: records,
                    expected: snapshot.record_tail,
                },
                acyclic_stream::CommitCondition::Tail {
                    path: epochs.clone(),
                    expected: snapshot.epoch_tail,
                },
            ],
            mutations: vec![acyclic_stream::CommitMutation::Append {
                path: epochs,
                records: vec![encode_epoch(EPOCH_DOMAIN, epoch)],
            }],
            idempotency_key: stream_key(b"fence", &fence_identity)
                .map_err(OperationFailure::before_work)?,
        };
        let work = authority_write_work(1, 0);
        admit_authority(work, budget)?;
        match self.provider.commit(request).await {
            Ok(acyclic_stream::CommitOutcome::Committed(_)) => authority_success(
                FenceOutcome::Advanced(Head { epoch, ..expected }),
                work,
                budget,
            ),
            Ok(acyclic_stream::CommitOutcome::Conflict(_)) => {
                let actual = self
                    .snapshot(authority_id)
                    .await
                    .map_err(|error| OperationFailure::new(error, work))?
                    .head;
                authority_success(FenceOutcome::Conflict { actual }, work, budget)
            }
            Err(error) => Err(OperationFailure::new(map_stream_error(error), work)),
        }
    }

    async fn find_operation(
        &self,
        authority_id: AuthorityId,
        operation_id: OperationId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> AuthorityResult<Option<DurableCommit>> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(AuthorityStoreError::Cancelled))?;
        let value = self
            .operation(authority_id, operation_id)
            .await
            .map_err(OperationFailure::before_work)?;
        authority_success(value, authority_read_work(1), budget)
    }
}

/// Immutable filesystem-object storage over one exact public Objects bucket.
///
/// The adapter contains no durable state. Object identity remains the canonical
/// filesystem digest; the Objects version is transport/storage evidence only.
#[derive(Clone)]
pub struct ProviderObjectStore<P> {
    provider: Arc<P>,
    bucket: wire::BucketRef,
}

impl<P> ProviderObjectStore<P> {
    /// Binds an authenticated provider to the account's dedicated filesystem bucket.
    #[must_use]
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self { provider, bucket }
    }

    /// Returns the exact backing bucket identity used by this adapter.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
    }
}

impl<P: ObjectsProvider> AsyncObjectStore for ProviderObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(OperationFailure::before_work(
                ObjectStoreError::DigestMismatch,
            ));
        }
        let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let mut work = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: byte_count,
            bytes_hashed: byte_count,
            bytes_copied: byte_count,
            allocation_operations: u64::from(!bytes.is_empty()),
            peak_allocation_bytes: byte_count,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        let request = PutRequest {
            bucket: self.bucket.clone(),
            object_key: object_key(object_id),
            body: bytes.to_vec(),
            metadata: wire::ObjectMetadata {
                content_type: "application/vnd.acyclic.fs-object-v1".to_owned(),
                ..wire::ObjectMetadata::default()
            },
            condition: Some(Condition::IfAbsent),
            idempotency_key: Some(format!(
                "fs-object-{}",
                hex::encode(object_id.digest.as_bytes())
            )),
        };
        match self.provider.put(request).await {
            Ok(version) if version.size == byte_count => success((), work, budget),
            Ok(_) => Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
            Err(ObjectsError::PreconditionFailed) => {
                work.backend_read_operations = work.backend_read_operations.saturating_add(1);
                let existing = self
                    .provider
                    .get(read_request(&self.bucket, object_id))
                    .await
                    .map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
                let existing_bytes = u64::try_from(existing.body.len()).unwrap_or(u64::MAX);
                work.object_bytes_read = work.object_bytes_read.saturating_add(existing_bytes);
                work.bytes_hashed = work.bytes_hashed.saturating_add(existing_bytes);
                if existing_bytes != byte_count
                    || object_digest(object_id.kind, &existing.body) != object_id.digest
                {
                    return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
                }
                success((), work, budget)
            }
            Err(error) => Err(OperationFailure::new(map_objects_error(error), work)),
        }
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let value = self
            .provider
            .get(read_request(&self.bucket, object_id))
            .await
            .map_err(|error| OperationFailure::before_work(map_objects_error(error)))?;
        let observed = u64::try_from(value.body.len()).unwrap_or(u64::MAX);
        let work = WorkCounters {
            backend_read_operations: 1,
            object_bytes_read: observed,
            bytes_hashed: observed,
            bytes_copied: observed,
            allocation_operations: u64::from(!value.body.is_empty()),
            peak_allocation_bytes: observed,
            ..WorkCounters::default()
        };
        if observed > maximum_bytes {
            return Err(OperationFailure::new(
                ObjectStoreError::TooLarge {
                    observed,
                    maximum: maximum_bytes,
                },
                work,
            ));
        }
        if object_digest(object_id.kind, &value.body) != object_id.digest {
            return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
        }
        success(
            ObjectRead {
                bytes: Bytes::from(value.body),
                retention: ObjectReadRetention::Owned {
                    logical_bytes: observed,
                },
            },
            work,
            budget,
        )
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        if requests.is_empty() {
            return Err(OperationFailure::before_work(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            )));
        }
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            OperationFailure::before_work(ObjectStoreError::Rejected(
                "object batch allocation failed".to_owned(),
            ))
        })?;
        let mut work = WorkCounters::default();
        for request in requests {
            let remaining = work
                .remaining(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            let receipt = self
                .read(
                    request.object_id,
                    request.maximum_bytes,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    let combined = work.checked_add(*failure.work).unwrap_or(work);
                    OperationFailure::new(failure.error, combined)
                })?;
            work = work
                .checked_add(receipt.work)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            values.push(receipt.value);
        }
        success(values, work, budget)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        match self
            .provider
            .get(read_request(&self.bucket, object_id))
            .await
        {
            Ok(value) => {
                let valid = object_digest(object_id.kind, &value.body) == object_id.digest;
                if valid {
                    success(true, work, budget)
                } else {
                    Err(OperationFailure::new(ObjectStoreError::Corrupt, work))
                }
            }
            Err(ObjectsError::NotFound) => success(false, work, budget),
            Err(error) => Err(OperationFailure::new(map_objects_error(error), work)),
        }
    }
}

fn object_key(object_id: ObjectId) -> String {
    format!(
        "fs/v1/{}/{}",
        object_id.kind.canonical_tag(),
        hex::encode(object_id.digest.as_bytes())
    )
}

fn authority_prefix(authority_id: AuthorityId) -> String {
    format!("fs/authorities/{}", hex::encode(authority_id.into_bytes()))
}

fn records_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/records", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn epochs_path(
    authority_id: AuthorityId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!("{}/epochs", authority_prefix(authority_id)))
        .map_err(map_stream_error)
}

fn operation_path(
    authority_id: AuthorityId,
    operation_id: OperationId,
) -> Result<acyclic_stream::StreamPath, AuthorityStoreError> {
    acyclic_stream::StreamPath::new(format!(
        "{}/operations/{}",
        authority_prefix(authority_id),
        hex::encode(operation_id.into_bytes())
    ))
    .map_err(map_stream_error)
}

fn stream_key(
    domain: &[u8],
    identity: &[u8],
) -> Result<acyclic_stream::IdempotencyKey, AuthorityStoreError> {
    let mut value = Vec::with_capacity(
        domain
            .len()
            .saturating_add(identity.len())
            .saturating_add(1),
    );
    value.extend_from_slice(domain);
    value.push(0);
    value.extend_from_slice(identity);
    acyclic_stream::IdempotencyKey::new(Bytes::from(value)).map_err(map_stream_error)
}

fn encode_epoch(domain: &[u8], epoch: Epoch) -> Bytes {
    let mut value = Vec::with_capacity(domain.len().saturating_add(8));
    value.extend_from_slice(domain);
    value.extend_from_slice(&epoch.get().to_le_bytes());
    Bytes::from(value)
}

fn decode_epoch(encoded: &[u8], domain: &[u8]) -> Result<Epoch, AuthorityStoreError> {
    if encoded.len() != domain.len().saturating_add(8) || !encoded.starts_with(domain) {
        return Err(AuthorityStoreError::Corrupt(
            "invalid Stream epoch record".to_owned(),
        ));
    }
    let bytes: [u8; 8] = encoded[domain.len()..]
        .try_into()
        .map_err(|_| AuthorityStoreError::Corrupt("truncated Stream epoch record".to_owned()))?;
    Epoch::new(u64::from_le_bytes(bytes))
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))
}

async fn read_one<P: acyclic_stream::StreamProvider>(
    provider: &P,
    path: acyclic_stream::StreamPath,
    from: u64,
) -> Result<acyclic_stream::Record, AuthorityStoreError> {
    let mut records = provider
        .read(acyclic_stream::ReadRequest {
            path,
            from,
            limit: 1,
        })
        .await
        .map_err(map_stream_error)?;
    let record = records
        .next()
        .await
        .ok_or_else(|| {
            AuthorityStoreError::Corrupt("Stream returned an empty exact read".to_owned())
        })?
        .map_err(map_stream_error)?;
    if record.sequence != from {
        return Err(AuthorityStoreError::Corrupt(
            "Stream returned a non-contiguous record".to_owned(),
        ));
    }
    Ok(record)
}

fn decode_durable(
    authority_id: AuthorityId,
    encoded: &[u8],
) -> Result<DurableCommit, AuthorityStoreError> {
    let durable = StreamsDurableRecord::decode(encoded, STREAM_RECORD_LIMIT)
        .map_err(|error| AuthorityStoreError::Corrupt(error.to_string()))?
        .0;
    let expected = authority_commit_digest(
        authority_id,
        durable.epoch,
        durable.sequence,
        durable.operation_id,
        durable.fingerprint,
        durable.previous_digest,
        &durable.payload,
    );
    if expected != durable.digest {
        return Err(AuthorityStoreError::Corrupt(
            "Stream authority record digest mismatch".to_owned(),
        ));
    }
    Ok(durable)
}

fn map_stream_error(error: acyclic_stream::StreamError) -> AuthorityStoreError {
    match error {
        acyclic_stream::StreamError::NotFound => AuthorityStoreError::Missing,
        acyclic_stream::StreamError::Capacity => {
            AuthorityStoreError::Rejected("Stream capacity exhausted".to_owned())
        }
        other => AuthorityStoreError::Rejected(other.to_string()),
    }
}

fn authority_read_work(records: u64) -> WorkCounters {
    WorkCounters {
        authority_records_read: records,
        backend_read_operations: records,
        ..WorkCounters::default()
    }
}

fn authority_write_work(records: u64, payload_bytes: u64) -> WorkCounters {
    WorkCounters {
        authority_records_appended: records,
        authority_bytes_written: payload_bytes,
        backend_write_operations: 1,
        durability_operations: 1,
        ..WorkCounters::default()
    }
}

fn admit_authority(
    work: WorkCounters,
    budget: WorkBudget,
) -> Result<(), OperationFailure<AuthorityStoreError>> {
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn authority_success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> AuthorityResult<T> {
    admit_authority(work, budget)?;
    Ok(AuthorityReceipt { value, work })
}

fn read_request(bucket: &wire::BucketRef, object_id: ObjectId) -> GetRequest {
    GetRequest {
        target: ReadTarget::Bucket(bucket.clone()),
        object_key: object_key(object_id),
        version_id: None,
        range: None,
        if_match: None,
        if_none_match: None,
    }
}

fn map_objects_error(error: ObjectsError) -> ObjectStoreError {
    match error {
        ObjectsError::NotFound => ObjectStoreError::Missing,
        ObjectsError::Capacity => {
            ObjectStoreError::Rejected("Objects capacity exhausted".to_owned())
        }
        other => ObjectStoreError::Rejected(other.to_string()),
    }
}

fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), OperationFailure<ObjectStoreError>> {
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> ObjectResult<T> {
    admit(work, budget)?;
    Ok(ObjectReceipt { value, work })
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_objects::MemoryObjects;
    use acyclic_stream::MemoryStream;

    #[tokio::test]
    async fn public_memory_objects_is_the_filesystem_object_backend()
    -> Result<(), Box<dyn std::error::Error>> {
        let provider = Arc::new(MemoryObjects::default());
        let bucket = provider
            .create_bucket(
                "filesystem".to_owned(),
                Some("create-filesystem".to_owned()),
            )
            .await?
            .bucket
            .ok_or("memory bucket identity missing")?;
        let store = ProviderObjectStore::new(provider, bucket);
        let bytes = Bytes::from_static(b"canonical-object");
        let object_id = ObjectId {
            kind: crate::storage::ObjectKind::BlobChunk,
            digest: object_digest(crate::storage::ObjectKind::BlobChunk, &bytes),
        };
        let cancellation = CancellationToken::new();
        store
            .put(
                object_id,
                bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let read = store
            .read(object_id, 1024, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        assert_eq!(read.value.bytes, bytes);
        assert!(
            store
                .contains(object_id, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
        );
        Ok(())
    }

    #[tokio::test]
    async fn public_memory_stream_is_the_filesystem_authority_backend()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = StreamAuthorityStore::new(Arc::new(MemoryStream::default()));
        let authority_id = AuthorityId::from_bytes([7; 16]);
        let cancellation = CancellationToken::new();
        let created = store
            .create_authority(
                authority_id,
                Epoch::GENESIS,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(
            created.value,
            CreateAuthorityOutcome::Created(Head::genesis(Epoch::GENESIS))
        );
        let proposal = ProposedCommit {
            operation_id: OperationId::from_bytes([8; 16]),
            fingerprint: crate::Digest::from_bytes([9; 32]),
            payload: Bytes::from_static(b"published-generation"),
        };
        let appended = store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposal.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let FsAppendOutcome::Committed(commit) = appended.value else {
            return Err("first append was not committed".into());
        };
        let replay = store
            .replay(
                authority_id,
                Sequence::GENESIS,
                ReplayLimit {
                    records: 4,
                    payload_bytes: 4096,
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(replay.value, vec![commit.clone()]);
        let retried = store
            .compare_and_append(
                authority_id,
                Epoch::GENESIS,
                Head::genesis(Epoch::GENESIS),
                proposal,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(retried.value, FsAppendOutcome::AlreadyCommitted(commit));
        Ok(())
    }

    #[tokio::test]
    async fn complete_filesystem_runs_on_public_memory_primitives()
    -> Result<(), Box<dyn std::error::Error>> {
        let stream = Arc::new(MemoryStream::default());
        let objects = Arc::new(MemoryObjects::default());
        let bucket = objects
            .create_bucket(
                "filesystem".to_owned(),
                Some("filesystem-bucket-v1".to_owned()),
            )
            .await?
            .bucket
            .ok_or("memory bucket identity missing")?;
        let fs = crate::Fs::from_public_primitives(
            stream,
            objects,
            bucket,
            crate::EmbeddedCapabilities::MEMORY,
        );
        let workspace = fs.create_workspace("public-memory").await?;
        workspace
            .write("/answer", Bytes::from_static(b"42"))
            .await?;
        assert_eq!(
            workspace.read("/answer", 2).await?,
            Bytes::from_static(b"42")
        );
        Ok(())
    }
}
