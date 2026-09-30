//! Filesystem immutable content over canonical logical Objects v2.
//!
//! Native batches are explicit capabilities, never loops of remote RPCs billed
//! as one operation. The adapter keeps no object bytes, versions or durable state.
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::distributed::object_key;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, ObjectWrite, object_digest,
};
use acyclic_objects::v2::{Error, NativeBatchObjects, Object, wire};
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};
type PutRequest = (wire::PutObjectHeader, Bytes);
type GetRequest = (wire::GetObjectRequest, u64);

fn read_request(bucket: &wire::BucketRef, object_id: ObjectId, maximum_bytes: u64) -> GetRequest {
    (
        wire::GetObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: object_key(object_id),
            ..Default::default()
        },
        maximum_bytes,
    )
}

fn map_objects_error(error: Error) -> ObjectStoreError {
    match error.code {
        wire::ErrorCode::NotFound => ObjectStoreError::Missing,
        wire::ErrorCode::QuotaExceeded => {
            ObjectStoreError::Rejected("Objects capacity exhausted".to_owned())
        }
        _ => ObjectStoreError::Rejected(error.to_string()),
    }
}

/// Immutable filesystem-object storage over one exact public Objects bucket.
///
/// The adapter contains no durable state. Object identity remains the canonical
/// filesystem digest; the logical Objects `ETag` is storage evidence only.
#[derive(Clone)]
pub struct LogicalObjectStore<P> {
    provider: Arc<P>,
    bucket: wire::BucketRef,
    maximum_object_bytes: u64,
}

fn provider_put_request(bucket: &wire::BucketRef, write: &ObjectWrite) -> PutRequest {
    (
        wire::PutObjectHeader {
            bucket: Some(bucket.clone()),
            object_key: object_key(write.object_id),
            metadata: Some(wire::ObjectMetadata {
                content_type: "application/vnd.acyclic.fs-object-v1".to_owned(),
                ..Default::default()
            }),
            preconditions: Some(wire::Preconditions {
                condition: Some(wire::preconditions::Condition::IfAbsent(true)),
            }),
            // No retry receipt may answer after collection without storing the bytes.
            mutation: None,
        },
        write.bytes.clone(),
    )
}

impl<P> LogicalObjectStore<P> {
    /// Binds an authenticated provider to the account's dedicated filesystem bucket.
    #[must_use]
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self {
            provider,
            bucket,
            maximum_object_bytes: 5 * 1024 * 1024 * 1024,
        }
    }

    /// Applies the composition's existing positive per-object bound.
    ///
    /// # Errors
    /// Rejects zero or process-unrepresentable limits.
    pub fn with_object_limit(mut self, maximum: u64) -> Result<Self, ObjectStoreError> {
        if maximum == 0 || usize::try_from(maximum).is_err() {
            return Err(ObjectStoreError::Rejected(
                "invalid object size limit".to_owned(),
            ));
        }
        self.maximum_object_bytes = maximum;
        Ok(self)
    }

    /// Returns the exact backing bucket identity used by this adapter.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
    }

    /// Returns the exact public provider used by this stateless adapter.
    #[must_use]
    pub fn provider(&self) -> &Arc<P> {
        &self.provider
    }
}

impl<P: NativeBatchObjects> LogicalObjectStore<P> {
    async fn fetch(&self, request: GetRequest) -> Result<Object, Error> {
        self.provider
            .get(request.0, request.1.min(self.maximum_object_bytes))
            .await
    }

    async fn publish(&self, request: PutRequest) -> Result<wire::ObjectInfo, Error> {
        if request.1.len() as u64 > self.maximum_object_bytes {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        self.provider.put(request.0, request.1).await
    }
}

impl<P: NativeBatchObjects> AsyncObjectStore for LogicalObjectStore<P> {
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
        if bytes.len() as u64 > self.maximum_object_bytes {
            return Err(OperationFailure::before_work(map_objects_error(
                wire::ErrorCode::QuotaExceeded.into(),
            )));
        }
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
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        let request = provider_put_request(&self.bucket, &ObjectWrite { object_id, bytes });
        match self.publish(request).await {
            Ok(version) if version.size == byte_count => success((), work, budget),
            Ok(_) => Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
            Err(Error {
                code: wire::ErrorCode::PreconditionFailed,
            }) => {
                work.backend_read_operations = work.backend_read_operations.saturating_add(1);
                let existing = self
                    .fetch(read_request(&self.bucket, object_id, byte_count))
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

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        if writes.is_empty() {
            return Err(OperationFailure::before_work(ObjectStoreError::Rejected(
                "object write batch is empty".to_owned(),
            )));
        }
        let (requests, unique_writes, mut work) =
            prepare_provider_put_batch(&self.bucket, writes, budget, cancellation)?;
        if requests
            .iter()
            .any(|request| request.1.len() as u64 > self.maximum_object_bytes)
        {
            return Err(OperationFailure::new(
                ObjectStoreError::Rejected("object size limit exceeded".to_owned()),
                work,
            ));
        }
        work.backend_write_operations = 1;
        admit(work, budget)?;
        let results = self.provider.put_batch(requests).await;
        if results.len() != unique_writes.len() {
            return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
        }
        for (write_index, result) in unique_writes.into_iter().zip(results) {
            let write = writes
                .get(write_index)
                .ok_or_else(|| OperationFailure::new(ObjectStoreError::Corrupt, work))?;
            let byte_count = u64::try_from(write.bytes.len()).unwrap_or(u64::MAX);
            match result {
                Ok(version) if version.size == byte_count => {}
                Ok(_) => return Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
                Err(Error {
                    code: wire::ErrorCode::PreconditionFailed,
                }) => {
                    cancellation
                        .check()
                        .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
                    work.backend_read_operations = work.backend_read_operations.saturating_add(1);
                    let existing = self
                        .fetch(read_request(&self.bucket, write.object_id, byte_count))
                        .await
                        .map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
                    let existing_bytes = u64::try_from(existing.body.len()).unwrap_or(u64::MAX);
                    work.object_bytes_read = work.object_bytes_read.saturating_add(existing_bytes);
                    work.bytes_hashed = work.bytes_hashed.saturating_add(existing_bytes);
                    if existing_bytes != byte_count
                        || object_digest(write.object_id.kind, &existing.body)
                            != write.object_id.digest
                    {
                        return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
                    }
                }
                Err(error) => {
                    return Err(OperationFailure::new(map_objects_error(error), work));
                }
            }
        }
        success((), work, budget)
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
        let admitted = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        admit(admitted, budget)?;
        let value = self
            .fetch(read_request(&self.bucket, object_id, maximum_bytes))
            .await
            .map_err(|error| OperationFailure::new(map_objects_error(error), admitted))?;
        let observed = u64::try_from(value.body.len()).unwrap_or(u64::MAX);
        let work = WorkCounters {
            object_probes: 1,
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
                bytes: value.body,
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
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let vector_bytes = u64::try_from(
            requests
                .len()
                .saturating_mul(size_of::<ObjectRead>() + size_of::<GetRequest>()),
        )
        .unwrap_or(u64::MAX);
        let admitted = WorkCounters {
            backend_read_operations: 1,
            object_probes: u64::try_from(requests.len()).unwrap_or(u64::MAX),
            allocation_operations: 2,
            peak_allocation_bytes: vector_bytes,
            ..WorkCounters::default()
        };
        admitted
            .verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(|_| {
            OperationFailure::before_work(ObjectStoreError::Rejected(
                "object batch allocation failed".to_owned(),
            ))
        })?;
        let provider_requests = requests
            .iter()
            .map(|request| {
                read_request(
                    &self.bucket,
                    request.object_id,
                    request.maximum_bytes.min(self.maximum_object_bytes),
                )
            })
            .collect();
        let results = self.provider.get_batch(provider_requests).await;
        if results.len() != requests.len() {
            return Err(OperationFailure::new(ObjectStoreError::Corrupt, admitted));
        }
        let retained_bytes = results.iter().try_fold(0_u64, |total, result| {
            total.checked_add(result.as_ref().map_or(0, |value| {
                u64::try_from(value.body.len()).unwrap_or(u64::MAX)
            }))
        });
        let retained_bytes = retained_bytes.ok_or_else(|| {
            OperationFailure::new(ObjectStoreError::Work(WorkError::Overflow), admitted)
        })?;
        let mut work = WorkCounters {
            peak_allocation_bytes: vector_bytes.checked_add(retained_bytes).ok_or_else(|| {
                OperationFailure::new(ObjectStoreError::Work(WorkError::Overflow), admitted)
            })?,
            ..admitted
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        for (request, result) in requests.iter().zip(results) {
            cancellation
                .check()
                .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
            let value =
                result.map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
            let observed = u64::try_from(value.body.len()).unwrap_or(u64::MAX);
            work = work
                .checked_add(WorkCounters {
                    object_bytes_read: observed,
                    bytes_hashed: observed,
                    bytes_copied: observed,
                    ..WorkCounters::default()
                })
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            work.verify(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?;
            if observed > request.maximum_bytes {
                return Err(OperationFailure::new(
                    ObjectStoreError::TooLarge {
                        observed,
                        maximum: request.maximum_bytes,
                    },
                    work,
                ));
            }
            if object_digest(request.object_id.kind, &value.body) != request.object_id.digest {
                return Err(OperationFailure::new(ObjectStoreError::Corrupt, work));
            }
            values.push(ObjectRead {
                bytes: value.body,
                retention: ObjectReadRetention::Owned {
                    logical_bytes: observed,
                },
            });
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
        let mut work = WorkCounters {
            object_probes: 1,
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        admit(work, budget)?;
        match self
            .fetch(read_request(
                &self.bucket,
                object_id,
                budget.object_bytes_read,
            ))
            .await
        {
            Ok(value) => {
                let observed = value.body.len() as u64;
                work.object_bytes_read = observed;
                work.bytes_hashed = observed;
                admit(work, budget)?;
                let valid = object_digest(object_id.kind, &value.body) == object_id.digest;
                if valid {
                    success(true, work, budget)
                } else {
                    Err(OperationFailure::new(ObjectStoreError::Corrupt, work))
                }
            }
            Err(Error {
                code: wire::ErrorCode::NotFound,
            }) => success(false, work, budget),
            Err(error) => Err(OperationFailure::new(map_objects_error(error), work)),
        }
    }
}

fn prepare_provider_put_batch(
    bucket: &wire::BucketRef,
    writes: &[ObjectWrite],
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(Vec<PutRequest>, Vec<usize>, WorkCounters), OperationFailure<ObjectStoreError>> {
    let minimum_bytes =
        u64::try_from(writes.len().saturating_mul(size_of::<PutRequest>())).unwrap_or(u64::MAX);
    let prospective = WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: minimum_bytes,
        ..WorkCounters::default()
    };
    prospective
        .verify(budget)
        .map_err(|error| OperationFailure::before_work(error.into()))?;
    let mut requests = Vec::new();
    requests.try_reserve_exact(writes.len()).map_err(|_| {
        OperationFailure::before_work(ObjectStoreError::Rejected(
            "object batch allocation failed".to_owned(),
        ))
    })?;
    let mut work = WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: u64::try_from(
            requests.capacity().saturating_mul(size_of::<PutRequest>()),
        )
        .unwrap_or(u64::MAX),
        ..WorkCounters::default()
    };
    admit(work, budget)?;
    let mut unique: BTreeMap<ObjectId, usize> = BTreeMap::new();
    let mut unique_writes = Vec::new();
    for (write_index, write) in writes.iter().enumerate() {
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
        if object_digest(write.object_id.kind, &write.bytes) != write.object_id.digest {
            return Err(OperationFailure::new(
                ObjectStoreError::DigestMismatch,
                work,
            ));
        }
        let byte_count = u64::try_from(write.bytes.len()).unwrap_or(u64::MAX);
        work.object_bytes_written = work
            .object_bytes_written
            .checked_add(byte_count)
            .ok_or_else(|| {
                OperationFailure::new(
                    ObjectStoreError::Rejected("object batch byte count overflowed".to_owned()),
                    work,
                )
            })?;
        work.bytes_hashed = work.bytes_hashed.checked_add(byte_count).ok_or_else(|| {
            OperationFailure::new(
                ObjectStoreError::Rejected("object batch byte count overflowed".to_owned()),
                work,
            )
        })?;
        admit(work, budget)?;
        if let Some(existing_index) = unique.get(&write.object_id) {
            if writes
                .get(*existing_index)
                .is_none_or(|existing| existing.bytes != write.bytes)
            {
                return Err(OperationFailure::new(
                    ObjectStoreError::DigestMismatch,
                    work,
                ));
            }
        } else {
            unique.insert(write.object_id, write_index);
            unique_writes.push(write_index);
            requests.push(provider_put_request(bucket, write));
        }
    }
    Ok((requests, unique_writes, work))
}

fn admit(work: WorkCounters, budget: WorkBudget) -> Result<(), OperationFailure<ObjectStoreError>> {
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))
}

fn success<T>(value: T, work: WorkCounters, budget: WorkBudget) -> ObjectResult<T> {
    admit(work, budget)?;
    Ok(ObjectReceipt { value, work })
}
