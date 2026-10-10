//! Filesystem immutable content over canonical logical Objects v1.
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
use acyclic_objects::v1::{Error, NativeBatchObjects, Object, ObjectsProvider, wire};
use bytes::Bytes;
use std::{collections::BTreeMap, sync::Arc};
type PutRequest = (wire::PutObjectHeader, Bytes);
type GetRequest = (wire::GetObjectRequest, u64);

fn read_request(
    bucket: &wire::BucketRef,
    prefix: &str,
    object_id: ObjectId,
    maximum_bytes: u64,
) -> GetRequest {
    (
        wire::GetObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: prefixed_key(prefix, object_id),
            ..Default::default()
        },
        maximum_bytes,
    )
}

fn prefixed_key(prefix: &str, object_id: ObjectId) -> String {
    if prefix.is_empty() {
        return object_key(object_id);
    }
    use std::fmt::Write;
    let mut key = String::with_capacity(prefix.len() + 80);
    let _ = write!(key, "{prefix}fs/v1/{}/", object_id.kind.canonical_tag());
    for byte in object_id.digest.as_bytes() {
        let _ = write!(key, "{byte:02x}");
    }
    key
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
    key_prefix: Arc<str>,
}

fn provider_put_request(bucket: &wire::BucketRef, prefix: &str, write: &ObjectWrite) -> PutRequest {
    (
        wire::PutObjectHeader {
            bucket: Some(bucket.clone()),
            object_key: prefixed_key(prefix, write.object_id),
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
            key_prefix: "".into(),
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

impl<P: ObjectsProvider> LogicalObjectStore<P> {
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

impl<P: ObjectsProvider> LogicalObjectStore<P> {
    fn validate_write(
        &self,
        object_id: ObjectId,
        bytes: &Bytes,
        mut work: WorkCounters,
        budget: WorkBudget,
    ) -> Result<WorkCounters, OperationFailure<ObjectStoreError>> {
        if bytes.len() as u64 > self.maximum_object_bytes {
            return Err(OperationFailure::new(
                map_objects_error(wire::ErrorCode::QuotaExceeded.into()),
                work,
            ));
        }
        let hashing = WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::default()
        };
        work.admit(&hashing, &budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work = work
            .checked_add(hashing)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        if object_digest(object_id.kind, bytes) != object_id.digest {
            return Err(OperationFailure::new(
                ObjectStoreError::DigestMismatch,
                work,
            ));
        }
        Ok(work)
    }

    async fn put_single(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::before_work(ObjectStoreError::Cancelled))?;
        let work = self.validate_write(object_id, &bytes, WorkCounters::default(), budget)?;
        self.put_verified(
            object_id,
            provider_put_request(
                &self.bucket,
                &self.key_prefix,
                &ObjectWrite { object_id, bytes },
            ),
            work,
            budget,
            cancellation,
        )
        .await
    }

    async fn put_verified(
        &self,
        object_id: ObjectId,
        request: PutRequest,
        mut work: WorkCounters,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        cancellation
            .check()
            .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
        let byte_count = request.1.len() as u64;
        let writing = WorkCounters {
            backend_write_operations: 1,
            object_bytes_written: byte_count,
            ..WorkCounters::default()
        };
        work.admit(&writing, &budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work = work
            .checked_add(writing)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        match self.publish(request).await {
            Ok(version) if version.size == byte_count => success((), work, budget),
            Ok(_) => Err(OperationFailure::new(ObjectStoreError::Corrupt, work)),
            Err(Error {
                code: wire::ErrorCode::PreconditionFailed,
            }) => {
                cancellation
                    .check()
                    .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
                let reading = WorkCounters {
                    backend_read_operations: 1,
                    ..WorkCounters::default()
                };
                work.admit(&reading, &budget)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                work = work
                    .checked_add(reading)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                let existing = self
                    .fetch(read_request(
                        &self.bucket,
                        &self.key_prefix,
                        object_id,
                        byte_count,
                    ))
                    .await
                    .map_err(|error| OperationFailure::new(map_objects_error(error), work))?;
                let existing_bytes = existing.body.len() as u64;
                work = work
                    .checked_add(WorkCounters {
                        object_bytes_read: existing_bytes,
                        ..WorkCounters::default()
                    })
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                let hashing = WorkCounters {
                    bytes_hashed: existing_bytes,
                    ..WorkCounters::default()
                };
                work.admit(&hashing, &budget)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
                work = work
                    .checked_add(hashing)
                    .map_err(|error| OperationFailure::new(error.into(), work))?;
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
}

impl<P: NativeBatchObjects> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.put_single(object_id, bytes, budget, cancellation)
            .await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        self.read_single(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.contains_single(object_id, budget, cancellation).await
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
        let (requests, unique_writes, mut work) = prepare_provider_put_batch(
            &self.bucket,
            &self.key_prefix,
            writes,
            budget,
            cancellation,
        )?;
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
                        .fetch(read_request(
                            &self.bucket,
                            &self.key_prefix,
                            write.object_id,
                            byte_count,
                        ))
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
                    &self.key_prefix,
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
}

impl<P: ObjectsProvider> LogicalObjectStore<P> {
    async fn read_single(
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
        admitted
            .verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let value = self
            .fetch(read_request(
                &self.bucket,
                &self.key_prefix,
                object_id,
                maximum_bytes,
            ))
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

    async fn contains_single(
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
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        match self
            .fetch(read_request(
                &self.bucket,
                &self.key_prefix,
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

/// Stateless immutable filesystem objects over individual authenticated RPCs.
///
/// Unlike [`LogicalObjectStore`], this adapter needs only [`ObjectsProvider`].
/// Groups execute explicitly in order, charging every actual provider operation;
/// they do not claim a native batch or all-or-nothing group publication.
#[derive(Clone)]
pub struct RemoteLogicalObjectStore<P> {
    inner: LogicalObjectStore<P>,
}

impl<P> RemoteLogicalObjectStore<P> {
    /// Binds an authenticated provider to the exact dedicated filesystem bucket.
    #[must_use]
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self {
            inner: LogicalObjectStore::new(provider, bucket),
        }
    }

    /// Restricts every remote object lookup and publication to this exact prefix.
    ///
    /// # Errors
    /// Rejects noncanonical or oversized prefixes. The caller must derive the
    /// prefix from authenticated tenant identity, never from request contents.
    pub fn with_key_prefix(mut self, prefix: String) -> Result<Self, ObjectStoreError> {
        if prefix.len() > 512
            || (!prefix.is_empty()
                && (!prefix.ends_with('/')
                    || prefix.split('/').rev().skip(1).any(|part| {
                        part.is_empty()
                            || part == "."
                            || part == ".."
                            || !part.bytes().all(|byte| {
                                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                            })
                    })))
        {
            return Err(ObjectStoreError::Rejected(
                "invalid filesystem object prefix".to_owned(),
            ));
        }
        self.inner.key_prefix = prefix.into();
        Ok(self)
    }

    /// Applies the composition's positive per-object size bound.
    ///
    /// # Errors
    /// Rejects zero or process-unrepresentable limits.
    pub fn with_object_limit(mut self, maximum: u64) -> Result<Self, ObjectStoreError> {
        self.inner = self.inner.with_object_limit(maximum)?;
        Ok(self)
    }

    /// Returns the exact backing bucket identity.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        self.inner.bucket()
    }

    /// Returns the exact authenticated provider.
    #[must_use]
    pub fn provider(&self) -> &Arc<P> {
        self.inner.provider()
    }
}

impl<P: ObjectsProvider> AsyncObjectStore for RemoteLogicalObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: Bytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        self.inner
            .put_single(object_id, bytes, budget, cancellation)
            .await
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
        // One compact index array interns IDs without constructing unused RPC
        // headers, retaining object bytes, or allocating a node per input.
        let mut work = WorkCounters {
            allocation_operations: 1,
            peak_allocation_bytes: (writes.len() as u64)
                .checked_mul(size_of::<usize>() as u64)
                .ok_or_else(|| OperationFailure::before_work(WorkError::Overflow.into()))?,
            ..WorkCounters::default()
        };
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut indices = Vec::new();
        indices.try_reserve_exact(writes.len()).map_err(|_| {
            OperationFailure::before_work(ObjectStoreError::Rejected(
                "object batch allocation failed".to_owned(),
            ))
        })?;
        for (index, write) in writes.iter().enumerate() {
            cancellation
                .check()
                .map_err(|_| OperationFailure::new(ObjectStoreError::Cancelled, work))?;
            work = self
                .inner
                .validate_write(write.object_id, &write.bytes, work, budget)?;
            indices.push(index);
        }
        indices.sort_unstable_by_key(|index| {
            writes.get(*index).map(|write| (write.object_id, *index))
        });
        for [first_index, second_index] in indices.array_windows::<2>() {
            let first = writes
                .get(*first_index)
                .ok_or_else(|| OperationFailure::new(ObjectStoreError::Corrupt, work))?;
            let second = writes
                .get(*second_index)
                .ok_or_else(|| OperationFailure::new(ObjectStoreError::Corrupt, work))?;
            if first.object_id == second.object_id && first.bytes != second.bytes {
                return Err(OperationFailure::new(
                    ObjectStoreError::DigestMismatch,
                    work,
                ));
            }
        }
        indices.dedup_by_key(|index| writes.get(*index).map(|write| write.object_id));
        // Select the first occurrence, not an arbitrary equal-key representative.
        // Sorting by ID and input position makes the eventual RPC order stable.
        indices.sort_unstable();
        for index in indices {
            let write = writes
                .get(index)
                .ok_or_else(|| OperationFailure::new(ObjectStoreError::Corrupt, work))?;
            work = self
                .inner
                .put_verified(
                    write.object_id,
                    provider_put_request(self.bucket(), &self.inner.key_prefix, write),
                    work,
                    budget,
                    cancellation,
                )
                .await?
                .work;
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
        self.inner
            .read_single(object_id, maximum_bytes, budget, cancellation)
            .await
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        crate::async_storage::read_many_sequential_async(self, requests, budget, cancellation).await
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        self.inner
            .contains_single(object_id, budget, cancellation)
            .await
    }
}

fn prepare_provider_put_batch(
    bucket: &wire::BucketRef,
    prefix: &str,
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
            requests.push(provider_put_request(bucket, prefix, write));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ObjectKind;
    #[tokio::test]
    async fn provider_object_batch_reads_once_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = Bytes::from_static(b"first");
        let second_bytes = Bytes::from_static(b"second");
        let first = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
        };
        let second = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
        };
        let cancellation = CancellationToken::new();
        store
            .put(
                first,
                first_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        store
            .put(
                second,
                second_bytes.clone(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        let receipt = store
            .read_many(
                &[
                    ObjectReadRequest {
                        object_id: second,
                        maximum_bytes: 6,
                    },
                    ObjectReadRequest {
                        object_id: first,
                        maximum_bytes: 5,
                    },
                ],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        assert_eq!(
            receipt.value.first().map(|value| &value.bytes),
            Some(&second_bytes)
        );
        assert_eq!(
            receipt.value.get(1).map(|value| &value.bytes),
            Some(&first_bytes)
        );
        assert_eq!(receipt.work.backend_read_operations, 1);
        assert_eq!(receipt.work.object_probes, 2);
        Ok(())
    }

    #[tokio::test]
    async fn provider_object_batch_counts_one_backend_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = Bytes::from_static(b"first");
        let second_bytes = Bytes::from_static(b"second");
        let writes = [
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(ObjectKind::BlobChunk, &first_bytes),
                },
                bytes: first_bytes,
            },
            ObjectWrite {
                object_id: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: object_digest(ObjectKind::BlobChunk, &second_bytes),
                },
                bytes: second_bytes,
            },
        ];
        let receipt = store
            .put_many(&writes, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?;
        assert_eq!(receipt.work.backend_write_operations, 1);
        assert_eq!(receipt.work.object_bytes_written, 11);
        Ok(())
    }

    #[tokio::test]
    async fn provider_put_batch_rejects_unadmitted_request_allocation() {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let bytes = Bytes::from_static(b"body");
        let writes = [ObjectWrite {
            object_id: ObjectId {
                kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, &bytes),
            },
            bytes,
        }];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.allocation_operations = 0;
        let failure = store
            .put_many(&writes, budget, &CancellationToken::new())
            .await
            .err()
            .unwrap_or_else(|| {
                OperationFailure::before_work(ObjectStoreError::Rejected(
                    "unadmitted allocation unexpectedly succeeded".to_owned(),
                ))
            });
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
    }
}
