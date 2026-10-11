//! Filesystem immutable content over canonical logical Objects v1.
//!
//! Native batches are explicit capabilities, never loops of remote RPCs billed
//! as one operation. The adapter keeps no object bytes, versions or durable state.
//! Writes share one request/hash/provider engine across standalone and original
//! native accounting. Request Strings and batch backing capacity are admitted
//! before allocation; immutable bodies retain their existing physical owner.
//! Provider acknowledgements inventory logical bytes, not transport delivery,
//! provider-internal allocations, or RSS. A failed batch records every reported
//! successful item in the original counters and returns its provider failure.
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::distributed::object_key;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, ObjectWrite, object_digest,
};
use acyclic_objects::v1::{Error, NativeBatchObjects, Object, wire};
use bytes::Bytes;
use std::sync::Arc;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use std::fmt::Write as _;
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

fn put_request_bytes(request: &PutRequest) -> u64 {
    request.0.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity() as u64)
        + request.0.object_key.capacity() as u64
        + request.0.metadata.as_ref().map_or(0, |metadata| metadata.content_type.capacity() as u64)
}

fn put_request_original(
    bucket: &wire::BucketRef, object_id: ObjectId, bytes: Bytes,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(PutRequest, KernelOwnedAllocation), crate::FsError> {
    let mut request_custody;
    let ((request, _), custody) = read_request_original(bucket, object_id, 0, accounting)?;
    request_custody = custody;
    let content_type = "application/vnd.acyclic.fs-object-v1";
    let copy = WorkCounters { bytes_copied: content_type.len() as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(copy)?;
    let mut metadata_custody = accounting.claim_owned(content_type.len() as u64, 1)?;
    let mut owned_type = String::new();
    owned_type.try_reserve_exact(content_type.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    accounting.reconcile_owned(&mut metadata_custody, content_type.len() as u64, owned_type.capacity() as u64)?;
    owned_type.push_str(content_type);
    accounting.record_completed(copy)?;
    request_custody.absorb(&mut metadata_custody)?;
    Ok(((wire::PutObjectHeader {
        bucket: request.bucket, object_key: request.object_key,
        metadata: Some(wire::ObjectMetadata { content_type: owned_type, ..Default::default() }),
        preconditions: Some(wire::Preconditions {
            condition: Some(wire::preconditions::Condition::IfAbsent(true)),
        }),
        mutation: None,
    }, bytes), request_custody))
}

fn record_write_completion(accounting: &mut KernelAccounting<'_>, bytes: u64) -> Result<(), crate::FsError> {
    let delta = WorkCounters { object_bytes_written: bytes, ..WorkCounters::UNCHARGED };
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = accounting.native_work() {
        work.record_admitted(delta)?;
        return Ok(());
    }
    match accounting {
        KernelAccounting::Standalone { work, .. } => { *work = work.checked_add(delta)?; Ok(()) }
        KernelAccounting::BorrowedStandalone { work, .. } => { **work = work.checked_add(delta)?; Ok(()) }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { .. } => unreachable!(),
    }
}

fn retire_write_capacity(
    accounting: &mut KernelAccounting<'_>, custody: &mut KernelOwnedAllocation, bytes: u64,
) -> Result<(), crate::FsError> {
    // Retirement has no cancellation boundary: physical requests are already
    // gone, and cancellation must not hide an acknowledged write prefix.
    match custody {
        KernelOwnedAllocation::Standalone => match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.release(bytes),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.release(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return Err(ObjectStoreError::Corrupt.into()),
        }.map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed)),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(custody) => { drop(custody.split(bytes)?); Ok(()) }
    }
}

fn release_write_custody(
    accounting: &mut KernelAccounting<'_>, mut custody: KernelOwnedAllocation, bytes: u64,
) -> Result<(), crate::FsError> {
    retire_write_capacity(accounting, &mut custody, bytes)
}

fn inventory_write_records(
    accounting: &mut KernelAccounting<'_>, bytes: u64,
) -> Result<(KernelOwnedAllocation, Result<(), crate::FsError>), crate::FsError> {
    accounting.record_actual_owned(bytes)
}

fn put_info_bytes(info: &wire::ObjectInfo) -> Result<u64, crate::FsError> {
    let mut bytes = info.etag.capacity() as u64;
    if let Some(metadata) = &info.metadata {
        for value in [&metadata.content_type, &metadata.content_encoding, &metadata.cache_control,
            &metadata.content_disposition, &metadata.content_language] {
            bytes = bytes.checked_add(value.capacity() as u64).ok_or(WorkError::Overflow)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes.checked_add(size_of::<(String, String)>() as u64)
                .and_then(|bytes| bytes.checked_add(key.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(value.capacity() as u64)).ok_or(WorkError::Overflow)?;
        }
    }
    Ok(bytes)
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

    // Both entry points use this physical producer. Opaque provider storage is
    // a logical quota, never an invented provider allocator/copy observation.
    async fn read_original(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(ObjectRead, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        let request_custody;
        let (request, custody) = read_request_original(
            &self.bucket, object_id, maximum_bytes, accounting)?;
        request_custody = custody;
        let request_bytes = request.0.object_key.capacity() as u64
            + request.0.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity() as u64);
        let remaining = accounting.remaining()?;
        let maximum = maximum_bytes.min(self.maximum_object_bytes)
            .min(remaining.object_bytes_read).min(remaining.peak_allocation_bytes);
        let mut body_custody = accounting.claim_owned(maximum, 0)?;
        // Declared before the DTO so unwinding destroys provider payload first.
        let header_custody;
        accounting.charge(WorkCounters {
            object_probes: 1, backend_read_operations: 1, ..WorkCounters::UNCHARGED
        })?;
        let result = self.fetch((request.0, maximum)).await;
        release_read_custody(accounting, request_custody, request_bytes)?;
        let value = result.map_err(|error| match error.code {
            wire::ErrorCode::NotFound => ObjectStoreError::Missing,
            _ => ObjectStoreError::LogicalObjects(error),
        })?;
        let observed = value.body.len() as u64;
        let read_delta = WorkCounters { object_bytes_read: observed, ..WorkCounters::UNCHARGED };
        let read_completion = accounting.record_completed(read_delta);
        if observed > maximum {
            read_completion?;
            return Err(ObjectStoreError::TooLarge { observed, maximum }.into());
        }
        accounting.release_owned(&mut body_custody, maximum - observed)?;
        let header_bytes = read_header_bytes(&value.header)?;
        // These opaque returned records already exist. Inventory their logical
        // data without claiming an allocator attempt or letting cancellation
        // erase the completed provider prefix.
        let (custody, header_completion) = inventory_read_records(accounting, header_bytes)?;
        header_custody = custody;
        read_completion?;
        header_completion?;
        let hash_work = WorkCounters { bytes_hashed: observed, ..WorkCounters::UNCHARGED };
        accounting.admit(hash_work)?;
        let digest = object_digest(object_id.kind, &value.body);
        accounting.record_completed(hash_work)?;
        if digest != object_id.digest {
            return Err(ObjectStoreError::Corrupt.into());
        }
        let Object { header, body } = value;
        drop(header);
        release_read_custody(accounting, header_custody, header_bytes)?;
        Ok((ObjectRead {
            bytes: body, retention: ObjectReadRetention::Owned { logical_bytes: observed },
        }, body_custody))
    }

    async fn read_many_original(
        &self,
        requests: &[ObjectReadRequest],
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<ObjectRead>, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        if requests.is_empty() {
            return Err(ObjectStoreError::EmptyReadBatch.into());
        }
        let requested_vector_bytes = (requests.len() as u64)
            .checked_mul(size_of::<GetRequest>() as u64).ok_or(WorkError::Overflow)?;
        let mut request_custody = accounting.claim_owned(requested_vector_bytes, 1)?;
        let mut provider_requests = Vec::new();
        provider_requests.try_reserve_exact(requests.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        accounting.reconcile_owned(&mut request_custody, requested_vector_bytes,
            (provider_requests.capacity() as u64).checked_mul(size_of::<GetRequest>() as u64)
                .ok_or(WorkError::Overflow)?)?;
        for request in requests {
            let mut incoming;
            let (value, custody) = read_request_original(&self.bucket, request.object_id,
                request.maximum_bytes.min(self.maximum_object_bytes), accounting)?;
            incoming = custody;
            request_custody.absorb(&mut incoming)?;
            provider_requests.push(value);
        }
        let request_bytes = provider_requests.iter().try_fold(
            (provider_requests.capacity() as u64).checked_mul(size_of::<GetRequest>() as u64)
                .ok_or(WorkError::Overflow)?,
            |bytes, request| bytes.checked_add(request.0.object_key.capacity() as u64)
                .and_then(|bytes| bytes.checked_add(request.0.bucket.as_ref()
                    .map_or(0, |bucket| bucket.name.capacity() as u64)))
                .ok_or(WorkError::Overflow),
        )?;
        let requested_output_bytes = (requests.len() as u64)
            .checked_mul(size_of::<ObjectRead>() as u64).ok_or(WorkError::Overflow)?;
        let mut output_custody = accounting.claim_owned(requested_output_bytes, 1)?;
        let mut values = Vec::new();
        values.try_reserve_exact(requests.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        accounting.reconcile_owned(&mut output_custody, requested_output_bytes,
            (values.capacity() as u64).checked_mul(size_of::<ObjectRead>() as u64)
                .ok_or(WorkError::Overflow)?)?;
        let result_bytes = (requests.len() as u64)
            .checked_mul(size_of::<Result<Object, Error>>() as u64).ok_or(WorkError::Overflow)?;
        let result_custody = accounting.claim_owned(result_bytes, 0)?;
        let maximum = provider_requests.iter().try_fold(0_u64,
            |total, request| total.checked_add(request.1).ok_or(WorkError::Overflow))?;
        let remaining = accounting.remaining()?;
        let quota = maximum.min(remaining.object_bytes_read).min(remaining.peak_allocation_bytes);
        let mut body_custody = accounting.claim_owned(quota, 0)?;
        let mut left = quota;
        for request in &mut provider_requests {
            request.1 = request.1.min(left);
            left -= request.1;
        }
        let header_custody;
        accounting.charge(WorkCounters { backend_read_operations: 1,
            object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
        let results = self.provider.get_batch(provider_requests).await;
        release_read_custody(accounting, request_custody, request_bytes)?;
        let observed = results.iter().try_fold(0_u64, |total, result| {
            total.checked_add(result.as_ref().map_or(0, |value| value.body.len() as u64))
                .ok_or(WorkError::Overflow)
        })?;
        let delta = WorkCounters { object_bytes_read: observed, ..WorkCounters::UNCHARGED };
        let read_completion = accounting.record_completed(delta);
        let header_bytes = results.iter().try_fold(
            (results.len() as u64).checked_mul(size_of::<Result<Object, Error>>() as u64)
                .ok_or(WorkError::Overflow)?.saturating_sub(result_bytes),
            |total, result| {
                let bytes = match result { Ok(value) => read_header_bytes(&value.header)?, Err(_) => 0 };
                total.checked_add(bytes).ok_or_else(|| crate::FsError::Work(WorkError::Overflow))
            },
        )?;
        let (custody, header_completion) = inventory_read_records(accounting, header_bytes)?;
        header_custody = custody;
        if results.len() != requests.len() { return Err(ObjectStoreError::Corrupt.into()); }
        let mut left = quota;
        for (request, result) in requests.iter().zip(&results) {
            let maximum = request.maximum_bytes.min(self.maximum_object_bytes).min(left);
            left -= maximum;
            if let Ok(value) = result {
                let observed = value.body.len() as u64;
                if observed > maximum {
                    return Err(ObjectStoreError::TooLarge { observed, maximum }.into());
                }
            }
        }
        accounting.release_owned(&mut body_custody, quota - observed)?;
        if read_completion.is_err() || header_completion.is_err() {
            if let Some(error) = results.iter().find_map(|result| result.as_ref().err().copied()) {
                return Err(match error.code {
                    wire::ErrorCode::NotFound => ObjectStoreError::Missing,
                    _ => ObjectStoreError::LogicalObjects(error),
                }.into());
            }
        }
        read_completion?;
        header_completion?;
        for (request, result) in requests.iter().zip(results) {
            let Object { header, body } = result.map_err(|error| match error.code {
                wire::ErrorCode::NotFound => ObjectStoreError::Missing,
                _ => ObjectStoreError::LogicalObjects(error),
            })?;
            let observed = body.len() as u64;
            let hash_work = WorkCounters { bytes_hashed: observed, ..WorkCounters::UNCHARGED };
            accounting.admit(hash_work)?;
            let digest = object_digest(request.object_id.kind, &body);
            accounting.record_completed(hash_work)?;
            if digest != request.object_id.digest {
                return Err(ObjectStoreError::Corrupt.into());
            }
            drop(header);
            let read = ObjectRead { bytes: body,
                retention: ObjectReadRetention::Owned { logical_bytes: observed } };
            let read = match &mut body_custody {
                KernelOwnedAllocation::Standalone => read,
                #[cfg(not(target_arch = "wasm32"))]
                KernelOwnedAllocation::NativeOriginal(custody) =>
                    crate::async_storage::NativeObjectRead::owned(read, custody.split(observed)?)?.into_read(),
            };
            values.push(read);
        }
        // The provider result vector and all of its metadata have now retired.
        release_read_custody(accounting, result_custody, result_bytes)?;
        release_read_custody(accounting, header_custody, header_bytes)?;
        Ok((values, output_custody))
    }

    async fn put_original(
        &self, object_id: ObjectId, bytes: Bytes, verified: bool,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        let length = bytes.len() as u64;
        if length > self.maximum_object_bytes {
            return Err(ObjectStoreError::TooLarge { observed: length, maximum: self.maximum_object_bytes }.into());
        }
        if !verified {
            let hash_work = WorkCounters {
                bytes_hashed: length.saturating_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(hash_work)?;
            let digest = object_digest(object_id.kind, &bytes);
            let late = accounting.record_completed(hash_work);
            if digest != object_id.digest {
                return Err(ObjectStoreError::DigestMismatch.into());
            }
            late?;
        }
        let request_custody;
        let (request, custody) = put_request_original(&self.bucket, object_id, bytes, accounting)?;
        request_custody = custody;
        let request_bytes = put_request_bytes(&request);
        accounting.admit(WorkCounters { object_bytes_written: length, ..WorkCounters::UNCHARGED })?;
        accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
        let response_custody;
        let result = self.publish(request).await;
        release_write_custody(accounting, request_custody, request_bytes)?;
        match result {
            Ok(version) => {
                let result_size = version.size;
                record_write_completion(accounting, length)?;
                let response_bytes = put_info_bytes(&version)?;
                let (custody, late) = inventory_write_records(accounting, response_bytes)?;
                response_custody = custody;
                if result_size != length { return Err(ObjectStoreError::Corrupt.into()); }
                late?;
                drop(version);
                release_write_custody(accounting, response_custody, response_bytes)?;
            }
            Err(Error { code: wire::ErrorCode::PreconditionFailed }) => {
                let (existing, custody) = self.read_original(object_id, length, accounting).await?;
                let actual = existing.bytes.len() as u64;
                drop(existing);
                release_write_custody(accounting, custody, actual)?;
                if actual != length { return Err(ObjectStoreError::Corrupt.into()); }
                // A verified idempotent match acknowledges logical payload,
                // never a fresh transport write or object creation.
                record_write_completion(accounting, length)?;
            }
            Err(error) => return Err(ObjectStoreError::LogicalObjects(error).into()),
        }
        accounting.check()
    }

    async fn put_many_original(
        &self, writes: &[ObjectWrite], accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        accounting.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let requested_requests = writes.len().checked_mul(size_of::<PutRequest>()).ok_or(WorkError::Overflow)? as u64;
        let mut request_custody = accounting.claim_owned(requested_requests, 1)?;
        let mut requests = Vec::new();
        requests.try_reserve_exact(writes.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        accounting.reconcile_owned(&mut request_custody, requested_requests,
            (requests.capacity() * size_of::<PutRequest>()) as u64)?;
        let requested_unique = writes.len().checked_mul(size_of::<usize>()).ok_or(WorkError::Overflow)? as u64;
        let mut unique_custody = accounting.claim_owned(requested_unique, 1)?;
        let mut unique = Vec::<usize>::new();
        unique.try_reserve_exact(writes.len()).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
        accounting.reconcile_owned(&mut unique_custody, requested_unique,
            (unique.capacity() * size_of::<usize>()) as u64)?;
        request_custody.absorb(&mut unique_custody)?;
        for (index, write) in writes.iter().enumerate() {
            accounting.check()?;
            let length = write.bytes.len() as u64;
            if length > self.maximum_object_bytes {
                return Err(ObjectStoreError::TooLarge { observed: length, maximum: self.maximum_object_bytes }.into());
            }
            let hash_work = WorkCounters {
                bytes_hashed: length.saturating_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES),
                ..WorkCounters::UNCHARGED
            };
            accounting.admit(hash_work)?;
            let digest = object_digest(write.object_id.kind, &write.bytes);
            let late = accounting.record_completed(hash_work);
            if digest != write.object_id.digest { return Err(ObjectStoreError::DigestMismatch.into()); }
            late?;
            if let Some(&previous) = unique.iter().find(|&&previous| writes[previous].object_id == write.object_id) {
                if writes[previous].bytes != write.bytes { return Err(ObjectStoreError::DigestMismatch.into()); }
                continue;
            }
            let mut incoming;
            let (request, custody) = put_request_original(&self.bucket, write.object_id, write.bytes.clone(), accounting)?;
            incoming = custody;
            request_custody.absorb(&mut incoming)?;
            requests.push(request);
            unique.push(index);
        }
        let unique_bytes = (unique.capacity() * size_of::<usize>()) as u64;
        let request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64
            + requests.iter().map(put_request_bytes).sum::<u64>();
        let total = unique.iter().try_fold(0_u64, |total, &index| {
            total.checked_add(writes[index].bytes.len() as u64).ok_or(WorkError::Overflow)
        })?;
        accounting.admit(WorkCounters { object_bytes_written: total, ..WorkCounters::UNCHARGED })?;
        accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
        let response_custody;
        let results = self.provider.put_batch(requests).await;
        // Request DTO backing is no longer SDK-owned after this provider
        // future returns. The still-live identity vector keeps its own claim.
        retire_write_capacity(accounting, &mut request_custody, request_bytes)?;
        // Provider reports are not allocator observations or proof of transport
        // delivery. Every acknowledged item is recorded even after an earlier
        // item's failure; the original failure stays primary.
        let mut completed = 0_u64;
        for (&index, result) in unique.iter().zip(&results) {
            if result.is_ok() {
                completed = completed.checked_add(writes[index].bytes.len() as u64).ok_or(WorkError::Overflow)?;
            }
        }
        record_write_completion(accounting, completed)?;
        let response_bytes = results.iter().try_fold(
            (results.capacity() * size_of::<Result<wire::ObjectInfo, Error>>()) as u64,
            |bytes, result| bytes.checked_add(match result {
                Ok(info) => put_info_bytes(info)?,
                Err(_) => 0,
            }).ok_or_else(|| crate::FsError::from(WorkError::Overflow)),
        )?;
        let (custody, response_late) = inventory_write_records(accounting, response_bytes)?;
        response_custody = custody;
        let response_denial = response_late.err();
        if results.len() != unique.len() { return Err(ObjectStoreError::Corrupt.into()); }
        let mut failure = unique.iter().zip(&results).find_map(|(&index, result)| match result {
            Ok(info) if info.size != writes[index].bytes.len() as u64 => Some(crate::FsError::from(ObjectStoreError::Corrupt)),
            Err(error) if error.code != wire::ErrorCode::PreconditionFailed =>
                Some(crate::FsError::from(ObjectStoreError::LogicalObjects(Error { code: error.code }))),
            _ => None,
        });
        for (&index, result) in unique.iter().zip(results) {
            let write = &writes[index];
            let length = write.bytes.len() as u64;
            let result = match result {
                Ok(info) if info.size == length => Ok(()),
                Ok(_) => Err(crate::FsError::from(ObjectStoreError::Corrupt)),
                Err(Error { code: wire::ErrorCode::PreconditionFailed }) if failure.is_none() => {
                    match self.read_original(write.object_id, length, accounting).await {
                        Ok((existing, custody)) => {
                            let actual = existing.bytes.len() as u64;
                            drop(existing);
                            release_write_custody(accounting, custody, actual)?;
                            if actual == length {
                                record_write_completion(accounting, length)?;
                                Ok(())
                            } else { Err(ObjectStoreError::Corrupt.into()) }
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(ObjectStoreError::LogicalObjects(error).into()),
            };
            if failure.is_none() { failure = result.err(); }
        }
        release_write_custody(accounting, response_custody, response_bytes)?;
        drop(unique);
        release_write_custody(accounting, request_custody, unique_bytes)?;
        if let Some(error) = failure { return Err(error); }
        if let Some(error) = response_denial { return Err(error.into()); }
        accounting.check()
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
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_original(object_id, bytes, false, &mut accounting).await {
            Ok(()) => Ok(ObjectReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    async fn put_hashed(
        &self, object: crate::storage::HashedObject, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let (object_id, bytes) = object.into_parts();
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_original(object_id, bytes, true, &mut accounting).await {
            Ok(()) => Ok(ObjectReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    async fn put_many(
        &self,
        writes: &[ObjectWrite],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_many_original(writes, &mut accounting).await {
            Ok(()) => Ok(ObjectReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: Bytes, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_original(object_id, bytes, false, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(read_accounting_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        self.put_original(object_id, bytes, true, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(read_accounting_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite], work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_many_original(writes, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(read_accounting_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn flush_before_publish_with_native_work(
        &self, _scope: crate::async_storage::PublicationScope<'_>, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        work.admit(WorkCounters::UNCHARGED)?;
        Ok(crate::PublicationHold::none())
    }

    /// Like the ordinary PR237 preparation default, this local provider supplies
    /// no hosted or historical custody. It performs no archive allocation or I/O.
    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_generation_publication_with_native_work(
        &self, _request: &crate::kernel::PublishGenerationRequest,
        _proof: &crate::kernel::GenerationProof, _append: &crate::GuardedAppend,
        _scope: crate::PublicationScope<'_>, original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        cancellation.check().map_err(|_| ObjectStoreError::Cancelled)?;
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(())
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<ObjectRead> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.read_original(object_id, maximum_bytes, &mut accounting).await {
            Ok((value, _custody)) => Ok(ObjectReceipt { value, work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_with_native_work(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectRead, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (read, custody) = self.read_original(object_id, maximum_bytes, &mut accounting)
            .await.map_err(read_accounting_error)?;
        match custody {
            KernelOwnedAllocation::NativeOriginal(custody) =>
                crate::async_storage::NativeObjectRead::owned(read, custody),
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.read_many_original(requests, &mut accounting).await {
            Ok((value, _custody)) => Ok(ObjectReceipt { value, work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn read_many_with_native_work(
        &self,
        requests: &[ObjectReadRequest],
        work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::async_storage::NativeObjectReadBatch, ObjectStoreError> {
        let mut accounting = KernelAccounting::native(work, cancellation);
        let (reads, custody) = self.read_many_original(requests, &mut accounting)
            .await.map_err(read_accounting_error)?;
        match custody {
            KernelOwnedAllocation::NativeOriginal(custody) =>
                crate::async_storage::NativeObjectReadBatch::new(reads, custody),
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
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

fn read_accounting_error(error: crate::FsError) -> ObjectStoreError {
    match error {
        crate::FsError::Object(error) => error,
        crate::FsError::Work(error) => ObjectStoreError::Work(error),
        crate::FsError::Cancelled(_) => ObjectStoreError::Cancelled,
        #[cfg(not(target_arch = "wasm32"))]
        crate::FsError::NativeKernel(error) => ObjectStoreError::Native(error),
        crate::FsError::HandleAllocationFailed => ObjectStoreError::SdkAllocationFailed,
        _ => ObjectStoreError::Corrupt,
    }
}

fn release_read_custody(
    accounting: &mut KernelAccounting<'_>, mut custody: KernelOwnedAllocation, bytes: u64,
) -> Result<(), crate::FsError> {
    accounting.release_owned(&mut custody, bytes)?;
    drop(custody);
    Ok(())
}

fn read_request_original(
    bucket: &wire::BucketRef, object_id: ObjectId, maximum_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(GetRequest, KernelOwnedAllocation), crate::FsError> {
    accounting.check()?;
    let bucket_bytes = bucket.name.len() as u64;
    let bucket_copy = WorkCounters { bytes_copied: bucket_bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(bucket_copy)?;
    let mut custody = accounting.claim_owned(bucket_bytes, u64::from(bucket_bytes != 0))?;
    let bucket = bucket.clone();
    let copied = accounting.record_completed(bucket_copy);
    let reconciled = accounting.reconcile_owned(
        &mut custody, bucket_bytes, bucket.name.capacity() as u64);
    copied?;
    reconciled?;
    let key_bytes = 7 + object_id.kind.canonical_tag().len() + 64;
    let key_copy = WorkCounters { bytes_copied: key_bytes as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(key_copy)?;
    let mut key_custody = accounting.claim_owned(key_bytes as u64, 1)?;
    let mut key = String::new();
    key.try_reserve_exact(key_bytes).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    accounting.reconcile_owned(&mut key_custody, key_bytes as u64, key.capacity() as u64)?;
    let mut digest = [0_u8; 64];
    hex::encode_to_slice(object_id.digest.as_bytes(), &mut digest).map_err(|_| ObjectStoreError::Corrupt)?;
    let digest = std::str::from_utf8(&digest).map_err(|_| ObjectStoreError::Corrupt)?;
    write!(&mut key, "fs/v1/{}/{digest}", object_id.kind.canonical_tag())
        .map_err(|_| ObjectStoreError::Corrupt)?;
    accounting.record_completed(key_copy)?;
    custody.absorb(&mut key_custody)?;
    Ok(((wire::GetObjectRequest {
        bucket: Some(bucket), object_key: key, ..Default::default()
    }, maximum_bytes), custody))
}

fn inventory_read_records(
    accounting: &mut KernelAccounting<'_>, bytes: u64,
) -> Result<(KernelOwnedAllocation, Result<(), crate::FsError>), crate::FsError> {
    // Return completed custody before its late refusal; caller binds the guard
    // behind the already-visible records before propagating that refusal.
    accounting.record_actual_owned(bytes)
}

fn read_header_bytes(header: &wire::GetObjectHeader) -> Result<u64, crate::FsError> {
    let Some(info) = &header.object else { return Ok(0) };
    let mut bytes = info.etag.len() as u64;
    if let Some(metadata) = &info.metadata {
        for value in [&metadata.content_type, &metadata.content_encoding,
            &metadata.cache_control, &metadata.content_disposition, &metadata.content_language] {
            bytes = bytes.checked_add(value.len() as u64).ok_or(WorkError::Overflow)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes.checked_add(size_of::<(String, String)>() as u64)
                .and_then(|bytes| bytes.checked_add(key.len() as u64))
                .and_then(|bytes| bytes.checked_add(value.len() as u64))
                .ok_or(WorkError::Overflow)?;
        }
    }
    Ok(bytes)
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
    use std::collections::BTreeMap;
    use crate::storage::ObjectKind;
    #[cfg(not(target_arch = "wasm32"))]
    use acyclic_objects::v1::ObjectsProvider as _;
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
    async fn provider_put_batch_interns_identical_object_requests()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let bytes = Bytes::from_static(b"shared");
        let object_id = ObjectId {
            kind: ObjectKind::BlobChunk,
            digest: object_digest(ObjectKind::BlobChunk, &bytes),
        };
        let writes = [
            ObjectWrite {
                object_id,
                bytes: bytes.clone(),
            },
            ObjectWrite { object_id, bytes },
        ];
        let receipt = store.put_many(&writes, WorkBudget::UNBOUNDED, &CancellationToken::new()).await?;
        assert_eq!(receipt.work.backend_write_operations, 1);
        assert_eq!(receipt.work.object_bytes_written, 6);
        assert_eq!(receipt.work.bytes_hashed, 12 + 2 * crate::OBJECT_DIGEST_ENVELOPE_BYTES);
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
        let failure = store.put_many(&writes, budget, &CancellationToken::new()).await
            .err().unwrap_or_else(|| panic!("unadmitted allocation unexpectedly succeeded"));
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(*failure.work, WorkCounters::default());
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct ObservedProvider {
        inner: acyclic_objects::v1::MemoryObjects,
        original: crate::NativeMaterializationWork,
        calls: std::sync::atomic::AtomicU64,
        maximum: std::sync::atomic::AtomicU64,
        cancel: bool,
        panic: bool,
        error: bool,
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait::async_trait]
    impl acyclic_objects::v1::ObjectsProvider for ObservedProvider {
        async fn create_bucket(&self, q: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> { self.inner.create_bucket(q).await }
        async fn head_bucket(&self, q: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> { self.inner.head_bucket(q).await }
        async fn delete_bucket(&self, q: wire::DeleteBucketRequest) -> Result<wire::DeleteBucketResponse, Error> { self.inner.delete_bucket(q).await }
        async fn put(&self, q: wire::PutObjectHeader, b: Bytes) -> Result<wire::ObjectInfo, Error> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            assert_eq!(self.original.receipt().backend_write_operations, 1);
            assert!(self.original.receipt().bytes_copied > 0);
            let result = self.inner.put(q, b).await;
            if self.cancel { self.original.cancellation().cancel(); }
            if self.error { Err(wire::ErrorCode::Unavailable.into()) } else { result }
        }
        async fn get(&self, q: wire::GetObjectRequest, maximum: u64) -> Result<Object, Error> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            self.maximum.store(maximum, SeqCst);
            assert_eq!(self.original.receipt().backend_read_operations, 1);
            assert!(self.original.receipt().peak_allocation_bytes >=
                crate::NativeMaterializationWork::accounting_state_bytes()
                + q.bucket.as_ref().map_or(0, |bucket| bucket.name.len() as u64)
                + q.object_key.len() as u64 + maximum);
            assert_eq!(q.object_key, object_key(native_test_id()));
            assert!(!self.panic, "provider boundary panic");
            if self.error { return Err(wire::ErrorCode::Unavailable.into()); }
            if self.cancel { self.original.cancellation().cancel(); }
            Ok(Object { header: wire::GetObjectHeader::default(), body: Bytes::from_static(b"body") })
        }
        async fn head(&self, q: wire::HeadObjectRequest) -> Result<wire::HeadObjectResponse, Error> { self.inner.head(q).await }
        async fn delete(&self, q: wire::DeleteObjectRequest) -> Result<wire::DeleteObjectResponse, Error> { self.inner.delete(q).await }
        async fn list(&self, q: wire::ListObjectsRequest) -> Result<wire::ListObjectsResponse, Error> { self.inner.list(q).await }
        async fn create_multipart(&self, q: wire::CreateMultipartRequest) -> Result<wire::MultipartUpload, Error> { self.inner.create_multipart(q).await }
        async fn upload_part(&self, q: wire::UploadPartHeader, b: Bytes) -> Result<wire::UploadedPart, Error> { self.inner.upload_part(q, b).await }
        async fn list_parts(&self, q: wire::ListPartsRequest) -> Result<wire::ListPartsResponse, Error> { self.inner.list_parts(q).await }
        async fn complete_multipart(&self, q: wire::CompleteMultipartRequest) -> Result<wire::ObjectInfo, Error> { self.inner.complete_multipart(q).await }
        async fn abort_multipart(&self, q: wire::AbortMultipartRequest) -> Result<wire::AbortMultipartResponse, Error> { self.inner.abort_multipart(q).await }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait::async_trait]
    impl NativeBatchObjects for ObservedProvider {
        async fn put_batch(&self, q: Vec<PutRequest>) -> Vec<Result<wire::ObjectInfo, Error>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            assert_eq!(self.original.receipt().backend_write_operations, 1);
            let mut results = Vec::new();
            for (index, (header, body)) in q.into_iter().enumerate() {
                if self.error && index == 1 { results.push(Err(wire::ErrorCode::Unavailable.into())); }
                else { results.push(self.inner.put(header, body).await); }
            }
            let header_bytes = self.maximum.load(SeqCst) as usize;
            if header_bytes != 0 && let Some(Ok(info)) = results.first_mut() {
                info.etag = "e".repeat(header_bytes);
            }
            if self.cancel { self.original.cancellation().cancel(); }
            results
        }
        async fn get_batch(&self, q: Vec<GetRequest>) -> Vec<Result<Object, Error>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            let maximum = q.iter().map(|request| request.1).sum();
            self.maximum.store(maximum, SeqCst);
            assert_eq!(self.original.receipt().backend_read_operations, 1);
            assert_eq!(self.original.receipt().object_probes, q.len() as u64);
            assert!(self.original.receipt().peak_allocation_bytes >=
                crate::NativeMaterializationWork::accounting_state_bytes() + maximum);
            assert!(!self.panic, "provider batch boundary panic");
            if self.cancel { self.original.cancellation().cancel(); }
            q.into_iter().map(|request| {
                assert_eq!(request.0.object_key, object_key(native_test_id()));
                if self.error { Err(wire::ErrorCode::Unavailable.into()) }
                else { Ok(Object { header: wire::GetObjectHeader::default(),
                    body: Bytes::from_static(b"body") }) }
            }).collect()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_write_denies_request_and_call_before_effects() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        for deny_copy in [true, false] {
            let mut budget = WorkBudget::UNBOUNDED;
            if deny_copy { budget.bytes_copied = 0; } else { budget.backend_write_operations = 0; }
            let (store, work, cancellation) = observed_native_store(budget, false, false, false);
            assert!(store.put_with_native_work(native_test_id(), Bytes::from_static(b"body"), &work, &cancellation).await.is_err());
            assert_eq!(store.provider.calls.load(SeqCst), 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert_eq!(work.receipt().object_bytes_written, 0);
            assert_eq!(work.receipt().bytes_hashed, 4 + crate::OBJECT_DIGEST_ENVELOPE_BYTES);
            if deny_copy { assert_eq!(work.receipt().bytes_copied, 0); }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_keeps_all_acknowledged_prefix_and_original_error() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, true);
        let writes = [b"first".as_slice(), b"second".as_slice(), b"third".as_slice()].map(|body| {
            ObjectWrite { object_id: ObjectId { kind: ObjectKind::BlobChunk,
                digest: object_digest(ObjectKind::BlobChunk, body) }, bytes: Bytes::copy_from_slice(body) }
        });
        let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
            .err().ok_or("partial batch unexpectedly succeeded")?;
        assert!(matches!(error, ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })));
        assert_eq!(work.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().object_bytes_written, 10);
        assert_eq!(work.receipt().bytes_hashed, 16 + 3 * crate::OBJECT_DIGEST_ENVELOPE_BYTES);
        for index in [0, 2] {
            assert_eq!(store.provider.inner.get(read_request(store.bucket(), writes[index].object_id, 10).0, 10)
                .await?.body, writes[index].bytes);
        }
        assert!(store.provider.inner.get(read_request(store.bucket(), writes[1].object_id, 10).0, 10).await.is_err());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_response_denial_preserves_completed_writes_and_primary_error() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        for partial_failure in [false, true] {
            let mut budget = WorkBudget::UNBOUNDED;
            budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes() + 4096;
            let (store, work, cancellation) = observed_native_store(budget, false, false, partial_failure);
            store.provider.maximum.store(8192, SeqCst);
            let writes = [b"body".as_slice(), b"other".as_slice()].map(|body| ObjectWrite {
                object_id: ObjectId { kind: ObjectKind::BlobChunk, digest: object_digest(ObjectKind::BlobChunk, body) },
                bytes: Bytes::copy_from_slice(body),
            });
            let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
                .err().ok_or("unbounded response was accepted")?;
            if partial_failure {
                assert!(matches!(error, ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })));
                assert_eq!(work.receipt().object_bytes_written, 4);
            } else {
                assert!(matches!(error, ObjectStoreError::Work(_) | ObjectStoreError::Native(_)));
                assert_eq!(work.receipt().object_bytes_written, 9);
            }
            assert_eq!(store.provider.calls.load(SeqCst), 1);
            assert_eq!(work.receipt().backend_write_operations, 1);
            assert!(work.receipt().peak_allocation_bytes > budget.peak_allocation_bytes);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_provider_alias_retains_original_vec_custody() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)?;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let mut custody = work.claim_allocation(4, 1)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(4).map_err(crate::NativeTreeMaterializationError::AllocationFailed)?;
        custody.reconcile_capacity(bytes.capacity() as u64)?;
        let copy = WorkCounters { bytes_copied: 4, ..WorkCounters::UNCHARGED };
        work.admit(copy)?;
        bytes.extend_from_slice(b"body");
        work.record_admitted(copy)?;
        work.admit(WorkCounters::UNCHARGED)?;
        let object = crate::storage::HashedObject::from_native_vec(ObjectKind::BlobChunk,
            &mut bytes, &mut custody, &work, &cancellation)?;
        let id = object.object_id();
        let caller_alias = object.bytes().slice(1..);
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        store.put_hashed_with_native_work(object, &work, &cancellation).await?;
        drop(custody);
        let provider_alias = store.provider.get(read_request(store.bucket(), id, 4).0, 4).await?.body;
        drop(caller_alias);
        let retained = work.remaining()?.peak_allocation_bytes;
        assert!(retained < baseline);
        drop(store);
        assert_eq!(work.remaining()?.peak_allocation_bytes, retained);
        drop(provider_alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_post_mutation_cancellation_keeps_acknowledgement() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, true, false, false);
        assert!(store.put_with_native_work(native_test_id(), Bytes::from_static(b"body"), &work, &cancellation).await.is_err());
        assert_eq!(work.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().object_bytes_written, 4);
        assert_eq!(store.provider.inner.get(read_request(store.bucket(), native_test_id(), 4).0, 4).await?.body,
            Bytes::from_static(b"body"));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_lost_ack_retry_inspects_existing_with_original_work() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
        store.provider.inner.put(wire::PutObjectHeader { bucket: Some(store.bucket().clone()),
            object_key: object_key(native_test_id()), ..Default::default() }, Bytes::from_static(b"body")).await?;
        store.put_with_native_work(native_test_id(), Bytes::from_static(b"body"), &work, &cancellation).await?;
        assert_eq!(work.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_bytes_written, 4);
        assert_eq!(work.receipt().object_bytes_read, 4);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_test_id() -> ObjectId {
        ObjectId { kind: ObjectKind::BlobChunk, digest: object_digest(ObjectKind::BlobChunk, b"body") }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_native_store(
        budget: WorkBudget, cancel: bool, panic: bool, error: bool,
    ) -> (LogicalObjectStore<ObservedProvider>, crate::NativeMaterializationWork, CancellationToken) {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let provider = ObservedProvider { inner, original: work.clone(),
            calls: 0.into(), maximum: 0.into(), cancel, panic, error };
        (LogicalObjectStore::new(Arc::new(provider), bucket), work, cancellation)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_original_quota_and_all_byte_aliases() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 4;
        budget.peak_allocation_bytes = 100_000;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let baseline = work.remaining()?.peak_allocation_bytes;
        let initial_operations = work.receipt().allocation_operations;
        let read = store.read_with_native_work(native_test_id(), u64::MAX, &work, &cancellation).await?;
        assert_eq!(store.provider.maximum.load(SeqCst), 4);
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().allocation_operations - initial_operations, 3);
        assert_eq!(work.receipt().object_bytes_read, 4);
        assert_eq!(work.receipt().bytes_hashed, 4);
        assert_eq!(work.receipt().bytes_copied,
            store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64);
        let read = read.into_read();
        let alias = read.bytes.slice(1..);
        let retained = work.remaining()?.peak_allocation_bytes;
        assert!(retained < baseline - 4);
        drop(read);
        assert_eq!(work.remaining()?.peak_allocation_bytes, retained);
        drop(alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_denies_named_data_and_hash_before_their_boundaries() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let before = work.receipt();
        assert!(store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt(), before);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_hashed = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().object_bytes_read, 4);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_error_cancellation_and_panic_keep_attempted_prefix() -> Result<(), Box<dyn std::error::Error>> {
        use futures::FutureExt as _;
        for (cancel, panic, error) in [(true, false, false), (false, true, false),
            (false, false, true), (true, false, true)] {
            let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
            let future = store.read_with_native_work(native_test_id(), 4, &work, &cancellation);
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if panic { assert!(result.is_err()); }
            else if error {
                assert!(matches!(result, Ok(Err(ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })))));
            } else {
                assert!(matches!(result, Ok(Err(ObjectStoreError::Cancelled | ObjectStoreError::Native(_)))));
                assert_eq!(work.receipt().object_bytes_read, 4);
            }
            assert_eq!(work.receipt().backend_read_operations, 1);
            assert_eq!(work.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_opaque_dto_is_not_known_memory_shared_authority() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let memory = crate::MemoryObjectStore::default();
        memory.put(native_test_id(), Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let shared = memory.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(shared.read().retention, ObjectReadRetention::Shared);
        assert_eq!(work.receipt().allocation_operations, before);
        let (logical, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
        let opaque = logical.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(opaque.read().retention, ObjectReadRetention::Owned { logical_bytes: 4 });
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_live_peak_ceiling_is_passed_before_provider() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let (inner, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let mut budget = WorkBudget::UNBOUNDED;
        budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes()
            + bucket.name.len() as u64 + object_key(native_test_id()).len() as u64 + 2;
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let provider = ObservedProvider { inner, original: work.clone(),
            calls: 0.into(), maximum: 0.into(), cancel: false, panic: false, error: false };
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let error = store.read_with_native_work(native_test_id(), 100, &work, &cancellation).await
            .err().ok_or("oversized provider response was accepted")?;
        assert!(matches!(error, ObjectStoreError::TooLarge { observed: 4, maximum: 2 }));
        assert_eq!(store.provider.maximum.load(SeqCst), 2);
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn ordinary_and_native_logical_reads_share_request_and_payload_accounting() -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancellation = CancellationToken::new();
        store.put(native_test_id(), Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let ordinary = store.read(native_test_id(), 4, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(native.read(), &ordinary.value);
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert_eq!(work.receipt().bytes_copied, ordinary.work.bytes_copied);
        assert_eq!(work.receipt().allocation_operations - before,
            ordinary.work.allocation_operations + 1);
        Ok(())
    }

    #[test]
    fn logical_response_header_inventory_counts_named_record_data() -> Result<(), Box<dyn std::error::Error>> {
        let header = wire::GetObjectHeader {
            object: Some(wire::ObjectInfo {
                etag: "etag".to_owned(),
                metadata: Some(wire::ObjectMetadata {
                    content_type: "type".to_owned(),
                    content_encoding: "zip".to_owned(),
                    cache_control: "cache".to_owned(),
                    content_disposition: "inline".to_owned(),
                    content_language: "en".to_owned(),
                    user: BTreeMap::from([("key".to_owned(), "value".to_owned())]),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(read_header_bytes(&header)?,
            4 + 4 + 3 + 5 + 6 + 2 + 3 + 5 + size_of::<(String, String)>() as u64);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_is_one_original_provider_call_with_retained_vector_and_aliases()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 8;
        budget.peak_allocation_bytes = 100_000;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        let initial_operations = work.receipt().allocation_operations;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let batch = store.read_many_with_native_work(&requests, &work, &cancellation).await?;
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(store.provider.maximum.load(SeqCst), 8);
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_bytes_read, 8);
        assert_eq!(work.receipt().bytes_hashed, 8);
        // Two SDK vectors, two request string pairs and two byte owner controls.
        assert_eq!(work.receipt().allocation_operations - initial_operations, 8);
        assert_eq!(work.receipt().bytes_copied,
            2 * (store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64));
        let alias = batch.reads()[0].bytes.clone();
        let (reads, vector_custody) = batch.into_parts();
        assert!(vector_custody.work().shares_ledger(&work));
        assert_eq!(vector_custody.bytes(), reads.capacity() as u64 * size_of::<ObjectRead>() as u64);
        drop(reads);
        let with_vector = work.remaining()?.peak_allocation_bytes;
        drop(vector_custody);
        assert!(work.remaining()?.peak_allocation_bytes > with_vector);
        assert!(work.remaining()?.peak_allocation_bytes < baseline);
        drop(alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_finite_data_and_budget_denial_precede_provider_and_hash()
        -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_copied = 0;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().bytes_copied, 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.bytes_hashed = 4;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.calls.load(SeqCst), 1);
        assert_eq!(work.receipt().object_bytes_read, 8);
        assert_eq!(work.receipt().bytes_hashed, 4);
        let mut budget = WorkBudget::UNBOUNDED;
        budget.object_bytes_read = 7;
        let (store, work, cancellation) = observed_native_store(budget, false, false, false);
        assert!(store.read_many_with_native_work(&requests, &work, &cancellation).await.is_err());
        assert_eq!(store.provider.maximum.load(SeqCst), 7);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_batch_error_cancellation_and_panic_keep_one_call_prefix()
        -> Result<(), Box<dyn std::error::Error>> {
        use futures::FutureExt as _;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        for (cancel, panic, error) in [(true, false, false), (false, true, false),
            (false, false, true), (true, false, true)] {
            let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, cancel, panic, error);
            let future = store.read_many_with_native_work(&requests, &work, &cancellation);
            let result = std::panic::AssertUnwindSafe(future).catch_unwind().await;
            if panic { assert!(result.is_err()); }
            else if error {
                assert!(matches!(result, Ok(Err(ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })))));
            } else {
                assert!(matches!(result, Ok(Err(ObjectStoreError::Cancelled | ObjectStoreError::Native(_)))));
                assert_eq!(work.receipt().object_bytes_read, 8);
            }
            assert_eq!(work.receipt().backend_read_operations, 1);
            assert_eq!(work.receipt().object_probes, 2);
            assert_eq!(work.receipt().bytes_hashed, 0);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn ordinary_and_native_logical_batches_share_the_genuine_batch_engine()
        -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancellation = CancellationToken::new();
        store.put(native_test_id(), Bytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let ordinary = store.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_many_with_native_work(&requests, &work, &cancellation).await?;
        assert_eq!(native.reads(), ordinary.value.as_slice());
        assert_eq!(ordinary.work.backend_read_operations, 1);
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert_eq!(work.receipt().bytes_copied, ordinary.work.bytes_copied);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert_eq!(work.receipt().allocation_operations - before,
            ordinary.work.allocation_operations + 2);
        Ok(())
    }
}
