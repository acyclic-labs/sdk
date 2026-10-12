//! Filesystem immutable content over canonical logical Objects v1.
use crate::AsyncObjectStore;
use crate::cancellation::CancellationToken;
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{
    ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectReceipt, ObjectResult,
    ObjectStoreError, ObjectWrite, object_digest,
};
use acyclic_objects::v1::{Error, NativeBatchObjects, Object, wire};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use std::fmt::Write as _;
type PutRequest = (wire::PutObjectHeader, OwnedBytes);
type GetRequest = (wire::GetObjectRequest, u64);

/// Immutable filesystem-object storage over one exact public Objects bucket.
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

fn put_request(
    bucket: &wire::BucketRef, object_id: ObjectId, bytes: OwnedBytes,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(PutRequest, KernelOwnedAllocation), crate::FsError> {
    let mut request_custody;
    let ((request, _), custody) = read_request(bucket, object_id, 0, accounting)?;
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

fn release_write_custody(
    accounting: &mut KernelAccounting<'_>, mut custody: KernelOwnedAllocation, bytes: u64,
) -> Result<(), crate::FsError> {
    match &mut custody {
        KernelOwnedAllocation::Standalone => match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.release(bytes),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.release(bytes),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { .. } => return Err(ObjectStoreError::Corrupt.into()),
        }.map_err(|error| error.into_work_or(crate::FsError::HandleAllocationFailed)),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(custody) => { drop(custody.split_off(bytes)?); Ok(()) }
    }
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

fn service_read_error(error: Error) -> ObjectStoreError {
    match error.code {
        wire::ErrorCode::NotFound => ObjectStoreError::Missing,
        _ => ObjectStoreError::LogicalObjects(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_objects_error(error: acyclic_objects::v1::NativeObjectsError) -> ObjectStoreError {
    match error {
        acyclic_objects::v1::NativeObjectsError::Service(error) => service_read_error(error),
        acyclic_objects::v1::NativeObjectsError::Unsupported => ObjectStoreError::NativeUnavailable,
        error => ObjectStoreError::NativeObjects(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_request<T>(
    data: T, custody: KernelOwnedAllocation, work: &crate::NativeMaterializationWork,
) -> Result<acyclic_objects::v1::ObjectsNativeRequest<T>, crate::FsError> {
    match (data, custody) {
        (data, KernelOwnedAllocation::NativeOriginal(custody)) if custody.matches_work(work) =>
            acyclic_objects::v1::ObjectsNativeRequest::new(data, custody, &work.runtime_meter_owner())
                .map_err(|error| native_objects_error(error).into()),
        (data, custody) => {
            drop(data);
            drop(custody);
            Err(ObjectStoreError::Corrupt.into())
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_completed_read_bytes(error: &acyclic_objects::v1::NativeObjectsError) -> Result<u64, WorkError> {
    use acyclic_objects::v1::NativeObjectsError;
    match error {
        NativeObjectsError::ReadFailure(failure) => [
            failure.object.as_ref().map_or(0, |value| value.body().len() as u64),
            failure.secondary_object.as_ref().map_or(0, |value| value.body().len() as u64),
            failure.partial.as_ref().map_or(0, |bytes| bytes.len() as u64),
            failure.transport_chunk.as_ref().map_or(0, |bytes| bytes.len() as u64),
        ].into_iter().try_fold(0_u64, |total, bytes| total.checked_add(bytes).ok_or(WorkError::Overflow)),
        NativeObjectsError::SharedFailure(failure) => native_completed_read_bytes(failure.error()),
        _ => Ok(0),
    }
}

fn completed_read_bytes(result: &Result<Object, ObjectStoreError>) -> Result<u64, WorkError> {
    match result {
        Ok(value) => Ok(value.body().len() as u64),
        #[cfg(not(target_arch = "wasm32"))]
        Err(ObjectStoreError::NativeObjects(error)) => native_completed_read_bytes(error),
        Err(_) => Ok(0),
    }
}

fn read_retention(bytes: &OwnedBytes, accounting: &KernelAccounting<'_>) -> ObjectReadRetention {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = accounting.native_work() {
        if bytes.matches_meter(&work.runtime_meter_owner()) {
            if let Some(capacity) = bytes.retained_capacity() {
                return ObjectReadRetention::Owned { logical_bytes: capacity as u64 };
            }
        }
        return ObjectReadRetention::Shared;
    }
    ObjectReadRetention::Owned { logical_bytes: bytes.len() as u64 }
}

enum LogicalReadResults {
    Ordinary(Vec<Result<Object, Error>>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_objects::v1::ObjectsBatch<Object>),
}

impl LogicalReadResults {
    fn len(&self) -> usize {
        match self {
            Self::Ordinary(values) => values.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => values.results().len(),
        }
    }

    fn completed_bytes(&self) -> Result<u64, WorkError> {
        match self {
            Self::Ordinary(values) => values.iter().try_fold(0_u64, |total, value|
                total.checked_add(value.as_ref().map_or(0, |value| value.body().len() as u64)).ok_or(WorkError::Overflow)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => values.results().iter().try_fold(0_u64, |total, value| {
                let bytes = match value { Ok(value) => value.body().len() as u64,
                    Err(error) => native_completed_read_bytes(error)? };
                total.checked_add(bytes).ok_or(WorkError::Overflow)
            }),
        }
    }

    fn ordinary_records_bytes(&self) -> Result<Option<u64>, crate::FsError> {
        match self {
            Self::Ordinary(values) => values.iter().try_fold(
                (values.capacity() as u64).checked_mul(size_of::<Result<Object, Error>>() as u64).ok_or(WorkError::Overflow)?,
                |total, value| total.checked_add(match value { Ok(value) => read_header_bytes(value.header())?, Err(_) => 0 })
                    .ok_or_else(|| WorkError::Overflow.into()),
            ).map(Some),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => Ok(None),
        }
    }

    fn finish_error(self, fallback: crate::FsError) -> crate::FsError {
        match self {
            Self::Ordinary(values) => {
                for value in values { if let Err(error) = value { return service_read_error(error).into(); } }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => {
                for value in values.into_results() { if let Err(error) = value { return native_objects_error(error).into(); } }
            }
        }
        fallback
    }

    fn validate_bounds(
        &self, requests: &[ObjectReadRequest], object_limit: u64, quota: u64,
    ) -> Result<(), crate::FsError> {
        fn validate<E>(
            values: &[Result<Object, E>], requests: &[ObjectReadRequest], object_limit: u64, mut left: u64,
        ) -> Result<(), crate::FsError> {
            for (value, request) in values.iter().zip(requests) {
                let maximum = request.maximum_bytes.min(object_limit).min(left);
                left -= maximum;
                if let Ok(value) = value {
                    let observed = value.body().len() as u64;
                    if observed > maximum {
                        return Err(ObjectStoreError::TooLarge { observed, maximum }.into());
                    }
                }
            }
            Ok(())
        }
        match self {
            Self::Ordinary(values) => validate(values, requests, object_limit, quota),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => validate(values.results(), requests, object_limit, quota),
        }
    }

    fn for_each(
        self, mut visit: impl FnMut(Result<Object, ObjectStoreError>) -> Result<(), crate::FsError>,
    ) -> Result<(), crate::FsError> {
        match self {
            Self::Ordinary(values) => {
                for value in values { visit(value.map_err(service_read_error))?; }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => {
                for value in values.into_results() { visit(value.map_err(native_objects_error))?; }
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_error_code(error: &acyclic_objects::v1::NativeObjectsError) -> Option<wire::ErrorCode> {
    use acyclic_objects::v1::{NativeObjectsCause, NativeObjectsError};
    match error {
        NativeObjectsError::Service(error) => Some(error.code),
        NativeObjectsError::SharedFailure(error) => native_error_code(error.error()),
        NativeObjectsError::ReadFailure(failure) => match &failure.cause {
            NativeObjectsCause::Service(error) => Some(error.code),
            NativeObjectsCause::SharedFailure(error) => native_error_code(error.error()),
            _ => None,
        },
        _ => None,
    }
}

fn is_precondition(error: &ObjectStoreError) -> bool {
    match error {
        ObjectStoreError::LogicalObjects(error) => error.code == wire::ErrorCode::PreconditionFailed,
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreError::NativeObjects(error) => native_error_code(error) == Some(wire::ErrorCode::PreconditionFailed),
        _ => false,
    }
}

enum LogicalPutResults {
    Ordinary(Vec<Result<wire::ObjectInfo, Error>>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(acyclic_objects::v1::ObjectsBatch<acyclic_objects::v1::ObjectsPutReceipt>),
}
impl LogicalPutResults {
    fn len(&self) -> usize {
        match self {
            Self::Ordinary(values) => values.len(),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => values.results().len(),
        }
    }

    fn completed_bytes(&self, unique: &[usize], writes: &[ObjectWrite]) -> Result<u64, WorkError> {
        fn total<T, E>(results: &[Result<T, E>], unique: &[usize], writes: &[ObjectWrite]) -> Result<u64, WorkError> {
            unique.iter().zip(results).try_fold(0_u64, |total, (&index, result)| {
                total.checked_add(if result.is_ok() { writes[index].bytes.len() as u64 } else { 0 }).ok_or(WorkError::Overflow)
            })
        }
        match self {
            Self::Ordinary(values) => total(values, unique, writes),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => total(values.results(), unique, writes),
        }
    }

    fn ordinary_records_bytes(&self) -> Result<Option<u64>, crate::FsError> {
        match self {
            Self::Ordinary(values) => values.iter().try_fold(
                (values.capacity() as u64).checked_mul(size_of::<Result<wire::ObjectInfo, Error>>() as u64).ok_or(WorkError::Overflow)?,
                |total, value| total.checked_add(match value { Ok(info) => put_info_bytes(info)?, Err(_) => 0 })
                    .ok_or_else(|| WorkError::Overflow.into()),
            ).map(Some),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => Ok(None),
        }
    }

    fn has_failure(&self, unique: &[usize], writes: &[ObjectWrite]) -> bool {
        match self {
            Self::Ordinary(values) => unique.iter().zip(values).any(|(&index, result)| match result {
                Ok(info) => info.size != writes[index].bytes.len() as u64,
                Err(error) => error.code != wire::ErrorCode::PreconditionFailed,
            }),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(values) => unique.iter().zip(values.results()).any(|(&index, result)| match result {
                Ok(receipt) => receipt.info().size != writes[index].bytes.len() as u64,
                Err(error) => native_error_code(error) != Some(wire::ErrorCode::PreconditionFailed),
            }),
        }
    }
}

impl<P: NativeBatchObjects> LogicalObjectStore<P> {
    async fn fetch(
        &self, request: GetRequest, custody: KernelOwnedAllocation,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Result<Object, ObjectStoreError>, crate::FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let maximum = request.1;
            let packet = native_request(request.0, custody, work)?;
            let future = self.provider.get_native(packet, maximum, work.runtime_meter_owner())
                .map_err(native_objects_error)?;
            accounting.charge(WorkCounters { object_probes: 1, ..WorkCounters::UNCHARGED })?;
            return Ok(future.await.map_err(native_objects_error));
        }
        let request_bytes = request.0.object_key.capacity() as u64
            + request.0.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity() as u64);
        accounting.charge(WorkCounters {
            object_probes: 1, backend_read_operations: 1, ..WorkCounters::UNCHARGED
        })?;
        let result = self.provider.get(request.0, request.1.min(self.maximum_object_bytes)).await.map_err(service_read_error);
        release_read_custody(accounting, custody, request_bytes)?;
        Ok(result)
    }

    async fn read_accounted(
        &self, object_id: ObjectId, maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(ObjectRead, KernelOwnedAllocation), crate::FsError> {
        accounting.check()?;
        let request_custody;
        let (mut request, custody) = read_request(
            &self.bucket, object_id, maximum_bytes, accounting)?;
        request_custody = custody;
        let remaining = accounting.remaining()?;
        let native = accounting.standalone_parts().is_none();
        let maximum = maximum_bytes.min(self.maximum_object_bytes).min(remaining.object_bytes_read);
        let maximum = if native { maximum } else { maximum.min(remaining.peak_allocation_bytes) };
        request.1 = maximum;
        let mut body_custody = if native { KernelOwnedAllocation::Standalone } else {
            accounting.claim_owned(maximum, 0)?
        };
        let header_custody;
        let result = self.fetch(request, request_custody, accounting).await?;
        let observed = completed_read_bytes(&result)?;
        let read_completion = accounting.record_completed(WorkCounters {
            object_bytes_read: observed, ..WorkCounters::UNCHARGED
        });
        let value = result?;
        if observed > maximum {
            read_completion?;
            return Err(ObjectStoreError::TooLarge { observed, maximum }.into());
        }
        let header_bytes;
        let header_completion;
        if native {
            header_custody = KernelOwnedAllocation::Standalone;
            header_bytes = 0;
            header_completion = Ok(());
        } else {
            accounting.release_owned(&mut body_custody, maximum - observed)?;
            header_bytes = read_header_bytes(value.header())?;
            (header_custody, header_completion) = accounting.record_actual_owned(header_bytes)?;
        }
        read_completion?;
        header_completion?;
        let hash_work = WorkCounters { bytes_hashed: observed, ..WorkCounters::UNCHARGED };
        accounting.admit(hash_work)?;
        let digest = object_digest(object_id.kind, value.body());
        accounting.record_completed(hash_work)?;
        if digest != object_id.digest { return Err(ObjectStoreError::Corrupt.into()); }
        let body = value.into_body();
        if !native { release_read_custody(accounting, header_custody, header_bytes)?; }
        let retention = read_retention(&body, accounting);
        Ok((ObjectRead { bytes: body, retention }, body_custody))
    }

    async fn read_many_accounted(
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
            let (value, custody) = read_request(&self.bucket, request.object_id,
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
        let native = accounting.standalone_parts().is_none();
        let result_bytes = (requests.len() as u64)
            .checked_mul(size_of::<Result<Object, Error>>() as u64).ok_or(WorkError::Overflow)?;
        let result_custody = if native { KernelOwnedAllocation::Standalone } else {
            accounting.claim_owned(result_bytes, 0)?
        };
        let maximum = provider_requests.iter().try_fold(0_u64,
            |total, request| total.checked_add(request.1).ok_or(WorkError::Overflow))?;
        let remaining = accounting.remaining()?;
        let quota = maximum.min(remaining.object_bytes_read);
        let quota = if native { quota } else { quota.min(remaining.peak_allocation_bytes) };
        let mut body_custody = if native { KernelOwnedAllocation::Standalone } else {
            accounting.claim_owned(quota, 0)?
        };
        let mut left = quota;
        for request in &mut provider_requests {
            request.1 = request.1.min(left);
            left -= request.1;
        }
        let header_custody;
        let results;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let packet = native_request(provider_requests, request_custody, work)?;
            let future = self.provider.get_batch_native(packet, work.runtime_meter_owner())
                .map_err(native_objects_error)?;
            accounting.charge(WorkCounters { object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
            results = LogicalReadResults::Native(future.await.map_err(native_objects_error)?);
        } else {
            accounting.charge(WorkCounters { backend_read_operations: 1,
                object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
            results = LogicalReadResults::Ordinary(self.provider.get_batch(provider_requests).await);
            release_read_custody(accounting, request_custody, request_bytes)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            accounting.charge(WorkCounters { backend_read_operations: 1,
                object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
            results = LogicalReadResults::Ordinary(self.provider.get_batch(provider_requests).await);
            release_read_custody(accounting, request_custody, request_bytes)?;
        }
        let observed = results.completed_bytes()?;
        let read_completion = accounting.record_completed(WorkCounters {
            object_bytes_read: observed, ..WorkCounters::UNCHARGED
        });
        let header_bytes;
        let header_completion;
        if let Some(bytes) = results.ordinary_records_bytes()? {
            header_bytes = bytes.saturating_sub(result_bytes);
            (header_custody, header_completion) = accounting.record_actual_owned(header_bytes)?;
        } else {
            header_bytes = 0;
            header_custody = KernelOwnedAllocation::Standalone;
            header_completion = Ok(());
        }
        if results.len() != requests.len() { return Err(ObjectStoreError::Corrupt.into()); }
        if let Err(error) = read_completion.and(header_completion) {
            return Err(results.finish_error(error));
        }
        results.validate_bounds(requests, self.maximum_object_bytes, quota)?;
        if !native {
            if observed > quota { return Err(ObjectStoreError::TooLarge { observed, maximum: quota }.into()); }
            accounting.release_owned(&mut body_custody, quota - observed)?;
        }
        let mut request_index = 0;
        results.for_each(|result| {
            let request = &requests[request_index];
            request_index += 1;
            let value = result?;
            let observed = value.body().len() as u64;
            let hash_work = WorkCounters { bytes_hashed: observed, ..WorkCounters::UNCHARGED };
            accounting.admit(hash_work)?;
            let digest = object_digest(request.object_id.kind, value.body());
            accounting.record_completed(hash_work)?;
            if digest != request.object_id.digest { return Err(ObjectStoreError::Corrupt.into()); }
            let body = value.into_body();
            values.push(ObjectRead { retention: read_retention(&body, accounting), bytes: body });
            Ok(())
        })?;
        if !native {
            release_read_custody(accounting, result_custody, result_bytes)?;
            release_read_custody(accounting, header_custody, header_bytes)?;
        }
        Ok((values, output_custody))
    }

    async fn put_accounted(
        &self, object_id: ObjectId, bytes: OwnedBytes, verified: bool,
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
        let (request, custody) = put_request(&self.bucket, object_id, bytes, accounting)?;
        request_custody = custody;
        let request_bytes = put_request_bytes(&request);
        accounting.admit(WorkCounters { object_bytes_written: length, ..WorkCounters::UNCHARGED })?;
        let native = accounting.standalone_parts().is_none();
        let result = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let packet = native_request(request, request_custody, work)?;
                self.provider.put_native(packet, work.runtime_meter_owner()).map_err(native_objects_error)?
                    .await.map(|receipt| receipt.info().size).map_err(native_objects_error)
            }
            _ => {
                accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
                let result = self.provider.put(request.0, request.1).await.map_err(ObjectStoreError::LogicalObjects);
                release_write_custody(accounting, request_custody, request_bytes)?;
                match result {
                    Ok(version) => {
                        let size = version.size;
                        record_write_completion(accounting, length)?;
                        let response_bytes = put_info_bytes(&version)?;
                        let (response_custody, late) = accounting.record_actual_owned(response_bytes)?;
                        if size != length { return Err(ObjectStoreError::Corrupt.into()); }
                        late?;
                        drop(version);
                        release_write_custody(accounting, response_custody, response_bytes)?;
                        Ok(size)
                    }
                    Err(error) => Err(error),
                }
            }
        };
        match result {
            Ok(size) => {
                if native { record_write_completion(accounting, length)?; }
                if size != length { return Err(ObjectStoreError::Corrupt.into()); }
            }
            Err(error) if is_precondition(&error) => {
                let (existing, custody) = self.read_accounted(object_id, length, accounting).await?;
                let actual = existing.bytes.len() as u64;
                drop(existing);
                if !native { release_write_custody(accounting, custody, actual)?; }
                if actual != length { return Err(ObjectStoreError::Corrupt.into()); }
                record_write_completion(accounting, length)?;
            }
            Err(error) => return Err(error.into()),
        }
        accounting.check()
    }

    async fn put_many_accounted(
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
            let (request, custody) = put_request(&self.bucket, write.object_id, write.bytes.clone(), accounting)?;
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
        let native = accounting.standalone_parts().is_none();
        let response_custody;
        let results;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            let packet = native_request(requests, request_custody, work)?;
            let future = self.provider.put_batch_native(packet, work.runtime_meter_owner())
                .map_err(native_objects_error)?;
            results = LogicalPutResults::Native(future.await.map_err(native_objects_error)?);
        } else {
            accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
            results = LogicalPutResults::Ordinary(self.provider.put_batch(requests).await);
            release_write_custody(accounting, request_custody, request_bytes)?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
            results = LogicalPutResults::Ordinary(self.provider.put_batch(requests).await);
            release_write_custody(accounting, request_custody, request_bytes)?;
        }
        // Account every reported ACK, even beyond an earlier failed item.
        record_write_completion(accounting, results.completed_bytes(&unique, writes)?)?;
        let response_bytes;
        let response_denial;
        if let Some(bytes) = results.ordinary_records_bytes()? {
            response_bytes = bytes;
            let late;
            (response_custody, late) = accounting.record_actual_owned(bytes)?;
            response_denial = late.err();
        } else {
            response_bytes = 0;
            response_custody = KernelOwnedAllocation::Standalone;
            response_denial = None;
        }
        if results.len() != unique.len() { return Err(ObjectStoreError::Corrupt.into()); }
        let allow_idempotent = !results.has_failure(&unique, writes);
        let completion = match results {
            LogicalPutResults::Ordinary(results) => self.finish_put_many_results(
                results.into_iter().map(|result| result.map(|info| info.size).map_err(ObjectStoreError::LogicalObjects)),
                &unique, writes, allow_idempotent, accounting,
            ).await,
            #[cfg(not(target_arch = "wasm32"))]
            LogicalPutResults::Native(results) => self.finish_put_many_results(
                results.into_results().map(|result| result.map(|receipt| receipt.info().size).map_err(native_objects_error)),
                &unique, writes, allow_idempotent, accounting,
            ).await,
        };
        if !native { release_write_custody(accounting, response_custody, response_bytes)?; }
        drop(unique);
        release_write_custody(accounting, unique_custody, unique_bytes)?;
        completion?;
        if let Some(error) = response_denial { return Err(error); }
        accounting.check()
    }

    async fn finish_put_many_results(
        &self, results: impl IntoIterator<Item = Result<u64, ObjectStoreError>>,
        unique: &[usize], writes: &[ObjectWrite], allow_idempotent: bool,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let mut failure = None;
        for (&index, result) in unique.iter().zip(results) {
            let write = &writes[index];
            let length = write.bytes.len() as u64;
            let result = match result {
                Ok(size) if size == length => Ok(()),
                Ok(_) => Err(crate::FsError::from(ObjectStoreError::Corrupt)),
                Err(error) if is_precondition(&error) && !allow_idempotent => continue,
                Err(error) if is_precondition(&error) && failure.is_none() => {
                    match self.read_accounted(write.object_id, length, accounting).await {
                        Ok((existing, custody)) => {
                            let actual = existing.bytes.len() as u64;
                            drop(existing);
                            if accounting.standalone_parts().is_some() {
                                release_write_custody(accounting, custody, actual)?;
                            }
                            if actual == length { record_write_completion(accounting, length) }
                            else { Err(ObjectStoreError::Corrupt.into()) }
                        }
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error.into()),
            };
            if failure.is_none() { failure = result.err(); }
        }
        match failure { Some(error) => Err(error), None => Ok(()) }
    }


}

impl<P: NativeBatchObjects> AsyncObjectStore for LogicalObjectStore<P> {
    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<()> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.put_accounted(object_id, bytes, false, &mut accounting).await {
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
        match self.put_accounted(object_id, bytes, true, &mut accounting).await {
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
        match self.put_many_accounted(writes, &mut accounting).await {
            Ok(()) => Ok(ObjectReceipt { value: (), work: accounting.spent() }),
            Err(error) => Err(OperationFailure::new(read_accounting_error(error), accounting.spent())),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_with_native_work(
        &self, object_id: ObjectId, bytes: OwnedBytes, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_accounted(object_id, bytes, false, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(read_accounting_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_hashed_with_native_work(
        &self, object: crate::storage::HashedObject, work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        let (object_id, bytes) = object.into_parts();
        self.put_accounted(object_id, bytes, true, &mut KernelAccounting::native(work, cancellation))
            .await.map_err(read_accounting_error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn put_many_with_native_work(
        &self, writes: &[ObjectWrite], work: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(), ObjectStoreError> {
        self.put_many_accounted(writes, &mut KernelAccounting::native(work, cancellation))
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

    /// Like the ordinary PR237 preparation default, this local provider supplies no hosted or historical custody.
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
        match self.read_accounted(object_id, maximum_bytes, &mut accounting).await {
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
        let (read, _custody) = self.read_accounted(object_id, maximum_bytes, &mut accounting)
            .await.map_err(read_accounting_error)?;
        match read.retention {
            ObjectReadRetention::Shared => crate::async_storage::NativeObjectRead::shared(read),
            ObjectReadRetention::Owned { .. } => crate::async_storage::NativeObjectRead::intrinsic(read, work),
        }
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> ObjectResult<Vec<ObjectRead>> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        match self.read_many_accounted(requests, &mut accounting).await {
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
        let (reads, custody) = self.read_many_accounted(requests, &mut accounting)
            .await.map_err(read_accounting_error)?;
        match custody {
            KernelOwnedAllocation::NativeOriginal(custody) =>
                crate::async_storage::NativeObjectReadBatch::new(reads, custody),
            KernelOwnedAllocation::Standalone => Err(ObjectStoreError::Corrupt),
        }
    }

    async fn contains(
        &self, object_id: ObjectId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> ObjectResult<bool> {
        match self.read(object_id, budget.object_bytes_read, budget, cancellation).await {
            Ok(receipt) => Ok(ObjectReceipt { value: true, work: receipt.work }),
            Err(failure) if matches!(failure.error, ObjectStoreError::Missing) =>
                Ok(ObjectReceipt { value: false, work: *failure.work }),
            Err(failure) => Err(failure),
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

fn read_request(
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
    let mut family_storage = [0_u8; 3];
    let mut family = std::io::Cursor::new(family_storage.as_mut_slice());
    std::io::Write::write_fmt(&mut family, format_args!("{}", object_id.kind.canonical_tag()))
        .map_err(ObjectStoreError::Io)?;
    let family_bytes = family.position() as usize;
    let family = std::str::from_utf8(&family.get_ref()[..family_bytes])
        .map_err(|_| ObjectStoreError::Corrupt)?;
    let key_bytes = "fs/v1/".len() + family.len() + "/".len() + 64;
    let key_copy = WorkCounters { bytes_copied: key_bytes as u64, ..WorkCounters::UNCHARGED };
    accounting.admit(key_copy)?;
    let mut key_custody = accounting.claim_owned(key_bytes as u64, 1)?;
    let mut key = String::new();
    key.try_reserve_exact(key_bytes).map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    accounting.reconcile_owned(&mut key_custody, key_bytes as u64, key.capacity() as u64)?;
    let mut digest = [0_u8; 64];
    hex::encode_to_slice(object_id.digest.as_bytes(), &mut digest).map_err(|_| ObjectStoreError::Corrupt)?;
    let digest = std::str::from_utf8(&digest).map_err(|_| ObjectStoreError::Corrupt)?;
    write!(&mut key, "fs/v1/{family}/{digest}")
        .map_err(|_| ObjectStoreError::Corrupt)?;
    accounting.record_completed(key_copy)?;
    custody.absorb(&mut key_custody)?;
    Ok(((wire::GetObjectRequest {
        bucket: Some(bucket), object_key: key, ..Default::default()
    }, maximum_bytes), custody))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::object_key;
    use crate::storage::ObjectKind;
    #[cfg(not(target_arch = "wasm32"))]
    use acyclic_objects::v1::ObjectsProvider as _;

    #[test]
    fn original_request_accounts_canonical_textual_object_keys()
    -> Result<(), Box<dyn std::error::Error>> {
        let bucket = wire::BucketRef { name: "filesystem".to_owned(), ..Default::default() };
        let cancellation = CancellationToken::new();
        for tag in 0..=u8::MAX {
            let Ok(kind) = ObjectKind::from_canonical_tag(tag) else { continue };
            let object_id = ObjectId { kind, digest: object_digest(kind, b"key") };
            let expected = object_key(object_id);
            let copied = (bucket.name.len() + expected.len()) as u64;
            let mut accounting = KernelAccounting::standalone(
                WorkBudget { bytes_copied: copied, ..WorkBudget::UNBOUNDED }, &cancellation);
            let ((request, maximum), custody) =
                read_request(&bucket, object_id, 42, &mut accounting)?;
            assert_eq!(request.object_key, expected);
            assert_eq!(maximum, 42);
            assert_eq!(accounting.spent().bytes_copied, copied);
            let retained = request.object_key.capacity() as u64
                + request.bucket.as_ref().expect("original bucket").name.capacity() as u64;
            drop(request);
            release_read_custody(&mut accounting, custody, retained)?;

            let mut denied = KernelAccounting::standalone(
                WorkBudget { bytes_copied: copied - 1, ..WorkBudget::UNBOUNDED }, &cancellation);
            assert!(matches!(
                read_request(&bucket, object_id, 42, &mut denied),
                Err(crate::FsError::Work(WorkError::BudgetExceeded { counter: "bytes_copied", .. }))
            ));
            assert_eq!(denied.spent().bytes_copied, bucket.name.len() as u64);
        }
        Ok(())
    }
    #[tokio::test]
    async fn provider_object_batch_reads_once_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let first_bytes = OwnedBytes::from_static(b"first");
        let second_bytes = OwnedBytes::from_static(b"second");
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
        assert!(store.contains(first, WorkBudget::UNBOUNDED, &cancellation).await?.value);
        let missing = ObjectId { kind: first.kind, digest: crate::Digest::from_bytes([99; 32]) };
        assert!(!store.contains(missing, WorkBudget::UNBOUNDED, &cancellation).await?.value);
        let denied = WorkBudget { allocation_operations: 0, ..WorkBudget::UNBOUNDED };
        let failure = store.contains(first, denied, &cancellation).await.expect_err("unfunded presence read");
        assert!(matches!(failure.error, ObjectStoreError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 0);
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
    async fn provider_put_batch_rejects_unadmitted_request_allocation() {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let bytes = OwnedBytes::from_static(b"body");
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
    fn prepare_observed_body(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>, acyclic_objects::v1::NativeObjectsError> {
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, 4)?;
        let mut data = Vec::new();
        data.try_reserve_exact(4).map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(data.capacity())?;
        data.resize(data.capacity(), 0);
        acyclic_stream::PreparedOwnedBytes::new(data, allocation, Arc::clone(meter))
            .map_err(|failure| match failure.into_error() {
                acyclic_stream::PreparedOwnedBytesError::Admission(error) => error.into(),
                _ => acyclic_objects::v1::NativeObjectsError::Admission(std::io::ErrorKind::InvalidData.into()),
            })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_body(
        mut prepared: acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Object, acyclic_objects::v1::NativeObjectsError> {
        meter.admit_codec(0, 4, 0)?;
        prepared.writable()[..4].copy_from_slice(b"body");
        meter.record_codec(0, 4, 0)?;
        Ok(Object::new_native(wire::GetObjectHeader::default(),
            prepared.freeze(4).expect("fixture prefix fits its prefunded storage"), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observed_text(
        value: &str, meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        custody: &mut acyclic_native_runtime::ReadAllocation,
    ) -> Result<String, acyclic_objects::v1::NativeObjectsError> {
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, value.len())?;
        let mut text = String::new();
        text.try_reserve_exact(value.len()).map_err(|error| meter.allocation_failed(error))?;
        allocation.reconcile_capacity(text.capacity())?;
        meter.admit_codec(0, value.len() as u64, 0)?;
        text.push_str(value);
        meter.record_codec(0, value.len() as u64, 0)?;
        custody.absorb(&mut allocation)?;
        Ok(text)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait::async_trait]
    impl acyclic_objects::v1::ObjectsProvider for ObservedProvider {
        async fn create_bucket(&self, q: wire::CreateBucketRequest) -> Result<wire::Bucket, Error> { self.inner.create_bucket(q).await }
        async fn head_bucket(&self, q: wire::HeadBucketRequest) -> Result<wire::Bucket, Error> { self.inner.head_bucket(q).await }
        async fn delete_bucket(&self, q: wire::DeleteBucketRequest) -> Result<wire::DeleteBucketResponse, Error> { self.inner.delete_bucket(q).await }
        async fn put(&self, q: wire::PutObjectHeader, b: OwnedBytes) -> Result<wire::ObjectInfo, Error> {
            use std::sync::atomic::Ordering::SeqCst;
            self.calls.fetch_add(1, SeqCst);
            assert_eq!(self.original.receipt().backend_write_operations, 1);
            assert!(self.original.receipt().bytes_copied > 0);
            let result = self.inner.put(q, b).await;
            if self.cancel { self.original.cancellation().cancel(); }
            if self.error { Err(wire::ErrorCode::Unavailable.into()) } else { result }
        }

        fn put_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<PutRequest>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsPutReceipt>,
            acyclic_objects::v1::NativeObjectsError> {
            let future = self.inner.put_native(request, Arc::clone(&meter))?;
            acyclic_objects::v1::native_future(meter, async move {
                let result = future.await;
                if self.original.receipt().backend_write_operations != 0 {
                    self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                if self.cancel { self.original.cancellation().cancel(); }
                if self.error { Err(Error::from(wire::ErrorCode::Unavailable).into()) } else { result }
            })
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
            Ok(Object::new(wire::GetObjectHeader::default(), OwnedBytes::from_static(b"body")))
        }

        fn get_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<wire::GetObjectRequest>,
            maximum: u64, meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, Object>, acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let prepared = prepare_observed_body(&meter)?;
                meter.admit_read(0)?;
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.maximum.store(maximum, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(request.data().object_key, object_key(native_test_id()));
                assert!(!self.panic, "provider boundary panic");
                if self.error { return Err(Error::from(wire::ErrorCode::Unavailable).into()); }
                let value = observed_body(prepared, &meter)?;
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                Ok(value)
            })
        }
        async fn head(&self, q: wire::HeadObjectRequest) -> Result<wire::HeadObjectResponse, Error> { self.inner.head(q).await }
        async fn delete(&self, q: wire::DeleteObjectRequest) -> Result<wire::DeleteObjectResponse, Error> { self.inner.delete(q).await }
        async fn list(&self, q: wire::ListObjectsRequest) -> Result<wire::ListObjectsResponse, Error> { self.inner.list(q).await }
        async fn create_multipart(&self, q: wire::CreateMultipartRequest) -> Result<wire::MultipartUpload, Error> { self.inner.create_multipart(q).await }
        async fn upload_part(&self, q: wire::UploadPartHeader, b: OwnedBytes) -> Result<wire::UploadedPart, Error> { self.inner.upload_part(q, b).await }
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
                else { Ok(Object::new(wire::GetObjectHeader::default(), OwnedBytes::from_static(b"body"))) }
            }).collect()
        }

        fn get_batch_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<Vec<GetRequest>>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsBatch<Object>>,
            acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let count = request.data().len();
                let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<Result<Object, acyclic_objects::v1::NativeObjectsError>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut results = Vec::new();
                results.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                allocation.reconcile_capacity(results.capacity() *
                    size_of::<Result<Object, acyclic_objects::v1::NativeObjectsError>>())?;
                let mut prepared_allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut prepared = Vec::new();
                prepared.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                prepared_allocation.reconcile_capacity(prepared.capacity() *
                    size_of::<acyclic_stream::PreparedOwnedBytes<acyclic_native_runtime::ReadAllocation>>())?;
                for _ in 0..count { prepared.push(prepare_observed_body(&meter)?); }
                meter.admit_read(0)?;
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.maximum.store(request.data().iter().map(|request| request.1).sum(), std::sync::atomic::Ordering::SeqCst);
                assert_eq!(self.original.receipt().object_probes, count as u64);
                assert!(!self.panic, "provider batch boundary panic");
                for (query, prepared) in request.data().iter().zip(prepared.into_iter()) {
                    assert_eq!(query.0.object_key, object_key(native_test_id()));
                    results.push(if self.error { Err(Error::from(wire::ErrorCode::Unavailable).into()) }
                        else { observed_body(prepared, &meter) });
                }
                drop(prepared_allocation);
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                acyclic_objects::v1::ObjectsBatch::new(results, allocation)
            })
        }

        fn put_batch_native(
            &self, request: acyclic_objects::v1::ObjectsNativeRequest<Vec<PutRequest>>,
            meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        ) -> Result<acyclic_objects::v1::ObjectsNativeFuture<'_, acyclic_objects::v1::ObjectsBatch<acyclic_objects::v1::ObjectsPutReceipt>>,
            acyclic_objects::v1::NativeObjectsError> {
            acyclic_objects::v1::native_future(Arc::clone(&meter), async move {
                let count = request.data().len();
                let mut allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<Result<acyclic_objects::v1::ObjectsPutReceipt,
                        acyclic_objects::v1::NativeObjectsError>>()).ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut results = Vec::new();
                results.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                allocation.reconcile_capacity(results.capacity() * size_of::<Result<acyclic_objects::v1::ObjectsPutReceipt,
                    acyclic_objects::v1::NativeObjectsError>>())?;
                let mut inputs_allocation = acyclic_native_runtime::ReadAllocation::new(&meter,
                    count.checked_mul(size_of::<acyclic_objects::v1::ObjectsNativeRequest<PutRequest>>())
                        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?)?;
                let mut inputs = Vec::new();
                inputs.try_reserve_exact(count).map_err(|error| meter.allocation_failed(error))?;
                inputs_allocation.reconcile_capacity(inputs.capacity() *
                    size_of::<acyclic_objects::v1::ObjectsNativeRequest<PutRequest>>())?;
                for (header, body) in request.data() {
                    assert!(header.mutation.is_none());
                    assert!(matches!(header.preconditions.as_ref().and_then(|value| value.condition.as_ref()),
                        Some(wire::preconditions::Condition::IfAbsent(true))));
                    let mut custody = acyclic_native_runtime::ReadAllocation::empty(&meter);
                    let name = observed_text(&header.bucket.as_ref().expect("fixture bucket").name, &meter, &mut custody)?;
                    let object_key = observed_text(&header.object_key, &meter, &mut custody)?;
                    let metadata = header.metadata.as_ref().expect("fixture content type");
                    assert!(metadata.user.is_empty() && metadata.content_encoding.is_empty() &&
                        metadata.cache_control.is_empty() && metadata.content_disposition.is_empty() && metadata.content_language.is_empty());
                    let content_type = observed_text(&metadata.content_type, &meter, &mut custody)?;
                    let header = wire::PutObjectHeader {
                        bucket: Some(wire::BucketRef { name }), object_key,
                        metadata: Some(wire::ObjectMetadata { content_type, ..Default::default() }),
                        preconditions: Some(wire::Preconditions { condition: Some(wire::preconditions::Condition::IfAbsent(true)) }),
                        mutation: None,
                    };
                    inputs.push(acyclic_objects::v1::ObjectsNativeRequest::new((header, body.clone()), custody, &meter)?);
                }
                // Even injected failure response metadata is admitted before mutation.
                let response_bytes = self.maximum.load(std::sync::atomic::Ordering::SeqCst) as usize;
                let mut response_allocation = acyclic_native_runtime::ReadAllocation::new(&meter, response_bytes)?;
                let mut response_etag = String::new();
                response_etag.try_reserve_exact(response_bytes).map_err(|error| meter.allocation_failed(error))?;
                response_allocation.reconcile_capacity(response_etag.capacity())?;
                meter.admit_codec(0, response_bytes as u64, 0)?;
                for _ in 0..response_bytes { response_etag.push('e'); }
                meter.record_codec(0, response_bytes as u64, 0)?;
                let mut response = (response_bytes != 0).then_some((response_etag, response_allocation));
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for (index, input) in inputs.into_iter().enumerate() {
                    results.push(if self.error && index == 1 {
                        Err(Error::from(wire::ErrorCode::Unavailable).into())
                    } else {
                        match self.inner.put_native(input, Arc::clone(&meter)) {
                            Ok(future) => match future.await {
                                Ok(receipt) if index == 0 && response.is_some() => {
                                    let (etag, custody) = response.take().expect("prepared fixture response");
                                    Ok(acyclic_objects::v1::ObjectsPutReceipt::new(wire::ObjectInfo {
                                        size: receipt.info().size, etag, ..Default::default()
                                    }, Some(custody)))
                                }
                                result => result,
                            },
                            Err(error) => Err(error),
                        }
                    });
                }
                drop(inputs_allocation);
                if self.cancel { self.original.cancellation().cancel(); }
                drop(request);
                acyclic_objects::v1::ObjectsBatch::new(results, allocation)
            })
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
            assert!(store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await.is_err());
            assert_eq!(store.provider.calls.load(SeqCst), 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert_eq!(work.receipt().object_bytes_written, 0);
            let fs_hash_bytes = 4 + crate::OBJECT_DIGEST_ENVELOPE_BYTES;
            if deny_copy { assert_eq!(work.receipt().bytes_hashed, fs_hash_bytes); }
            else { assert!(work.receipt().bytes_hashed >= fs_hash_bytes + 4); }
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
                digest: object_digest(ObjectKind::BlobChunk, body) }, bytes: OwnedBytes::copy_from_slice(body) }
        });
        let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
            .err().ok_or("partial batch unexpectedly succeeded")?;
        assert!(matches!(error, ObjectStoreError::LogicalObjects(Error { code: wire::ErrorCode::Unavailable })));
        assert_eq!(work.receipt().backend_write_operations, 2);
        assert_eq!(work.receipt().object_bytes_written, 10);
        assert!(work.receipt().bytes_hashed >= 26 + 3 * crate::OBJECT_DIGEST_ENVELOPE_BYTES);
        for index in [0, 2] {
            assert_eq!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(writes[index].object_id), ..Default::default() }, 10)
                .await?.body().as_ref(), writes[index].bytes.as_ref());
        }
        assert!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(writes[1].object_id), ..Default::default() }, 10).await.is_err());
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_response_controls_deny_before_mutation() -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::atomic::Ordering::SeqCst;
        for partial_failure in [false, true] {
            let mut budget = WorkBudget::UNBOUNDED;
            budget.peak_allocation_bytes = crate::NativeMaterializationWork::accounting_state_bytes() + 4096;
            let (store, work, cancellation) = observed_native_store(budget, false, false, partial_failure);
            store.provider.maximum.store(8192, SeqCst);
            let writes = [b"body".as_slice(), b"other".as_slice()].map(|body| ObjectWrite {
                object_id: ObjectId { kind: ObjectKind::BlobChunk, digest: object_digest(ObjectKind::BlobChunk, body) },
                bytes: OwnedBytes::copy_from_slice(body),
            });
            let error = store.put_many_with_native_work(&writes, &work, &cancellation).await
                .err().ok_or("unbounded response was accepted")?;
            assert!(matches!(error, ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError::Admission(_))
                | ObjectStoreError::Work(_) | ObjectStoreError::Native(_)));
            assert_eq!(work.receipt().object_bytes_written, 0);
            assert_eq!(store.provider.calls.load(SeqCst), 0);
            assert_eq!(work.receipt().backend_write_operations, 0);
            assert!(work.receipt().peak_allocation_bytes <= budget.peak_allocation_bytes);
            for write in &writes {
                assert!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(write.object_id), ..Default::default() }, 10).await.is_err());
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_provider_alias_retains_original_vec_custody() -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).map_err(|(error, _)| error)?;
        let baseline = work.remaining()?.peak_allocation_bytes;
        let mut custody = work.claim_allocation(4, 1, crate::WorkCounters::UNCHARGED)?;
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
        let provider_alias = store.provider.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(id), ..Default::default() }, 4).await?.into_body();
        drop(caller_alias);
        let retained = work.remaining()?.peak_allocation_bytes;
        assert!(retained < baseline);
        drop(store);
        assert!(work.remaining()?.peak_allocation_bytes >= retained);
        assert!(work.remaining()?.peak_allocation_bytes < baseline);
        drop(provider_alias);
        assert_eq!(work.remaining()?.peak_allocation_bytes, baseline);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_post_mutation_cancellation_keeps_acknowledgement() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, true, false, false);
        assert!(store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await.is_err());
        assert_eq!(work.receipt().backend_write_operations, 1);
        assert_eq!(work.receipt().object_bytes_written, 4);
        assert_eq!(store.provider.inner.get(wire::GetObjectRequest { bucket: Some(store.bucket().clone()), object_key: object_key(native_test_id()), ..Default::default() }, 4).await?.body(),
            &OwnedBytes::from_static(b"body"));
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_logical_lost_ack_retry_inspects_existing_with_original_work() -> Result<(), Box<dyn std::error::Error>> {
        let (store, work, cancellation) = observed_native_store(WorkBudget::UNBOUNDED, false, false, false);
        store.provider.inner.put(wire::PutObjectHeader { bucket: Some(store.bucket().clone()),
            object_key: object_key(native_test_id()), ..Default::default() }, OwnedBytes::from_static(b"body")).await?;
        store.put_with_native_work(native_test_id(), OwnedBytes::from_static(b"body"), &work, &cancellation).await?;
        assert_eq!(work.receipt().backend_write_operations, 0);
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
        // Original request, future and prepared-byte controls are real pre-I/O allocations.
        assert!(work.receipt().allocation_operations - initial_operations >= 7);
        assert_eq!(work.receipt().object_bytes_read, 4);
        assert_eq!(work.receipt().bytes_hashed, 4);
        assert_eq!(work.receipt().bytes_copied,
            store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64 + 4);
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
        memory.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
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
    async fn native_logical_live_control_ceiling_denies_before_provider() -> Result<(), Box<dyn std::error::Error>> {
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
            .err().ok_or("unfunded native controls reached the provider")?;
        assert!(matches!(error, ObjectStoreError::NativeObjects(acyclic_objects::v1::NativeObjectsError::Admission(_))
            | ObjectStoreError::Native(_) | ObjectStoreError::Work(_)));
        assert_eq!(store.provider.maximum.load(SeqCst), 0);
        assert_eq!(store.provider.calls.load(SeqCst), 0);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn ordinary_and_native_logical_reads_share_request_and_payload_accounting() -> Result<(), Box<dyn std::error::Error>> {
        let (provider, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        let store = LogicalObjectStore::new(Arc::new(provider), bucket);
        let cancellation = CancellationToken::new();
        store.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let ordinary = store.read(native_test_id(), 4, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_with_native_work(native_test_id(), 4, &work, &cancellation).await?;
        assert_eq!(native.read().bytes, ordinary.value.bytes);
        assert_eq!(native.read().retention, ObjectReadRetention::Shared);
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert!(work.receipt().bytes_copied >= ordinary.work.bytes_copied);
        assert!(work.receipt().allocation_operations - before > ordinary.work.allocation_operations);
        assert_eq!(work.receipt().backend_read_operations, 1);
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
        // Both vector frames, original request controls and both prepared owners are funded.
        assert!(work.receipt().allocation_operations - initial_operations >= 16);
        assert_eq!(work.receipt().bytes_copied,
            2 * (store.bucket.name.len() as u64 + object_key(native_test_id()).len() as u64) + 8);
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
        store.put(native_test_id(), OwnedBytes::from_static(b"body"), WorkBudget::UNBOUNDED, &cancellation).await?;
        let requests = [ObjectReadRequest { object_id: native_test_id(), maximum_bytes: 4 }; 2];
        let ordinary = store.read_many(&requests, WorkBudget::UNBOUNDED, &cancellation).await?;
        let work = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .unwrap_or_else(|_| panic!("test accounting construction failed"));
        let before = work.receipt().allocation_operations;
        let native = store.read_many_with_native_work(&requests, &work, &cancellation).await?;
        assert_eq!(native.reads().len(), ordinary.value.len());
        for (native, ordinary) in native.reads().iter().zip(&ordinary.value) {
            assert_eq!(native.bytes, ordinary.bytes);
            assert_eq!(native.retention, ObjectReadRetention::Shared);
        }
        assert_eq!(ordinary.work.backend_read_operations, 1);
        assert_eq!(work.receipt().backend_read_operations, 2);
        assert_eq!(work.receipt().object_bytes_read, ordinary.work.object_bytes_read);
        assert!(work.receipt().bytes_copied >= ordinary.work.bytes_copied);
        assert_eq!(work.receipt().bytes_hashed, ordinary.work.bytes_hashed);
        assert!(work.receipt().allocation_operations - before > ordinary.work.allocation_operations);
        Ok(())
    }
}
