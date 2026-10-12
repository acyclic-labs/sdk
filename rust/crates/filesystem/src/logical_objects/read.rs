use super::*;

pub(super) enum LogicalReadResults {
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
                for value in values { if let Err(error) = value { return native_objects_error(error).into(); } }
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
}

pub(super) fn finish_reads(
    results: impl IntoIterator<Item = Result<Object, ObjectStoreError>>, requests: &[ObjectReadRequest],
    accounting: &mut KernelAccounting<'_>, values: &mut Vec<ObjectRead>,
) -> Result<(), crate::FsError> {
    for (result, request) in results.into_iter().zip(requests) {
        let value = result?;
        let observed = value.body().len() as u64;
        let hash_work = WorkCounters { bytes_hashed: observed, ..WorkCounters::UNCHARGED };
        accounting.admit(hash_work)?;
        let digest = object_digest(request.object_id.kind, value.body());
        accounting.record_completed(hash_work)?;
        if digest != request.object_id.digest { return Err(ObjectStoreError::Corrupt.into()); }
        let body = value.into_body();
        values.push(ObjectRead { retention: read_retention(&body, accounting), bytes: body });
    }
    Ok(())
}

impl<P: ObjectsProvider> LogicalObjectStore<P> {

    pub(super) async fn read_accounted(
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
        let result = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let work = *work;
                let packet = native_request(request.0, request_custody, work)?;
                let future = self.provider.get_native(packet, request.1, work.runtime_meter())
                    .map_err(native_objects_error)?;
                accounting.charge(WorkCounters { object_probes: 1, ..WorkCounters::UNCHARGED })?;
                future.await.map_err(native_objects_error)
            }
            _ => {
                let request_bytes = request.0.object_key.capacity() as u64
                    + request.0.bucket.as_ref().map_or(0, |bucket| bucket.name.capacity() as u64);
                accounting.charge(WorkCounters {
                    object_probes: 1, backend_read_operations: 1, ..WorkCounters::UNCHARGED
                })?;
                let result = self.provider.get(request.0, request.1.min(self.maximum_object_bytes)).await.map_err(service_read_error);
                release_read_custody(accounting, request_custody, request_bytes)?;
                result
            }
        };
        let observed = match &result {
            Ok(value) => value.body().len() as u64,
            #[cfg(not(target_arch = "wasm32"))]
            Err(ObjectStoreError::NativeObjects(error)) => native_completed_read_bytes(error)?,
            Err(_) => 0,
        };
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


    pub(super) async fn read_many_accounted(
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
        let results = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let work = *work;
                let packet = native_request(provider_requests, request_custody, work)?;
                let future = self.provider.get_batch_native(packet, work.runtime_meter())
                    .map_err(native_objects_error)?;
                accounting.charge(WorkCounters { object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
                LogicalReadResults::Native(future.await.map_err(native_objects_error)?)
            }
            _ => {
                accounting.charge(WorkCounters { backend_read_operations: 1,
                    object_probes: requests.len() as u64, ..WorkCounters::UNCHARGED })?;
                let results = self.provider.get_batch(provider_requests).await;
                release_read_custody(accounting, request_custody, request_bytes)?;
                LogicalReadResults::Ordinary(results)
            }
        };
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
        match results {
            LogicalReadResults::Ordinary(results) => finish_reads(
                results.into_iter().map(|result| result.map_err(service_read_error)), requests, accounting, &mut values,
            )?,
            #[cfg(not(target_arch = "wasm32"))]
            LogicalReadResults::Native(results) => finish_reads(
                results.map(|result| result.map_err(native_objects_error)), requests, accounting, &mut values,
            )?,
        }
        if !native {
            release_read_custody(accounting, result_custody, result_bytes)?;
            release_read_custody(accounting, header_custody, header_bytes)?;
        }
        Ok((values, output_custody))
    }}
