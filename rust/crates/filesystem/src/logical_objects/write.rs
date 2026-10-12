use super::*;

pub(super) fn record_write_completion(accounting: &mut KernelAccounting<'_>, bytes: u64) -> Result<(), crate::FsError> {
    let delta = WorkCounters { object_bytes_written: bytes, ..WorkCounters::UNCHARGED };
    match accounting {
        KernelAccounting::Standalone { work, .. } => { *work = work.checked_add(delta)?; Ok(()) }
        KernelAccounting::BorrowedStandalone { work, .. } => { **work = work.checked_add(delta)?; Ok(()) }
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => work.record_admitted(delta).map_err(Into::into),
    }
}

pub(super) fn release_write_custody(
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

pub(super) fn put_info_bytes(info: &wire::ObjectInfo) -> Result<u64, crate::FsError> {
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

pub(super) enum LogicalPutResults {
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

impl<P: ObjectsProvider> LogicalObjectStore<P> {


    pub(super) async fn put_accounted(
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
                self.provider.put_native(packet, work.runtime_meter()).map_err(native_objects_error)?
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


    pub(super) async fn put_many_accounted(
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
        let results = match accounting {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => {
                let packet = native_request(requests, request_custody, work)?;
                let future = self.provider.put_batch_native(packet, work.runtime_meter())
                    .map_err(native_objects_error)?;
                LogicalPutResults::Native(future.await.map_err(native_objects_error)?)
            }
            _ => {
                accounting.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
                let results = self.provider.put_batch(requests).await;
                release_write_custody(accounting, request_custody, request_bytes)?;
                LogicalPutResults::Ordinary(results)
            }
        };
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
                results.map(|result| result.map(|receipt| receipt.info().size).map_err(native_objects_error)),
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


    pub(super) async fn finish_put_many_results(
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
    }}
