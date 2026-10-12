use super::*;
impl<P: ObjectsProvider> LogicalObjectStore<P> {
    pub(super) fn validate_write(
        &self,
        id: ObjectId,
        bytes: &OwnedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        self.limit(bytes.len() as u64)?;
        let delta = WorkCounters {
            bytes_hashed: (bytes.len() as u64)
                .checked_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        work.admit(delta)?;
        let digest = object_digest(id.kind, bytes);
        let completed = work.record_completed(delta);
        if digest != id.digest {
            return Err(ObjectStoreError::DigestMismatch.into());
        }
        completed
    }
    fn limit(&self, length: u64) -> Result<(), crate::FsError> {
        if length > self.maximum_object_bytes {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum: self.maximum_object_bytes,
            }
            .into());
        }
        Ok(())
    }
    pub(super) async fn publish(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        let length = bytes.len() as u64;
        self.limit(length)?;
        let (request, custody) = put_request(&self.bucket, id, bytes, work)?;
        work.admit(WorkCounters {
            object_bytes_written: length,
            ..WorkCounters::UNCHARGED
        })?;
        let result = match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => self
                .provider
                .put_native(
                    packet(request, custody, original)?,
                    original.runtime_meter(),
                )
                .map_err(native_error)?
                .await
                .map(|info| info.info().size)
                .map_err(native_error),
            _ => {
                let bytes = request.0.object_key.capacity() as u64
                    + request
                        .0
                        .bucket
                        .as_ref()
                        .map_or(0, |bucket| bucket.name.capacity() as u64)
                    + request
                        .0
                        .metadata
                        .as_ref()
                        .map_or(0, |metadata| metadata.content_type.capacity() as u64);
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let result = self.provider.put(request.0, request.1).await;
                release(work, custody, bytes)?;
                match result {
                    Ok(info) => {
                        record_write(work, length)?;
                        let bytes = info_bytes(&info)?;
                        let (custody, late) = work.record_actual_owned(bytes)?;
                        let size = info.size;
                        drop(info);
                        release(work, custody, bytes)?;
                        if size != length {
                            return Err(ObjectStoreError::Corrupt.into());
                        }
                        late?;
                        return work.check();
                    }
                    Err(error) => Err(ObjectStoreError::LogicalObjects(error)),
                }
            }
        };
        match result {
            Ok(size) => {
                record_write(work, length)?;
                if size != length {
                    return Err(ObjectStoreError::Corrupt.into());
                }
            }
            Err(error) if precondition(&error) => self.verify_existing(id, length, work).await?,
            Err(error) => return Err(error.into()),
        }
        work.check()
    }
    async fn verify_existing(
        &self,
        id: ObjectId,
        length: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let (read, custody) = self.fetch(id, length, work).await?;
        let actual = read.bytes.len() as u64;
        drop(read);
        if work.standalone_parts().is_some() {
            release(work, custody, actual)?;
        }
        if actual != length {
            return Err(ObjectStoreError::Corrupt.into());
        }
        record_write(work, length)
    }
    pub(super) async fn publish_batch(
        &self,
        writes: &[ObjectWrite],
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let (mut requests, mut custody) = vector::<PutRequest>(writes.len(), work)?;
        let (mut unique, unique_custody) = vector::<usize>(writes.len(), work)?;
        let mut request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64;
        let mut total = 0u64;
        for (index, write) in writes.iter().enumerate() {
            self.validate_write(write.object_id, &write.bytes, work)?;
            if let Some(&previous) = unique
                .iter()
                .find(|&&previous| writes[previous].object_id == write.object_id)
            {
                if writes[previous].bytes != write.bytes {
                    return Err(ObjectStoreError::DigestMismatch.into());
                }
                continue;
            }
            let (request, mut allocation) =
                put_request(&self.bucket, write.object_id, write.bytes.clone(), work)?;
            request_bytes = request_bytes
                .checked_add(request.0.object_key.capacity() as u64)
                .and_then(|bytes| {
                    bytes.checked_add(
                        request
                            .0
                            .bucket
                            .as_ref()
                            .map_or(0, |bucket| bucket.name.capacity() as u64),
                    )
                })
                .and_then(|bytes| {
                    bytes.checked_add(
                        request
                            .0
                            .metadata
                            .as_ref()
                            .map_or(0, |metadata| metadata.content_type.capacity() as u64),
                    )
                })
                .ok_or(WorkError::Overflow)?;
            custody.absorb(&mut allocation)?;
            requests.push(request);
            unique.push(index);
            total = total
                .checked_add(write.bytes.len() as u64)
                .ok_or(WorkError::Overflow)?;
        }
        work.admit(WorkCounters {
            object_bytes_written: total,
            ..WorkCounters::UNCHARGED
        })?;
        let result = match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let results = self
                    .provider
                    .put_batch_native(
                        packet(requests, custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?
                    .await
                    .map_err(native_error)?;
                self.finish_writes(
                    results.map(|value| value.map(|info| info.info().size).map_err(native_error)),
                    &mut unique,
                    writes,
                    work,
                )
                .await
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.provider.put_batch(requests).await;
                release(work, custody, request_bytes)?;
                let bytes = results.iter().try_fold(
                    (results.capacity() * size_of::<Result<wire::ObjectInfo, Error>>()) as u64,
                    |total, value| {
                        total
                            .checked_add(
                                value
                                    .as_ref()
                                    .ok()
                                    .map(info_bytes)
                                    .transpose()?
                                    .unwrap_or(0),
                            )
                            .ok_or_else(|| crate::FsError::from(WorkError::Overflow))
                    },
                )?;
                let (controls, late) = work.record_actual_owned(bytes)?;
                let result = self
                    .finish_writes(
                        results.into_iter().map(|value| {
                            value
                                .map(|info| info.size)
                                .map_err(ObjectStoreError::LogicalObjects)
                        }),
                        &mut unique,
                        writes,
                        work,
                    )
                    .await;
                release(work, controls, bytes)?;
                result.and(late)
            }
        };
        let bytes = (unique.capacity() * size_of::<usize>()) as u64;
        drop(unique);
        release(work, unique_custody, bytes)?;
        result?;
        work.check()
    }
    async fn finish_writes(
        &self,
        results: impl ExactSizeIterator<Item = Result<u64, ObjectStoreError>>,
        unique: &mut [usize],
        writes: &[ObjectWrite],
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let count_valid = results.len() == unique.len();
        let mut failure = None;
        for (index, result) in unique.iter_mut().zip(results) {
            let length = writes[*index].bytes.len() as u64;
            match result {
                Ok(size) => {
                    record_write(work, length)?;
                    if size != length && failure.is_none() {
                        failure = Some(ObjectStoreError::Corrupt);
                    }
                    *index = usize::MAX;
                }
                Err(error) if precondition(&error) => {}
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                    *index = usize::MAX;
                }
            }
        }
        if !count_valid {
            return Err(ObjectStoreError::Corrupt.into());
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        for &index in unique.iter().filter(|&&index| index != usize::MAX) {
            self.verify_existing(
                writes[index].object_id,
                writes[index].bytes.len() as u64,
                work,
            )
            .await?;
        }
        Ok(())
    }
}
