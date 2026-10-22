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
        let result: Result<u64, ObjectStoreError> = match work {
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
    pub(super) async fn publish_batch<const HASHED: bool, T, V>(
        &self,
        writes: &[T],
        parts: V,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>
    where
        V: Copy + for<'a> Fn(&'a T) -> (ObjectId, &'a OwnedBytes),
    {
        work.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let (mut requests, mut custody) = vector::<PutRequest>(writes.len(), work)?;
        let (mut unique, unique_custody) = vector::<Option<&T>>(writes.len(), work)?;
        // Different object kinds cannot have equal ObjectIds. Remember kind
        // membership in registers; only repeated kinds need an identity table.
        // The existing unique list supplies the seed if promotion is necessary.
        let mut kinds = [0u64; 4];
        let mut index = None;
        #[cfg(all(test, feature = "memory"))]
        if super::tests::eager_index() {
            index = Some(super::index::Index::new(writes.len(), work)?);
        }
        let mut request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64;
        let mut total = 0u64;
        for write in writes {
            let (id, body) = parts(write);
            if HASHED {
                // HashedObject already authenticated these immutable bytes.
                work.check()?;
                self.limit(body.len() as u64)?;
            } else {
                self.validate_write(id, body, work)?;
            }
            #[cfg(all(test, feature = "memory"))]
            super::tests::replay_duplicate_scan(&unique, id, parts, work)?;
            let tag = usize::from(id.kind.canonical_tag());
            let bit = 1u64 << (tag % 64);
            if index.is_none() && kinds[tag / 64] & bit != 0 {
                let mut promoted = super::index::Index::new(writes.len(), work)?;
                for previous in unique.iter().flatten() {
                    if promoted
                        .insert(parts(previous).0, *previous, work)?
                        .is_some()
                    {
                        return Err(ObjectStoreError::Corrupt.into());
                    }
                }
                index = Some(promoted);
            }
            if let Some(index) = &mut index
                && let Some(previous) = index.insert(id, write, work)?
            {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if parts(previous).1 != body {
                    return Err(ObjectStoreError::DigestMismatch.into());
                }
                continue;
            }
            kinds[tag / 64] |= bit;
            let (request, mut allocation) = put_request(&self.bucket, id, body.clone(), work)?;
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
            unique.push(Some(write));
            total = total
                .checked_add(body.len() as u64)
                .ok_or(WorkError::Overflow)?;
        }
        if let Some(index) = index {
            index.release(work)?;
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
                    parts,
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
                        parts,
                        work,
                    )
                    .await;
                release(work, controls, bytes)?;
                result.and(late)
            }
        };
        let bytes = (unique.capacity() * size_of::<Option<&T>>()) as u64;
        drop(unique);
        release(work, unique_custody, bytes)?;
        result?;
        work.check()
    }
    async fn finish_writes<T, V>(
        &self,
        results: impl ExactSizeIterator<Item = Result<u64, ObjectStoreError>>,
        unique: &mut [Option<&T>],
        parts: V,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError>
    where
        V: Copy + for<'a> Fn(&'a T) -> (ObjectId, &'a OwnedBytes),
    {
        let count_valid = results.len() == unique.len();
        let mut failure = None;
        for (write, result) in unique.iter_mut().zip(results) {
            let length = parts(write.ok_or(ObjectStoreError::Corrupt)?).1.len() as u64;
            match result {
                Ok(size) => {
                    record_write(work, length)?;
                    if size != length && failure.is_none() {
                        failure = Some(ObjectStoreError::Corrupt);
                    }
                    *write = None;
                }
                Err(error) if precondition(&error) => {}
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                    *write = None;
                }
            }
        }
        if !count_valid {
            return Err(ObjectStoreError::Corrupt.into());
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        for write in unique.iter().flatten() {
            let (id, body) = parts(write);
            self.verify_existing(id, body.len() as u64, work).await?;
        }
        Ok(())
    }
}

pub(super) fn raw_parts(write: &ObjectWrite) -> (ObjectId, &OwnedBytes) {
    (write.object_id, &write.bytes)
}

pub(super) fn hashed_parts(object: &crate::storage::HashedObject) -> (ObjectId, &OwnedBytes) {
    (object.object_id(), object.bytes())
}
