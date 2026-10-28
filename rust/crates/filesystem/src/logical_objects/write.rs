use super::*;
impl<P: Blobs + ?Sized> LogicalObjectStore<P> {
    pub(super) fn validate_write(
        &self,
        id: ObjectId,
        bytes: &OwnedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        self.limit(bytes.len() as u64)?;
        let delta = WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        work.admit(delta)?;
        let digest = object_digest(bytes);
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
        let custody = guard(work)?;
        work.admit(WorkCounters {
            object_bytes_written: length,
            ..WorkCounters::UNCHARGED
        })?;
        let put = match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let receipt = self
                    .provider
                    .put_native(
                        packet((bytes, Vec::new()), custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?
                    .await
                    .map_err(native_error)?;
                receipt.put().clone()
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let put = self.provider.put(bytes, Vec::new()).await;
                release(work, custody, 0)?;
                put.map_err(ObjectStoreError::LogicalObjects)?
            }
        };
        settle(id, length, put)?;
        record_write(work, length)?;
        work.check()
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
        let (mut requests, custody) = vector::<PutRequest>(writes.len(), work)?;
        let request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64;
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
            // Equal identities carry equal bytes; the store dedupes them.
            requests.push((body.clone(), Vec::new()));
            total = total
                .checked_add(body.len() as u64)
                .ok_or(WorkError::Overflow)?;
        }
        work.admit(WorkCounters {
            object_bytes_written: total,
            ..WorkCounters::UNCHARGED
        })?;
        let result = match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let results = self
                    .provider
                    .put_batch_native(
                        packet(requests, custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?
                    .await
                    .map_err(native_error)?;
                finish_writes(
                    results.map(|value| {
                        value
                            .map(|receipt| receipt.put().clone())
                            .map_err(native_error)
                    }),
                    writes,
                    parts,
                    work,
                )
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.provider.put_batch(requests).await;
                release(work, custody, request_bytes)?;
                let bytes = (results.capacity() * size_of::<Result<Put, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(bytes)?;
                let result = finish_writes(
                    results
                        .into_iter()
                        .map(|value| value.map_err(ObjectStoreError::LogicalObjects)),
                    writes,
                    parts,
                    work,
                );
                release(work, controls, bytes)?;
                result.and(late)
            }
        };
        result?;
        work.check()
    }
}

/// Accepts only a verified outcome for exactly these bytes.
fn settle(id: ObjectId, length: u64, put: Put) -> Result<(), ObjectStoreError> {
    match put.verified() {
        Some(verified) if verified.digest == blob(id) && verified.length == length => Ok(()),
        Some(_) => Err(ObjectStoreError::Corrupt),
        None => Err(ObjectStoreError::LogicalObjects(
            wire::ErrorCode::OutcomeUnknown.into(),
        )),
    }
}

fn finish_writes<T, V>(
    results: impl ExactSizeIterator<Item = Result<Put, ObjectStoreError>>,
    writes: &[T],
    parts: V,
    work: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError>
where
    V: Copy + for<'a> Fn(&'a T) -> (ObjectId, &'a OwnedBytes),
{
    if results.len() != writes.len() {
        return Err(ObjectStoreError::Corrupt.into());
    }
    let mut failure = None;
    for (write, result) in writes.iter().zip(results) {
        let (id, body) = parts(write);
        let length = body.len() as u64;
        match result.and_then(|put| settle(id, length, put)) {
            Ok(()) => record_write(work, length)?,
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

pub(super) fn raw_parts(write: &ObjectWrite) -> (ObjectId, &OwnedBytes) {
    (write.object_id, &write.bytes)
}

pub(super) fn hashed_parts(object: &crate::storage::HashedObject) -> (ObjectId, &OwnedBytes) {
    (object.object_id(), object.bytes())
}
