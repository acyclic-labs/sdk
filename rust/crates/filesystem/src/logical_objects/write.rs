use super::*;
use acyclic_stream::SealedBytes;
impl<P: Blobs + ?Sized> LogicalObjectStore<P> {
    /// Hashes caller bytes once; the seal carries that digest to the store.
    pub(super) fn validate_write(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<SealedBytes, crate::FsError> {
        work.check()?;
        self.limit(bytes.len() as u64)?;
        let delta = WorkCounters {
            bytes_hashed: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        };
        work.admit(delta)?;
        let body = SealedBytes::new(bytes);
        let completed = work.record_completed(delta);
        if crate::Digest::from_bytes(*body.digest()) != id.digest {
            return Err(ObjectStoreError::DigestMismatch.into());
        }
        completed.map(|()| body)
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
        body: SealedBytes,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        let length = body.bytes().len() as u64;
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
                        packet((body, Vec::new()), custody, original)?,
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
                let put = self.provider.put_sealed(body, Vec::new()).await;
                release(work, custody, 0)?;
                put.map_err(ObjectStoreError::LogicalObjects)?
            }
        };
        settle(id, length, put)?;
        self.published([id]);
        record_write(work, length)?;
        work.check()
    }
    pub(super) async fn publish_batch<T: Write>(
        &self,
        writes: &[T],
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let (mut requests, custody) = vector::<PutRequest>(writes.len(), work)?;
        let request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64;
        let mut total = 0u64;
        for write in writes {
            let (id, bytes) = write.parts();
            let body = if let Some(body) = write.sealed() {
                // HashedObject already authenticated these immutable bytes.
                work.check()?;
                self.limit(bytes.len() as u64)?;
                body.clone()
            } else {
                self.validate_write(id, bytes.clone(), work)?
            };
            // Equal identities carry equal bytes; the store dedupes them.
            requests.push((body, Vec::new()));
            total = total
                .checked_add(bytes.len() as u64)
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
                    work,
                )
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.provider.put_batch_sealed(requests).await;
                release(work, custody, request_bytes)?;
                let bytes = (results.capacity() * size_of::<Result<Put, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(bytes)?;
                let result = finish_writes(
                    results
                        .into_iter()
                        .map(|value| value.map_err(ObjectStoreError::LogicalObjects)),
                    writes,
                    work,
                );
                release(work, controls, bytes)?;
                result.and(late)
            }
        };
        result?;
        self.published(writes.iter().map(|write| write.parts().0));
        work.check()
    }
}

impl<P: Blobs + ?Sized> LogicalObjectStore<P> {
    /// Retains `scope` under `root` within the caller's work, before the
    /// publication's record is written.
    ///
    /// An enumerated closure is retained as listed; the root's earlier
    /// batches already cover objects a reused proof omits. A closure that
    /// cannot be enumerated retains every blob staged since this root's last
    /// such retention, which covers everything this store admitted. Blobs
    /// stay staged for the root until its retention succeeds, so an
    /// overlapping publication retains them too rather than trusting one
    /// that may still fail.
    pub(super) async fn retain_scope(
        &self,
        root: acyclic_objects::RootId,
        scope: crate::PublicationScope<'_>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        let owed = |staged: &super::Staged| {
            let from = staged.roots.get(&root).copied().unwrap_or(staged.base);
            from.max(staged.base)
        };
        let (end, count) = match scope {
            crate::PublicationScope::Closure { objects, .. } => (None, objects.len()),
            crate::PublicationScope::Everything => {
                let staged = self.staged();
                let end = staged.end();
                (Some(end), end.saturating_sub(owed(&staged)) as usize)
            }
        };
        if count == 0 {
            return Ok(());
        }
        let (mut references, custody) = vector::<acyclic_objects::Digest>(count, work)?;
        let bytes = (references.capacity() * size_of::<acyclic_objects::Digest>()) as u64;
        match (scope, end) {
            (crate::PublicationScope::Everything, Some(end)) => {
                // This root's own retention may have advanced it, and the
                // log's base with it, past `end` meanwhile; what it passed
                // is retained.
                let staged = self.staged();
                let from = owed(&staged);
                if from < end {
                    let range = (from - staged.base) as usize..(end - staged.base) as usize;
                    references.extend(staged.digests.range(range).copied());
                }
            }
            (crate::PublicationScope::Closure { objects, .. }, _) => {
                references.extend(objects.iter().copied().map(blob));
            }
            _ => return Err(ObjectStoreError::Corrupt.into()),
        }
        if references.is_empty() {
            return release(work, custody, bytes);
        }
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                self.provider
                    .retain_native(
                        packet((root.clone(), references), custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?
                    .await
                    .map_err(native_error)?;
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let retained = self.provider.retain(&root, references).await;
                release(work, custody, bytes)?;
                retained.map_err(ObjectStoreError::LogicalObjects)?;
            }
        }
        if let Some(end) = end {
            self.staged().retained(&root, end);
        }
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

fn finish_writes<T: Write>(
    results: impl ExactSizeIterator<Item = Result<Put, ObjectStoreError>>,
    writes: &[T],
    work: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    if results.len() != writes.len() {
        return Err(ObjectStoreError::Corrupt.into());
    }
    let mut failure = None;
    for (write, result) in writes.iter().zip(results) {
        let (id, body) = write.parts();
        let length = body.len() as u64;
        // Only the put that created the blob wrote its bytes.
        let created = result.and_then(|put| {
            let created = matches!(put, Put::Created { .. });
            settle(id, length, put).map(|()| created)
        });
        match created {
            Ok(true) => record_write(work, length)?,
            Ok(false) => {}
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

/// One object a batch publishes.
pub(super) trait Write {
    fn parts(&self) -> (ObjectId, &OwnedBytes);
    /// The seal a caller already proved, so the batch hashes nothing again.
    fn sealed(&self) -> Option<&SealedBytes>;
}

impl Write for ObjectWrite {
    fn parts(&self) -> (ObjectId, &OwnedBytes) {
        (self.object_id, &self.bytes)
    }
    fn sealed(&self) -> Option<&SealedBytes> {
        None
    }
}

impl Write for crate::storage::HashedObject {
    fn parts(&self) -> (ObjectId, &OwnedBytes) {
        (self.object_id(), self.bytes())
    }
    fn sealed(&self) -> Option<&SealedBytes> {
        Some(crate::storage::HashedObject::sealed(self))
    }
}
