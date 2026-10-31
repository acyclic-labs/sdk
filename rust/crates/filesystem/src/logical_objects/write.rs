use super::*;
use acyclic_stream::SealedBytes;
impl<P: Blobs + ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    /// One admission boundary for raw, authenticated and transferred writes.
    fn admit_write(
        &self,
        id: ObjectId,
        stored: &OwnedBytes,
        plaintext: &OwnedBytes,
        authenticated: Option<&SealedBytes>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<SealedBytes, crate::FsError> {
        work.check()?;
        if authenticated.is_none() && id.storage.is_some() {
            // Raw writes carry plaintext; sealed writes need prepared CAS bytes.
            return Err(ObjectStoreError::DigestMismatch.into());
        }
        self.limit(plaintext.len() as u64)?;
        let maximum = self.sealer.stored_limit(id, plaintext.len() as u64)?;
        if stored.len() as u64 > maximum {
            return Err(ObjectStoreError::TooLarge {
                observed: stored.len() as u64,
                maximum,
            }
            .into());
        }
        if let Some(body) = authenticated {
            return Ok(body.clone());
        }
        let body = seal(stored.clone(), work)?;
        if crate::Digest::from_bytes(*body.digest()) != id.digest {
            return Err(ObjectStoreError::DigestMismatch.into());
        }
        Ok(body)
    }
    fn limit(&self, length: u64) -> Result<(), crate::FsError> {
        if let Some(maximum) = self.maximum_object_bytes
            && length > maximum
        {
            return Err(ObjectStoreError::TooLarge {
                observed: length,
                maximum,
            }
            .into());
        }
        Ok(())
    }
    pub(super) async fn publish(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        plaintext: &OwnedBytes,
        authenticated: Option<&SealedBytes>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let body = self.admit_write(id, &bytes, plaintext, authenticated, work)?;
        let length = bytes.len() as u64;
        let (references, custody) = children(id, plaintext, work)?;
        let reference_bytes = (references.capacity() * size_of::<acyclic_objects::Digest>()) as u64;
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
                        packet((body, references), custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
                receipt.put().clone()
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let put = self.provider.put_sealed(body, references).await;
                release(work, custody, reference_bytes)?;
                put.map_err(ObjectStoreError::LogicalObjects)?
            }
        };
        let written = settle(id, length, put)?;
        record_write(work, written)?;
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
        let (mut requests, mut custody) = vector::<PutRequest>(writes.len(), work)?;
        let mut request_bytes = (requests.capacity() * size_of::<PutRequest>()) as u64;
        let mut total = 0u64;
        for write in writes {
            let (id, bytes, plaintext) = write.parts();
            let body = self.admit_write(id, bytes, plaintext, write.sealed(), work)?;
            // Equal identities carry equal bytes; the store dedupes them.
            let (references, mut reference_custody) = children(id, plaintext, work)?;
            request_bytes = request_bytes
                .checked_add((references.capacity() * size_of::<acyclic_objects::Digest>()) as u64)
                .ok_or(WorkError::Overflow)?;
            custody.absorb(&mut reference_custody)?;
            requests.push((body, references));
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
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
                finish_writes(
                    results.map(|value| {
                        value
                            .map(|receipt| receipt.put().clone())
                            .map_err(|error| native_error(error, original))
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
        work.check()
    }
}

impl<P: Blobs + ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    /// Retains `scope` under the current epoch's root within the caller's
    /// work, before the publication's record is written.
    ///
    /// The closure is retained as listed; a reused proof's parent is live,
    /// so the epoch already covers the objects it omits.
    pub(super) async fn retain_scope(
        &self,
        scope: crate::PublicationScope<'_>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        if scope.objects.is_empty() {
            return Ok(());
        }
        // Retaining makes the closure durable in one round.
        work.charge(WorkCounters {
            durability_operations: 1,
            ..WorkCounters::UNCHARGED
        })?;
        let (root, mut references) = retention(self.epoch.current(), scope.objects, work)?;
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let request = acyclic_objects::ObjectsNativeRequest::new(
                    (root.value, references.value),
                    (root.allocation, references.allocation),
                    &original.runtime_meter(),
                )
                .map_err(|error| native_error(error, original))?;
                self.provider
                    .retain_native(request, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?
                    .await
                    .map_err(|error| native_error(error, original))?;
            }
            _ => {
                work.charge(WorkCounters {
                    backend_write_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let retained = self
                    .provider
                    .retain(&root.value, std::mem::take(&mut references.value))
                    .await;
                drop(root);
                drop(references);
                retained.map_err(ObjectStoreError::LogicalObjects)?;
            }
        }
        #[cfg(all(test, not(target_arch = "wasm32")))]
        self.epoch.hooks.retained().await;
        work.check()
    }
}

/// Accepts only a verified outcome for exactly these bytes.
fn settle(id: ObjectId, length: u64, put: Put) -> Result<u64, ObjectStoreError> {
    match put.verified() {
        Some(verified) if verified.digest == blob(id) && verified.length == length => {
            Ok(if matches!(put, Put::Created { .. }) {
                length
            } else {
                0
            })
        }
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
        let (id, body, _) = write.parts();
        let length = body.len() as u64;
        match result.and_then(|put| settle(id, length, put)) {
            Ok(written) => record_write(work, written)?,
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

/// A single canonical reference with its authenticated immutable bytes.
pub(super) trait Write {
    fn parts(&self) -> (ObjectId, &OwnedBytes, &OwnedBytes);
    fn sealed(&self) -> Option<&SealedBytes>;
}
impl Write for ObjectWrite {
    fn parts(&self) -> (ObjectId, &OwnedBytes, &OwnedBytes) {
        (self.object_id, &self.bytes, &self.bytes)
    }
    fn sealed(&self) -> Option<&SealedBytes> {
        None
    }
}
impl Write for crate::HashedObject {
    fn parts(&self) -> (ObjectId, &OwnedBytes, &OwnedBytes) {
        (self.object_id(), self.stored_bytes(), self.bytes())
    }
    fn sealed(&self) -> Option<&SealedBytes> {
        Some(self.sealed())
    }
}

pub(super) fn seal(
    bytes: OwnedBytes,
    work: &mut KernelAccounting<'_>,
) -> Result<SealedBytes, crate::FsError> {
    let delta = WorkCounters {
        bytes_hashed: bytes.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta)?;
    let sealed = SealedBytes::new(bytes);
    work.record_completed(delta)?;
    Ok(sealed)
}
