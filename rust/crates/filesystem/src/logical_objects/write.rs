use super::*;
use crate::storage::ObjectWriteSource;
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
        let references = children(id, plaintext, work)?;
        work.admit(WorkCounters {
            object_bytes_written: length,
            ..WorkCounters::UNCHARGED
        })?;
        let put = invocation::put(
            &*self.provider,
            crate::kernel::Owned::new(
                (body, references.value),
                references.allocation,
                references.bytes,
            ),
            work,
        )
        .await?;
        let written = settle(id, length, put)?;
        record_write(work, written)?;
        work.check()
    }
    pub(super) async fn publish_batch<T: ObjectWriteSource>(
        &self,
        writes: &[T],
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        work.check()?;
        if writes.is_empty() {
            return Err(ObjectStoreError::EmptyWriteBatch.into());
        }
        let mut requests = crate::kernel::Owned::<Vec<PutRequest>>::allocate(writes.len(), work)?;
        let mut total = 0u64;
        for write in writes {
            let (id, bytes, plaintext) = write.parts();
            let body = self.admit_write(id, bytes, plaintext, write.sealed(), work)?;
            // Equal identities carry equal bytes; the store dedupes them.
            let references = children(id, plaintext, work)?.transfer_to(
                &mut requests.allocation,
                &mut requests.bytes,
                work,
            )?;
            requests.value.push((body, references));
            total = total
                .checked_add(bytes.len() as u64)
                .ok_or(WorkError::Overflow)?;
        }
        work.admit(WorkCounters {
            object_bytes_written: total,
            ..WorkCounters::UNCHARGED
        })?;
        invocation::put_batch(&*self.provider, requests, work, WriteBatch(writes)).await?;
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
        let request = retention(self.epoch.current(), scope.objects, work)?;
        invocation::retain(&*self.provider, request, work).await?;
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

struct WriteBatch<'a, T>(&'a [T]);

impl<T: ObjectWriteSource> invocation::BatchFinish<Put> for WriteBatch<'_, T> {
    type Output = ();

    fn finish(
        self,
        results: impl ExactSizeIterator<Item = Result<Put, ObjectStoreError>>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        let writes = self.0;
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
