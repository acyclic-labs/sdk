use super::*;
fn retained(body: &OwnedBytes, work: &KernelAccounting<'_>) -> ObjectReadRetention {
    let _ = work;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = work.native_work() {
        if body.matches_meter(&original.runtime_meter())
            && let Some(capacity) = body.retained_capacity()
        {
            return ObjectReadRetention::Owned {
                logical_bytes: capacity as u64,
            };
        }
        return ObjectReadRetention::Shared;
    }
    ObjectReadRetention::Owned {
        logical_bytes: body.len() as u64,
    }
}
fn completed_bytes(error: &ObjectStoreError) -> Result<u64, WorkError> {
    let _ = error;
    #[cfg(not(target_arch = "wasm32"))]
    if let ObjectStoreError::NativeObjects(error) = error {
        return native_payload_bytes(error);
    }
    Ok(0)
}
#[cfg(not(target_arch = "wasm32"))]
fn native_payload_bytes(error: &acyclic_objects::NativeObjectsError) -> Result<u64, WorkError> {
    Ok(error
        .object
        .as_ref()
        .map_or(0, |object| object.body().len() as u64))
}
impl<P: Blobs + ?Sized, S: ObjectSealer> LogicalObjectStore<P, S> {
    fn read_representation<const STORED: bool>(
        &self,
        id: ObjectId,
        read: ObjectRead,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<ObjectRead, crate::FsError> {
        if STORED {
            // The destination opens the seal once. Here the reference's CAS
            // digest authenticates the exact transfer representation.
            let physical = ObjectId {
                digest: id.storage.map_or(id.digest, |value| value.locator),
                storage: None,
                ..id
            };
            hash(physical, &read.bytes, read.bytes.len() as u64, work)?;
            Ok(read)
        } else {
            self.open_stored(id, read, maximum, work)
        }
    }
    pub(super) async fn fetch<const STORED: bool>(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(ObjectRead, KernelOwnedAllocation), crate::FsError> {
        let plaintext_maximum = maximum.min(self.maximum_object_bytes.unwrap_or(maximum));
        let maximum = self.sealer.stored_limit(id, plaintext_maximum)?;
        let maximum = maximum.min(invocation::read_capacity(work)?);
        let mut body_custody = invocation::read_custody(maximum, work)?;
        let custody = guard(work)?;
        let result = invocation::Provider::get(
            &*self.provider,
            crate::kernel::Owned::new((blob(id), ByteRange::FULL, maximum), custody, 0),
            work,
        )
        .await?;
        let observed = match &result {
            Ok(value) => value.body().len() as u64,
            Err(error) => completed_bytes(error)?,
        };
        let completed = work.record_completed(WorkCounters {
            object_bytes_read: observed,
            ..WorkCounters::UNCHARGED
        });
        let value = result?;
        if observed > maximum {
            completed?;
            return Err(ObjectStoreError::TooLarge { observed, maximum }.into());
        }
        if work.standalone_parts().is_some() {
            work.release_owned(&mut body_custody, maximum - observed)?;
        }
        completed?;
        let bytes = value.into_body();
        let read = ObjectRead {
            retention: retained(&bytes, work),
            bytes,
        };
        Ok((
            self.read_representation::<STORED>(id, read, plaintext_maximum, work)?,
            body_custody,
        ))
    }
    pub(super) async fn fetch_batch<const STORED: bool>(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut KernelAccounting<'_>,
    ) -> Result<crate::kernel::Owned<Vec<ObjectRead>>, crate::FsError> {
        work.check()?;
        if requests.is_empty() {
            return Err(ObjectStoreError::EmptyReadBatch.into());
        }
        let mut incoming = crate::kernel::Owned::<Vec<GetRequest>>::allocate(requests.len(), work)?;
        for request in requests {
            incoming.value.push((
                blob(request.object_id),
                ByteRange::FULL,
                self.sealer.stored_limit(
                    request.object_id,
                    request
                        .maximum_bytes
                        .min(self.maximum_object_bytes.unwrap_or(request.maximum_bytes)),
                )?,
            ));
        }
        let mut values = crate::kernel::Owned::<Vec<ObjectRead>>::allocate(requests.len(), work)?;
        let remaining = invocation::read_capacity(work)?;
        let total = incoming
            .value
            .iter()
            .try_fold(0u64, |total, request| total.checked_add(request.2));
        let quota = total.filter(|total| *total <= remaining);
        let Some(quota) = quota else {
            incoming.retire(work)?;
            for request in requests {
                let read = {
                    let (read, _allocation) = self
                        .fetch::<STORED>(request.object_id, request.maximum_bytes, work)
                        .await?;
                    read
                };
                values.bytes = values
                    .bytes
                    .checked_add(read.retention.owned_bytes())
                    .ok_or(WorkError::Overflow)?;
                values.value.push(read);
            }
            return Ok(values);
        };
        let body = invocation::read_custody(quota, work)?;
        let mut left = quota;
        for request in &mut incoming.value {
            request.2 = request.2.min(left);
            left -= request.2;
        }
        invocation::Provider::get_batch(
            &*self.provider,
            incoming,
            work,
            ReadBatch::<_, _, STORED> {
                store: self,
                requests,
                quota,
                body,
                values,
            },
        )
        .await
    }
}

struct ReadBatch<'a, P: ?Sized, S, const STORED: bool> {
    store: &'a LogicalObjectStore<P, S>,
    requests: &'a [ObjectReadRequest],
    quota: u64,
    values: crate::kernel::Owned<Vec<ObjectRead>>,
    body: KernelOwnedAllocation,
}

impl<P: Blobs + ?Sized, S: ObjectSealer, const STORED: bool> invocation::BatchFinish<Object>
    for ReadBatch<'_, P, S, STORED>
{
    type Output = crate::kernel::Owned<Vec<ObjectRead>>;

    fn finish(
        self,
        results: impl ExactSizeIterator<Item = Result<Object, ObjectStoreError>>,
        work: &mut KernelAccounting<'_>,
    ) -> Result<Self::Output, crate::FsError> {
        let Self {
            store,
            requests,
            quota,
            mut body,
            mut values,
        } = self;
        let mut left = quota;
        let count_valid = results.len() == requests.len();
        let mut observed = 0u64;
        let mut failure = None;
        for (index, result) in results.enumerate() {
            let maximum = match requests.get(index) {
                Some(request) => store
                    .sealer
                    .stored_limit(
                        request.object_id,
                        request
                            .maximum_bytes
                            .min(store.maximum_object_bytes.unwrap_or(request.maximum_bytes)),
                    )?
                    .min(left),
                None => 0,
            };
            left -= maximum;
            match result {
                Ok(value) => {
                    let length = value.body().len() as u64;
                    observed = observed.checked_add(length).ok_or(WorkError::Overflow)?;
                    if length > maximum && failure.is_none() {
                        failure = Some(ObjectStoreError::TooLarge {
                            observed: length,
                            maximum,
                        });
                    }
                    let bytes = value.into_body();
                    if index < requests.len() {
                        values.value.push(ObjectRead {
                            retention: retained(&bytes, work),
                            bytes,
                        });
                    }
                }
                Err(error) => {
                    observed = observed
                        .checked_add(completed_bytes(&error)?)
                        .ok_or(WorkError::Overflow)?;
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        let completed = work.record_completed(WorkCounters {
            object_bytes_read: observed,
            ..WorkCounters::UNCHARGED
        });
        if !count_valid {
            return Err(ObjectStoreError::Corrupt.into());
        }
        if let Some(error) = failure {
            return Err(error.into());
        }
        completed?;
        if work.standalone_parts().is_some() {
            work.release_owned(
                &mut body,
                quota.checked_sub(observed).ok_or(WorkError::Overflow)?,
            )?;
        }
        let mut retained = values.bytes;
        for (value, request) in values.value.iter_mut().zip(requests) {
            let read = std::mem::replace(
                value,
                ObjectRead {
                    bytes: OwnedBytes::from_static(b""),
                    retention: ObjectReadRetention::Shared,
                },
            );
            *value = store.read_representation::<STORED>(
                request.object_id,
                read,
                request
                    .maximum_bytes
                    .min(store.maximum_object_bytes.unwrap_or(request.maximum_bytes)),
                work,
            )?;
            retained = retained
                .checked_add(value.retention.owned_bytes())
                .ok_or(WorkError::Overflow)?;
        }
        values.bytes = retained;
        Ok(values)
    }
}
pub(super) fn hash(
    id: ObjectId,
    bytes: &OwnedBytes,
    count: u64,
    work: &mut KernelAccounting<'_>,
) -> Result<(), crate::FsError> {
    let delta = WorkCounters {
        bytes_hashed: count,
        ..WorkCounters::UNCHARGED
    };
    work.admit(delta)?;
    let digest = object_digest(bytes);
    let completed = work.record_completed(delta);
    if digest != id.digest {
        return Err(ObjectStoreError::Corrupt.into());
    }
    completed
}
