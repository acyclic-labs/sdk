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
impl<P: Blobs + ?Sized> LogicalObjectStore<P> {
    pub(super) async fn fetch(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut KernelAccounting<'_>,
    ) -> Result<(ObjectRead, KernelOwnedAllocation), crate::FsError> {
        let remaining = work.remaining()?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let maximum = maximum
            .min(self.maximum_object_bytes)
            .min(remaining.object_bytes_read);
        let maximum = if native {
            maximum
        } else {
            maximum.min(remaining.peak_allocation_bytes)
        };
        let mut body_custody = if native {
            KernelOwnedAllocation::Standalone
        } else {
            work.claim_owned(maximum, 0)?
        };
        let custody = guard(work)?;
        let result = match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let future = self
                    .provider
                    .get_native(
                        packet((blob(id), ByteRange::FULL, maximum), custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?;
                work.charge(WorkCounters {
                    object_probes: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                future.await.map_err(native_error)
            }
            _ => {
                work.charge(WorkCounters {
                    object_probes: 1,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let result = self
                    .provider
                    .get(blob(id), ByteRange::FULL, maximum)
                    .await
                    .map_err(service_error);
                release(work, custody, 0)?;
                result
            }
        };
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
        if !native {
            work.release_owned(&mut body_custody, maximum - observed)?;
        }
        completed?;
        hash(id, value.body(), observed, work)?;
        let body = value.into_body();
        Ok((
            ObjectRead {
                retention: retained(&body, work),
                bytes: body,
            },
            body_custody,
        ))
    }
    pub(super) async fn fetch_batch(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut KernelAccounting<'_>,
    ) -> Result<(Vec<ObjectRead>, KernelOwnedAllocation), crate::FsError> {
        work.check()?;
        if requests.is_empty() {
            return Err(ObjectStoreError::EmptyReadBatch.into());
        }
        let (mut incoming, custody) = vector::<GetRequest>(requests.len(), work)?;
        let request_bytes = (incoming.capacity() * size_of::<GetRequest>()) as u64;
        for request in requests {
            incoming.push((
                blob(request.object_id),
                ByteRange::FULL,
                request.maximum_bytes.min(self.maximum_object_bytes),
            ));
        }
        let (mut values, output) = vector::<ObjectRead>(requests.len(), work)?;
        let remaining = work.remaining()?;
        let total = incoming.iter().try_fold(0u64, |total, request| {
            total.checked_add(request.2).ok_or(WorkError::Overflow)
        })?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let mut quota = total.min(remaining.object_bytes_read);
        if !native {
            quota = quota.min(remaining.peak_allocation_bytes);
        }
        if total > quota {
            drop(incoming);
            release(work, custody, request_bytes)?;
            for request in requests {
                let read = {
                    let (read, _allocation) = self
                        .fetch(request.object_id, request.maximum_bytes, work)
                        .await?;
                    read
                };
                values.push(read);
            }
            return Ok((values, output));
        }
        let body = if native {
            KernelOwnedAllocation::Standalone
        } else {
            work.claim_owned(quota, 0)?
        };
        let mut left = quota;
        for request in &mut incoming {
            request.2 = request.2.min(left);
            left -= request.2;
        }
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let future = self
                    .provider
                    .get_batch_native(
                        packet(incoming, custody, original)?,
                        original.runtime_meter(),
                    )
                    .map_err(native_error)?;
                work.charge(WorkCounters {
                    object_probes: requests.len() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = future.await.map_err(native_error)?;
                values = self.finish_reads(
                    results.map(|value| value.map_err(native_error)),
                    requests,
                    quota,
                    work,
                    values,
                    body,
                )?;
            }
            _ => {
                work.charge(WorkCounters {
                    object_probes: requests.len() as u64,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.provider.get_batch(incoming).await;
                release(work, custody, request_bytes)?;
                let records = (results.capacity() * size_of::<Result<Object, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(records)?;
                let result = self.finish_reads(
                    results
                        .into_iter()
                        .map(|value| value.map_err(service_error)),
                    requests,
                    quota,
                    work,
                    values,
                    body,
                );
                release(work, controls, records)?;
                values = result?;
                late?;
            }
        }
        Ok((values, output))
    }
    fn finish_reads(
        &self,
        results: impl ExactSizeIterator<Item = Result<Object, ObjectStoreError>>,
        requests: &[ObjectReadRequest],
        mut left: u64,
        work: &mut KernelAccounting<'_>,
        mut values: Vec<ObjectRead>,
        mut body: KernelOwnedAllocation,
    ) -> Result<Vec<ObjectRead>, crate::FsError> {
        let count_valid = results.len() == requests.len();
        let quota = left;
        let mut observed = 0u64;
        let mut failure = None;
        for (index, result) in results.enumerate() {
            let maximum = requests.get(index).map_or(0, |request| {
                request
                    .maximum_bytes
                    .min(self.maximum_object_bytes)
                    .min(left)
            });
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
                        values.push(ObjectRead {
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
        for (value, request) in values.iter().zip(requests) {
            hash(
                request.object_id,
                &value.bytes,
                value.bytes.len() as u64,
                work,
            )?;
        }
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
