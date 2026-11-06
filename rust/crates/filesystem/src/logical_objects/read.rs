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
        let remaining = work.remaining()?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let maximum = maximum.min(remaining.object_bytes_read);
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
                        packet(
                            crate::kernel::Owned::new(
                                (blob(id), ByteRange::FULL, maximum),
                                custody,
                                0,
                            ),
                            original,
                        )?,
                        original.runtime_meter(),
                    )
                    .map_err(|error| native_error(error, original))?;
                work.charge(WorkCounters {
                    object_probes: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                future.await.map_err(|error| native_error(error, original))
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
        let remaining = work.remaining()?;
        let total = incoming
            .value
            .iter()
            .try_fold(0u64, |total, request| total.checked_add(request.2));
        #[cfg(not(target_arch = "wasm32"))]
        let native = work.native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        let quota = total.and_then(|total| {
            let mut quota = remaining.object_bytes_read;
            if !native {
                quota = quota.min(remaining.peak_allocation_bytes);
            }
            (total <= quota).then_some(total)
        });
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
        let body = if native {
            KernelOwnedAllocation::Standalone
        } else {
            work.claim_owned(quota, 0)?
        };
        let mut left = quota;
        for request in &mut incoming.value {
            request.2 = request.2.min(left);
            left -= request.2;
        }
        match work {
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work: original, .. } => {
                let original = *original;
                let future = self
                    .provider
                    .get_batch_native(packet(incoming, original)?, original.runtime_meter())
                    .map_err(|error| native_error(error, original))?;
                work.charge(WorkCounters {
                    object_probes: requests.len() as u64,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = future
                    .await
                    .map_err(|error| native_error(error, original))?;
                values = self.finish_reads::<STORED>(
                    results.map(|value| value.map_err(|error| native_error(error, original))),
                    requests,
                    quota,
                    work,
                    body,
                    values,
                )?;
            }
            _ => {
                work.charge(WorkCounters {
                    object_probes: requests.len() as u64,
                    backend_read_operations: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                let results = self.provider.get_batch(incoming.value).await;
                release(work, incoming.allocation, incoming.bytes)?;
                let records = (results.capacity() * size_of::<Result<Object, Error>>()) as u64;
                let (controls, late) = work.record_actual_owned(records)?;
                let result = self.finish_reads::<STORED>(
                    results
                        .into_iter()
                        .map(|value| value.map_err(service_error)),
                    requests,
                    quota,
                    work,
                    body,
                    values,
                );
                release(work, controls, records)?;
                values = result?;
                late?;
            }
        }
        Ok(values)
    }
    fn finish_reads<const STORED: bool>(
        &self,
        results: impl ExactSizeIterator<Item = Result<Object, ObjectStoreError>>,
        requests: &[ObjectReadRequest],
        mut left: u64,
        work: &mut KernelAccounting<'_>,
        mut body: KernelOwnedAllocation,
        mut values: crate::kernel::Owned<Vec<ObjectRead>>,
    ) -> Result<crate::kernel::Owned<Vec<ObjectRead>>, crate::FsError> {
        let count_valid = results.len() == requests.len();
        let quota = left;
        let mut observed = 0u64;
        let mut failure = None;
        for (index, result) in results.enumerate() {
            let maximum = match requests.get(index) {
                Some(request) => self
                    .sealer
                    .stored_limit(
                        request.object_id,
                        request
                            .maximum_bytes
                            .min(self.maximum_object_bytes.unwrap_or(request.maximum_bytes)),
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
            *value = self.read_representation::<STORED>(
                request.object_id,
                read,
                request
                    .maximum_bytes
                    .min(self.maximum_object_bytes.unwrap_or(request.maximum_bytes)),
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
