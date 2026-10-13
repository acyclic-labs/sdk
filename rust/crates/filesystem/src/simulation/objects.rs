use super::*;

fn altered_read_data(
    read: &ObjectRead,
    fault: Option<SimulationFault>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<ObjectRead>, crate::FsError> {
    if !matches!(
        fault,
        Some(SimulationFault::CorruptObjectRead | SimulationFault::PartialObjectRead,)
    ) {
        return Ok(None);
    }
    if read.bytes.is_empty() {
        return Err(ObjectStoreError::Corrupt.into());
    }
    accounting.check()?;
    let retained = if matches!(fault, Some(SimulationFault::PartialObjectRead)) {
        read.bytes.len() - 1
    } else {
        read.bytes.len()
    };
    let requested = u64::try_from(retained).map_err(|_| crate::WorkError::Overflow)?;
    accounting.admit(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    let allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
    let mut bytes = (Vec::new(), allocation);
    bytes
        .0
        .try_reserve_exact(retained)
        .map_err(ObjectStoreError::MemoryTableAllocationFailed)?;
    let actual = u64::try_from(bytes.0.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    accounting.reconcile_owned(&mut bytes.1, requested, actual)?;
    accounting.admit(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    bytes.0.extend_from_slice(&read.bytes[..retained]);
    accounting.record_completed(WorkCounters {
        bytes_copied: requested,
        ..WorkCounters::UNCHARGED
    })?;
    if matches!(fault, Some(SimulationFault::CorruptObjectRead))
        && let Some(first) = bytes.0.first_mut()
    {
        *first ^= 0x80;
    }
    let logical_bytes =
        u64::try_from(bytes.0.capacity()).map_err(|_| crate::WorkError::Overflow)?;
    let (bytes, allocation) = bytes;
    let bytes = match allocation {
        KernelOwnedAllocation::Standalone => OwnedBytes::from(bytes),
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => {
            let original = accounting.native_work().ok_or(ObjectStoreError::Corrupt)?;
            OwnedBytes::from_vec_with_guard(bytes, allocation, original.runtime_meter()).map_err(
                |failure| match failure.into_error() {
                    acyclic_stream::PreparedOwnedBytesError::Admission(error) => {
                        crate::FsError::from(original.runtime_error(error))
                    }
                    _ => ObjectStoreError::Corrupt.into(),
                },
            )?
        }
    };
    Ok(Some(ObjectRead {
        bytes,
        retention: ObjectReadRetention::Owned { logical_bytes },
    }))
}

fn write_error(reason: &'static str, work: &crate::AuthorityWork<'_>) -> ObjectStoreError {
    if work.is_native() {
        ObjectStoreError::Simulation(reason)
    } else {
        ObjectStoreError::Rejected(reason.to_owned())
    }
}

impl<O: AsyncObjectStore> SimulatedStore<O> {
    fn object_fault(
        &self,
        operation: SimulationOperation,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<SimulationFault>, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        if work.is_native()
            || matches!(
                operation,
                SimulationOperation::ObjectRead | SimulationOperation::ObjectReadMany
            )
        {
            state
                .prepare_trace(&mut work.accounting())
                .map_err(crate::async_storage::object_error)?;
        }
        let fault = state.intercept(operation).map_err(|reason| {
            if matches!(
                operation,
                SimulationOperation::ObjectPut | SimulationOperation::ObjectContains
            ) {
                write_error(reason, work)
            } else {
                ObjectStoreError::Simulation(reason)
            }
        })?;
        let reason = match fault {
            Some(SimulationFault::RejectBefore) => "scheduled rejection",
            Some(SimulationFault::PartitionBefore) => "scheduled partition",
            _ => return Ok(fault),
        };
        Err(
            if operation == SimulationOperation::ObjectPut
                || operation == SimulationOperation::ObjectContains
            {
                write_error(reason, work)
            } else {
                ObjectStoreError::Simulation(reason)
            },
        )
    }

    fn delay_object(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
        if let Some(existing) = state.pending_objects.get(&object_id) {
            return if existing == &bytes {
                Ok(())
            } else {
                Err(ObjectStoreError::Corrupt)
            };
        }
        let next_count = state
            .pending_objects
            .len()
            .checked_add(1)
            .ok_or(crate::WorkError::Overflow)?;
        let next_bytes = state
            .pending_object_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        if next_count > state.options.maximum_pending_objects
            || next_bytes > state.options.maximum_pending_object_bytes
        {
            return Err(write_error(
                "simulation delayed-object bound exhausted",
                work,
            ));
        }
        state.pending_objects.insert(object_id, bytes);
        state.pending_object_bytes = next_bytes;
        Ok(())
    }
}

impl<O: AsyncObjectStore> AsyncObjectStore for SimulatedStore<O> {
    fn collection(&self) -> Option<&Arc<crate::Collection>> {
        self.inner.collection()
    }

    fn collection_sweeps(&self) -> u64 {
        self.inner.collection_sweeps()
    }

    async fn historical_generation_evidence(
        &self,
        authority_id: AuthorityId,
        record: &DurableCommit,
        maximum_bytes: u64,
        maximum_objects: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<Option<crate::kernel::HistoricalGenerationEvidence>, ObjectStoreError> {
        if work.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.inner
            .historical_generation_evidence(
                authority_id,
                record,
                maximum_bytes,
                maximum_objects,
                work,
            )
            .await
    }

    async fn put(
        &self,
        object_id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let fault = self.object_fault(SimulationOperation::ObjectPut, work)?;
        if !matches!(fault, Some(SimulationFault::DelayObjectVisibility)) {
            return self.inner.put(object_id, bytes, work).await;
        }
        let hashed = (bytes.len() as u64)
            .checked_add(crate::OBJECT_DIGEST_ENVELOPE_BYTES)
            .ok_or(crate::WorkError::Overflow)?;
        work.accounting()
            .charge(WorkCounters {
                bytes_hashed: hashed,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        if object_digest(object_id.kind, &bytes) != object_id.digest {
            return Err(ObjectStoreError::DigestMismatch);
        }
        self.delay_object(object_id, bytes, work)
    }

    async fn put_many(
        &self,
        writes: &[crate::storage::ObjectWrite],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        for write in writes {
            self.put(write.object_id, write.bytes.clone(), work).await?;
        }
        Ok(())
    }

    async fn put_hashed(
        &self,
        object: crate::storage::HashedObject,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), ObjectStoreError> {
        let fault = self.object_fault(SimulationOperation::ObjectPut, work)?;
        if matches!(fault, Some(SimulationFault::DelayObjectVisibility)) {
            let (object_id, bytes) = object.into_parts();
            self.delay_object(object_id, bytes, work)
        } else {
            self.inner.put_hashed(object, work).await
        }
    }

    async fn flush_before_publish(
        &self,
        scope: crate::async_storage::PublicationScope<'_>,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::PublicationHold, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        self.inner.flush_before_publish(scope, work).await
    }

    async fn read(
        &self,
        object_id: ObjectId,
        maximum_bytes: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let fault = self.object_fault(SimulationOperation::ObjectRead, work)?;
        let mut value = self.inner.read(object_id, maximum_bytes, work).await?;
        if let Some(altered) = altered_read_data(&value, fault, &mut work.accounting())
            .map_err(crate::async_storage::object_error)?
        {
            value = altered;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = work.original {
            if matches!(value.retention, ObjectReadRetention::Owned { .. }) {
                return crate::async_storage::owned_read(value, original);
            }
        }
        Ok(value)
    }

    async fn read_many(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        let fault = self.object_fault(SimulationOperation::ObjectReadMany, work)?;
        let mut values = self.inner.read_many(requests, work).await?;
        if let Some(first) = values.0.first_mut() {
            if let Some(altered) = altered_read_data(first, fault, &mut work.accounting())
                .map_err(crate::async_storage::object_error)?
            {
                *first = altered;
            }
        }
        Ok(values)
    }

    async fn contains(
        &self,
        object_id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, ObjectStoreError> {
        if work.is_native() {
            return Err(ObjectStoreError::NativeUnavailable);
        }
        self.object_fault(SimulationOperation::ObjectContains, work)?;
        self.inner.contains(object_id, work).await
    }
}

impl<A, O: AsyncObjectStore> Simulation<A, O> {
    /// Publishes delayed immutable objects in canonical identity order.
    pub async fn flush_object_visibility(
        &self,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<u64, ObjectStoreError> {
        let mut published = 0_u64;
        loop {
            work.accounting()
                .check()
                .map_err(crate::async_storage::object_error)?;
            let next = self
                .state
                .lock()
                .map_err(|_| ObjectStoreError::Corrupt)?
                .pending_objects
                .first_key_value()
                .map(|(object_id, bytes)| (*object_id, bytes.clone()));
            let Some((object_id, bytes)) = next else {
                return Ok(published);
            };
            self.objects.put(object_id, bytes, work).await?;
            let mut state = self.state.lock().map_err(|_| ObjectStoreError::Corrupt)?;
            if let Some(removed) = state.pending_objects.remove(&object_id) {
                state.pending_object_bytes = state
                    .pending_object_bytes
                    .checked_sub(removed.len() as u64)
                    .ok_or(ObjectStoreError::Corrupt)?;
            }
            published = published.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
    }
}
