use super::*;

impl<S: AsyncObjectStore> CachedObjectStore<S> {
    pub(super) async fn load_leader(
        &self,
        object_id: ObjectId,
        maximum: u64,
        flight: Arc<Flight>,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let guard = ActiveFlightGuard {
            store: self,
            object_id,
            flight,
        };
        let read = match self.inner.read(object_id, maximum, work).await {
            Ok(read) => read,
            Err(error) => {
                guard.finish(None)?;
                return Err(error);
            }
        };
        let bytes = read.bytes.clone();
        if self.insert(object_id, bytes.clone())? {
            guard.finish(Some(bytes.clone()))?;
            return shared_read(bytes, maximum);
        }
        guard.finish(None)?;
        Ok(read)
    }

    pub(super) fn prepare_batch(
        &self,
        requests: &[ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<PreparedBatch, ObjectStoreError> {
        work.accounting()
            .check()
            .map_err(crate::async_storage::object_error)?;
        if requests.is_empty() {
            return Err(ObjectStoreError::Rejected(
                "object read batch is empty".to_owned(),
            ));
        }
        let count = requests.len() as u64;
        work.accounting()
            .admit(WorkCounters {
                items_returned: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        work.accounting()
            .charge(WorkCounters {
                object_probes: count,
                items_examined: count,
                ..WorkCounters::UNCHARGED
            })
            .map_err(crate::async_storage::object_error)?;
        let slots = Owned::allocate(requests.len(), &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        let leaders = Owned::allocate(requests.len(), &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut prepared = PreparedBatch {
            slots,
            leaders,
            shared: self.shared.clone(),
        };
        let mut local_leaders = BTreeMap::<ObjectId, usize>::new();
        for request in requests {
            let slot = match self.read_plan(
                request.object_id,
                #[cfg(not(target_arch = "wasm32"))]
                work.original,
            )? {
                ReadPlan::Hit(bytes) => BatchSlot::Hit(bytes),
                ReadPlan::Leader(flight) => {
                    let index = prepared.leaders.value.len();
                    prepared.leaders.value.push(BatchLeader {
                        object_id: request.object_id,
                        maximum_bytes: request.maximum_bytes,
                        flight,
                    });
                    local_leaders.insert(request.object_id, index);
                    BatchSlot::LocalLeader(index)
                }
                ReadPlan::Follower(flight) => {
                    if let Some(&index) = local_leaders.get(&request.object_id)
                        && let Some(leader) = prepared.leaders.value.get_mut(index)
                        && Arc::ptr_eq(&leader.flight, &flight)
                    {
                        leader.maximum_bytes = leader.maximum_bytes.max(request.maximum_bytes);
                        BatchSlot::LocalLeader(index)
                    } else {
                        BatchSlot::ExternalFollower(flight)
                    }
                }
            };
            prepared.slots.value.push(slot);
        }
        Ok(prepared)
    }

    pub(super) async fn load_batch_leaders(
        &self,
        prepared: &PreparedBatch,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<ObjectRead>, crate::AuthorityAllocation), ObjectStoreError> {
        if prepared.leaders.value.is_empty() {
            return Ok((Vec::new(), None));
        }
        let count = prepared.leaders.value.len();
        let delta = WorkCounters {
            bytes_copied: count
                .checked_mul(size_of::<ObjectReadRequest>())
                .ok_or(crate::WorkError::Overflow)? as u64,
            ..WorkCounters::UNCHARGED
        };
        work.accounting()
            .admit(delta)
            .map_err(crate::async_storage::object_error)?;
        let mut requests = Owned::allocate(count, &mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        requests.value.extend(
            prepared
                .leaders
                .value
                .iter()
                .map(|leader| ObjectReadRequest {
                    object_id: leader.object_id,
                    maximum_bytes: leader.maximum_bytes,
                }),
        );
        work.accounting()
            .record_completed(delta)
            .map_err(crate::async_storage::object_error)?;
        let result = self.inner.read_many(&requests.value, work).await;
        requests
            .retire(&mut work.accounting())
            .map_err(crate::async_storage::object_error)?;
        let mut reads = result?;
        if reads.0.len() != count {
            return Err(ObjectStoreError::Corrupt);
        }
        for (leader, read) in prepared.leaders.value.iter().zip(&mut reads.0) {
            let bytes = read.bytes.clone();
            let retained = self.insert(leader.object_id, bytes.clone())?;
            if retained {
                read.retention = ObjectReadRetention::Shared;
            }
            leader.flight.finish(retained.then_some(bytes));
            self.remove_flight(leader.object_id, &leader.flight)?;
        }
        Ok(reads)
    }

    pub(super) async fn resolve_batch_slot(
        &self,
        request: &ObjectReadRequest,
        slot: BatchSlot,
        leaders: &[ObjectRead],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<ObjectRead, ObjectStoreError> {
        let bytes = match slot {
            BatchSlot::Hit(bytes) => bytes,
            BatchSlot::LocalLeader(index) => leaders
                .get(index)
                .ok_or(ObjectStoreError::Corrupt)?
                .bytes
                .clone(),
            BatchSlot::ExternalFollower(flight) => {
                let result = wait_for_flight(
                    &flight,
                    work.cancellation,
                    #[cfg(not(target_arch = "wasm32"))]
                    work.original,
                )
                .await?;
                self.record_coalesced()?;
                if let Some(bytes) = result {
                    bytes
                } else {
                    self.remove_flight(request.object_id, &flight)?;
                    return self
                        .read(request.object_id, request.maximum_bytes, work)
                        .await;
                }
            }
        };
        shared_read(bytes, request.maximum_bytes)
    }
}
