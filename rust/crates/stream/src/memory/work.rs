#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeOperationKind, NativeOperationReservation, NativeReadMeter, ReadAllocation,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Allocation = Option<()>;

struct RetainedReservation {
    slot: Arc<RetainedFact>,
    growth: Option<RetainedGrowthReservation>,
}

struct RetainedGrowthReservation {
    growth: Arc<RetainedGrowth>,
    #[cfg(not(target_arch = "wasm32"))]
    codec: Option<acyclic_native_runtime::NativeCodecReservation<Arc<dyn NativeReadMeter>>>,
}

#[derive(Default)]
struct RetainedPool(crate::query_sync::SyncMutex<std::sync::Weak<RetainedGrowth>>);

struct RetainedGrowth {
    buffer: crate::query_sync::SyncMutex<Option<(Vec<Retained>, Allocation)>>,
    pool: std::sync::Weak<RetainedPool>,
    _allocation: Allocation,
}

impl Drop for RetainedGrowth {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let mut pending = pool
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if std::ptr::eq(pending.as_ptr(), self) {
                *pending = std::sync::Weak::new();
            }
        }
    }
}

impl MemoryStream {
    fn install_retained(
        &self,
        state: &mut State,
        reservation: RetainedReservation,
    ) -> Result<Arc<RetainedFact>, StreamError> {
        let RetainedReservation { slot, growth } = reservation;
        if let Some(reservation) = growth {
            #[cfg(not(target_arch = "wasm32"))]
            let mut codec = reservation.codec;
            let mut pending = reservation
                .growth
                .buffer
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if pending
                .as_ref()
                .is_some_and(|(buffer, _)| buffer.capacity() > state.retained.capacity())
            {
                let (mut buffer, allocation) = pending.take().ok_or(StreamError::Capacity)?;
                #[cfg(not(target_arch = "wasm32"))]
                let copied = (state.retained.len() * std::mem::size_of::<Retained>()) as u64;
                buffer.append(&mut state.retained);
                state.retained = buffer;
                state.retained_allocation = allocation;
                #[cfg(not(target_arch = "wasm32"))]
                if let (Some(work), Some(codec)) = (&self.work, &mut codec) {
                    let _ = work.record(codec.record(acyclic_native_runtime::NativeCodecWork {
                        copied,
                        ..Default::default()
                    }));
                }
            }
        }
        if state.retained.len() >= state.retained.capacity() {
            return Err(StreamError::Capacity);
        }
        Ok(slot)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_context(&self, work: Arc<Work>) -> Self {
        Self {
            work: Some(work),
            ..self.clone()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn has_replay(&self, key: &IdempotencyKey) -> Result<bool, StreamError> {
        self.replay(&*self.state.read().await, key.as_bytes())
            .map(|entry| entry.is_some())
    }

    fn publish<T>(
        &self,
        bytes: usize,
        operation: impl FnOnce() -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work.publish(bytes, operation);
        }
        let _ = bytes;
        operation()
    }

    fn allocate(&self, _bytes: usize) -> Result<Allocation, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work
                .record(ReadAllocation::new(&work.meter, _bytes))
                .map(Some);
        }
        Ok(Default::default())
    }

    fn adopt(&self, _bytes: usize) -> Result<Allocation, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work
                .record(ReadAllocation::adopt(&work.meter, _bytes))
                .map(Some);
        }
        Ok(Default::default())
    }

    fn vector<T>(&self, length: usize) -> Result<(Vec<T>, Allocation), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let mut allocation = ReadAllocation::empty(&work.meter);
            let value = work.record(crate::native::vector(length, &work.meter, &mut allocation))?;
            return Ok((value, Some(allocation)));
        }
        let mut value = Vec::new();
        value
            .try_reserve_exact(length)
            .map_err(|_| StreamError::Capacity)?;
        Ok((value, Default::default()))
    }
    fn absorb(&self, _allocation: &mut Allocation, _other: Allocation) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(mut other) = _other {
            if let Some(allocation) = _allocation {
                self.work
                    .as_ref()
                    .ok_or(StreamError::Unavailable)?
                    .record(allocation.absorb(&mut other))?;
            } else {
                *_allocation = Some(other);
            }
        }
        Ok(())
    }

    fn copy_envelope(
        &self,
        value: &CommittedEnvelope,
    ) -> Result<(CommittedEnvelope, Allocation), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let output = work.record(crate::native::clone_envelope(
                value,
                &work.meter,
                acyclic_native_runtime::NativeOperationKind::Write,
            ))?;
            let (value, allocation) = output.into_parts();
            return Ok((value, Some(allocation)));
        }
        Ok((value.clone(), Default::default()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl MemoryStream {
    pub(crate) fn request_claim(&self, request: &CommitRequest) -> Result<Allocation, StreamError> {
        let bytes = request.conditions.capacity() * std::mem::size_of::<CommitCondition>()
            + request.mutations.capacity() * std::mem::size_of::<CommitMutation>()
            + request
                .mutations
                .iter()
                .map(|mutation| {
                    let (records, exclusions) = match mutation {
                        CommitMutation::Append { records, .. } => (records, 0),
                        CommitMutation::Fork {
                            records, exclude, ..
                        } => (
                            records,
                            exclude.capacity() * std::mem::size_of::<crate::RelativePath>(),
                        ),
                    };
                    exclusions
                        + records.capacity() * std::mem::size_of::<AppendRecord>()
                        + records
                            .iter()
                            .map(|record| record.headers.owned_bytes())
                            .sum::<usize>()
                })
                .sum::<usize>();
        self.adopt(bytes)
    }

    fn digest(&self, request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
        let bytes = commit_hash_bytes(request);
        let work = self.work.as_ref().ok_or(StreamError::Unavailable)?;
        work.record(work.meter.admit_codec(0, 0, bytes))?;
        let digest = commit_digest(request)?;
        work.record(work.meter.record_codec(0, 0, bytes))?;
        work.record(work.meter.check())?;
        Ok(digest)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn commit_hash_bytes(request: &CommitRequest) -> u64 {
    crate::request::commit_hash_bytes(request)
}

#[cfg(not(target_arch = "wasm32"))]
fn inspect<T>(meter: &Arc<dyn NativeReadMeter>, lookup: impl FnOnce() -> T) -> std::io::Result<T> {
    let mut operation =
        NativeOperationReservation::new(Arc::clone(meter), NativeOperationKind::Read, 1)?;
    meter.admit_examination(1, 0)?;
    let (value, recorded) = operation.attempt(lookup);
    let examined = meter.record_examination(1, 0);
    recorded?;
    examined?;
    Ok(value)
}
