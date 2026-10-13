#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    ReadAllocation,
};

#[cfg(not(target_arch = "wasm32"))]
type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
type Allocation = ();

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Work {
    pub(crate) meter: Arc<dyn NativeReadMeter>,
    error: std::sync::Mutex<Option<std::io::Error>>,
    write: std::sync::Mutex<
        Option<(
            NativeOperationReservation<Arc<dyn NativeReadMeter>>,
            NativeByteReservation<Arc<dyn NativeReadMeter>>,
        )>,
    >,
    durability: std::sync::Mutex<Option<NativeOperationReservation<Arc<dyn NativeReadMeter>>>>,
    _allocation: ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl Work {
    pub(crate) fn new(meter: Arc<dyn NativeReadMeter>) -> std::io::Result<Arc<Self>> {
        let allocation = ReadAllocation::new(&meter, std::mem::size_of::<Self>())?;
        Ok(Arc::new(Self {
            meter,
            error: std::sync::Mutex::new(None),
            write: std::sync::Mutex::new(None),
            durability: std::sync::Mutex::new(None),
            _allocation: allocation,
        }))
    }

    pub(crate) fn record<T>(&self, result: std::io::Result<T>) -> Result<T, StreamError> {
        result.map_err(|error| {
            self.error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert(error);
            StreamError::Unavailable
        })
    }

    pub(crate) fn reserve_write(&self, bytes: usize) -> Result<(), StreamError> {
        let operation = self.record(NativeOperationReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Write,
            1,
        ))?;
        let bytes = self.record(NativeByteReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Write,
            bytes,
        ))?;
        *self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((operation, bytes));
        Ok(())
    }

    fn publish<T>(
        &self,
        bytes: usize,
        operation: impl FnOnce() -> Result<T, StreamError>,
    ) -> Result<T, StreamError> {
        let mut allowance = self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let (result, observed) = match &mut allowance {
            Some((write, _)) => write.attempt(operation),
            None => (operation(), Ok(())),
        };
        let transferred = match &mut allowance {
            Some((_, transfer)) if result.is_ok() && bytes != 0 => transfer.record(bytes),
            _ => Ok(()),
        };
        self.record(observed)?;
        self.record(transferred)?;
        result
    }

    pub(crate) fn reserve_durability(&self, count: u64) -> Result<(), StreamError> {
        let ticket = self.record(NativeOperationReservation::new(
            Arc::clone(&self.meter),
            NativeOperationKind::Durability,
            count,
        ))?;
        *self
            .durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ticket);
        Ok(())
    }
    pub(crate) fn sync(
        &self,
        operation: impl FnOnce() -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let mut ticket = self
            .durability
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (physical, observed) = ticket
            .as_mut()
            .expect("durability admitted before publication")
            .attempt(operation);
        let _ = self.record(observed);
        match physical {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = self.record::<()>(Err(error));
                Err(std::io::ErrorKind::Other.into())
            }
        }
    }

    pub(crate) fn finish<T, E>(
        &self,
        result: Result<(T, Allocation), E>,
    ) -> crate::NativeStreamResult<T, E> {
        if let Some(error) = self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            return Err(error);
        }
        Ok(result.map(|(value, allocation)| {
            crate::NativeStreamOutput::new(
                value,
                allocation.unwrap_or_else(|| ReadAllocation::empty(&self.meter)),
            )
        }))
    }
}

impl MemoryStream {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_context(&self, work: Arc<Work>) -> Self {
        Self {
            work: Some(work),
            ..self.clone()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn has_replay(&self, key: &IdempotencyKey) -> bool {
        self.state.read().await.replays.contains_key(key.as_bytes())
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
                    .expect("native claims carry their context")
                    .record(allocation.absorb(&mut other))?;
            } else {
                *_allocation = Some(other);
            }
        }
        Ok(())
    }

    fn copy_commit(
        &self,
        value: &CommitOutcome,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let output = work.record(crate::native::clone_commit(
                value,
                &work.meter,
                NativeOperationKind::Write,
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
                .map(|mutation| match mutation {
                    CommitMutation::Append { records, .. }
                    | CommitMutation::Fork { records, .. } => {
                        records.capacity() * std::mem::size_of::<Bytes>()
                    }
                })
                .sum::<usize>();
        self.adopt(bytes)
    }

    fn digest(&self, request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
        let bytes = commit_hash_bytes(request);
        let work = self.work.as_ref().expect("native digest context");
        work.record(work.meter.admit_codec(0, 0, bytes))?;
        let digest = commit_digest(request)?;
        work.record(work.meter.record_codec(0, 0, bytes))?;
        work.record(work.meter.check())?;
        Ok(digest)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn commit_hash_bytes(request: &CommitRequest) -> u64 {
    let path_bytes = |path: &StreamPath| 8 + path.as_str().len() as u64;
    let record_bytes = |records: &[Bytes]| {
        8 + records
            .iter()
            .map(|record| 8 + record.len() as u64)
            .sum::<u64>()
    };
    b"acyclic.stream.request.v1\0".len() as u64
        + b"commit".len() as u64
        + request
            .conditions
            .iter()
            .map(|condition| match condition {
                CommitCondition::Tail { path, .. } => 1 + path_bytes(path) + 8,
                CommitCondition::Absent { path } => 1 + path_bytes(path),
            })
            .sum::<u64>()
        + request
            .mutations
            .iter()
            .map(|mutation| match mutation {
                CommitMutation::Append { path, records } => {
                    1 + path_bytes(path) + record_bytes(records)
                }
                CommitMutation::Fork {
                    source,
                    destination,
                    records,
                    ..
                } => 1 + path_bytes(source) + path_bytes(destination) + 8 + record_bytes(records),
            })
            .sum::<u64>()
}
