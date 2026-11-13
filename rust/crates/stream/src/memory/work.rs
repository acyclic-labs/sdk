#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeOperationKind, NativeOperationReservation, NativeReadMeter, ReadAllocation,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Allocation = Option<()>;

#[cfg(not(target_arch = "wasm32"))]
type CaptureWork<'a> = Option<&'a Work>;
#[cfg(target_arch = "wasm32")]
type CaptureWork<'a> = Option<&'a ()>;

struct RetainedReservation {
    slot: RetainedFactRef,
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
    ) -> Result<RetainedFactRef, StreamError> {
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

    /// The cut a guarded Commit's guard can observe: whether its key has a
    /// replay, the tails of the request's own paths (conditions and mutation
    /// paths) and of the paths the guard declares it reads, and the subtree
    /// each fork shares. Paths are permanent and tails only grow, so the sum
    /// of `tail + 1` over present paths moves exactly when one of them
    /// changes. A fork shares its source's whole subtree, and the namespace
    /// rebuilds every ancestor of a changed path, so a source's node is a new
    /// node exactly when anything at or under it changed; the cut holds that
    /// node, so its identity is never reused while compared. A change under a
    /// branch the fork excludes moves it too, refusing conservatively.
    /// Commits anywhere else in the store never move it. Both native
    /// providers compare this one cut before and after the guard.
    #[cfg(not(target_arch = "wasm32"))]
    fn guard_cut_locked(
        &self,
        state: &State,
        request: &CommitRequest,
        reads: &[StreamPath],
    ) -> Result<GuardCut, StreamError> {
        let replayed = self
            .replay(state, request.idempotency_key.as_bytes())?
            .is_some();
        let tails = request
            .conditions()
            .map(CommitCondition::path)
            .chain(request.mutations().map(CommitMutation::path))
            .chain(reads)
            .map(|path| state.tail(path).map_or(0, |tail| u128::from(tail) + 1))
            .sum();
        let sources = request.mutations().filter_map(|mutation| match mutation {
            CommitMutation::Fork { source, .. } => Some(source),
            CommitMutation::Append { .. } => None,
        });
        let (mut subtrees, allocation) = self.vector(sources.clone().count())?;
        subtrees.extend(sources.map(|source| state.paths.node(source).cloned()));
        Ok(GuardCut {
            replayed,
            tails,
            subtrees,
            _allocation: allocation,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn guard_cut(
        &self,
        request: &CommitRequest,
        reads: &[StreamPath],
    ) -> Result<GuardCut, StreamError> {
        self.guard_cut_locked(&*self.state.read().await, request, reads)
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

    #[cfg(not(target_arch = "wasm32"))]
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
        let bytes = request.effects.capacity() * std::mem::size_of::<crate::CommitEffect>()
            + request
                .effects
                .iter()
                .map(|effect| {
                    effect.conditions.capacity() * std::mem::size_of::<CommitCondition>()
                        + effect.mutations.capacity() * std::mem::size_of::<CommitMutation>()
                })
                .sum::<usize>()
            + request
                .mutations()
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

    /// Charges a guard's declared read list, held while its guard runs, to
    /// the commit's meter: `capacity` is the list's capacity.
    pub(crate) fn reads_claim(&self, capacity: usize) -> Result<Allocation, StreamError> {
        self.adopt(capacity * std::mem::size_of::<StreamPath>())
    }

    fn digest(&self, request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
        crate::request::normalize_commit_with(request, |length| self.vector(length))?;
        let bytes = commit_hash_bytes(request);
        let work = self.work.as_ref().ok_or(StreamError::Unavailable)?;
        work.record(work.meter.admit_codec(0, 0, bytes))?;
        let digest = crate::request::canonical_digest(request);
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

/// See [`MemoryStream::guard_cut`].
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct GuardCut {
    replayed: bool,
    tails: u128,
    subtrees: Vec<Option<Arc<NamespaceNode>>>,
    _allocation: Allocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl PartialEq for GuardCut {
    fn eq(&self, other: &Self) -> bool {
        self.replayed == other.replayed
            && self.tails == other.tails
            && self.subtrees.len() == other.subtrees.len()
            && self
                .subtrees
                .iter()
                .zip(&other.subtrees)
                .all(|pair| match pair {
                    (Some(before), Some(after)) => Arc::ptr_eq(before, after),
                    (None, None) => true,
                    _ => false,
                })
    }
}
