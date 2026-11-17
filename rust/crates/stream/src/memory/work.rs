#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{
    NativeOperationKind, NativeOperationReservation, NativeReadMeter, ReadAllocation,
};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type Allocation = Option<ReadAllocation>;
#[cfg(target_arch = "wasm32")]
pub(crate) type Allocation = Option<()>;

/// One write's payload custody, as its owner and its allocation-meter view.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct WriteCustody {
    owner: Arc<acyclic_native_runtime::NativeCustody>,
    meter: Arc<dyn NativeReadMeter>,
}

/// Store capacity reserved for a planned write, completed when it installs.
#[cfg(not(target_arch = "wasm32"))]
type Retention = Option<acyclic_native_runtime::CustodyTransfer>;
#[cfg(target_arch = "wasm32")]
type Retention = ();

#[cfg(not(target_arch = "wasm32"))]
type CaptureWork<'a> = Option<&'a Work>;
#[cfg(target_arch = "wasm32")]
type CaptureWork<'a> = Option<&'a ()>;

struct RetainedReservation {
    slot: RetainedFactRef,
    growth: Option<Arc<RetainedGrowth>>,
}

/// The newest funded segment awaiting installation. Concurrent reservations
/// share it, so a boundary funds each segment once.
#[derive(Default)]
struct RetainedPool(crate::query_sync::SyncMutex<std::sync::Weak<RetainedGrowth>>);

/// One funded segment awaiting installation, above the pending segments it
/// extends. Segments strictly decrease along `below`.
struct RetainedGrowth {
    segment: usize,
    buffer: crate::query_sync::SyncMutex<Option<RetainedSegment>>,
    below: Option<Arc<RetainedGrowth>>,
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
            // A live lower segment stays shareable after its extension retires.
            if std::ptr::eq(pending.as_ptr(), self) {
                *pending = self
                    .below
                    .as_ref()
                    .map_or_else(std::sync::Weak::new, Arc::downgrade);
            }
        }
    }
}

impl RetainedGrowth {
    /// Takes the pending allocation for `segment`, if this chain still holds it.
    fn take(&self, segment: usize) -> Option<RetainedSegment> {
        self.find(segment)?
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    /// The pending node for `segment`, if this chain funds it.
    fn find(&self, segment: usize) -> Option<&Self> {
        let mut node = Some(self);
        while let Some(current) = node {
            if current.segment <= segment {
                return (current.segment == segment).then_some(current);
            }
            node = current.below.as_deref();
        }
        None
    }
}

impl MemoryStream {
    /// A presence hint for read-side refresh, never authority to skip a guard
    /// or its original write allowance. Such callers retain `pin_replay`.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn has_replay(&self, key: &IdempotencyKey) -> Result<bool, StreamError> {
        self.replay_in(&*self.state.read().await, key.as_bytes())
            .map(|entry| entry.is_some())
    }

    fn install_retained(
        &self,
        state: &mut State,
        reservation: RetainedReservation,
    ) -> Result<RetainedFactRef, StreamError> {
        let RetainedReservation { slot, growth } = reservation;
        if let Some(growth) = growth {
            // Install from the lowest missing segment upward. A segment another
            // installation already took is skipped; nothing is moved or copied.
            while let Some(segment) = growth.take(state.retained.next_segment()) {
                state.retained.install(segment);
            }
        }
        if state.retained.len() >= state.retained.capacity() {
            return Err(StreamError::Capacity);
        }
        Ok(slot)
    }

    /// A planning clone whose allocations are one write's custody. The writer
    /// funds them while it prepares; `reserve_custody` and `keep` then move
    /// the charge for what the store keeps to the store's own capacity.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn in_custody(&self) -> Result<Self, StreamError> {
        let Some(work) = &self.work else {
            return Ok(self.clone());
        };
        let owner = work.record(acyclic_native_runtime::NativeCustody::new(&work.meter))?;
        let meter: Arc<dyn NativeReadMeter> = Arc::clone(&owner) as _;
        Ok(Self {
            custody: Some(WriteCustody { owner, meter }),
            ..self.clone()
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub(crate) fn in_custody(&self) -> Result<Self, StreamError> {
        Ok(self.clone())
    }

    /// Hands a result's claim back to the writer: the caller, not the store,
    /// keeps what a write returns, whether or not the write lands.
    fn hand_back(&self, _output: &mut Allocation) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(work), Some(custody), Some(output)) = (&self.work, &self.custody, _output) {
            work.record(output.leave_custody(&custody.owner))?;
        }
        Ok(())
    }

    /// Reserves store capacity for everything this write's custody owns, after
    /// its last allocation and before publication. `output` is handed back to
    /// the writer first.
    fn reserve_custody(&self, output: &mut Allocation) -> Result<Retention, StreamError> {
        self.hand_back(output)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(work), Some(custody)) = (&self.work, &self.custody) {
            return match custody.owner.reserve(&self.capacity) {
                Err(error) if error.kind() == std::io::ErrorKind::StorageFull => {
                    Err(StreamError::Capacity)
                }
                reserved => work.record(reserved).map(Some),
            };
        }
        Ok(Default::default())
    }

    /// Hands a landed write's payload to the store. It cannot fail.
    fn keep(_retention: Retention) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(transfer) = _retention {
            transfer.complete();
        }
    }

    /// The store's own copy of `path`, funded by this write's custody, so
    /// what a landed write keeps leaves its writer's ledger with the rest.
    fn keep_path(&self, path: &StreamPath) -> Result<StreamPath, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work.record(path.bind_native(self.allocation_meter(work)));
        }
        Ok(path.clone())
    }

    /// The store's own copy of a retry key it keeps for replay.
    fn keep_key(&self, key: &IdempotencyKey) -> Result<IdempotencyKey, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work
                .record(
                    key.0
                        .bind_native(self.allocation_meter(work), &self.capacity),
                )
                .map(IdempotencyKey);
        }
        Ok(key.clone())
    }

    /// The store's own copy of a kept record body. A body its caller funds
    /// would otherwise keep the caller's buffer, and the caller's charge.
    fn keep_body(
        &self,
        _value: &mut Bytes,
        _headers: &mut crate::RecordHeaders,
    ) -> Result<(), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let meter = self.allocation_meter(work);
            *_value = work.record(_value.bind_native(meter, &self.capacity))?;
            *_headers = work.record(_headers.bind_native(meter))?;
        }
        Ok(())
    }

    /// Binds every path a write's facts keep to the store. Each mutation's
    /// records then share its one bound path.
    fn keep_facts(&self, facts: &mut [CommittedMutation]) -> Result<(), StreamError> {
        for fact in facts {
            let (path, records) = match fact {
                CommittedMutation::Append(append) => (&mut append.path, &mut append.records),
                CommittedMutation::Fork(fork) => {
                    fork.source = self.keep_path(&fork.source)?;
                    for path in &mut fork.exclude {
                        *path = self.keep_path(path)?;
                    }
                    (&mut fork.destination, &mut fork.records)
                }
            };
            *path = self.keep_path(path)?;
            for record in records {
                record.origin = path.clone();
            }
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn allocation_meter<'w>(&'w self, work: &'w Work) -> &'w Arc<dyn NativeReadMeter> {
        self.custody
            .as_ref()
            .map_or(&work.meter, |custody| &custody.meter)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn with_context(&self, work: Arc<Work>) -> Self {
        Self {
            work: Some(work),
            ..self.clone()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn pin_replay(
        &self,
        key: &IdempotencyKey,
    ) -> Result<Option<ReplayPin>, StreamError> {
        let state = self.state.read().await;
        Ok(state
            .lookup_fact(key.as_bytes(), |items| {
                if let Some(work) = &self.work {
                    work.record(work.meter.examine(items, 0))?;
                }
                Ok::<_, StreamError>(())
            })?
            .map(|(fact, ephemeral)| ReplayPin {
                fact: fact.clone(),
                ephemeral,
            }))
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
    /// The identities of leases owning observed paths prevent removal and
    /// recreation from restoring a cut. Unrelated leases never enter the cut.
    #[cfg(not(target_arch = "wasm32"))]
    fn guard_cut_locked(
        &self,
        state: &State,
        request: &CommitRequest,
        reads: &[StreamPath],
    ) -> Result<GuardCut, StreamError> {
        let replayed = self
            .replay_in(state, request.idempotency_key.as_bytes())?
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
        let leases = request
            .conditions()
            .map(CommitCondition::path)
            .chain(request.mutations().map(CommitMutation::path))
            .chain(reads)
            .chain(sources.clone())
            .filter_map(|path| state.ephemeral.root_of(path.as_str()))
            .filter_map(|root| state.ephemeral.roots.get(root).map(|held| held.lease));
        let (mut observed_leases, lease_allocation) = self.vector(leases.clone().count())?;
        observed_leases.extend(leases);
        let (mut subtrees, allocation) = self.vector(sources.clone().count())?;
        subtrees.extend(sources.map(|source| state.paths.node(source).cloned()));
        Ok(GuardCut {
            replayed,
            tails,
            observed_leases,
            _lease_allocation: lease_allocation,
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
                .record(ReadAllocation::new(self.allocation_meter(work), _bytes))
                .map(Some);
        }
        Ok(Default::default())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn adopt(&self, _bytes: usize) -> Result<Allocation, StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            return work
                .record(ReadAllocation::adopt(self.allocation_meter(work), _bytes))
                .map(Some);
        }
        Ok(Default::default())
    }

    fn vector<T>(&self, length: usize) -> Result<(Vec<T>, Allocation), StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            let meter = self.allocation_meter(work);
            let mut allocation = ReadAllocation::empty(meter);
            let value = work.record(crate::native::vector(length, meter, &mut allocation))?;
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
                self.allocation_meter(work),
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
    observed_leases: Vec<u64>,
    _lease_allocation: Allocation,
    subtrees: Vec<Option<Arc<NamespaceNode>>>,
    _allocation: Allocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl PartialEq for GuardCut {
    fn eq(&self, other: &Self) -> bool {
        self.replayed == other.replayed
            && self.observed_leases == other.observed_leases
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
