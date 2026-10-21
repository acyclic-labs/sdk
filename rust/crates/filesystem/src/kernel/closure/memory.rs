use super::*;

pub(crate) struct StandaloneProofState {
    allocations: crate::kernel::allocation::AllocationLedger,
    work: WorkCounters,
    budget: WorkBudget,
}

pub(super) fn proof_standalone_control_bytes() -> u64 {
    usize_to_u64(size_of::<std::sync::Mutex<StandaloneProofState>>() + 2 * size_of::<usize>())
}

#[derive(Clone)]
pub(crate) enum ProofMeter<'a> {
    Standalone {
        state: std::sync::Arc<std::sync::Mutex<StandaloneProofState>>,
        cancellation: &'a CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        original: &'a crate::NativeMaterializationWork,
        cancellation: &'a CancellationToken,
    },
}

impl<'a> ProofMeter<'a> {
    pub(crate) fn standalone(
        budget: WorkBudget,
        cancellation: &'a CancellationToken,
    ) -> Result<Self, crate::FsError> {
        cancellation.check()?;
        let mut state = StandaloneProofState {
            allocations: crate::kernel::allocation::AllocationLedger::default(),
            work: WorkCounters::UNCHARGED,
            budget,
        };
        state
            .allocations
            .claim_bytes(proof_standalone_control_bytes(), 1, &mut state.work, budget)
            .map_err(proof_allocation_error)?;
        Ok(Self::Standalone {
            state: std::sync::Arc::new(std::sync::Mutex::new(state)),
            cancellation,
        })
    }

    #[inline]
    pub(super) fn with_accounting<T>(
        &self,
        operation: impl FnOnce(
            &mut crate::kernel::allocation::KernelAccounting<'_>,
        ) -> Result<T, crate::FsError>,
    ) -> Result<T, crate::FsError> {
        match self {
            Self::Standalone {
                state,
                cancellation,
            } => {
                let mut state = state.lock().map_err(|_| WorkError::Overflow)?;
                let StandaloneProofState {
                    allocations,
                    work,
                    budget,
                } = &mut *state;
                operation(
                    &mut crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        allocations,
                        work,
                        *budget,
                        cancellation,
                    ),
                )
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native {
                original,
                cancellation,
            } => operation(&mut crate::kernel::allocation::KernelAccounting::native(
                original,
                cancellation,
            )),
        }
    }

    pub(super) fn owned<T>(
        &self,
        operation: impl FnOnce(
            &mut crate::kernel::allocation::KernelAccounting<'_>,
        ) -> Result<
            (T, crate::kernel::allocation::KernelOwnedAllocation, u64),
            crate::FsError,
        >,
    ) -> Result<(T, ProofGuard), crate::FsError> {
        let (value, allocation, bytes) = self.with_accounting(operation)?;
        let guard = match (self, allocation) {
            (Self::Standalone { state, .. }, _) => ProofGuard::Standalone {
                state: state.clone(),
                bytes,
            },
            #[cfg(not(target_arch = "wasm32"))]
            (
                Self::Native { .. },
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard),
            ) => ProofGuard::Native(guard),
            #[cfg(not(target_arch = "wasm32"))]
            (Self::Native { .. }, _) => {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
        };
        Ok((value, guard))
    }

    fn empty_guard(&self) -> Result<ProofGuard, crate::FsError> {
        self.owned(|accounting| Ok(((), accounting.claim_owned(0, 0)?, 0)))
            .map(|owned| owned.1)
    }

    pub(crate) fn work(&self) -> WorkCounters {
        match self {
            Self::Standalone { state, .. } => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .work
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { original, .. } => original.receipt(),
        }
    }

    pub(super) fn charge(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.charge(delta))
    }

    fn record_completed(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.record_completed(delta))
    }

    fn admit(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        self.with_accounting(|accounting| accounting.admit(delta))
    }

    pub(super) async fn read<S: crate::AsyncObjectStore>(
        &self,
        store: &S,
        object: ObjectId,
        maximum: u64,
    ) -> Result<(crate::storage::ObjectRead, ProofGuard), crate::FsError> {
        match self {
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native {
                original,
                cancellation,
            } => {
                cancellation.check()?;
                let mut work = crate::AuthorityWork::native(original, cancellation);
                let read = store
                    .read(object, maximum, &mut work)
                    .await
                    .map_err(crate::kernel::file_read::read_store_error)?;
                Ok((read, self.empty_guard()?))
            }
            Self::Standalone { cancellation, .. } => {
                cancellation.check()?;
                let remaining = self.with_accounting(|accounting| accounting.remaining())?;
                let mut work = crate::AuthorityWork::new(remaining, cancellation);
                let result = store.read(object, maximum, &mut work).await;
                self.owned(|accounting| {
                    crate::kernel::file_read::accept_standalone_object_read(
                        work.receipt(result),
                        accounting,
                    )
                    .map(|(read, bytes)| {
                        (
                            read,
                            crate::kernel::allocation::KernelOwnedAllocation::Standalone,
                            bytes,
                        )
                    })
                })
            }
        }
    }

    pub(super) fn decode_steps<T>(
        &self,
        decode: impl FnOnce(
            &mut dyn FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), crate::FsError>,
        ) -> Result<T, crate::FsError>,
    ) -> Result<(T, ProofGuard), crate::FsError> {
        self.owned(|accounting| {
            let mut collector = crate::kernel::file_read::OriginalReadAccounting::new(accounting)?;
            match decode(&mut |step| collector.observe(step)) {
                Ok(value) => {
                    let (allocation, bytes) = collector.into_parts();
                    Ok((value, allocation, bytes))
                }
                Err(error) => {
                    collector.release_failed()?;
                    Err(error)
                }
            }
        })
    }

    pub(super) fn clone_key<F: crate::kernel::persistent_btree::Format>(
        &self,
        key: &F::Key,
        maximum: u32,
    ) -> Result<(F::Key, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| F::try_clone_key(key, maximum, &mut |step| observe(step)))
    }
}

fn proof_allocation_error(error: crate::kernel::allocation::AllocationError) -> crate::FsError {
    error.into_work_or(crate::FsError::Decode(
        crate::kernel::CanonicalDecodeError::AllocationFailed,
    ))
}

fn proof_capacity_bytes<T>(capacity: usize) -> Result<u64, crate::FsError> {
    capacity
        .checked_mul(size_of::<T>())
        .map(usize_to_u64)
        .ok_or_else(|| WorkError::Overflow.into())
}

pub(crate) enum ProofGuard {
    Standalone {
        state: std::sync::Arc<std::sync::Mutex<StandaloneProofState>>,
        bytes: u64,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeAllocation),
}

impl ProofGuard {
    // A retained standalone result also keeps this proof's shared meter alive.
    // Count its one admitted Arc body, never the original native root state.
    #[allow(
        clippy::expect_used,
        reason = "These guards retire only the allocation bytes admitted by their constructors."
    )]
    pub(crate) fn retained_bytes(&self) -> u64 {
        match self {
            ProofGuard::Standalone { bytes, .. } => bytes
                .checked_add(proof_standalone_control_bytes())
                .expect("retained proof control ownership invariant"),
            #[cfg(not(target_arch = "wasm32"))]
            ProofGuard::Native(guard) => guard.bytes(),
        }
    }

    pub(super) fn absorb(&mut self, incoming: &mut Self) -> Result<(), crate::FsError> {
        match (self, incoming) {
            (
                ProofGuard::Standalone { state, bytes },
                ProofGuard::Standalone {
                    state: source,
                    bytes: added,
                },
            ) if std::sync::Arc::ptr_eq(state, source) => {
                *bytes = bytes.checked_add(*added).ok_or(WorkError::Overflow)?;
                *added = 0;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            (ProofGuard::Native(target), ProofGuard::Native(source)) => {
                target.absorb(source).map_err(Into::into)
            }
            _ => Err(WorkError::Overflow.into()),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn take_native(&mut self) -> Result<crate::NativeAllocation, crate::FsError> {
        match self {
            ProofGuard::Native(guard) => guard.split_off(guard.bytes()).map_err(Into::into),
            ProofGuard::Standalone { .. } => {
                Err(crate::NativeTreeMaterializationError::InvalidLayout.into())
            }
        }
    }
}

impl Drop for ProofGuard {
    #[allow(
        clippy::expect_used,
        reason = "These guards retire only the allocation bytes admitted by their constructors."
    )]
    fn drop(&mut self) {
        match self {
            Self::Standalone { state, bytes } => {
                if *bytes != 0 {
                    state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .allocations
                        .release(*bytes)
                        .expect("proof allocation ownership invariant");
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => {}
        }
    }
}

pub(super) struct ProofVec<'a, T> {
    pub(super) values: Vec<T>,
    guard: Option<ProofGuard>,
    meter: ProofMeter<'a>,
}

#[derive(Default)]
pub(crate) struct CachedProofs {
    entries: Vec<CachedSlot>,
    guard: Option<ProofGuard>,
}

struct CachedSlot {
    volume: crate::VolumeId,
    slot: Arc<ProofSlot<CachedProof>>,
    // Last: the slot's own allocation retires after the slot.
    _guard: ProofGuard,
}

impl CachedProofs {
    fn position(
        &self,
        volume: crate::VolumeId,
        meter: &ProofMeter<'_>,
    ) -> Result<Result<usize, usize>, crate::FsError> {
        let (mut low, mut high) = (0, self.entries.len());
        while low < high {
            meter.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            let middle = low + (high - low) / 2;
            let entry = self.entries.get(middle).ok_or(ObjectStoreError::Corrupt)?;
            match entry.volume.cmp(&volume) {
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
            }
        }
        Ok(Err(low))
    }

    /// The volume's proof slot, created empty on first use.
    pub(super) fn slot(
        &mut self,
        volume: crate::VolumeId,
        meter: &ProofMeter<'_>,
    ) -> Result<Arc<ProofSlot<CachedProof>>, crate::FsError> {
        let index = match self.position(volume, meter)? {
            Ok(index) => {
                return Ok(Arc::clone(
                    &self
                        .entries
                        .get(index)
                        .ok_or(ObjectStoreError::Corrupt)?
                        .slot,
                ));
            }
            Err(index) => index,
        };
        let (slot, guard) = meter.owned(|accounting| {
            let bytes = usize_to_u64(size_of::<ProofSlot<CachedProof>>() + 2 * size_of::<usize>());
            let allocation = accounting.claim_owned(bytes, 1)?;
            Ok((Arc::new(ProofSlot::default()), allocation, bytes))
        })?;
        let mut entries = ProofVec {
            values: std::mem::take(&mut self.entries),
            guard: self.guard.take(),
            meter: meter.clone(),
        };
        let result = entries.insert(
            index,
            CachedSlot {
                volume,
                slot: Arc::clone(&slot),
                _guard: guard,
            },
        );
        self.entries = std::mem::take(&mut entries.values);
        self.guard = entries.guard.take();
        result.map(|()| slot)
    }
}

impl<'a, T> ProofVec<'a, T> {
    pub(super) fn new(meter: &ProofMeter<'a>) -> Self {
        Self {
            values: Vec::new(),
            guard: None,
            meter: meter.clone(),
        }
    }

    fn ensure_capacity(&mut self) -> Result<(), crate::FsError> {
        if self.values.len() < self.values.capacity() {
            return Ok(());
        }
        let capacity = self
            .values
            .len()
            .checked_add(1)
            .ok_or(WorkError::Overflow)?
            .checked_next_power_of_two()
            .ok_or(WorkError::Overflow)?;
        let mut replacement = self.meter.owned(|accounting| {
            let requested = proof_capacity_bytes::<T>(capacity)?;
            let mut allocation = accounting.claim_owned(requested, u64::from(requested != 0))?;
            let mut values = Vec::new();
            if let Err(error) = values.try_reserve_exact(capacity) {
                drop(values);
                accounting.release_owned(&mut allocation, requested)?;
                #[cfg(not(target_arch = "wasm32"))]
                if matches!(&self.meter, ProofMeter::Native { .. }) {
                    return Err(
                        crate::NativeTreeMaterializationError::AllocationFailed(error).into(),
                    );
                }
                let _ = error;
                return Err(crate::kernel::CanonicalDecodeError::AllocationFailed.into());
            }
            let actual = proof_capacity_bytes::<T>(values.capacity())?;
            if let Err(error) = accounting.reconcile_owned(&mut allocation, requested, actual) {
                drop(values);
                accounting.release_owned(&mut allocation, actual)?;
                return Err(error);
            }
            Ok((values, allocation, actual))
        })?;
        let copied = WorkCounters {
            bytes_copied: proof_capacity_bytes::<T>(self.values.len())?,
            ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        replacement.0.append(&mut self.values);
        self.meter.record_completed(copied)?;
        let (values, guard) = replacement;
        let old_values = std::mem::replace(&mut self.values, values);
        let old_guard = self.guard.replace(guard);
        drop(old_values);
        drop(old_guard);
        Ok(())
    }

    pub(super) fn push(&mut self, value: T) -> Result<(), crate::FsError> {
        self.ensure_capacity()?;
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(size_of::<T>()),
            ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        self.values.push(value);
        self.meter.record_completed(copied)?;
        Ok(())
    }

    fn insert(&mut self, index: usize, value: T) -> Result<(), crate::FsError> {
        self.ensure_capacity()?;
        let moved = self
            .values
            .len()
            .checked_sub(index)
            .ok_or(WorkError::Overflow)?
            .checked_add(1)
            .ok_or(WorkError::Overflow)?;
        let copied = WorkCounters {
            bytes_copied: proof_capacity_bytes::<T>(moved)?,
            ..WorkCounters::UNCHARGED
        };
        self.meter.admit(copied)?;
        self.values.insert(index, value);
        self.meter.record_completed(copied)?;
        Ok(())
    }

    pub(super) fn into_parts(mut self) -> Result<(Vec<T>, ProofGuard), crate::FsError> {
        let guard = match self.guard.take() {
            Some(guard) => guard,
            None => self.meter.empty_guard()?,
        };
        Ok((std::mem::take(&mut self.values), guard))
    }
}

impl<T> Drop for ProofVec<'_, T> {
    #[allow(
        clippy::expect_used,
        reason = "These guards retire only the allocation bytes admitted by their constructors."
    )]
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.values));
        drop(self.guard.take());
    }
}

pub(super) fn map_get<'v, K: Ord, V>(entries: &'v ProofVec<'_, (K, V)>, key: &K) -> Option<&'v V> {
    entries
        .values
        .binary_search_by(|entry| entry.0.cmp(key))
        .ok()
        .and_then(|index| entries.values.get(index).map(|entry| &entry.1))
}

pub(super) fn map_insert<K: Ord, V>(
    entries: &mut ProofVec<'_, (K, V)>,
    key: K,
    value: V,
) -> Result<Option<V>, crate::FsError> {
    match entries.values.binary_search_by(|entry| entry.0.cmp(&key)) {
        Ok(index) => {
            let copied = WorkCounters {
                bytes_copied: usize_to_u64(size_of::<V>()),
                ..WorkCounters::UNCHARGED
            };
            entries.meter.admit(copied)?;
            let entry = entries
                .values
                .get_mut(index)
                .ok_or(crate::ObjectStoreError::Corrupt)?;
            let previous = std::mem::replace(&mut entry.1, value);
            entries.meter.record_completed(copied)?;
            Ok(Some(previous))
        }
        Err(index) => {
            entries.insert(index, (key, value))?;
            Ok(None)
        }
    }
}

pub(super) fn set_insert<K: Ord>(
    entries: &mut ProofVec<'_, K>,
    key: K,
) -> Result<bool, crate::FsError> {
    match entries.values.binary_search(&key) {
        Ok(_) => Ok(false),
        Err(index) => {
            entries.insert(index, key)?;
            Ok(true)
        }
    }
}

#[cfg(test)]
#[cfg(all(feature = "memory", not(acyclic_fs_loom)))]
#[path = "memory/tests.rs"]
mod tests;
