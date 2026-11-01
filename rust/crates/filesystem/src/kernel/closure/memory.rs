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
    ) -> Result<(F::Key, ProofGuard), crate::FsError> {
        self.decode_steps(|observe| F::try_clone_key(key, &mut |step| observe(step)))
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
    entries: crate::kernel::allocation::fixed_index::FixedIndex<CachedProofSlot>,
}

pub(crate) enum ObservedProof {
    Published(PublishedProof),
    Held(RetainedArc<crate::collection::Hold>),
}

impl CachedProofs {
    /// Binds an existing exact proof while its original registration is already live.
    /// A miss keeps that registration without manufacturing a semantic proof.
    pub(crate) fn observed(
        &self,
        volume: crate::VolumeId,
        generation: GenerationId,
        limits: ClosureLimits,
        epochs: Option<&crate::collection::Epochs>,
        hold: Option<(crate::VolumeConfig, crate::collection::GenerationHold)>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<Option<ObservedProof>, crate::FsError> {
        if let Some((config, hold)) = &hold {
            accounting.charge_items(1)?;
            if !hold.matches(generation, *config, epochs) {
                return Err(crate::FsError::InvalidDiff);
            }
        }
        let published = self
            .entries
            .get(volume.into_bytes(), accounting)?
            .and_then(|slot| {
                slot.published_if(epochs, |proof| {
                    proof.generation_id == generation
                        && proof.root.volume_id == volume
                        && proof.limits == limits
                })
            });
        Ok(match (published, hold) {
            (Some(mut published), hold) => {
                if let Some((_, hold)) = hold {
                    published.retain(hold.into_original());
                }
                Some(ObservedProof::Published(published))
            }
            (None, Some((_, hold))) => Some(ObservedProof::Held(hold.into_original())),
            (None, None) => None,
        })
    }

    /// The volume's proof slot, created empty on first use.
    pub(super) fn slot(
        &mut self,
        volume: crate::VolumeId,
        meter: &ProofMeter<'_>,
    ) -> Result<CachedProofSlot, crate::FsError> {
        let key = volume.into_bytes();
        if let Some(slot) = meter.with_accounting(|accounting| self.entries.get(key, accounting))? {
            return Ok(slot.clone());
        }
        // Refusal leaves the original cache untouched and constructs no slot.
        // The alias retains its original funding after this cache is retired.
        let admitted = meter.with_accounting(RetainedArc::admit)?;
        let slot = admitted.into_value(ProofSlot::default());
        meter.with_accounting(|accounting| {
            self.entries
                .get_or_try_insert_with(key, accounting, |_| Ok(slot))
                .map(|slot| slot.clone())
        })
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

pub(super) trait ProofKey<const N: usize>: Copy {
    fn proof_key(self) -> [u8; N];
}

impl ProofKey<16> for FileId {
    fn proof_key(self) -> [u8; 16] {
        self.into_bytes()
    }
}

impl ProofKey<{ ObjectId::ENCODED_LEN }> for ObjectId {
    fn proof_key(self) -> [u8; ObjectId::ENCODED_LEN] {
        self.index_key()
    }
}

/// Identity lookup and discovery order have different owners. The index stores
/// only stable ordinals; moving the funded output buffer never changes them.
pub(super) struct ProofMap<'a, K, V, const N: usize = 16> {
    entries: ProofVec<'a, (K, V)>,
    index: crate::kernel::allocation::fixed_index::FixedIndex<usize, N>,
}

impl<'a, K, V, const N: usize> ProofMap<'a, K, V, N> {
    pub(super) fn new(meter: &ProofMeter<'a>) -> Self {
        Self {
            entries: ProofVec::new(meter),
            index: Default::default(),
        }
    }
}

impl<'a, K, V, const N: usize> std::ops::Deref for ProofMap<'a, K, V, N> {
    type Target = ProofVec<'a, (K, V)>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl<K: ProofKey<N>, V, const N: usize> ProofMap<'_, K, V, N> {
    pub(super) fn position(&self, key: K) -> Result<Option<usize>, crate::FsError> {
        self.entries.meter.with_accounting(|accounting| {
            self.index
                .get(key.proof_key(), accounting)
                .map(|position| position.copied())
        })
    }
}

pub(super) fn map_get<'v, K: ProofKey<N>, V, const N: usize>(
    entries: &'v ProofMap<'_, K, V, N>,
    key: &K,
) -> Result<Option<&'v V>, crate::FsError> {
    Ok(entries
        .position(*key)?
        .map(|position| &entries.values[position].1))
}

pub(super) fn map_insert<K: ProofKey<N>, V, const N: usize>(
    entries: &mut ProofMap<'_, K, V, N>,
    key: K,
    value: V,
) -> Result<Option<V>, crate::FsError> {
    if let Some(position) = entries.position(key)? {
        let copied = WorkCounters {
            bytes_copied: usize_to_u64(size_of::<V>()),
            ..WorkCounters::UNCHARGED
        };
        entries.entries.meter.admit(copied)?;
        let previous = std::mem::replace(&mut entries.entries.values[position].1, value);
        entries.entries.meter.record_completed(copied)?;
        return Ok(Some(previous));
    }

    // Reserve outside with_accounting: a value may own a ProofGuard whose
    // destructor needs that same accounting mutex. Refusal must drop it outside.
    entries.entries.ensure_capacity()?;
    let position = entries.entries.values.len();
    let mut candidate = Some((key, value));
    let mut appended = false;
    let result = entries.entries.meter.with_accounting(|accounting| {
        entries
            .index
            .get_or_try_insert_with(key.proof_key(), accounting, |accounting| {
                let copied = WorkCounters {
                    bytes_copied: usize_to_u64(size_of::<(K, V)>()),
                    ..WorkCounters::UNCHARGED
                };
                accounting.admit(copied)?;
                // All fallible node/buffer admission precedes this append. The
                // factory publishes its ordinal only after completed work is recorded.
                let value = candidate.take().ok_or(crate::ObjectStoreError::Corrupt)?;
                entries.entries.values.push(value);
                appended = true;
                accounting.record_completed(copied)?;
                Ok(position)
            })
            .map(|_| ())
    });
    if result.is_err() && appended {
        drop(entries.entries.values.pop());
    }
    result?;
    Ok(None)
}

const ORDERED_OBJECT_KEY_BYTES: usize = ObjectId::ENCODED_LEN + 1;

/// Only membership is retained while walking. Required canonical output is
/// materialized once, without shifting any previously authenticated identity.
pub(super) struct ProofSet<'a> {
    index: crate::kernel::allocation::fixed_index::FixedIndex<ObjectId, ORDERED_OBJECT_KEY_BYTES>,
    count: usize,
    meter: ProofMeter<'a>,
}

impl<'a> ProofSet<'a> {
    pub(super) fn new(meter: &ProofMeter<'a>) -> Self {
        Self {
            index: Default::default(),
            count: 0,
            meter: meter.clone(),
        }
    }

    fn key(object: ObjectId) -> [u8; ORDERED_OBJECT_KEY_BYTES] {
        // ObjectId::Ord is kind, digest, then Option<complete storage>. The
        // wire tag's seal bit precedes the digest, so it is not an ordered key.
        let encoded = object.index_key();
        let mut key = [0; ORDERED_OBJECT_KEY_BYTES];
        key[..33].copy_from_slice(&encoded[..33]);
        key[0] &= 127;
        key[33] = u8::from(object.storage.is_some());
        key[34..].copy_from_slice(&encoded[33..]);
        key
    }

    pub(super) fn len(&self) -> usize {
        self.count
    }

    pub(super) fn contains(&self, object: ObjectId) -> Result<bool, crate::FsError> {
        self.meter.with_accounting(|accounting| {
            self.index
                .get(Self::key(object), accounting)
                .map(|value| value.is_some())
        })
    }

    pub(super) fn insert(&mut self, object: ObjectId) -> Result<bool, crate::FsError> {
        let mut next_count = None;
        self.meter.with_accounting(|accounting| {
            self.index
                .get_or_try_insert_with(Self::key(object), accounting, |accounting| {
                    let count = self.count.checked_add(1).ok_or(WorkError::Overflow)?;
                    let copied = WorkCounters {
                        bytes_copied: usize_to_u64(size_of::<ObjectId>()),
                        ..WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    accounting.record_completed(copied)?;
                    next_count = Some(count);
                    Ok(object)
                })
                .map(|_| ())
        })?;
        if let Some(count) = next_count {
            self.count = count;
        }
        Ok(next_count.is_some())
    }

    pub(super) fn into_parts(self) -> Result<(Vec<ObjectId>, ProofGuard), crate::FsError> {
        let mut objects = ProofVec::new(&self.meter);
        let mut after = None;
        loop {
            let next = self.meter.with_accounting(|accounting| {
                let entry = match after {
                    None => self.index.first(accounting)?,
                    Some(key) => self.index.seek_after(key, accounting)?,
                };
                Ok(entry.map(|(key, object)| (*key, *object)))
            })?;
            let Some((key, object)) = next else {
                break;
            };
            objects.push(object)?;
            after = Some(key);
        }
        objects.into_parts()
    }
}

pub(super) fn set_insert(
    entries: &mut ProofSet<'_>,
    key: ObjectId,
) -> Result<bool, crate::FsError> {
    entries.insert(key)
}

#[cfg(test)]
#[cfg(all(feature = "memory", not(loom)))]
#[path = "memory/tests.rs"]
mod tests;
