//! Journaled pathwise application and crash recovery.

use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, next_revision};
use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use thiserror::Error;

const JOURNAL_VERSION: u32 = 2;

/// Native preparation installs with this prefix require an absent live binding.
#[cfg(not(target_arch = "wasm32"))]
pub const NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX: &[u8] = b"acyclic.native.prepare-absent/v1\0";

/// A funded invocation-local cancellation view of the same original engine.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeInvocationView {
    cancellation: crate::CancellationToken,
    self_ref: std::sync::Weak<Self>,
    _metadata: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeInvocationView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeInvocationView")
            .field("cancelled", &self.cancellation.is_cancelled()).finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeInvocationView {
    pub(crate) fn check(&self) -> Result<(), crate::DemandError> {
        self.cancellation.check().map_err(|_| crate::DemandError::Cancelled)
    }

    pub(crate) fn cancellation(&self) -> &crate::CancellationToken { &self.cancellation }

    pub(crate) fn owns_resource(&self, resource: Option<&std::sync::Arc<NativeResourceAllowance>>) -> bool {
        match (self._metadata.work().resource.as_ref(), resource) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        let mut original = self._metadata.work().state();
        original.with_invocation_scope(Some(self.self_ref.clone()), |work| {
            work.with_resource_scope(self._metadata.work().resource.as_ref().map(std::sync::Arc::downgrade), operation)
        })
    }

    fn with_io<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
        self.with_original_leaf(|original| {
            operation(original).map_err(|error| original.record_native_io_failure(error))
        })
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
pub(crate) struct NativeSourceExecutionView {
    _lease: crate::native_mount::NativeSourceIoLease,
    _metadata: NativeAllocation,
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl std::fmt::Debug for NativeSourceExecutionView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeSourceExecutionView").finish_non_exhaustive()
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl NativeSourceExecutionView {
    fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        self._metadata.work().with_original_leaf(operation)
    }

    fn with_io<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
        self.with_original_leaf(|original| {
            operation(original).map_err(|error| original.record_native_io_failure(error))
        })
    }
}

/// A resource allowance backed by the original engine.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub(crate) struct NativeResourceAllowance {
    state: std::sync::Mutex<NativeResourceState>,
    self_ref: std::sync::Weak<Self>,
    parent: Option<std::sync::Arc<Self>>,
    _root_only_metadata_guard: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct NativeResourceState {
    work: crate::WorkCounters,
    budget: crate::WorkBudget,
    pending: crate::WorkCounters,
    live: u64,
    reserved_allocation_bytes: u64,
    poisoned: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
pub(crate) enum NativeResourceCredit {
    Items, ReturnedItems, Copy, ReadOperations, WriteOperations, DurabilityOperations, ReadBytes, WriteBytes, AllocationOperations,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceCredit {
    pub(crate) fn counter(self, work: &mut crate::WorkCounters) -> &mut u64 {
        match self {
            Self::Items => &mut work.items_examined,
            Self::ReturnedItems => &mut work.items_returned,
            Self::Copy => &mut work.bytes_copied,
            Self::ReadOperations => &mut work.backend_read_operations,
            Self::WriteOperations => &mut work.backend_write_operations,
            Self::DurabilityOperations => &mut work.durability_operations,
            Self::ReadBytes => &mut work.source_bytes_read,
            Self::WriteBytes => &mut work.output_bytes,
            Self::AllocationOperations => &mut work.allocation_operations,
        }
    }

    pub(crate) fn delta(self, amount: u64) -> crate::WorkCounters {
        let mut delta = crate::WorkCounters::UNCHARGED;
        *self.counter(&mut delta) = amount;
        delta
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceAllowance {
    fn retirement_state(&self) -> std::sync::MutexGuard<'_, NativeResourceState> {
        self.state.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.poisoned = true;
            state
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, NativeResourceState>, crate::DemandError> {
        let state = self.state.lock().map_err(|_| crate::DemandError::OriginalWorkPoisoned)?;
        if state.poisoned { return Err(crate::DemandError::OriginalWorkPoisoned); }
        Ok(state)
    }

    pub(crate) fn reserve<T>(&self, maximum: crate::WorkCounters, bytes: u64, excluded: Option<&Self>, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) { return operation(); }
        let mut own = || {
            let mut state = self.lock()?;
            let delta = if maximum.allocation_operations == 0 { maximum } else { Self::admission_delta(&state, maximum, bytes)? };
            state.work.admit(&delta, &state.pending.remaining(state.budget).map_err(crate::DemandError::from)?)?;
            let pending = state.pending.checked_add(maximum)?;
            let reserved = state.reserved_allocation_bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
            let value = operation()?;
            state.pending = pending;
            state.reserved_allocation_bytes = reserved;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.reserve(maximum, bytes, excluded, &mut own),
            None => own(),
        }
    }

    pub(crate) fn refund_work(&self, unused: crate::WorkCounters) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.refund_work(unused));
        let mut state = self.retirement_state();
        match unused.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => { state.poisoned = true; return Err(error.into()); }
        }
        parent
    }

    pub(crate) fn complete_work(&self, actual: crate::WorkCounters) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.complete_work(actual));
        let mut state = self.retirement_state();
        let pending = actual.remaining(state.pending);
        state.work.try_add_assign(&actual)?;
        match pending {
            Ok(pending) => state.pending = pending,
            Err(error) => { state.poisoned = true; return Err(error.into()); }
        }
        let completed = if state.poisoned {
            Err(crate::DemandError::OriginalWorkPoisoned)
        } else {
            state.work.verify(state.pending.remaining(state.budget).map_err(crate::DemandError::from)?).map_err(crate::DemandError::from)
        };
        parent.and(completed)
    }

    fn start_reserved_work<T>(
        &self, actual: crate::WorkCounters,
        operation: &mut dyn FnMut() -> Result<T, crate::DemandError>,
    ) -> Result<T, crate::DemandError> {
        let mut own = || {
            let mut state = self.lock()?;
            let pending = actual.remaining(state.pending)?;
            let work = state.work.checked_add(actual)?;
            work.verify(pending.remaining(state.budget)?)?;
            let value = operation()?;
            state.pending = pending;
            state.work = work;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.start_reserved_work(actual, &mut own),
            None => own(),
        }
    }

    fn local_delta(state: &NativeResourceState, mut delta: crate::WorkCounters, bytes: u64) -> Result<crate::WorkCounters, crate::DemandError> {
        // A global receipt's peak never attributes resource ownership.
        delta.peak_allocation_bytes = state.live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        Ok(delta)
    }

    fn admission_delta(state: &NativeResourceState, delta: crate::WorkCounters, bytes: u64) -> Result<crate::WorkCounters, crate::DemandError> {
        Self::local_delta(state, delta, bytes.checked_add(state.reserved_allocation_bytes).ok_or(crate::WorkError::Overflow)?)
    }

    pub(crate) fn admit(&self, delta: crate::WorkCounters, bytes: u64) -> Result<(), crate::DemandError> {
        if let Some(parent) = &self.parent { parent.admit(delta, bytes)?; }
        let state = self.lock()?;
        state.work.admit(&Self::admission_delta(&state, delta, bytes)?, &state.pending.remaining(state.budget).map_err(crate::DemandError::from)?)?;
        Ok(())
    }

    pub(crate) fn admission<T>(&self, delta: crate::WorkCounters, bytes: u64, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut own = || {
            let mut state = self.lock()?;
            let local = Self::local_delta(&state, delta, bytes)?;
            state.work.admit(&Self::admission_delta(&state, delta, bytes)?, &state.pending.remaining(state.budget).map_err(crate::DemandError::from)?)?;
            let value = operation()?;
            state.work.try_add_assign(&local)?;
            state.live += bytes;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.admission(delta, bytes, &mut own),
            None => own(),
        }
    }

    pub(crate) fn completed(&self, delta: crate::WorkCounters, bytes: u64) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.completed(delta, bytes));
        let mut state = self.retirement_state();
        let local = Self::local_delta(&state, delta, bytes)?;
        state.work.try_add_assign(&local)?;
        state.live += bytes;
        let result = if state.poisoned {
            Err(crate::DemandError::OriginalWorkPoisoned)
        } else {
            state.work.verify(state.pending.remaining(state.budget).map_err(crate::DemandError::from)?).map_err(crate::DemandError::from)
        };
        parent.and(result)
    }

    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.retire(bytes));
        let mut state = self.retirement_state();
        let Some(live) = state.live.checked_sub(bytes) else {
            state.poisoned = true;
            return Err(crate::DemandError::OriginalWorkPoisoned);
        };
        state.live = live;
        parent
    }

    pub(crate) fn remaining(&self) -> Result<crate::WorkBudget, crate::DemandError> {
        let state = self.lock()?;
        let mut remaining = state.work.remaining(state.pending.remaining(state.budget).map_err(crate::DemandError::from)?)?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
            .checked_sub(state.live).and_then(|bytes| bytes.checked_sub(state.reserved_allocation_bytes)).ok_or(crate::WorkError::Overflow)?;
        drop(state);
        if let Some(parent) = &self.parent { remaining = remaining.intersect_budget(parent.remaining()?); }
        Ok(remaining)
    }

    pub(crate) fn settle_allocation<T>(&self, bytes: u64, record: bool, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut own = || {
            let mut state = self.retirement_state();
            if record && state.poisoned { return Err(crate::DemandError::OriginalWorkPoisoned); }
            let delta = NativeResourceCredit::AllocationOperations.delta(1);
            let pending = delta.remaining(state.pending)?;
            let reserved = state.reserved_allocation_bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
            let (live, work) = if record {
                let live = state.live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                let work = state.work.checked_add(Self::local_delta(&state, delta, bytes)?)?;
                (live, work)
            } else { (state.live, state.work) };
            let value = operation()?;
            state.pending = pending;
            state.reserved_allocation_bytes = reserved;
            state.live = live;
            state.work = work;
            Ok(value)
        };
        match &self.parent {
            Some(parent) => parent.settle_allocation(bytes, record, &mut own),
            None => own(),
        }
    }

    fn contains_scope(&self, scope: &Self) -> bool {
        std::ptr::eq(self, scope) || self.parent.as_ref().is_some_and(|parent| parent.contains_scope(scope))
    }

    pub(crate) fn complete_credit(&self, kind: NativeResourceCredit, reserved: u64, actual: u64) -> Result<(), crate::DemandError> {
        let refunded = self.refund_credit(kind, reserved, None);
        let recorded = self.completed(kind.delta(actual), 0);
        if actual > reserved { return Err(crate::DemandError::InvalidRequest); }
        refunded.and(recorded)
    }

    pub(crate) fn refund_credit(&self, kind: NativeResourceCredit, count: u64, excluded: Option<&Self>) -> Result<(), crate::DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) { return Ok(()); }
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.refund_credit(kind, count, excluded));
        let mut state = self.retirement_state();
        let pending = kind.counter(&mut state.pending);
        let Some(remaining) = pending.checked_sub(count) else {
            state.poisoned = true;
            return Err(crate::DemandError::OriginalWorkPoisoned);
        };
        *pending = remaining;
        parent
    }

    fn complete_credit_excluding(&self, kind: NativeResourceCredit, count: u64, excluded: Option<&Self>) -> Result<(), crate::DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) { return Ok(()); }
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.complete_credit_excluding(kind, count, excluded));
        let mut state = self.retirement_state();
        let pending = kind.counter(&mut state.pending);
        let Some(remaining) = pending.checked_sub(count) else {
            state.poisoned = true;
            return Err(crate::DemandError::OriginalWorkPoisoned);
        };
        *pending = remaining;
        state.work.try_add_assign(&kind.delta(count))?;
        let completed = if state.poisoned {
            Err(crate::DemandError::OriginalWorkPoisoned)
        } else {
            state.work.verify(state.pending.remaining(state.budget).map_err(crate::DemandError::from)?).map_err(crate::DemandError::from)
        };
        parent.and(completed)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceAllowance {
    fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        let mut original = self._root_only_metadata_guard.meter.state();
        original.with_resource_scope(Some(self.self_ref.clone()), operation)
    }

    fn with_io<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
        self.with_original_leaf(|original| {
            operation(original).map_err(|error| original.record_native_io_failure(error))
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
macro_rules! impl_original_native_read_meter {
    ($owner:ty $(, $allocation_owner:expr)?) => {
impl acyclic_native_runtime::NativeReadMeter for $owner {
    $(fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> { ($allocation_owner)(self) })?

    fn check(&self) -> std::io::Result<()> { self.with_io(|work| work.check()) }

    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.reserve(crate::demand::DemandWork::operation_credit(kind), count).map_err(|error| work.record_native_io_failure(error)))
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.with_original_leaf(|work| work.refund(crate::demand::DemandWork::operation_credit(kind), count).expect("original reservation is balanced"));
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.complete(crate::demand::DemandWork::operation_credit(kind), 1, 1).map_err(|error| work.record_native_io_failure(error)))
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.admit(crate::WorkCounters { output_bytes: maximum as u64, ..crate::WorkCounters::UNCHARGED }).map_err(|error| work.record_native_io_failure(error)))
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.reserve(crate::demand::DemandWork::byte_credit(kind)?, maximum).map_err(|error| work.record_native_io_failure(error)))
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.with_original_leaf(|work| work.refund(crate::demand::DemandWork::byte_credit(kind).expect("durability has no bytes"), maximum).expect("original reservation is balanced"));
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.complete(crate::demand::DemandWork::byte_credit(kind)?, maximum, actual).map_err(|error| work.record_native_io_failure(error)))
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.claim_native_owned(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED, &None))
    }
    fn reserve_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.reserve_native_allocation(bytes))
    }
    fn cancel_reserved_allocation(&self, bytes: u64) {
        self.with_original_leaf(|work| {
            if let Err(error) = work.cancel_reserved_native_allocation(bytes) {
                work.record_native_io_failure(error);
                work.poison_native_original_work();
            }
        });
    }
    fn record_reserved_allocation(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.record_reserved_native_allocation(bytes))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.with_original_leaf(|work| work.record_native_io_failure(crate::DemandError::Allocation(error)))
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.with_io(|work| work.charge(crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED }))
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.claim_native_owned(bytes, 0, crate::WorkCounters::UNCHARGED, &None))
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self.with_original_leaf(|work| {
            let (claimed, result) = work.record_native_owned_completed(bytes, &None);
            (claimed, result.map_err(|error| work.record_native_io_failure(error)))
        })
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.with_io(|work| work.admit(crate::WorkCounters {
            bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED
        }))
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters {
            bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED
        }))
    }
    fn examine(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.with_io(|work| work.charge(crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED }))
    }
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.with_io(|work| work.admit(crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED }))
    }
    fn record_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED }))
    }
    fn release(&self, bytes: u64) {
        self.with_original_leaf(|work| {
            if let Err(error) = work.release_native_owned(bytes, &None) {
                work.record_native_io_failure(error);
                work.poison_native_original_work();
            }
        });
    }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.with_io(|work| work.admit(crate::WorkCounters { source_bytes_read: maximum as u64, ..crate::WorkCounters::UNCHARGED }))
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        self.with_io(|work| {
            work.admit(crate::WorkCounters { source_bytes_read: maximum as u64, ..crate::WorkCounters::UNCHARGED })?;
            work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED })
        })
    }
    fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
        self.with_io(|work| work.admit(crate::WorkCounters { backend_read_operations: additional, ..crate::WorkCounters::UNCHARGED }))
    }
    fn record_operations(&self, completed: u64) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters { backend_read_operations: completed, ..crate::WorkCounters::UNCHARGED }))
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters { source_bytes_read: bytes as u64, ..crate::WorkCounters::UNCHARGED }))
    }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        self.with_io(|work| {
            work.admit(crate::WorkCounters { output_bytes: maximum as u64, ..crate::WorkCounters::UNCHARGED })?;
            work.charge(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED })
        })
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters { output_bytes: bytes as u64, ..crate::WorkCounters::UNCHARGED }))
    }
    fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
        self.with_io(|work| work.admit(crate::WorkCounters { backend_write_operations: additional, ..crate::WorkCounters::UNCHARGED }))
    }
    fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
        self.with_io(|work| work.record(crate::WorkCounters { backend_write_operations: completed, ..crate::WorkCounters::UNCHARGED }))
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        self.with_io(|work| work.charge(crate::WorkCounters { durability_operations: 1, ..crate::WorkCounters::UNCHARGED }))
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.with_io(|work| crate::native_host::NativeHostWork::reserve_cleanup_write(work))
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        self.with_original_leaf(|work| crate::native_host::NativeHostWork::cancel_cleanup_write(work, reservation));
    }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        self.with_io(|work| crate::native_host::NativeHostWork::attempt_cleanup_write(work, reservation, || ()).1)
    }
}
    };
}

#[cfg(not(target_arch = "wasm32"))]
impl_original_native_read_meter!(NativeResourceAllowance);
#[cfg(not(target_arch = "wasm32"))]
impl_original_native_read_meter!(NativeInvocationView);
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl_original_native_read_meter!(NativeSourceExecutionView, |owner: &NativeSourceExecutionView| Some(owner._metadata.work().runtime_meter_owner()));

/// Retains an allocation's resource ownership through physical retirement.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeResourceRetirement {
    resource: Option<std::sync::Arc<NativeResourceAllowance>>,
    _capture: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
    bytes: u64,
    original: std::sync::Arc<crate::demand::NativeDemandReadMeter>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceRetirement {
    pub(crate) const fn bytes(&self) -> u64 { self.bytes }
    pub(crate) fn original_meter(&self) -> &std::sync::Arc<crate::demand::NativeDemandReadMeter> { &self.original }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeResourceRetirement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeResourceRetirement")
            .field("bytes", &self.bytes).field("resource_scoped", &self.resource.is_some()).finish()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeResourceRetirement {
    fn drop(&mut self) {
        if let Some(resource) = &self.resource {
            if let Err(error) = resource.retire(self.bytes) {
                let mut original = self.original.lock();
                original.record_native_io_failure(error);
                original.poison_native_original_work();
            }
        }
    }
}

/// Shared accounting boundary for native host and durable journal work.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeMaterializationWork {
    pub(crate) resource: Option<std::sync::Arc<NativeResourceAllowance>>,
    pub(crate) capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    pub(crate) invocation: Option<std::sync::Arc<NativeInvocationView>>,
    #[cfg(feature = "native-mount")]
    pub(crate) execution: Option<std::sync::Arc<NativeSourceExecutionView>>,
    pub(crate) state: std::sync::Arc<crate::demand::NativeDemandReadMeter>,
    pub(crate) cancellation: crate::CancellationToken,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeMaterializationLeaf<'a> {
    original: std::sync::MutexGuard<'a, crate::demand::DemandWork>,
    previous: Option<crate::demand::NativeMaterializationLeafContext>,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::ops::Deref for NativeMaterializationLeaf<'_> {
    type Target = crate::demand::DemandWork;
    fn deref(&self) -> &Self::Target { &self.original }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::ops::DerefMut for NativeMaterializationLeaf<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.original }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeMaterializationLeaf<'_> {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.take() {
            self.original.restore_materialization_leaf(previous);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMaterializationWork {
    /// Canonical logical allocation body needed before constructing a meter.
    pub const fn accounting_state_bytes() -> u64 { std::mem::size_of::<crate::demand::NativeDemandReadMeter>() as u64 }

    /// Creates one operation-wide native and journal accounting boundary.
    pub fn new(budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<Self, (NativeTreeMaterializationError, crate::WorkCounters)> {
        let mut state = crate::demand::DemandWork::new(budget, cancellation);
        state.allocate(Self::accounting_state_bytes())
            .map_err(|error| (native_materialization_work_error(error), state.work))?;
        Ok(Self {
            state: std::sync::Arc::new(crate::demand::NativeDemandReadMeter(std::sync::Mutex::new(state))),
            cancellation: cancellation.clone(),
            resource: None,
            capture_origin: None,
            invocation: None,
            #[cfg(feature = "native-mount")]
            execution: None,
        })
    }

    /// Borrows this invocation's linked cancellation token.
    pub fn cancellation(&self) -> &crate::CancellationToken {
        self.invocation.as_ref().map_or(&self.cancellation, |view| view.cancellation())
    }

    /// Creates an owned local stop on the same original engine and resource.
    pub fn invocation_view(&self) -> Result<(Self, crate::CancellationToken), NativeTreeMaterializationError> {
        let cancellation = crate::CancellationToken::linked_child_with_native_work(self.cancellation(), self)?;
        let metadata = self.claim_allocation(std::mem::size_of::<NativeInvocationView>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let view = std::sync::Arc::new_cyclic(|self_ref| NativeInvocationView {
            cancellation: cancellation.clone(),
            self_ref: self_ref.clone(), _metadata: metadata,
        });
        let mut work = self.allocation_work();
        work.invocation = Some(view);
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            work = work.with_source_io_lease(execution._lease.clone())?;
        }
        Ok((work, cancellation))
    }

    /// Tests intrinsic original-ledger identity without locking or admission.
    #[must_use]
    pub fn shares_ledger(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.state, &other.state)
    }

    /// Pure identity of the original ledger and its exact resource attribution.
    #[must_use]
    pub fn shares_allocation_context(&self, other: &Self) -> bool {
        self.shares_ledger(other) && self.shares_resource_allowance(other)
    }

    pub(crate) fn shares_resource_allowance(&self, other: &Self) -> bool {
        match (&self.resource, &other.resource) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Retains the original invocation for native queue completion.
    pub(crate) fn original_demand_work(&self) -> crate::demand::OriginalDemandWork {
        crate::demand::OriginalDemandWork::from_native_materialization_work(self)
    }

    pub(crate) fn from_original_demand_work(work: &crate::demand::OriginalDemandWork) -> Result<Self, crate::DemandError> {
        let cancellation = work.with_original_leaf(|state| state.cancellation().clone())?;
        Ok(Self {
            state: std::sync::Arc::clone(work.meter()), cancellation,
            resource: work.resource_allowance().cloned(), capture_origin: work.capture_origin().cloned(),
            invocation: work.invocation_view_state().cloned(),
            #[cfg(feature = "native-mount")]
            execution: work.source_execution_view().cloned(),
        })
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn from_meter(state: std::sync::Arc<crate::demand::NativeDemandReadMeter>) -> Self {
        let cancellation = state.lock().cancellation().clone();
        Self { state, cancellation, resource: None, capture_origin: None, invocation: None, execution: None }
    }

    fn state(&self) -> NativeMaterializationLeaf<'_> {
        let mut original = self.state.lock();
        let previous = original.enter_materialization_leaf(self.invocation.as_ref().map(std::sync::Arc::downgrade));
        NativeMaterializationLeaf { original, previous: Some(previous) }
    }

    /// Borrows one accounting leaf of the original invocation.
    pub(crate) fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        let mut original = self.state();
        original.with_resource_scope(self.resource.as_ref().map(std::sync::Arc::downgrade), |work| {
            work.with_capture(self.capture_origin.clone(), operation)
        })
    }

    /// Borrows the existing runtime callback meter.
    pub(crate) fn native_read_meter(&self) -> &std::sync::Arc<crate::demand::NativeDemandReadMeter> {
        &self.state
    }

    pub(crate) fn resource_allowance(&self) -> Option<&std::sync::Arc<NativeResourceAllowance>> {
        self.resource.as_ref()
    }

    pub(crate) fn capture_origin(&self) -> Option<&std::sync::Weak<crate::demand::CaptureTransientCustody>> {
        self.capture_origin.as_ref()
    }

    pub(crate) fn invocation_view_state(&self) -> Option<&std::sync::Arc<NativeInvocationView>> {
        self.invocation.as_ref()
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn source_execution_view(&self) -> Option<&std::sync::Arc<NativeSourceExecutionView>> {
        self.execution.as_ref()
    }

    /// Keeps the original scope and stop without retaining source execution.
    pub(crate) fn allocation_work(&self) -> Self {
        let work = self.clone();
        #[cfg(feature = "native-mount")]
        let work = Self { execution: None, ..work };
        work
    }

    /// Pins an admitted source job for this execution view.
    #[cfg(feature = "native-mount")]
    pub fn with_source_io_lease(
        &self, lease: crate::native_mount::NativeSourceIoLease,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let mut work = self.allocation_work();
        if let Some(source) = lease.original_work() {
            if !self.shares_ledger(source) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            work.resource = match (&self.resource, &source.resource) {
                (None, resource) => resource.clone(),
                (resource, None) => resource.clone(),
                (Some(current), Some(source)) if current.contains_scope(source) => Some(current.clone()),
                (Some(current), Some(source)) if source.contains_scope(current) => Some(source.clone()),
                _ => return Err(NativeTreeMaterializationError::InvalidLayout),
            };
        }
        if let Some(invocation) = &work.invocation {
            if !invocation.owns_resource(work.resource.as_ref()) {
                let cancellation = invocation.cancellation.clone();
                let metadata = work.claim_allocation(std::mem::size_of::<NativeInvocationView>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
                let view = std::sync::Arc::new_cyclic(|self_ref| NativeInvocationView {
                    cancellation,
                    self_ref: self_ref.clone(), _metadata: metadata,
                });
                work.invocation = Some(view);
            }
        }
        let metadata = work.claim_allocation(std::mem::size_of::<NativeSourceExecutionView>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let view = std::sync::Arc::new(NativeSourceExecutionView {
            _lease: lease, _metadata: metadata,
        });
        work.execution = Some(view);
        Ok(work)
    }

    /// Associates an authenticated capture with its scratch admissions.
    pub(crate) fn with_capture_custody(
        &self, custody: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let mut view = self.allocation_work();
        view.capture_origin = Some(std::sync::Arc::downgrade(custody));
        #[cfg(feature = "native-mount")]
        if let Some(execution) = &self.execution {
            view = view.with_source_io_lease(execution._lease.clone())?;
        }
        Ok(view)
    }

    /// The authentic original grant, or this resource's immutable ceiling.
    #[must_use]
    pub fn budget(&self) -> crate::WorkBudget {
        match &self.resource {
            Some(resource) => match resource.state.lock() {
                Ok(state) => state.budget,
                Err(poisoned) => poisoned.into_inner().budget,
            },
            None => self.state().original_budget(),
        }
    }

    /// Limits resource work and live storage on the original ledger.
    pub fn native_volume_work(&self, budget: crate::WorkBudget) -> Result<Self, NativeTreeMaterializationError> {
        let metadata = self.claim_root_metadata(std::mem::size_of::<NativeResourceAllowance>() as u64)?;
        let resource = std::sync::Arc::new_cyclic(|self_ref| NativeResourceAllowance {
            state: std::sync::Mutex::new(NativeResourceState {
                work: crate::WorkCounters::UNCHARGED, budget,
                pending: crate::WorkCounters::UNCHARGED, live: 0, reserved_allocation_bytes: 0, poisoned: false,
            }),
            self_ref: self_ref.clone(),
            parent: self.resource.clone(),
            _root_only_metadata_guard: metadata,
        });
        let work = Self {
            state: std::sync::Arc::clone(&self.state), cancellation: self.cancellation.clone(),
            resource: Some(resource), capture_origin: None,
            invocation: self.invocation.clone(),
            #[cfg(feature = "native-mount")]
            execution: None,
        };
        #[cfg(feature = "native-mount")]
        let work = if let Some(execution) = &self.execution {
            work.with_source_io_lease(execution._lease.clone())?
        } else { work };
        Ok(work)
    }

    /// Borrows the original operation's native runtime admission interface.
    #[must_use]
    pub fn runtime_meter(&self) -> &dyn acyclic_native_runtime::NativeReadMeter {
        #[cfg(feature = "native-mount")]
        if let Some(view) = &self.execution { return view.as_ref(); }
        if let Some(view) = &self.invocation {
            if view.owns_resource(self.resource.as_ref()) { return view.as_ref(); }
        }
        match &self.resource {
            Some(resource) => resource.as_ref(),
            None => self.state.as_ref(),
        }
    }

    /// Pins the same original runtime meter for an asynchronous native queue.
    #[must_use]
    pub fn runtime_meter_owner(&self) -> std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        #[cfg(feature = "native-mount")]
        if let Some(view) = &self.execution {
            return std::sync::Arc::clone(view) as std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>;
        }
        if let Some(view) = &self.invocation {
            if view.owns_resource(self.resource.as_ref()) {
                return std::sync::Arc::clone(view) as std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>;
            }
        }
        match &self.resource {
            Some(resource) => std::sync::Arc::clone(resource) as std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
            None => std::sync::Arc::clone(&self.state) as std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        }
    }

    /// Recovers the first original runtime failure, or the supplied IO error.
    pub fn runtime_error(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        match self.state().take_native_io_failure() {
            Some(cause) => native_materialization_work_error(cause),
            None => NativeTreeMaterializationError::from_runtime_error(error),
        }
    }

    /// Copies a borrowed payload into an admitted native buffer.
    pub fn copy_bytes(&self, bytes: &[u8]) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        let mut writer = NativeImageWriter::new(self, false)?;
        let copied = writer.append(bytes)?;
        if copied != bytes.len() {
            self.admit(crate::WorkCounters { bytes_copied: 1, ..crate::WorkCounters::UNCHARGED })?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok((writer.image, writer.allocation))
    }

    /// Writes borrowed bytes through this original work's SDK I/O boundary.
    pub fn write_owned_bytes<W: std::io::Write + ?Sized>(
        &self, output: &mut W, bytes: &[u8],
    ) -> Result<(), NativeTreeMaterializationError> {
        use acyclic_native_runtime::{
            NativeOperationKind, NativeOperationReservation, NativeByteReservation,
        };
        let mut transfer = NativeByteReservation::new(
            self.runtime_meter_owner(), NativeOperationKind::Write, bytes.len(),
        ).map_err(|error| self.runtime_error(error))?;
        let mut offset = 0;
        while offset < bytes.len() {
            let mut operation = NativeOperationReservation::new(
                self.runtime_meter(), NativeOperationKind::Write, 1,
            ).map_err(|error| self.runtime_error(error))?;
            let (written, recorded) = operation.attempt(|| output.write(&bytes[offset..]));
            let count = match written {
                Ok(count) if count <= bytes.len() - offset => count,
                Ok(_) => return Err(NativeTreeMaterializationError::InvalidLayout),
                Err(error) => {
                    recorded.map_err(|error| self.runtime_error(error))?;
                    if error.kind() == std::io::ErrorKind::Interrupted { continue; }
                    return Err(self.runtime_error(error));
                }
            };
            let transferred = transfer.record_partial(count);
            recorded.map_err(|error| self.runtime_error(error))?;
            transferred.map_err(|error| self.runtime_error(error))?;
            if count == 0 {
                return Err(self.runtime_error(std::io::ErrorKind::WriteZero.into()));
            }
            offset += count;
        }
        Ok(())
    }

    /// Withholds additive completion limits on this same original and resource.
    pub fn reserve_work(&self, maximum: crate::WorkCounters) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        if maximum.allocation_operations != 0 || maximum.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state();
        let mut reserve = || state.reserve_native_work(maximum);
        match &self.resource {
            Some(resource) => resource.reserve(maximum, 0, None, &mut reserve),
            None => reserve(),
        }.map_err(native_materialization_work_error)?;
        Ok(NativeWorkReservation { meter: self.allocation_work(), remaining: maximum })
    }

    /// Admits and charges one actually attempted operation before its boundary.
    pub fn charge(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = match &self.resource {
            Some(resource) => resource.admission(delta, 0, &mut || state.charge(delta)),
            None => state.charge(delta),
        };
        drop(state);
        result.map_err(native_materialization_work_error)
    }

    /// Retains completed nested work with simultaneously live caller buffers.
    pub fn spent(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.spent(delta);
        let resource = self.resource.as_ref().map_or(Ok(()), |resource| resource.completed(delta, 0));
        drop(state);
        result.and(resource).map_err(native_materialization_work_error)
    }

    pub(crate) fn record_admitted(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.record(delta);
        let resource = self.resource.as_ref().map_or(Ok(()), |resource| resource.completed(delta, 0));
        drop(state);
        result.and(resource).map_err(native_materialization_work_error)
    }

    /// Pure existing-root membership, not permission or retained-byte adoption.
    pub(crate) fn owns_native_checkout_token(
        &self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        self.state().owns_native_checkout_token(token)
    }

    /// Obtains registered producer membership without an SDK loan.
    pub(crate) fn checkout_control_sink(&self, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state.checkout_control_witness(producer).ok_or(NativeTreeMaterializationError::InvalidLayout)
    }

    /// Registers the actual pre-funded constructor token on this original.
    pub(crate) fn register_native_checkout_control(
        &self, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        header_guard: &mut NativeAllocation,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state.register_native_checkout_control(self, producer, header_guard)
    }

    /// Constructs private controls through registered original custody.
    pub(crate) fn with_native_checkout_controls<T, E: From<NativeTreeMaterializationError>>(
        &self,
        operation: impl FnOnce(
            &mut crate::demand::NativeCheckoutControlSink<'_>,
            &mut crate::kernel::allocation::AllocationLedger,
            &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken,
        ) -> Result<T, E>,
    ) -> Result<T, E> {
        self.state.with_native_checkout_controls(operation)
            .map_err(|error| E::from(native_materialization_work_error(error)))?
    }

    /// The failure path must have destroyed all of the leaf's own backing.
    pub(crate) fn allocations<T, E: From<NativeTreeMaterializationError>>(&self, operation: impl FnOnce(&mut crate::kernel::allocation::AllocationLedger, &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken) -> Result<T, E>) -> Result<(T, NativeAllocation), E> {
        let mut state = self.state();
        if let Err(error) = state.reconcile_native_controls() {
            drop(state);
            return Err(E::from(NativeTreeMaterializationError::Work(error)));
        }
        let (allocations, work, budget, cancellation) = state.allocation_parts();
        let before = allocations.live_bytes();
        let result = operation(allocations, work, budget, cancellation);
        let Some(bytes) = allocations.live_bytes().checked_sub(before) else {
            drop(state);
            drop(result);
            return Err(E::from(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)));
        };
        match result {
            Ok(value) => {
                drop(state);
                Ok((value, NativeAllocation { meter: self.allocation_work(), bytes, capture: None }))
            }
            Err(error) => {
                let released = allocations.release(bytes);
                drop(state);
                released.map_err(|failure| E::from(failure.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))))?;
                Err(error)
            }
        }
    }

    /// Remaining cumulative allowance.
    pub fn remaining(&self) -> Result<crate::WorkBudget, NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.check().and_then(|()| state.remaining()).and_then(|remaining| {
            self.resource.as_ref().map_or(Ok(remaining), |resource| resource.remaining().map(|local| remaining.intersect_budget(local)))
        });
        drop(state);
        result.map_err(native_materialization_work_error)
    }

    /// Work spent by this invocation, not previous recovery invocations.
    pub fn receipt(&self) -> crate::WorkCounters { self.state().receipt() }

    /// Retains the durable work prefix once, without charging it to this call.
    pub fn set_original_work(&self, original: crate::WorkCounters) {
        self.state().original_work.get_or_insert(original);
    }

    /// Durable prefix plus actual work known before the next journal write.
    pub fn journal_work(&self) -> Result<crate::WorkCounters, NativeTreeMaterializationError> {
        let mut state = self.state();
        let work = state.receipt();
        Ok(state.original_work.unwrap_or_default().checked_add(work)?)
    }

    /// Checks a bounded operation without reporting work not yet performed.
    pub fn admit(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.admit(delta).and_then(|()| self.resource.as_ref().map_or(Ok(()), |resource| resource.admit(delta, 0)));
        drop(state);
        result.map_err(native_materialization_work_error)
    }

    /// Preflights additional owned logical capacity on the original live ledger.
    pub fn admit_retained(&self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.admit_retained(bytes).and_then(|()| self.resource.as_ref().map_or(Ok(()), |resource| resource.admit(crate::WorkCounters::UNCHARGED, bytes)));
        drop(state);
        result.map_err(native_materialization_work_error)
    }

    pub(crate) fn reserve_cleanup_write(&self) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        let mut state = self.state();
        let mut reserve = || state.reserve(crate::materializer::NativeResourceCredit::WriteOperations, 1).map(|()| 1);
        match &self.resource {
            Some(resource) => resource.reserve(NativeResourceCredit::WriteOperations.delta(1), 0, None, &mut reserve),
            None => reserve(),
        }.map_err(native_materialization_work_error)?;
        Ok(NativeCleanupWrite { meter: self.allocation_work(), additional_resource: None, reserved: true })
    }

    /// Admits an allocation and retains its ownership until deallocation.
    pub fn claim(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)
    }

    /// Claims buffer ownership and any immediate work on the original ledger.
    pub fn claim_allocation(&self, bytes: u64, operations: u64, delta: crate::WorkCounters) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let combined = delta.checked_add(crate::WorkCounters {
            allocation_operations: operations, ..crate::WorkCounters::UNCHARGED
        })?;
        let mut state = self.state();
        let mut claim = || state.claim_native_owned(bytes, operations, delta, &None);
        let result = match &self.resource {
            Some(resource) => resource.admission(combined, bytes, &mut claim),
            None => claim(),
        };
        drop(state);
        result.map_err(native_materialization_work_error)?;
        Ok(NativeAllocation { meter: self.allocation_work(), bytes, capture: None })
    }

    fn claim_root_metadata(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let root = Self { state: std::sync::Arc::clone(&self.state), cancellation: self.cancellation.clone(), resource: None, capture_origin: None, invocation: self.invocation.clone(), #[cfg(feature = "native-mount")] execution: None };
        root.claim_allocation(bytes, 1, crate::WorkCounters::UNCHARGED)
    }

    /// Funds only the actual capture token's compiled logical Self.
    pub(crate) fn claim_capture_custody_metadata(&self) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation(std::mem::size_of::<crate::demand::CaptureTransientCustody>() as u64, 1, crate::WorkCounters::UNCHARGED)
    }

    /// Funds only root-owned registered checkout-token bookkeeping.
    pub(crate) fn claim_checkout_token_metadata(&self) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_root_metadata(std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64)
    }

    /// Transfers a registered capture's paid scratch custody.
    pub(crate) fn transfer_capture_allocation(
        &self, bytes: u64, custody: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        custody.transfer_retained_bytes(bytes)?;
        Ok(NativeAllocation { meter: self.allocation_work(), bytes, capture: None })
    }

    /// Records a completed allocation with its original capture ownership.
    pub(crate) fn record_completed_allocation(
        &self, bytes: u64,
        capture: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
    ) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        let mut state = self.state();
        let (owned, result) = state.record_native_owned_completed(bytes, &None);
        let resource = self.resource.as_ref().map_or(Ok(()), |resource| resource.completed(crate::WorkCounters::UNCHARGED, owned));
        drop(state);
        (NativeAllocation { meter: self.allocation_work(), bytes: owned, capture }, result.and(resource).map_err(native_materialization_work_error))
    }

    /// Takes custody of an already measured buffer.
    pub fn retain_owned(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation(bytes, 0, crate::WorkCounters::UNCHARGED)
    }

    /// Formats actionable text into an actually admitted owned UTF-8 buffer.
    pub fn display<T: std::fmt::Display + ?Sized>(&self, value: &T) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        use std::io::Write as _;
        let mut writer = NativeImageWriter::new(self, true)?;
        if let Err(error) = write!(&mut writer, "{value}") {
            return Err(writer.failure.take().unwrap_or_else(|| error.into()));
        }
        let text = String::from_utf8(writer.image).map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
        Ok((text, writer.allocation))
    }

    /// Actual currently admitted live bytes, rather than historical peak.
    pub fn live_allocation_bytes(&self) -> u64 { self.state().live_allocation_bytes() }

    /// Observes the opened root allocation.
    pub fn root_identity(&self, path: &std::path::Path) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open(path, &mut &*self)?.identity())
    }

    /// Copies a lexical root and optional child into an admitted owned path.
    pub fn path(&self, root: &std::path::Path, leaf: Option<&std::path::Path>) -> Result<(std::path::PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        let copied = root.as_os_str().len().checked_add(leaf.map_or(0, |path| path.as_os_str().len())).ok_or(crate::WorkError::Overflow)?;
        let capacity = copied.checked_add(usize::from(leaf.is_some())).ok_or(crate::WorkError::Overflow)?;
        let copy = crate::WorkCounters { bytes_copied: copied as u64, ..crate::WorkCounters::default() };
        self.admit(copy)?;
        let allocation = self.claim(capacity as u64)?;
        let mut path = std::path::PathBuf::new();
        path.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        path.push(root);
        if let Some(leaf) = leaf { path.push(leaf); }
        self.spent(copy)?;
        Ok((path, allocation))
    }

    /// Opens a working directory through the expected actual root allocation.
    pub fn open_working_directory(&self, root: &std::path::Path, expected: crate::NativeRootIdentity, relative: &std::path::Path) -> Result<std::fs::File, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            if !matches!(component, std::path::Component::Normal(_) | std::path::Component::CurDir) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let held = crate::native_host::HostRoot::open(root, &mut &*self)?;
        if held.identity() != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }); }
        let directory = held.open_dir_held(relative, &mut &*self)?;
        let actual = self.root_identity(root)?;
        if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
        Ok(directory.into_std_file())
    }

    /// Maximum whole-file builtin read or UTF-8 write payload.
    pub const MAXIMUM_BUILTIN_BYTES: usize = 262_144;

    /// Reads an entire regular file through its witnessed root.
    pub async fn read_working_file<E, F, Fut>(
        &self, root: &Path, expected: crate::NativeRootIdentity, relative: &Path,
        maximum_bytes: usize, mut guard: F,
    ) -> Result<acyclic_stream::OwnedBytes, E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        self.validate_builtin_request(relative, maximum_bytes).map_err(E::from)?;
        let held = self.open_builtin_file(root, expected, relative,
            crate::native_host::NativeOpenOptions::Read, false, &mut guard).await?;
        self.read_builtin_held(&held, maximum_bytes, &mut guard).await
    }

    /// Writes bounded UTF-8 after checking the held file and preimage.
    #[allow(clippy::too_many_arguments, reason = "original root, edit preimage and caller guard must remain explicit")]
    pub async fn write_working_file<E, F, Fut>(
        &self, root: &Path, expected: crate::NativeRootIdentity, relative: &Path,
        content: &str, create_parents: bool, expected_sha256: Option<[u8; 32]>,
        mut guard: F,
    ) -> Result<(), E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        use sha2::{Digest as _, Sha256};
        self.validate_builtin_request(relative, Self::MAXIMUM_BUILTIN_BYTES).map_err(E::from)?;
        if content.len() > Self::MAXIMUM_BUILTIN_BYTES {
            return Err(E::from(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: Self::MAXIMUM_BUILTIN_BYTES as u64, actual: content.len() as u64,
            }));
        }
        let options = if expected_sha256.is_some() {
            crate::native_host::NativeOpenOptions::ReadWrite
        } else { crate::native_host::NativeOpenOptions::ReadWriteCreate };
        let held = self.open_builtin_file(root, expected, relative, options,
            create_parents && expected_sha256.is_none(), &mut guard).await?;
        let preimage_stamp = if let Some(expected_digest) = expected_sha256 {
            let stamp = crate::native_host::stat_file_with_work(&held.file, &mut &*self).map_err(E::from)?;
            let preimage = self.read_builtin_held(&held, Self::MAXIMUM_BUILTIN_BYTES, &mut guard).await?;
            self.charge(crate::WorkCounters { bytes_hashed: preimage.len() as u64,
                ..crate::WorkCounters::UNCHARGED }).map_err(E::from)?;
            let actual: [u8; 32] = Sha256::digest(&preimage).into();
            drop(preimage);
            if actual != expected_digest {
                return Err(E::from(NativeTreeMaterializationError::PreimageConflict));
            }
            Some(stamp)
        } else { None };
        guard().await?;
        held.validate(self).map_err(E::from)?;
        // The read phase advanced only this descriptor. Rewind it, never reopen.
        self.charge(crate::WorkCounters { backend_read_operations: 1,
            ..crate::WorkCounters::UNCHARGED }).map_err(E::from)?;
        std::io::Seek::rewind(&mut &held.file)
            .map_err(NativeTreeMaterializationError::from).map_err(E::from)?;
        let mut written = 0;
        while written < content.len() {
            guard().await?;
            held.validate(self).map_err(E::from)?;
            if written == 0 {
                if let Some(stamp) = &preimage_stamp {
                    let current = crate::native_host::stat_file_with_work(&held.file, &mut &*self).map_err(E::from)?;
                    if current.len() != stamp.len()
                        || current.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
                            != stamp.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
                    {
                        return Err(E::from(NativeTreeMaterializationError::PreimageConflict));
                    }
                }
            }
            let attempted = crate::native_host::write_builtin_file(&held.file, &content.as_bytes()[written..], self);
            let checked = guard().await;
            let count = match attempted {
                Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
                    checked?;
                    continue;
                }
                result => result.map_err(E::from)?,
            };
            checked?;
            if count == 0 {
                return Err(E::from(std::io::Error::from(std::io::ErrorKind::WriteZero).into()));
            }
            written += count;
            held.validate(self).map_err(E::from)?;
        }
        guard().await?;
        held.validate(self).map_err(E::from)?;
        if written == 0 {
            if let Some(stamp) = &preimage_stamp {
                let current = crate::native_host::stat_file_with_work(&held.file, &mut &*self).map_err(E::from)?;
                if current.len() != stamp.len()
                    || current.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
                        != stamp.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
                {
                    return Err(E::from(NativeTreeMaterializationError::PreimageConflict));
                }
            }
        }
        let truncated = crate::native_host::truncate_file_with_work(&held.file, content.len() as u64, &mut &*self);
        let checked = guard().await;
        truncated.map_err(E::from)?;
        checked?;
        held.validate(self).map_err(E::from)
    }

    fn validate_builtin_request(&self, relative: &Path, maximum_bytes: usize) -> Result<(), NativeTreeMaterializationError> {
        if !self.remaining()?.has_finite_allowances() {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        if maximum_bytes > Self::MAXIMUM_BUILTIN_BYTES {
            return Err(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: Self::MAXIMUM_BUILTIN_BYTES as u64, actual: maximum_bytes as u64,
            });
        }
        let text = relative.to_str().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        if text.is_empty() || text.contains(['\\', '\0'])
            || text.split('/').any(|component| component.is_empty() || component == "." || component == "..")
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        for component in relative.components() {
            self.charge(crate::WorkCounters { source_path_components: 1,
                ..crate::WorkCounters::UNCHARGED })?;
            if !matches!(component, std::path::Component::Normal(_)) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        Ok(())
    }

    async fn open_builtin_file<'a, E, F, Fut>(
        &self, root: &'a Path, expected: crate::NativeRootIdentity, relative: &'a Path,
        options: crate::native_host::NativeOpenOptions, create_parents: bool, guard: &mut F,
    ) -> Result<NativeBuiltinFile<'a>, E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        guard().await?;
        let held = crate::native_host::HostRoot::open(root, &mut &*self).map_err(E::from)?;
        if held.identity() != expected {
            return Err(E::from(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }));
        }
        let mut parent = held.open_dir_held(Path::new(""), &mut &*self).map_err(E::from)?;
        guard().await?;
        for component in relative.parent().unwrap_or_else(|| Path::new("")).components() {
            let std::path::Component::Normal(name) = component else {
                return Err(E::from(NativeTreeMaterializationError::InvalidLayout));
            };
            guard().await?;
            let opened = crate::native_host::open_builtin_directory_with_work(&parent, Path::new(name),
                create_parents, &mut &*self);
            let checked = guard().await;
            parent = opened.map_err(E::from)?;
            checked?;
            let actual = self.root_identity(root).map_err(E::from)?;
            if actual != expected {
                return Err(E::from(NativeTreeMaterializationError::AllocationChanged { expected, actual }));
            }
        }
        guard().await?;
        let parent_identity = crate::native_host::directory_identity(&parent, &mut &*self).map_err(E::from)?;
        native_builtin_parent(root, expected, relative, parent_identity, self).map_err(E::from)?;
        let leaf = Path::new(relative.file_name().ok_or_else(|| E::from(NativeTreeMaterializationError::InvalidLayout))?);
        let opened = crate::native_host::open_leaf_with_work(&parent, leaf, &options, &mut &*self);
        let checked = guard().await;
        let file = opened.map_err(E::from)?;
        checked?;
        crate::native_host::lock_builtin_file(&file,
            !matches!(options, crate::native_host::NativeOpenOptions::Read), self).map_err(E::from)?;
        guard().await?;
        let (file_identity, _) = crate::native_host::file_identity_and_length_with_work(&file, &mut &*self).map_err(E::from)?;
        let result = NativeBuiltinFile { root, relative, expected, parent_identity, file_identity, file };
        result.validate(self).map_err(E::from)?;
        Ok(result)
    }

    async fn read_builtin_held<E, F, Fut>(
        &self, held: &NativeBuiltinFile<'_>, maximum_bytes: usize, guard: &mut F,
    ) -> Result<acyclic_stream::OwnedBytes, E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        guard().await?;
        held.validate(self).map_err(E::from)?;
        let before = crate::native_host::stat_file_with_work(&held.file, &mut &*self).map_err(E::from)?;
        if before.len() > maximum_bytes as u64 {
            return Err(E::from(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: maximum_bytes as u64, actual: before.len(),
            }));
        }
        let capacity = before.len() as usize;
        let mut custody = self.claim(capacity as u64).map_err(E::from)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed).map_err(E::from)?;
        custody.resize_retained(bytes.capacity() as u64).map_err(E::from)?;
        let initialized = crate::WorkCounters {
            bytes_copied: bytes.capacity() as u64, ..crate::WorkCounters::UNCHARGED
        };
        self.admit(initialized).map_err(E::from)?;
        bytes.resize(bytes.capacity(), 0);
        self.spent(initialized).map_err(E::from)?;
        let mut bytes = acyclic_stream::PreparedOwnedBytes::new(
            bytes, custody, self.runtime_meter_owner(),
        ).map_err(|failure| E::from(match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => self.runtime_error(error),
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized
            => {
                NativeTreeMaterializationError::InvalidLayout
            }
        }))?;
        let mut filled = 0;
        while filled < capacity {
            guard().await?;
            held.validate(self).map_err(E::from)?;
            let attempted = crate::native_host::read_builtin_file(
                &held.file, &mut bytes.writable()[filled..capacity], self,
            );
            let checked = guard().await;
            let count = match attempted {
                Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
                    checked?;
                    continue;
                }
                result => result.map_err(E::from)?,
            };
            checked?;
            filled += count;
            if count == 0 {
                return Err(E::from(native_diagnostic(self,
                    format_args!("{} / {} (allocation {:?}, expected {} bytes, read {filled})", held.root.display(), held.relative.display(), held.file_identity, before.len()),
                    NativeTreeMaterializationError::ExternalMutation,
                ).map_err(E::from)?));
            }
        }
        held.validate(self).map_err(E::from)?;
        let after = crate::native_host::stat_file_with_work(&held.file, &mut &*self).map_err(E::from)?;
        if after.len() > maximum_bytes as u64 {
            return Err(E::from(NativeTreeMaterializationError::PayloadTooLarge {
                maximum: maximum_bytes as u64, actual: after.len(),
            }));
        }
        if after.len() != filled as u64 || before.len() != after.len()
            || before.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
                != after.modified().map_err(NativeTreeMaterializationError::from).map_err(E::from)?
        {
            return Err(E::from(native_diagnostic(self,
                format_args!("{} / {} (allocation {:?}, before {} bytes, after {} bytes)", held.root.display(), held.relative.display(), held.file_identity, before.len(), after.len()),
                NativeTreeMaterializationError::ExternalMutation,
            ).map_err(E::from)?));
        }
        guard().await?;
        bytes.freeze(filled).map_err(|bytes| {
            drop(bytes);
            E::from(NativeTreeMaterializationError::InvalidLayout)
        })
    }

    /// Creates or opens a service directory through its witnessed root.
    pub fn create_owned_directory(&self, root: &std::path::Path, expected: crate::NativeRootIdentity, relative: &std::path::Path, require_new: bool) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            if !matches!(component, std::path::Component::Normal(_)) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        }
        let held = crate::native_host::HostRoot::open(root, &mut &*self)?;
        if held.identity() != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }); }
        if relative.as_os_str().is_empty() {
            if require_new { return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into()); }
            return Ok(held.identity());
        }
        let base = held.open_dir_held(Path::new(""), &mut &*self)?;
        let parent = crate::native_host::create_directories_held(&base, relative.parent().unwrap_or_else(|| Path::new("")), &mut &*self)?;
        let leaf = Path::new(relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
        let directory = if require_new {
            crate::native_host::create_new_directory_held_with_work(&parent, leaf, &mut &*self)?
        } else {
            crate::native_host::create_directories_held(&parent, leaf, &mut &*self)?
        };
        crate::native_host::directory_identity(&directory, &mut &*self)
    }


}

#[cfg(not(target_arch = "wasm32"))]
struct NativeBuiltinFile<'a> {
    root: &'a Path,
    relative: &'a Path,
    expected: crate::NativeRootIdentity,
    parent_identity: crate::NativeRootIdentity,
    file_identity: crate::NativeRootIdentity,
    file: std::fs::File,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeBuiltinFile<'_> {
    fn validate(&self, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        let parent = native_builtin_parent(self.root, self.expected, self.relative, self.parent_identity, work)?;
        let leaf = Path::new(self.relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
        match crate::native_host::entry_identity_with_work(&parent, leaf, &mut &*work)? {
            Some(actual) if actual == self.file_identity => Ok(()),
            Some(actual) => Err(NativeTreeMaterializationError::AllocationChanged { expected: self.file_identity, actual }),
            None => Err(native_diagnostic(work,
                format_args!("{} / {} (missing original allocation {:?})", self.root.display(), self.relative.display(), self.file_identity),
                NativeTreeMaterializationError::ExternalMutation,
            )?),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_builtin_parent(
    path: &Path, expected: crate::NativeRootIdentity, relative: &Path,
    expected_parent: crate::NativeRootIdentity, work: &NativeMaterializationWork,
) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
    let root = crate::native_host::HostRoot::open(path, &mut &*work)?;
    if root.identity() != expected {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: root.identity() });
    }
    let parent = root.open_dir_held(relative.parent().unwrap_or_else(|| Path::new("")), &mut &*work)?;
    let actual = crate::native_host::directory_identity(&parent, &mut &*work)?;
    if actual != expected_parent {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual });
    }
    Ok(parent)
}

/// Additive quota custody for an already separately authorized SDK operation.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeWorkReservation {
    meter: NativeMaterializationWork,
    remaining: crate::WorkCounters,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeWorkReservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeWorkReservation").field("remaining", &self.remaining).finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeWorkReservation {
    /// Compares the original accounting ledger and exact resource attribution.
    #[must_use]
    pub fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.shares_allocation_context(work)
    }

    /// Returns this token's still-owned additive credit, not fresh allowance.
    #[must_use]
    pub const fn remaining_work(&self) -> crate::WorkCounters { self.remaining }

    /// Combines only already-withheld custody from this exact original family.
    pub(crate) fn absorb(&mut self, other: &mut Self) -> Result<(), NativeTreeMaterializationError> {
        let invocation_matches = match (&self.meter.invocation, &other.meter.invocation) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        };
        let capture_matches = match (&self.meter.capture_origin, &other.meter.capture_origin) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Weak::ptr_eq(left, right),
            _ => false,
        };
        if !self.meter.shares_allocation_context(&other.meter) || !invocation_matches || !capture_matches {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let remaining = self.remaining.checked_add(other.remaining)?;
        self.remaining = remaining;
        other.remaining = crate::WorkCounters::UNCHARGED;
        Ok(())
    }

    /// Borrows accepted cleanup credit and an optional caller wait token.
    pub(crate) fn loan<'a>(&'a mut self, wait: Option<&'a crate::CancellationToken>) -> NativeCleanupLoan<'a> {
        let original = self.meter.clone();
        NativeCleanupLoan { credit: parking_lot::Mutex::new(self), original, wait }
    }

    /// Transfers reserved work within the same aggregate allowance.
    pub(crate) fn partition_work(
        &mut self, maximum: crate::WorkCounters,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if maximum.allocation_operations != 0 || maximum.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let remaining = maximum.remaining(self.remaining)?;
        self.remaining = remaining;
        Ok(Self { meter: self.meter.clone(), remaining: maximum })
    }

    /// Records completed work before rejecting an overlarge prefix.
    pub fn record_completed(&mut self, actual: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        if actual.allocation_operations != 0 || actual.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let consumed = actual.intersect_budget(self.remaining);
        let excess = consumed.remaining(actual)?;
        let overrun = actual.remaining(self.remaining).map(|_| ());
        self.remaining = consumed.remaining(self.remaining)?;
        let mut state = self.meter.state();
        let credited = state.record_native_work(consumed);
        let received = state.record(excess);
        let resource_credit = self.meter.resource.as_ref()
            .map_or(Ok(()), |resource| resource.complete_work(consumed));
        let resource_receive = self.meter.resource.as_ref()
            .map_or(Ok(()), |resource| resource.completed(excess, 0));
        drop(state);
        credited.and(received).and(resource_credit).and(resource_receive)
            .map_err(native_materialization_work_error)?;
        overrun.map_err(Into::into)
    }
    /// Retains mandatory completed work even when its allowance was exceeded.
    pub(crate) fn record_deferred(&mut self, actual: crate::WorkCounters) {
        let Err(error) = self.record_completed(actual) else { return; };
        let mut state = self.meter.state();
        state.record_native_io_failure(match error {
            NativeTreeMaterializationError::Work(error) => crate::DemandError::Work(error),
            NativeTreeMaterializationError::Cancelled => crate::DemandError::Cancelled,
            NativeTreeMaterializationError::OriginalWorkPoisoned => crate::DemandError::OriginalWorkPoisoned,
            _ => crate::DemandError::InvalidRequest,
        });
        state.poison_native_original_work();
    }

    /// Charges only actual native attempt dimensions at their start boundary.
    fn started_io(&mut self, actual: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.try_started_io(actual).map_err(|error| match error {
            crate::DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
            error => native_materialization_work_error(error),
        })
    }

    fn try_started_io(&mut self, actual: crate::WorkCounters) -> Result<(), crate::DemandError> {
        let starts = crate::WorkCounters {
            backend_read_operations: actual.backend_read_operations,
            backend_write_operations: actual.backend_write_operations,
            durability_operations: actual.durability_operations,
            ..crate::WorkCounters::UNCHARGED
        };
        if actual != starts { return Err(crate::DemandError::InvalidRequest); }
        let remaining = actual.remaining(self.remaining)?;
        {
            let mut state = self.meter.state();
            let mut start = || state.start_native_reserved_work(actual);
            match &self.meter.resource {
                Some(resource) => resource.start_reserved_work(actual, &mut start),
                None => start(),
            }?;
        }
        self.remaining = remaining;
        Ok(())
    }

    /// Records mandatory disposal work immediately before execution.
    pub(crate) fn started_drop_io(&mut self, actual: crate::WorkCounters) {
        let Err(error) = self.try_started_io(actual) else { return; };
        let starts = crate::WorkCounters {
            backend_read_operations: actual.backend_read_operations,
            backend_write_operations: actual.backend_write_operations,
            durability_operations: actual.durability_operations,
            ..crate::WorkCounters::UNCHARGED
        };
        let credited = crate::WorkCounters {
            backend_read_operations: starts.backend_read_operations.min(self.remaining.backend_read_operations),
            backend_write_operations: starts.backend_write_operations.min(self.remaining.backend_write_operations),
            durability_operations: starts.durability_operations.min(self.remaining.durability_operations),
            ..crate::WorkCounters::UNCHARGED
        };
        let mut state = self.meter.state();
        state.record_native_io_failure(error);
        state.poison_native_original_work();
        match credited.remaining(self.remaining) {
            Ok(remaining) => self.remaining = remaining,
            Err(error) => { state.record_native_io_failure(error.into()); }
        }
        let original = state.record_native_work(credited);
        let scoped = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.complete_work(credited));
        if let Err(error) = original.and(scoped) { state.record_native_io_failure(error); }
        match credited.remaining(starts) {
            Ok(excess) => {
                let original = state.record(excess);
                let scoped = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.completed(excess, 0));
                if let Err(error) = original.and(scoped) { state.record_native_io_failure(error); }
            }
            Err(error) => { state.record_native_io_failure(error.into()); }
        }
    }


}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeWorkReservation {
    fn drop(&mut self) {
        if self.remaining == crate::WorkCounters::UNCHARGED { return; }
        let mut state = self.meter.state();
        let original = state.refund_native_work(self.remaining);
        let resource = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.refund_work(self.remaining));
        if let Err(error) = original.and(resource) {
            state.record_native_io_failure(error);
            state.poison_native_original_work();
        }
    }
}

/// A stack-only loan of the existing token, not another ledger or stop owner.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCleanupLoan<'a> {
    credit: parking_lot::Mutex<&'a mut NativeWorkReservation>,
    original: NativeMaterializationWork,
    wait: Option<&'a crate::CancellationToken>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCleanupLoan<'_> {
    pub(crate) fn original_work(&self) -> &NativeMaterializationWork { &self.original }
    pub(crate) fn cancellation(&self) -> Option<&crate::CancellationToken> { self.wait }
    pub(crate) fn check_wait(&self) -> Result<(), NativeTreeMaterializationError> {
        self.wait.map_or(Ok(()), |wait| {
            wait.check().map_err(|_| NativeTreeMaterializationError::Cancelled)
        })
    }
    pub(crate) fn remaining_work(&self) -> crate::WorkCounters { self.credit.lock().remaining_work() }

    /// Takes exclusive known CPU/output credit without reporting it early.
    pub(crate) fn admit_completed(
        &self, maximum: crate::WorkCounters,
    ) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        self.check_wait()?;
        self.credit.lock().partition_work(maximum)
    }

    /// Called immediately at the real attempt boundary, outside OS callbacks.
    pub(crate) fn started_io(
        &self, actual: crate::WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.check_wait()?;
        self.credit.lock().started_io(actual)
    }

    /// Actual CPU, bytes and visits are retained even after late wait refusal.
    pub(crate) fn record_completed(
        &self, actual: crate::WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        self.credit.lock().record_completed(actual)
    }


}

/// Physical-terminal custody of one moved, birth-paid original work token.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeOwnedCleanupCredit {
    allocation_owner: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    state: parking_lot::Mutex<NativeOwnedCleanupState>,
    _metadata: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeOwnedCleanupState {
    credit: NativeWorkReservation,
    // A partition of credit.remaining, never a second original allowance.
    pending: crate::WorkCounters,
    // Host preflights cannot consume accepted runtime token partitions.
    host_pending: crate::WorkCounters,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeOwnedCleanupCredit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeOwnedCleanupCredit")
            .field("remaining", &self.state.lock().credit.remaining_work())
            .finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeOwnedCleanupCredit {
    /// Transfers accepted cleanup credit to a funded runtime owner.
    pub(crate) fn from_reserved(mut credit: NativeWorkReservation) -> Result<std::sync::Arc<Self>, NativeTreeMaterializationError> {
        credit.meter = credit.meter.allocation_work();
        let original = credit.meter.clone();
        let normal = original.runtime_meter_owner();
        let allocation_owner = normal.allocation_owner().unwrap_or(normal);
        let metadata = original.claim_allocation(std::mem::size_of::<Self>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        Ok(std::sync::Arc::new(Self {
            allocation_owner,
            state: parking_lot::Mutex::new(NativeOwnedCleanupState {
                credit, pending: crate::WorkCounters::UNCHARGED,
                host_pending: crate::WorkCounters::UNCHARGED,
            }),
            _metadata: metadata,
        }))
    }

    pub(crate) fn original_work(&self) -> &NativeMaterializationWork { self._metadata.work() }

    /// Pure coercion of this already-funded owner; no new meter or allocation.
    pub(crate) fn runtime_meter_owner(
        self: &std::sync::Arc<Self>,
    ) -> std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> {
        std::sync::Arc::clone(self) as std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>
    }

    pub(crate) fn remaining_work(&self) -> crate::WorkCounters {
        self.state.lock().credit.remaining_work()
    }

    fn host_failure(&self, error: crate::DemandError) -> NativeTreeMaterializationError {
        let result = match &error {
            crate::DemandError::Work(error) => NativeTreeMaterializationError::Work(*error),
            crate::DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
            crate::DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
            crate::DemandError::OriginalWorkPoisoned => NativeTreeMaterializationError::OriginalWorkPoisoned,
            _ => return NativeTreeMaterializationError::Io(self.failure(error)),
        };
        self.failure(error);
        result
    }

    fn host_relay(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        self._metadata.work().with_original_leaf(|work| {
            let Some(cause) = work.take_native_io_failure() else {
                return NativeTreeMaterializationError::Io(error);
            };
            let result = match &cause {
                crate::DemandError::Work(error) => NativeTreeMaterializationError::Work(*error),
                crate::DemandError::InvalidRequest => NativeTreeMaterializationError::InvalidLayout,
                crate::DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
                crate::DemandError::OriginalWorkPoisoned => NativeTreeMaterializationError::OriginalWorkPoisoned,
                _ => NativeTreeMaterializationError::Io(error),
            };
            // Project the primitive relay without consuming the first cause.
            work.record_native_io_failure(cause);
            result
        })
    }

    fn host_admit(&self, maximum: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state.lock();
        let pending = state.host_pending.checked_add(maximum).map_err(crate::DemandError::from)
            .and_then(|pending| pending.checked_add(state.pending)
                .and_then(|total| total.remaining(state.credit.remaining)).map(|_| pending).map_err(Into::into))
            .map_err(|error| self.host_failure(error))?;
        state.host_pending = pending;
        Ok(())
    }

    fn host_start(&self, actual: crate::WorkCounters, require_preflight: bool) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state.lock();
        let pending = actual.remaining(state.host_pending);
        if pending.is_err() {
            if require_preflight { return Err(self.host_failure(crate::DemandError::InvalidRequest)); }
            Self::available(&state).and_then(|available| actual.remaining(available).map(|_| ()).map_err(Into::into))
                .map_err(|error| self.host_failure(error))?;
        }
        state.credit.try_started_io(actual).map_err(|error| self.host_failure(error))?;
        if let Ok(pending) = pending { state.host_pending = pending; }
        Ok(())
    }

    fn host_record(&self, actual: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let starts = crate::WorkCounters {
            backend_read_operations: actual.backend_read_operations,
            backend_write_operations: actual.backend_write_operations,
            durability_operations: actual.durability_operations,
            ..crate::WorkCounters::UNCHARGED
        };
        let completed = starts.remaining(actual)?;
        if starts != crate::WorkCounters::UNCHARGED { self.host_start(starts, true)?; }
        if completed == crate::WorkCounters::UNCHARGED { return Ok(()); }
        {
            let mut state = self.state.lock();
            let host_pending = completed.remaining(state.host_pending)
                .map_err(|error| self.host_failure(error.into()))?;
            let pending = state.pending.checked_add(completed)
                .map_err(|error| self.host_failure(error.into()))?;
            state.host_pending = host_pending;
            state.pending = pending;
        }
        self.complete(Some(completed), completed).map_err(|error| self.host_relay(error))
    }

    fn host_claim(&self, bytes: u64) -> Result<acyclic_native_runtime::ReadAllocation, NativeTreeMaterializationError> {
        if bytes == 0 { return Ok(acyclic_native_runtime::ReadAllocation::empty(&self.allocation_owner)); }
        Err(self.host_failure(crate::DemandError::InvalidRequest))
    }

    fn failure(&self, error: crate::DemandError) -> std::io::Error {
        self._metadata.work().with_original_leaf(|work| work.record_native_io_failure(error))
    }

    fn available(state: &NativeOwnedCleanupState) -> Result<crate::WorkCounters, crate::DemandError> {
        state.pending.checked_add(state.host_pending)
            .and_then(|pending| pending.remaining(state.credit.remaining)).map_err(Into::into)
    }

    fn preflight(&self, maximum: crate::WorkCounters) -> std::io::Result<()> {
        let state = self.state.lock();
        Self::available(&state).and_then(|available| maximum.remaining(available).map(|_| ()).map_err(Into::into))
            .map_err(|error| self.failure(error))
    }

    fn reserve(&self, maximum: crate::WorkCounters) -> std::io::Result<()> {
        let mut state = self.state.lock();
        let pending = state.pending.checked_add(maximum).map_err(crate::DemandError::from)
            .and_then(|pending| pending.checked_add(state.host_pending)
                .and_then(|total| total.remaining(state.credit.remaining)).map(|_| pending).map_err(Into::into))
            .map_err(|error| self.failure(error))?;
        state.pending = pending;
        Ok(())
    }

    fn cancel(&self, maximum: crate::WorkCounters) {
        let mut state = self.state.lock();
        match maximum.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => {
                self._metadata.work().with_original_leaf(|work| {
                    work.record_native_io_failure(error.into());
                    work.poison_native_original_work();
                });
            }
        }
    }

    fn operation(kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> crate::WorkCounters {
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => crate::WorkCounters {
                backend_read_operations: count, ..crate::WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Write => crate::WorkCounters {
                backend_write_operations: count, ..crate::WorkCounters::UNCHARGED
            },
            acyclic_native_runtime::NativeOperationKind::Durability => crate::WorkCounters {
                durability_operations: count, ..crate::WorkCounters::UNCHARGED
            },
        }
    }

    fn bytes(kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<crate::WorkCounters> {
        match kind {
            acyclic_native_runtime::NativeOperationKind::Read => Ok(crate::WorkCounters {
                source_bytes_read: count, ..crate::WorkCounters::UNCHARGED
            }),
            acyclic_native_runtime::NativeOperationKind::Write => Ok(crate::WorkCounters {
                output_bytes: count, ..crate::WorkCounters::UNCHARGED
            }),
            acyclic_native_runtime::NativeOperationKind::Durability => Err(std::io::ErrorKind::Unsupported.into()),
        }
    }

    fn start(&self, actual: crate::WorkCounters, reserved: bool) -> std::io::Result<()> {
        let mut state = self.state.lock();
        if reserved {
            state.pending = actual.remaining(state.pending)
                .map_err(|error| self.failure(error.into()))?;
        } else {
            Self::available(&state).and_then(|available| actual.remaining(available).map(|_| ()).map_err(Into::into))
                .map_err(|error| self.failure(error))?;
        }
        match state.credit.try_started_io(actual) {
            Ok(()) => Ok(()),
            Err(error) => {
                let failure = self.failure(error);
                if reserved {
                    state.credit.started_drop_io(actual);
                }
                Err(failure)
            }
        }
    }

    /// Records completed work before reporting allowance or cancellation errors.
    fn complete(&self, maximum: Option<crate::WorkCounters>, actual: crate::WorkCounters) -> std::io::Result<()> {
        let mut state = self.state.lock();
        let available = if let Some(maximum) = maximum {
            state.pending = maximum.remaining(state.pending)
                .map_err(|error| self.failure(error.into()))?;
            maximum
        } else {
            Self::available(&state).map_err(|error| self.failure(error))?
        };
        let credited = actual.intersect_budget(available);
        let excess = credited.remaining(actual).map_err(|error| self.failure(error.into()))?;
        state.credit.remaining = credited.remaining(state.credit.remaining)
            .map_err(|error| self.failure(error.into()))?;
        let overrun = excess != crate::WorkCounters::UNCHARGED;
        self._metadata.work().with_original_leaf(|work| {
            if overrun { work.record_native_io_failure(crate::DemandError::InvalidRequest); }
            // The explicit resource completion below owns this token's tags.
            let original = work.with_resource_scope(None, |work| {
                let credited = work.record_native_work(credited);
                let excess = if overrun { work.record(excess) } else { Ok(()) };
                credited.and(excess)
            });
            let resource = self._metadata.work().resource.as_ref().map_or(Ok(()), |resource| {
                let credited = resource.complete_work(credited);
                let excess = if overrun { resource.completed(excess, 0) } else { Ok(()) };
                credited.and(excess)
            });
            if overrun {
                work.poison_native_original_work();
                return Err(work.record_native_io_failure(crate::DemandError::InvalidRequest));
            }
            original.and(resource).map_err(|error| work.record_native_io_failure(error))
        })
    }

}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_native_runtime::NativeReadMeter for NativeOwnedCleanupCredit {
    fn check(&self) -> std::io::Result<()> {
        let state = self.state.lock();
        // No original cancellation reconciliation or caller wait lives here.
        Self::available(&state).map(|_| ()).map_err(|_| std::io::ErrorKind::Other.into())
    }
    fn allocation_owner(&self) -> Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>> {
        Some(std::sync::Arc::clone(&self.allocation_owner))
    }
    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.reserve(Self::operation(kind, count))
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.cancel(Self::operation(kind, count));
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.start(Self::operation(kind, 1), true)
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.preflight(Self::bytes(acyclic_native_runtime::NativeOperationKind::Write, maximum as u64)?)
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.reserve(Self::bytes(kind, maximum)?)
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        match Self::bytes(kind, maximum) {
            Ok(maximum) => self.cancel(maximum),
            Err(error) => { self.failure(crate::DemandError::Io(error)); }
        }
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.complete(Some(Self::bytes(kind, maximum)?), Self::bytes(kind, actual)?)
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        if bytes == 0 { self.check() } else { Err(self.failure(crate::DemandError::InvalidRequest)) }
    }
    fn reserve_allocation(&self, _bytes: u64) -> std::io::Result<()> {
        Err(self.failure(crate::DemandError::InvalidRequest))
    }
    fn cancel_reserved_allocation(&self, _bytes: u64) {
        self._metadata.work().with_original_leaf(|work| {
            work.record_native_io_failure(crate::DemandError::InvalidRequest);
            work.poison_native_original_work();
        });
    }
    fn record_reserved_allocation(&self, _bytes: u64) -> std::io::Result<()> {
        Err(self.failure(crate::DemandError::InvalidRequest))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.failure(crate::DemandError::Allocation(error))
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        // All real runtime controls are prepared on the normal projection.
        Err(std::io::ErrorKind::Unsupported.into())
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        if bytes == 0 { self.check() } else { Err(std::io::ErrorKind::Unsupported.into()) }
    }
    fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
        self._metadata.work().with_original_leaf(|work| {
            let (allocations, counters, budget, _) = work.allocation_parts();
            if let Err(error) = allocations.record_owned_bytes(bytes, counters) {
                return (0, Err(work.record_native_io_failure(error.into_work_or(crate::DemandError::InvalidRequest))));
            }
            let original = counters.verify(budget).map_err(crate::DemandError::from);
            let resource = self._metadata.work().resource.as_ref().map_or(Ok(()), |resource|
                resource.completed(crate::WorkCounters::UNCHARGED, bytes));
            (bytes, original.and(resource).map_err(|error| work.record_native_io_failure(error)))
        })
    }
    fn admit_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        self.reserve(crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED })
    }
    fn record_codec(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        let actual = crate::WorkCounters { bytes_encoded: encoded, bytes_copied: copied, bytes_hashed: hashed, ..crate::WorkCounters::UNCHARGED };
        self.complete(Some(actual), actual)
    }
    fn examine(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        let actual = crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED };
        self.reserve(actual)?;
        self.complete(Some(actual), actual)
    }
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        self.reserve(crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED })
    }
    fn record_examination(&self, items: u64, bytes_copied: u64) -> std::io::Result<()> {
        let actual = crate::WorkCounters { items_examined: items, bytes_copied, ..crate::WorkCounters::UNCHARGED };
        self.complete(Some(actual), actual)
    }
    fn release(&self, bytes: u64) { self.allocation_owner.release(bytes); }
    fn admit_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.preflight(Self::bytes(acyclic_native_runtime::NativeOperationKind::Read, maximum as u64)?)
    }
    fn admit_read(&self, maximum: usize) -> std::io::Result<()> {
        let mut state = self.state.lock();
        let actual = Self::operation(acyclic_native_runtime::NativeOperationKind::Read, 1);
        let maximum = actual.checked_add(Self::bytes(acyclic_native_runtime::NativeOperationKind::Read, maximum as u64)?)
            .map_err(|error| self.failure(error.into()))?;
        Self::available(&state).and_then(|available| maximum.remaining(available).map(|_| ()).map_err(Into::into))
            .map_err(|error| self.failure(error))?;
        state.credit.try_started_io(actual).map_err(|error| self.failure(error))
    }
    fn admit_operations(&self, additional: u64) -> std::io::Result<()> {
        self.preflight(Self::operation(acyclic_native_runtime::NativeOperationKind::Read, additional))
    }
    fn record_operations(&self, completed: u64) -> std::io::Result<()> {
        self.complete(None, Self::operation(acyclic_native_runtime::NativeOperationKind::Read, completed))
    }
    fn record_read(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, Self::bytes(acyclic_native_runtime::NativeOperationKind::Read, bytes as u64)?)
    }
    fn finish_read(&self, _requested: usize, _completed: usize) -> std::io::Result<()> { self.check() }
    fn admit_write(&self, maximum: usize) -> std::io::Result<()> {
        let mut state = self.state.lock();
        let actual = Self::operation(acyclic_native_runtime::NativeOperationKind::Write, 1);
        let maximum = actual.checked_add(Self::bytes(acyclic_native_runtime::NativeOperationKind::Write, maximum as u64)?)
            .map_err(|error| self.failure(error.into()))?;
        Self::available(&state).and_then(|available| maximum.remaining(available).map(|_| ()).map_err(Into::into))
            .map_err(|error| self.failure(error))?;
        state.credit.try_started_io(actual).map_err(|error| self.failure(error))
    }
    fn record_write(&self, bytes: usize) -> std::io::Result<()> {
        self.complete(None, Self::bytes(acyclic_native_runtime::NativeOperationKind::Write, bytes as u64)?)
    }
    fn admit_write_operations(&self, additional: u64) -> std::io::Result<()> {
        self.preflight(Self::operation(acyclic_native_runtime::NativeOperationKind::Write, additional))
    }
    fn record_write_operations(&self, completed: u64) -> std::io::Result<()> {
        self.complete(None, Self::operation(acyclic_native_runtime::NativeOperationKind::Write, completed))
    }
    fn admit_durability(&self) -> std::io::Result<()> {
        self.start(Self::operation(acyclic_native_runtime::NativeOperationKind::Durability, 1), false)
    }
    fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
        self.reserve(Self::operation(acyclic_native_runtime::NativeOperationKind::Write, 1))?;
        Ok(1)
    }
    fn cancel_cleanup_write(&self, reservation: u64) {
        self.cancel(Self::operation(acyclic_native_runtime::NativeOperationKind::Write, reservation));
    }
    fn record_cleanup_write(&self, reservation: u64) -> std::io::Result<()> {
        if reservation != 1 { return Err(self.failure(crate::DemandError::InvalidRequest)); }
        self.start(Self::operation(acyclic_native_runtime::NativeOperationKind::Write, 1), true)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl crate::native_host::NativeHostWork for &std::sync::Arc<NativeOwnedCleanupCredit> {
    type Error = NativeTreeMaterializationError;
    type Allocation = acyclic_native_runtime::ReadAllocation;
    type CleanupWrite = acyclic_native_runtime::NativeOperationReservation<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>;

    fn io_error_kind(error: &Self::Error) -> Option<std::io::ErrorKind> {
        match error { NativeTreeMaterializationError::Io(error) => Some(error.kind()), _ => None }
    }
    fn allocation_failed(error: std::collections::TryReserveError) -> Self::Error {
        NativeTreeMaterializationError::AllocationFailed(error)
    }
    fn invalid_request() -> Self::Error { NativeTreeMaterializationError::InvalidLayout }
    fn check(&self) -> Result<(), Self::Error> {
        acyclic_native_runtime::NativeReadMeter::check(self.as_ref())
            .map_err(|_| NativeTreeMaterializationError::OriginalWorkPoisoned)
    }
    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> { self.host_admit(delta) }
    fn record(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> { self.host_record(delta) }
    fn started_io(&mut self, delta: crate::WorkCounters) -> Result<(), Self::Error> { self.host_start(delta, false) }
    fn claim(&mut self, bytes: u64) -> Result<Self::Allocation, Self::Error> { self.host_claim(bytes) }
    fn reserve_opaque_decode(&mut self, _logical_max: u64) -> Result<Self::Allocation, Self::Error> {
        Err(self.host_failure(crate::DemandError::InvalidRequest))
    }
    fn reconcile_allocation(&mut self, allocation: &mut Self::Allocation, actual: u64) -> Result<(), Self::Error> {
        let actual = usize::try_from(actual).map_err(|_| crate::WorkError::Overflow)?;
        allocation.reconcile_capacity_with_meter(actual, self.as_ref())
            .map_err(|error| self.host_relay(error))
    }
    fn release(&mut self, allocation: Self::Allocation) -> Result<(), Self::Error> { drop(allocation); Ok(()) }
    fn reserve_cleanup_write(&mut self) -> Result<Self::CleanupWrite, Self::Error> {
        acyclic_native_runtime::NativeOperationReservation::new(
            self.runtime_meter_owner(), acyclic_native_runtime::NativeOperationKind::Write, 1,
        ).map_err(|error| self.host_relay(error))
    }
    fn cancel_cleanup_write(&mut self, reservation: Self::CleanupWrite) { drop(reservation); }
    fn attempt_cleanup_write<T>(
        &mut self, mut reservation: Self::CleanupWrite, operation: impl FnOnce() -> T,
    ) -> (T, Result<(), Self::Error>) {
        let (result, recorded) = reservation.attempt(operation);
        (result, recorded.map_err(|error| self.host_relay(error)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCleanupWrite {
    meter: NativeMaterializationWork,
    additional_resource: Option<std::sync::Arc<NativeResourceAllowance>>,
    reserved: bool,
}
#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeCleanupWrite {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeCleanupWrite").finish_non_exhaustive()
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl NativeCleanupWrite {
    /// Attributes accepted cleanup to the current volume.
    pub(crate) fn bind_current_work(&mut self, current: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        if !self.reserved || !self.meter.shares_ledger(current) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let Some(resource) = &current.resource else { return Ok(()); };
        if self.additional_resource.as_ref().is_some_and(|bound| std::sync::Arc::ptr_eq(bound, resource)) {
            return Ok(());
        }
        if self.additional_resource.is_some() { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let state = self.meter.state();
        state.check().map_err(native_materialization_work_error)?;
        resource.reserve(NativeResourceCredit::WriteOperations.delta(1), 0, self.meter.resource.as_deref(), &mut || Ok(()))
            .map_err(native_materialization_work_error)?;
        self.additional_resource = Some(std::sync::Arc::clone(resource));
        Ok(())
    }

    pub(crate) fn attempt<T>(self, operation: impl FnOnce() -> T) -> Result<T, NativeTreeMaterializationError> {
        let (value, recorded) = self.attempt_recorded(operation);
        recorded.map(|()| value)
    }

    pub(crate) fn attempt_recorded<T>(mut self, operation: impl FnOnce() -> T) -> (T, Result<(), NativeTreeMaterializationError>) {
        let recorded = {
            self.reserved = false;
            let mut state = self.meter.state();
            let original = state.complete(crate::materializer::NativeResourceCredit::WriteOperations, 1, 1);
            let birth = self.meter.resource.as_ref().map_or(Ok(()), |resource| {
                resource.complete_credit(NativeResourceCredit::WriteOperations, 1, 1)
            });
            let current = self.additional_resource.as_ref().map_or(Ok(()), |resource| {
                resource.complete_credit_excluding(NativeResourceCredit::WriteOperations, 1, self.meter.resource.as_deref())
            });
            original.and(birth).and(current).map_err(native_materialization_work_error)
        };
        (operation(), recorded)
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeCleanupWrite {
    fn drop(&mut self) {
        if self.reserved {
            let mut state = self.meter.state();
            state.refund(crate::materializer::NativeResourceCredit::WriteOperations, 1).expect("original reservation is balanced");
            let birth = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.refund_credit(NativeResourceCredit::WriteOperations, 1, None));
            let current = self.additional_resource.as_ref().map_or(Ok(()), |resource| {
                resource.refund_credit(NativeResourceCredit::WriteOperations, 1, self.meter.resource.as_deref())
            });
            if let Err(error) = birth.and(current) {
                state.record_native_io_failure(error);
                state.poison_native_original_work();
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// A scoped live-buffer admission in the native operation's allocation ledger.
pub struct NativeAllocation {
    meter: NativeMaterializationWork,
    // Keeps the actual claim-time context; these bytes are no longer scratch.
    capture: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
    bytes: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeAllocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeAllocation")
            .field("retained_logical_bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeAllocation {
    /// Transfers an existing runtime claim without readmission.
    pub fn from_runtime_allocation(
        original: &NativeMaterializationWork,
        incoming: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, (NativeTreeMaterializationError, acyclic_native_runtime::ReadAllocation)> {
        let owner = original.runtime_meter_owner();
        let expected = owner.allocation_owner().unwrap_or(owner);
        match incoming.consume_same_meter_owned_bytes(&expected) {
            Ok(bytes) => Ok(Self {
                meter: original.clone(),
                capture: None,
                bytes,
            }),
            Err(incoming) => Err((NativeTreeMaterializationError::InvalidLayout, incoming)),
        }
    }

    pub(crate) fn is_checkout_header_for(&self, original: &std::sync::Arc<crate::demand::NativeDemandReadMeter>) -> bool {
        self.is_root_metadata_for(original)
            && self.bytes == std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64
    }

    fn is_root_metadata_for(&self, original: &std::sync::Arc<crate::demand::NativeDemandReadMeter>) -> bool {
        std::sync::Arc::ptr_eq(&self.meter.state, original)
            && self.meter.resource.is_none() && self.capture.is_none()
    }

    /// Transfers a root bookkeeping claim to its original registry.
    pub(crate) fn transfer_checkout_metadata(
        &mut self, original: &std::sync::Arc<crate::demand::NativeDemandReadMeter>,
        expected: u64,
    ) -> Result<u64, NativeTreeMaterializationError> {
        if !self.is_root_metadata_for(original) || self.bytes != expected {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let bytes = self.bytes;
        self.bytes = 0;
        Ok(bytes)
    }

    /// Admits one actual buffer growth before reallocating.
    pub fn grow(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        let retained = self.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        let mut added = self.meter.claim_allocation(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
        added.capture = self.capture.clone();
        self.absorb(&mut added)?;
        debug_assert_eq!(self.bytes, retained);
        Ok(())
    }

    /// Returns this guard's currently owned byte capacity.
    pub fn bytes(&self) -> u64 { self.bytes }

    /// Borrows this guard's original operation ledger without creating a meter.
    pub const fn work(&self) -> &NativeMaterializationWork { &self.meter }

    /// Moves only this exclusive paid ticket into a physical proof primitive.
    pub(crate) fn transfer_resource_retirement(&mut self, bytes: u64) -> Result<NativeResourceRetirement, NativeTreeMaterializationError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        let retirement = NativeResourceRetirement {
            resource: self.meter.resource.clone(), bytes,
            _capture: self.capture.clone(),
            original: std::sync::Arc::clone(&self.meter.state),
        };
        self.bytes = remaining;
        Ok(retirement)
    }

    /// Transfers paid buffer custody into a registered checkout control.
    pub(crate) fn transfer_checkout_control(
        &mut self, owner: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        scope: &crate::demand::NativeCheckoutControlWitness,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if !scope.owns(&self.meter.state, owner) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let retirement = self.transfer_resource_retirement(self.bytes)?;
        Ok(crate::kernel::ProofDataClaim::native_control(owner, retirement))
    }

    pub(crate) fn transfer_checkout_control_original(
        &mut self, owner: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        let membership = self.meter.checkout_control_sink(owner)?;
        self.transfer_checkout_control(owner, &membership)
    }

    /// Transfers selected paid buffer custody to registered checkout data.
    pub(crate) fn transfer_checkout_data_bytes(
        &mut self, bytes: u64,
        owner: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        scope: &crate::demand::NativeCheckoutControlWitness,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        if bytes > self.bytes || !scope.owns(&self.meter.state, owner) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let retirement = self.transfer_resource_retirement(bytes)?;
        Ok(crate::kernel::ProofDataClaim::native_data(owner, retirement))
    }

    /// Tests whether both payload guards retain the same original ledger.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.meter.state, &other.meter.state)
    }

    /// Tests membership in the original ledger and resource allowance.
    #[must_use]
    pub fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.shares_ledger(work) && self.meter.shares_resource_allowance(work)
    }

    /// Transfers existing custody without another allocation.
    pub fn absorb(&mut self, other: &mut Self) -> Result<(), NativeTreeMaterializationError> {
        if !self.meter.shares_ledger(&other.meter) || !self.meter.shares_resource_allowance(&other.meter) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let same_capture = match (&self.capture, &other.capture) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        };
        if !same_capture && self.bytes != 0 && other.bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let combined = self.bytes.checked_add(other.bytes).ok_or(crate::WorkError::Overflow)?;
        if self.bytes == 0 { self.capture = other.capture.clone(); }
        self.bytes = combined;
        other.bytes = 0;
        Ok(())
    }

    /// Splits already-paid custody on this exact original ledger.
    pub fn split_off(&mut self, bytes: u64) -> Result<Self, NativeTreeMaterializationError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        let split = Self { meter: self.meter.clone(), bytes, capture: self.capture.clone() };
        self.bytes = remaining;
        Ok(split)
    }

    /// Releases capacity only after its buffer has been freed.
    pub fn release(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.release_original(bytes).map_err(native_materialization_work_error)
    }

    fn release_original(&mut self, bytes: u64) -> Result<(), crate::DemandError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        let mut state = self.meter.state();
        let original = state.release_native_owned(bytes, &None);
        let resource = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.retire(bytes));
        self.bytes = remaining;
        original.and(resource)
    }

    /// Retains typed accounting failures from infallible physical cleanup.
    pub(crate) fn release_deferred(&mut self, bytes: u64) {
        if let Err(error) = self.release_original(bytes) {
            let mut state = self.meter.state();
            state.record_native_io_failure(error);
            state.poison_native_original_work();
        }
    }

    /// Replaces an admitted quota with actual retained capacity.
    pub fn substitute(&mut self, retained: u64) -> Result<(), NativeTreeMaterializationError> {
        let released = self.bytes.checked_sub(retained).ok_or(crate::WorkError::Overflow)?;
        self.release(released)
    }

    /// Reconciles completed buffer capacity, including actual excess.
    pub fn reconcile_capacity(&mut self, actual: u64) -> Result<(), NativeTreeMaterializationError> {
        if actual < self.bytes { self.release(self.bytes - actual)?; }
        let (mut completed, result) = self.meter.record_completed_allocation(actual - self.bytes, self.capture.clone());
        self.absorb(&mut completed)?;
        result
    }

    /// Retains a logical quota or transfers already allocated payload custody.
    pub fn resize_retained(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes < self.bytes { return self.release(self.bytes - bytes); }
        let mut state = self.meter.state();
        let added = bytes - self.bytes;
        let result = match &self.meter.resource {
            Some(resource) => resource.admission(crate::WorkCounters::UNCHARGED, added, &mut || state.retain_native_owned(added, &None)),
            None => state.retain_native_owned(added, &None),
        };
        drop(state);
        result.map_err(native_materialization_work_error)?;
        self.bytes = bytes;
        Ok(())
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeAllocation {
    fn drop(&mut self) {
        if self.bytes == 0 { return; }
        let bytes = self.bytes;
        self.release_deferred(bytes);
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_materialization_work_error(error: crate::DemandError) -> NativeTreeMaterializationError {
    match error {
        crate::DemandError::Cancelled => NativeTreeMaterializationError::Cancelled,
        crate::DemandError::OriginalWorkPoisoned => NativeTreeMaterializationError::OriginalWorkPoisoned,
        crate::DemandError::Work(error) => NativeTreeMaterializationError::Work(error),
        crate::DemandError::Allocation(error) => NativeTreeMaterializationError::AllocationFailed(error),
        crate::DemandError::Io(error) => NativeTreeMaterializationError::Io(error),
        error => NativeTreeMaterializationError::Io(std::io::Error::other(error)),
    }
}

/// One declarative pathwise checkout edit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MaterializationEdit {
    /// Install a complete backend-defined entry image at one portable path.
    Install {
        /// Canonical relative path.
        path: String,
        /// Opaque entry image interpreted by the backend.
        image: Vec<u8>,
    },
    /// Remove one entry or subtree.
    Remove {
        /// Canonical relative path.
        path: String,
    },
    /// Rename one entry without flattening its metadata or link identity.
    Rename {
        /// Existing canonical relative path.
        from: String,
        /// Destination canonical relative path.
        to: String,
    },
    /// Change representable metadata without replacing a directory subtree.
    SetMetadata {
        /// Canonical relative path.
        path: String,
        /// Backend-defined declarative metadata image.
        image: Vec<u8>,
    },
}

/// Immutable materialization plan bound to exact original SDK inputs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPlan {
    /// Stable retry and recovery identity.
    pub operation_id: OperationId,
    /// Exact transition or qualified native preparation intent.
    pub intent: MaterializationIntent,
    /// Ordered edits with nonoverlapping destructive paths.
    pub edits: Vec<MaterializationEdit>,
}

/// Exact original authority domain of one physical materialization operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationIntent {
    /// A checkout transitioning between two real SDK generations.
    GenerationTransition {
        /// Actual represented baseline.
        from: GenerationId,
        /// Actual published target.
        to: GenerationId,
    },
    /// An empty physical checkout prepared from qualified original selections.
    NativePreparation {
        /// Ordered actual provider, volume, and generation inputs.
        selection: Vec<MaterializationPreparationSelection>,
        /// Canonical original request, including authority and fence provenance.
        original: Vec<u8>,
    },
}

/// One actual generation selection qualified by its original provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationSelection {
    /// Canonical actual provider/deployment identity descriptor.
    pub provider_identity: Vec<u8>,
    /// Actual SDK volume, not a path or storage-name-derived stand-in.
    pub volume: crate::VolumeId,
    /// Exact actual generation selected from that volume.
    pub generation: GenerationId,
}

/// Opaque complete preimage for one edit.
#[derive(Debug, Serialize, Deserialize)]
pub struct MaterializationPreimage {
    /// Backend-defined bytes sufficient to restore every representable fact.
    pub image: Vec<u8>,
    /// Allocation ownership of the captured bytes.
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    pub custody: Option<NativeAllocation>,
}

impl PartialEq for MaterializationPreimage {
    fn eq(&self, other: &Self) -> bool { self.image == other.image }
}
impl Eq for MaterializationPreimage {}

/// Exact physical state observed while resuming one journaled edit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationObservation {
    /// Backend delegates idempotency to its apply/restore implementations.
    Unverified,
    /// The edit has not yet been applied.
    Preimage,
    /// The edit was applied before its progress record became durable.
    Postimage,
    /// An operation-owned rename sequence stopped between its durable endpoints.
    Interrupted,
    /// Neither durable endpoint matches; an external writer intervened.
    Diverged,
}

/// Durable application phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPhase {
    /// Original immutable intent is durable; preimage capture is incomplete.
    Preparing,
    /// Every preimage is durable and no edit has been applied.
    Prepared,
    /// Forward application is in progress.
    Applying,
    /// Every edit is durable in the target checkout.
    Applied,
    /// Reverse preimage restoration is in progress.
    RollingBack,
    /// The original checkout has been restored.
    RolledBack,
}

/// Complete versioned recovery journal.
#[derive(Debug, Serialize, Deserialize)]
pub struct MaterializationJournal {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable plan.
    pub plan: MaterializationPlan,
    /// Actual backend allocation captured when the original intent was created.
    pub root_identity: Option<[u8; 16]>,
    /// Actual operation-owned staging and cleanup allocation identities.
    pub preparation_identity: Option<MaterializationPreparationIdentity>,
    /// Same-operation native allocation progress before the ready triple exists.
    pub preparation_progress: Option<MaterializationPreparationProgress>,
    /// Known work prefix before the last durable journal write.
    pub work: crate::WorkCounters,
    /// Preimages in exact edit order.
    pub preimages: Vec<MaterializationPreimage>,
    /// Current recovery phase.
    pub phase: MaterializationPhase,
    /// Number of forward edits known durable.
    pub applied: u32,
    /// Number of applied edits restored from the end.
    pub restored: u32,
    /// Allocation ownership of the journal's in-memory payload.
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    pub custody: Option<NativeAllocation>,
}

impl MaterializationJournal {
    /// Checks serialization and progress, not authority or physical readiness.
    #[must_use]
    pub fn is_valid_for(&self, operation: OperationId) -> bool { valid_journal(self, operation) }
}

/// Actual held allocations used to stage and restore one native operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationIdentity {
    /// Operation directory allocation.
    pub operation: [u8; 16],
    /// Staged target directory allocation.
    pub target: [u8; 16],
    /// Durable preimage directory allocation.
    pub backup: [u8; 16],
}

/// Durable allocation prefix of the original operation, not an ownership grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationProgress {
    /// Actual original existing held parent of the operation directory.
    pub parent: [u8; 16],
    /// Actually created and durably recorded operation-directory allocation.
    pub operation: Option<[u8; 16]>,
    /// Actually created and durably recorded staging-directory allocation.
    pub target: Option<[u8; 16]>,
    /// Actually created and durably recorded preimage-directory allocation.
    pub backup: Option<[u8; 16]>,
    /// Intent persisted before the corresponding physical mkdir.
    pub pending: Option<MaterializationPreparationAllocation>,
}

/// One original native allocation decision in the same journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationAllocation {
    /// Allocate the operation directory.
    Operation,
    /// Allocate its staged-target directory.
    Target,
    /// Allocate its preimage directory.
    Backup,
}

/// An accepted SDK mutation whose final durable acknowledgement is uncertain.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativeStageUncertainty<E> {
    primary: Box<Option<E>>,
    /// Last acknowledged original journal work, only a known lower bound.
    pub last_durable_work: crate::WorkCounters,
    /// Actual original work observed by the failing invocation.
    pub observed_work: crate::WorkCounters,
    // Outside and after the box through its physical deallocation.
    _custody: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> NativeStageUncertainty<E> {
    /// Borrows the actual original owner, provider or SDK cause.
    pub fn cause(&self) -> &E {
        self.primary.as_ref().as_ref().expect("finished native stage uncertainty")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E: std::fmt::Display> std::fmt::Display for NativeStageUncertainty<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "native stage completion is uncertain: {}", self.cause())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E: std::error::Error + 'static> std::error::Error for NativeStageUncertainty<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { Some(self.cause()) }
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> NativeStageUncertainty<E> {
    fn new(work: &NativeMaterializationWork) -> Result<(Box<Option<E>>, NativeAllocation), NativeTreeMaterializationError> {
        let custody = work.claim(std::mem::size_of::<Option<E>>() as u64)?;
        Ok((Box::new(None), custody))
    }

    fn finish((mut primary, custody): (Box<Option<E>>, NativeAllocation), cause: E, last_durable_work: crate::WorkCounters, observed_work: crate::WorkCounters) -> Self {
        *primary = Some(cause);
        Self { primary, _custody: custody, last_durable_work, observed_work }
    }
}

#[cfg(not(target_arch = "wasm32"))]
enum NativeStageJournal<C> {
    Journal(MaterializationJournal),
    Accepted { completion: C, last_durable_work: crate::WorkCounters },
    Completing { last_durable_work: crate::WorkCounters },
}

/// Durable optimistic-concurrency adapter for materialization journals.
pub trait MaterializationJournalStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Provider-owned completion accepted before one native SDK mutation.
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>: Future<Output = Result<MaterializationJournal, Self::Error>> + Send
    where Self: 'a;

    /// Admits journal completion before its authorized physical action.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_action<'a>(&'a self, journal: MaterializationJournal)
        -> impl Future<Output = Result<Self::ActionCompletion<'a>, Self::Error>> + Send;

    /// Loads one journal with its actual decoded or stored payload custody.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<MaterializationJournal>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a journal.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> impl Future<Output = Result<(bool, MaterializationJournal), Self::Error>> + Send;
}

/// Backend that can capture, apply, and restore every representable host fact.
pub trait MaterializationBackend: Send + Sync {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Actual allocation originally bound by this backend, not a path-derived ID.
    fn allocation_identity(&self) -> Option<[u8; 16]> { None }

    /// Actual staging and cleanup allocations, when independently observable.
    fn preparation_identity(&self) -> Option<MaterializationPreparationIdentity> { None }

    /// Revalidates all named staging and cleanup allocations.
    fn observe_preparation_identity(&self) -> impl Future<Output = Result<Option<MaterializationPreparationIdentity>, Self::Error>> + Send {
        async { Ok(self.preparation_identity()) }
    }

    /// Known cumulative work prefix, when this backend is measured.
    fn observed_work(&self) -> Result<Option<crate::WorkCounters>, Self::Error> { Ok(None) }

    /// Retains original durable work once for this invocation's recovery receipt.
    fn retain_original_work(&self, _work: crate::WorkCounters) {}

    /// Admits and validates actual plan scratch storage before allocating it.
    fn check_plan(&self, plan: &MaterializationPlan) -> Result<bool, Self::Error> {
        Ok(validate_plan(plan).is_ok())
    }

    /// Whether an observation error terminates instead of becoming Unavailable.
    fn observation_interrupted(&self, _error: &Self::Error) -> bool { false }

    /// Returns an observation diagnostic with its allocation ownership.
    #[cfg(not(target_arch = "wasm32"))]
    fn describe_observation_error(&self, error: Self::Error) -> Result<(String, Option<NativeAllocation>), Self::Error> { Err(error) }

    #[cfg(target_arch = "wasm32")]
    fn describe_observation_error(&self, error: Self::Error) -> Result<String, Self::Error> { Err(error) }

    /// Allocates the already admitted observation output at this backend.
    fn reserve_observations(
        &self,
        observations: &mut Vec<MaterializationObservation>,
        count: usize,
        #[cfg(not(target_arch = "wasm32"))] _allocation: &mut Option<NativeAllocation>,
    ) -> Result<(), Self::Error> {
        observations.reserve_exact(count);
        Ok(())
    }

    /// Completes barriers for a witnessed endpoint after a lost mutation acknowledgement.
    fn settle_observed(&self, _edit: &MaterializationEdit) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Retains allocation ownership of the supplied plan.
    #[cfg(not(target_arch = "wasm32"))]
    fn retain_plan(&self, _plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    /// Validates the journal's retained payload ownership.
    fn check_custody(&self, _journal: &mut MaterializationJournal, #[cfg(not(target_arch = "wasm32"))] _incoming: &mut [Option<NativeAllocation>]) -> Result<(), Self::Error> { Ok(()) }

    /// Transfers captured bytes and their actual ownership into this journal.
    fn append_preimage(&self, journal: &mut MaterializationJournal, preimage: MaterializationPreimage) -> Result<(), Self::Error>;

    /// Revalidates the allocation currently named at its physical boundary.
    fn observe_allocation_identity(
        &self,
    ) -> impl Future<Output = Result<Option<[u8; 16]>, Self::Error>> + Send {
        async { Ok(self.allocation_identity()) }
    }

    /// Verifies that the checkout still represents the plan's exact source.
    fn verify_source(
        &self,
        _from: GenerationId,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async { Ok(true) }
    }

    /// Captures a complete preimage without mutating the checkout.
    fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> impl Future<Output = Result<MaterializationPreimage, Self::Error>> + Send;

    /// Classifies the current path against both durable endpoints.
    fn observe(
        &self,
        _edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<MaterializationObservation, Self::Error>> + Send {
        async { Ok(MaterializationObservation::Unverified) }
    }

    /// Applies one declarative edit idempotently.
    fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Restores the exact preimage for one edit idempotently.
    fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Recovery decision for a nonterminal journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationRecovery {
    /// Continue applying the unpublished checkout candidate.
    Complete,
    /// Restore every path changed before interruption.
    RollBack,
}

/// Materialization failure.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum MaterializationError<S: std::error::Error + 'static, B: std::error::Error + 'static> {
    /// Durable journal access failed.
    #[error("materialization journal failed: {0}")]
    Store(S),
    /// Host checkout access failed.
    #[error("materialization backend failed: {0}")]
    Backend(B),
    /// A retry supplied different immutable plan input.
    #[error("materialization retry conflicts with its durable plan")]
    PlanConflict,
    /// Persisted progress is malformed or uses an unsupported version.
    #[error("materialization journal is incompatible")]
    IncompatibleJournal,
    /// Optimistic state contention exceeded the bounded retry policy.
    #[error("materialization journal remained contended")]
    Contended,
    /// Physical state no longer matches either durable endpoint.
    #[error("materialization paused because an external path mutation was observed")]
    ExternalMutation,
}

/// Actual physical-root status; neither diagnostics nor identities grant access.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationRootObservation {
    /// This adapter cannot independently witness its allocation.
    Unverified,
    /// The original and currently held native identities match.
    Identical([u8; 16]),
    /// The named root differs from the original immutable allocation.
    Diverged {
        /// Recorded allocation identity.
        original: [u8; 16],
        /// Current allocation identity, when present.
        current: Option<[u8; 16]>,
    },
    /// The original allocation remains recorded but cannot currently be inspected.
    Unavailable(String),
}

/// Non-authorizing observation of original staging and cleanup allocations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationObservation {
    /// No allocation witness was recorded.
    Unverified,
    /// The original allocation is incomplete; unknown pending IDs cannot be adopted.
    Incomplete(MaterializationPreparationProgress),
    /// All original preparation allocations remain present.
    Identical(MaterializationPreparationIdentity),
    /// At least one allocation differs from the original witness.
    Diverged {
        /// Recorded preparation identities.
        original: MaterializationPreparationIdentity,
        /// Current preparation identities, when present.
        current: Option<MaterializationPreparationIdentity>,
    },
    /// A physical observation failed; original witnesses remain retained.
    Unavailable(String),
}

/// Original durable intent and actual, non-authorizing physical observations.
#[derive(Debug)]
pub struct MaterializationInspection {
    /// Original operation, preparation identities and exact recovery progress.
    pub journal: MaterializationJournal,
    /// Root status even when preimage capture is incomplete.
    pub root: MaterializationRootObservation,
    /// Staging and cleanup status even when capture is incomplete.
    pub preparation: MaterializationPreparationObservation,
    /// Actual observations of captured endpoints, empty if the root is divergent.
    pub observations: Vec<MaterializationObservation>,
    /// Intrinsic ownership of allocated observation and diagnostic buffers.
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: [Option<NativeAllocation>; 3],
}

/// Core materializer coordinating one journal store and one host backend.
pub struct JournaledMaterializer<S, B> {
    store: S,
    backend: B,
}

impl PartialEq for MaterializationJournal {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.revision == other.revision
            && self.plan == other.plan
            && self.root_identity == other.root_identity
            && self.preparation_identity == other.preparation_identity
            && self.preparation_progress == other.preparation_progress
            && self.work == other.work
            && self.preimages == other.preimages
            && self.phase == other.phase
            && self.applied == other.applied
            && self.restored == other.restored
    }
}

impl Eq for MaterializationJournal {}

impl<S, B> JournaledMaterializer<S, B> {
    /// Creates a journaled materializer.
    #[must_use]
    pub const fn new(store: S, backend: B) -> Self {
        Self { store, backend }
    }

    /// Borrows the journal adapter.
    #[must_use]
    pub const fn store(&self) -> &S {
        &self.store
    }

    /// Borrows the host backend.
    #[must_use]
    pub const fn backend(&self) -> &B {
        &self.backend
    }
}

impl<S: MaterializationJournalStore, B: MaterializationBackend> JournaledMaterializer<S, B> {
    /// Captures preimages and applies the plan with authorization before effects.
    pub async fn apply<E, F, Fut>(&self, plan: MaterializationPlan, before_effect: &mut F) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let input = self.backend.retain_plan(&plan).map_err(MaterializationError::Backend)?;
        // Physical input buffers retire before their original custody on failure.
        let plan = plan;
        if !self.backend.check_plan(&plan).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::IncompatibleJournal.into());
        }
        let loaded = self.load(plan.operation_id).await?;
        let journal = match loaded {
            Some(existing) => {
                if existing.plan != plan {
                    return Err(MaterializationError::PlanConflict.into());
                }
                drop(plan);
                #[cfg(not(target_arch = "wasm32"))]
                drop(input);
                existing
            }
            None => self.prepare(plan, #[cfg(not(target_arch = "wasm32"))] input).await?,
        };
        if matches!(journal.phase, MaterializationPhase::RolledBack | MaterializationPhase::RollingBack) {
            return Err(MaterializationError::PlanConflict.into());
        }
        self.replay(journal, MaterializationRecovery::Complete, before_effect).await
    }

    /// Recovers with authorization before each effect.
    pub async fn recover<E, F, Fut>(&self, operation_id: OperationId, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<Option<MaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let Some(journal) = self.load(operation_id).await? else {
            return Ok(None);
        };
        self.replay(journal, recovery, before_effect).await.map(Some)
    }

    async fn replay<E, F, Fut>(&self, mut journal: MaterializationJournal, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        match recovery {
            MaterializationRecovery::Complete => {
                if journal.phase == MaterializationPhase::RollingBack || journal.restored != 0 {
                    return Err(MaterializationError::IncompatibleJournal.into());
                }
                if journal.phase == MaterializationPhase::RolledBack {
                    return Err(MaterializationError::PlanConflict.into());
                }
                self.verify_allocation(&journal).await?;
                if journal.phase == MaterializationPhase::Preparing {
                    journal = self.capture_preimages(journal, before_effect).await?;
                }
                if journal.phase == MaterializationPhase::Applied {
                    self.repair_applied(&journal, before_effect).await?;
                    return Ok(journal);
                }
                journal.phase = MaterializationPhase::Applying;
            }
            MaterializationRecovery::RollBack => {
                if journal.preparation_progress.is_some() { return Err(MaterializationError::IncompatibleJournal.into()); }
                if journal.phase == MaterializationPhase::RolledBack { return Ok(journal); }
                if journal.phase == MaterializationPhase::Preparing {
                    // No effect is permitted until every preimage is durable.
                    journal.phase = MaterializationPhase::RolledBack;
                    return self.persist(journal).await.map_err(Into::into);
                }
                self.verify_allocation(&journal).await?;
                if journal.phase == MaterializationPhase::Applying
                    && usize::try_from(journal.applied).is_ok_and(|applied| applied < journal.plan.edits.len()) {
                    journal.applied = journal.applied.saturating_add(1);
                }
                journal.phase = MaterializationPhase::RollingBack;
            }
        }
        journal = self.persist(journal).await?;
        loop {
            let index = match recovery {
                MaterializationRecovery::Complete => {
                    if usize::try_from(journal.applied).unwrap_or(usize::MAX) >= journal.plan.edits.len() { break; }
                    usize::try_from(journal.applied).map_err(|_| MaterializationError::IncompatibleJournal)?
                }
                MaterializationRecovery::RollBack => {
                    if journal.restored >= journal.applied { break; }
                    journal.applied.checked_sub(journal.restored.saturating_add(1))
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or(MaterializationError::IncompatibleJournal)?
                }
            };
            self.verify_allocation(&journal).await?;
            let edit = journal.plan.edits.get(index).ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal.preimages.get(index).ok_or(MaterializationError::IncompatibleJournal)?;
            let observation = self.backend.observe(edit, preimage).await.map_err(MaterializationError::Backend)?;
            if observation == MaterializationObservation::Diverged {
                return Err(MaterializationError::ExternalMutation.into());
            }
            let verify_source = recovery == MaterializationRecovery::Complete && journal.applied == 0
                && matches!(observation, MaterializationObservation::Unverified | MaterializationObservation::Preimage);
            before_effect(verify_source).await?;
            if verify_source {
                if let MaterializationIntent::GenerationTransition { from, .. } = &journal.plan.intent
                    && !self.backend.verify_source(*from).await.map_err(MaterializationError::Backend)? {
                    return Err(MaterializationError::PlanConflict.into());
                }
            }
            self.verify_effect_context(&journal).await?;
            match (recovery, observation) {
                (MaterializationRecovery::Complete, MaterializationObservation::Postimage)
                | (MaterializationRecovery::RollBack, MaterializationObservation::Preimage) => {
                    self.backend.settle_observed(edit).await.map_err(MaterializationError::Backend)?;
                }
                (MaterializationRecovery::Complete, _) => {
                    self.backend.apply(edit, preimage).await.map_err(MaterializationError::Backend)?;
                }
                (MaterializationRecovery::RollBack, _) => {
                    self.backend.restore(edit, preimage).await.map_err(MaterializationError::Backend)?;
                }
            }
            match recovery {
                MaterializationRecovery::Complete => journal.applied = journal.applied.saturating_add(1),
                MaterializationRecovery::RollBack => journal.restored = journal.restored.saturating_add(1),
            }
            journal = self.persist(journal).await?;
        }
        journal.phase = match recovery {
            MaterializationRecovery::Complete => MaterializationPhase::Applied,
            MaterializationRecovery::RollBack => MaterializationPhase::RolledBack,
        };
        self.persist(journal).await.map_err(Into::into)
    }

    /// Retains original intent and allocation witnesses before staging.
    pub async fn prepare(&self, plan: MaterializationPlan, #[cfg(not(target_arch = "wasm32"))] custody: Option<NativeAllocation>) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        #[cfg(not(target_arch = "wasm32"))]
        let input = match custody {
            Some(custody) => Some(custody),
            None => self.backend.retain_plan(&plan).map_err(MaterializationError::Backend)?,
        };
        // Preserve data-before-guard destruction through validation and CAS.
        let plan = plan;
        if !self.backend.check_plan(&plan).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::IncompatibleJournal);
        }
        let mut journal = MaterializationJournal {
            #[cfg(not(target_arch = "wasm32"))] custody: None,
            version: JOURNAL_VERSION,
            revision: 1,
            plan,
            root_identity: self.backend.allocation_identity(),
            preparation_identity: self.backend.preparation_identity(),
            preparation_progress: None,
            work: self.backend.observed_work().map_err(MaterializationError::Backend)?.unwrap_or_default(),
            preimages: Vec::new(),
            phase: MaterializationPhase::Preparing,
            applied: 0,
            restored: 0,
        };
        #[cfg(not(target_arch = "wasm32"))]
        { journal.custody = input; }
        self.backend.check_custody(&mut journal, #[cfg(not(target_arch = "wasm32"))] &mut []).map_err(MaterializationError::Backend)?;
        let (stored, mut journal) = self
            .store
            .compare_and_swap(journal.plan.operation_id, 0, journal)
            .await
            .map_err(MaterializationError::Store)?;
        if stored {
            self.backend.check_custody(&mut journal, #[cfg(not(target_arch = "wasm32"))] &mut []).map_err(MaterializationError::Backend)?;
            self.verify_allocation(&journal).await?;
            Ok(journal)
        } else {
            let retained = self
                .load(journal.plan.operation_id)
                .await?
                .ok_or(MaterializationError::Contended)?;
            if retained.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            self.verify_allocation(&retained).await?;
            Ok(retained)
        }
    }

    async fn capture_preimages<E, F, Fut>(
        &self,
        mut journal: MaterializationJournal,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        if journal.preparation_progress.is_some() { return Err(MaterializationError::IncompatibleJournal.into()); }
        before_effect(journal.preimages.is_empty()).await?;
        if let MaterializationIntent::GenerationTransition { from, .. } = &journal.plan.intent
            && !self.backend.verify_source(*from).await.map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::PlanConflict.into());
        }
        while journal.preimages.len() < journal.plan.edits.len() {
            self.verify_effect_context(&journal).await?;
            let edit = journal
                .plan
                .edits
                .get(journal.preimages.len())
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = self
                .backend
                .capture(edit)
                .await
                .map_err(MaterializationError::Backend)?;
            self.backend.append_preimage(&mut journal, preimage).map_err(MaterializationError::Backend)?;
            journal = self.persist(journal).await?;
            before_effect(false).await?;
            self.verify_effect_context(&journal).await?;
        }
        journal.phase = MaterializationPhase::Prepared;
        self.persist(journal).await.map_err(Into::into)
    }

    /// Reads original intent and durable progress without mutation.
    pub async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let mut journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MaterializationError::Store)?;
        if let Some(journal) = &mut journal {
            self.backend.check_custody(journal, #[cfg(not(target_arch = "wasm32"))] &mut []).map_err(MaterializationError::Backend)?;
            if !valid_journal(journal, operation_id)
                || !self.backend.check_plan(&journal.plan).map_err(MaterializationError::Backend)? {
                return Err(MaterializationError::IncompatibleJournal);
            }
        }
        self.backend.retain_original_work(journal.as_ref().map_or_else(crate::WorkCounters::default, |journal| journal.work));
        Ok(journal)
    }

    /// Queries actual endpoints alongside original durable allocation/progress.
    pub async fn inspect(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationInspection>, MaterializationError<S::Error, B::Error>> {
        // Declared before all payloads so failure drops physical buffers first.
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = [None, None, None];
        let Some(mut journal) = self.load(operation_id).await? else { return Ok(None); };
        let root = match journal.root_identity {
            None => MaterializationRootObservation::Unverified,
            Some(original) => match self.backend.observe_allocation_identity().await {
                Ok(Some(current)) if current == original => MaterializationRootObservation::Identical(original),
                Ok(current) => MaterializationRootObservation::Diverged { original, current },
                Err(error) if self.backend.observation_interrupted(&error) => return Err(MaterializationError::Backend(error)),
                Err(error) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    let text = {
                        let (text, allocation) = self.backend.describe_observation_error(error).map_err(MaterializationError::Backend)?;
                        custody[0] = allocation;
                        text
                    };
                    #[cfg(target_arch = "wasm32")]
                    let text = self.backend.describe_observation_error(error).map_err(MaterializationError::Backend)?;
                    MaterializationRootObservation::Unavailable(text)
                }
            },
        };
        let preparation = match journal.preparation_identity {
            None => journal.preparation_progress.map_or(MaterializationPreparationObservation::Unverified, MaterializationPreparationObservation::Incomplete),
            Some(original) => match self.backend.observe_preparation_identity().await {
                Ok(Some(current)) if current == original => MaterializationPreparationObservation::Identical(original),
                Ok(current) => MaterializationPreparationObservation::Diverged { original, current },
                Err(error) if self.backend.observation_interrupted(&error) => return Err(MaterializationError::Backend(error)),
                Err(error) => {
                    #[cfg(not(target_arch = "wasm32"))]
                    let text = {
                        let (text, allocation) = self.backend.describe_observation_error(error).map_err(MaterializationError::Backend)?;
                        custody[1] = allocation;
                        text
                    };
                    #[cfg(target_arch = "wasm32")]
                    let text = self.backend.describe_observation_error(error).map_err(MaterializationError::Backend)?;
                    MaterializationPreparationObservation::Unavailable(text)
                }
            },
        };
        let mut observations = Vec::new();
        if matches!(root, MaterializationRootObservation::Unverified | MaterializationRootObservation::Identical(_))
            && matches!(preparation, MaterializationPreparationObservation::Unverified | MaterializationPreparationObservation::Identical(_)) {
            self.backend.reserve_observations(
                &mut observations,
                journal.preimages.len(),
                #[cfg(not(target_arch = "wasm32"))]
                &mut custody[2],
            ).map_err(MaterializationError::Backend)?;
            for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
                observations.push(self.backend.observe(edit, preimage).await.map_err(MaterializationError::Backend)?);
            }
        }
        self.backend.check_custody(&mut journal, #[cfg(not(target_arch = "wasm32"))] &mut custody).map_err(MaterializationError::Backend)?;
        Ok(Some(MaterializationInspection {
            journal, root, preparation, observations,
            #[cfg(not(target_arch = "wasm32"))]
            custody,
        }))
    }

    async fn verify_allocation(
        &self,
        journal: &MaterializationJournal,
    ) -> Result<(), MaterializationError<S::Error, B::Error>> {
        if self.backend.allocation_identity() != journal.root_identity
            || self.backend.observe_allocation_identity().await.map_err(MaterializationError::Backend)? != journal.root_identity
            || self.backend.preparation_identity() != journal.preparation_identity
            || self.backend.observe_preparation_identity().await.map_err(MaterializationError::Backend)? != journal.preparation_identity
        {
            return Err(MaterializationError::ExternalMutation);
        }
        Ok(())
    }

    async fn verify_effect_context(&self, journal: &MaterializationJournal) -> Result<(), MaterializationError<S::Error, B::Error>> {
        let current = self.load(journal.plan.operation_id).await?.ok_or(MaterializationError::IncompatibleJournal)?;
        if current != *journal { return Err(MaterializationError::PlanConflict); }
        self.verify_allocation(journal).await
    }

    async fn repair_applied<E, F, Fut>(&self, journal: &MaterializationJournal, before_effect: &mut F) -> Result<(), E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
            self.verify_allocation(journal).await?;
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Unverified | MaterializationObservation::Postimage => {}
                MaterializationObservation::Preimage | MaterializationObservation::Interrupted => {
                    before_effect(false).await?;
                    self.verify_effect_context(journal).await?;
                    self.backend
                        .apply(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation.into());
                }
            }
        }
        Ok(())
    }

    async fn persist(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        for _ in 0..MAXIMUM_CAS_ATTEMPTS {
            if let Some(work) = self.backend.observed_work().map_err(MaterializationError::Backend)? {
                journal.work = work;
            }
            let (stored, replacement) = compare_next_journal::<S, B::Error>(&self.store, journal).await?;
            journal = replacement;
            if stored {
                return Ok(journal);
            }
            let Some(current) = self.load(journal.plan.operation_id).await? else {
                return Err(MaterializationError::IncompatibleJournal);
            };
            if current.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            journal = current;
        }
        Err(MaterializationError::Contended)
    }
}

async fn compare_next_journal<S: MaterializationJournalStore, B: std::error::Error + 'static>(
    store: &S, mut journal: MaterializationJournal,
) -> Result<(bool, MaterializationJournal), MaterializationError<S::Error, B>> {
    let expected = next_revision(&mut journal.revision);
    store.compare_and_swap(journal.plan.operation_id, expected, journal).await.map_err(MaterializationError::Store)
}

fn valid_journal(journal: &MaterializationJournal, operation_id: OperationId) -> bool {
    let edit_count = journal.plan.edits.len();
    let applied = usize::try_from(journal.applied).unwrap_or(usize::MAX);
    let restored = usize::try_from(journal.restored).unwrap_or(usize::MAX);
    let allocation_valid = match journal.preparation_progress {
        None => true,
        Some(progress) => journal.preparation_identity.is_none() && journal.root_identity.is_some()
            && journal.phase == MaterializationPhase::Preparing && journal.preimages.is_empty()
            && (progress.target.is_none() || progress.operation.is_some())
            && (progress.backup.is_none() || progress.target.is_some())
            && match progress.pending {
                None => true,
                Some(MaterializationPreparationAllocation::Operation) => progress.operation.is_none() && progress.target.is_none() && progress.backup.is_none(),
                Some(MaterializationPreparationAllocation::Target) => progress.operation.is_some() && progress.target.is_none() && progress.backup.is_none(),
                Some(MaterializationPreparationAllocation::Backup) => progress.operation.is_some() && progress.target.is_some() && progress.backup.is_none(),
            },
    };
    journal.version == JOURNAL_VERSION
        && allocation_valid
        && journal.plan.operation_id == operation_id
        && journal.preimages.len() <= edit_count
        && (journal.phase == MaterializationPhase::Preparing
            || journal.preimages.len() == edit_count
            || (journal.phase == MaterializationPhase::RolledBack && applied == 0))
        && applied <= edit_count
        && restored <= applied
        && match journal.phase {
            MaterializationPhase::Preparing => applied == 0 && restored == 0,
            MaterializationPhase::Prepared => applied == 0 && restored == 0,
            MaterializationPhase::Applying => restored == 0,
            MaterializationPhase::Applied => applied == edit_count && restored == 0,
            MaterializationPhase::RollingBack => true,
            MaterializationPhase::RolledBack => restored == applied,
        }
}

fn validate_plan(plan: &MaterializationPlan) -> Result<(), ()> {
    validate_plan_steps(
        plan,
        || Ok::<(), ()>(()),
        |capacity| { let mut paths = Vec::new(); paths.try_reserve_exact(capacity).map_err(|_| ())?; Ok(paths) },
        |path| Ok(crate::path::is_canonical_relative(path)),
        |paths| { paths.sort_unstable(); Ok(()) },
        |left, right| Ok(left == right || right.strip_prefix(left).is_some_and(|suffix| suffix.starts_with('/'))),
    ).and_then(|valid| if valid { Ok(()) } else { Err(()) })
}

fn validate_plan_steps<'a, E>(
    plan: &'a MaterializationPlan,
    mut step: impl FnMut() -> Result<(), E>,
    allocate: impl FnOnce(usize) -> Result<Vec<&'a str>, E>,
    mut path_valid: impl FnMut(&str) -> Result<bool, E>,
    sort: impl FnOnce(&mut [&str]) -> Result<(), E>,
    mut overlaps: impl FnMut(&str, &str) -> Result<bool, E>,
) -> Result<bool, E> {
    let Some(capacity) = plan.edits.len().checked_mul(2) else { return Ok(false); };
    let mut paths = allocate(capacity)?;
    if let MaterializationIntent::NativePreparation { original, .. } = &plan.intent
        && original.is_empty() {
        return Ok(false);
    }
    for edit in &plan.edits {
        step()?;
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(plan.intent, MaterializationIntent::NativePreparation { .. })
            && !matches!(edit, MaterializationEdit::Install { image, .. } if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) {
            return Ok(false);
        }
        match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                if !path_valid(path)? { return Ok(false); }
                paths.push(path.as_str());
            }
            MaterializationEdit::SetMetadata { path, .. } => {
                if !path_valid(path)? { return Ok(false); }
            }
            MaterializationEdit::Rename { from, to } => {
                if !path_valid(from)?
                    || !path_valid(to)? { return Ok(false); }
                paths.push(from.as_str());
                paths.push(to.as_str());
            }
        }
    }
    sort(&mut paths)?;
    for pair in paths.windows(2) {
        step()?;
        let [left, right] = pair else { continue };
        if overlaps(left, right)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(not(target_arch = "wasm32"))]
fn owned_size(capacity: usize, width: usize) -> Result<u64, NativeTreeMaterializationError> {
    u64::try_from(capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)?)
        .map_err(|_| crate::WorkError::Overflow.into())
}

#[cfg(not(target_arch = "wasm32"))]
fn edit_owned_bytes(edit: &MaterializationEdit) -> Result<u64, NativeTreeMaterializationError> {
    let (first, second) = match edit {
        MaterializationEdit::Install { path, image } | MaterializationEdit::SetMetadata { path, image } => (path.capacity(), image.capacity()),
        MaterializationEdit::Remove { path } => (path.capacity(), 0),
        MaterializationEdit::Rename { from, to } => (from.capacity(), to.capacity()),
    };
    owned_size(first.checked_add(second).ok_or(crate::WorkError::Overflow)?, 1)
}

#[cfg(not(target_arch = "wasm32"))]
fn plan_owned_bytes(plan: &MaterializationPlan) -> Result<u64, NativeTreeMaterializationError> {
    let mut bytes = owned_size(plan.edits.capacity(), std::mem::size_of::<MaterializationEdit>())?;
    for edit in &plan.edits {
        bytes = bytes.checked_add(edit_owned_bytes(edit)?).ok_or(crate::WorkError::Overflow)?;
    }
    if let MaterializationIntent::NativePreparation { selection, original } = &plan.intent {
        bytes = bytes.checked_add(owned_size(original.capacity(), 1)?).ok_or(crate::WorkError::Overflow)?;
        bytes = bytes.checked_add(owned_size(selection.capacity(), std::mem::size_of::<MaterializationPreparationSelection>())?).ok_or(crate::WorkError::Overflow)?;
        for selected in selection {
            bytes = bytes.checked_add(owned_size(selected.provider_identity.capacity(), 1)?).ok_or(crate::WorkError::Overflow)?;
        }
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
/// Returns the owned capacity of a journal payload.
pub fn journal_owned_bytes(journal: &MaterializationJournal) -> Result<u64, NativeTreeMaterializationError> {
    let mut bytes = plan_owned_bytes(&journal.plan)?.checked_add(owned_size(journal.preimages.capacity(), std::mem::size_of::<MaterializationPreimage>())?).ok_or(crate::WorkError::Overflow)?;
    for preimage in &journal.preimages {
        bytes = bytes.checked_add(owned_size(preimage.image.capacity(), 1)?).ok_or(crate::WorkError::Overflow)?;
    }
    Ok(bytes)
}

#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_string(value: &str, work: &NativeMaterializationWork, allocation: &mut NativeAllocation) -> Result<String, NativeTreeMaterializationError> {
    let delta = crate::WorkCounters { bytes_copied: value.len() as u64, ..crate::WorkCounters::default() };
    work.admit(delta)?;
    allocation.grow(value.len() as u64)?;
    let mut output = String::new();
    output.try_reserve_exact(value.len()).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    output.push_str(value);
    work.spent(delta)?;
    Ok(output)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_vec_push<T>(values: &mut Vec<T>, value: T, custody: &mut NativeAllocation) -> Result<(), NativeTreeMaterializationError> {
    if values.len() == values.capacity() {
        let additional = values.capacity().max(1);
        custody.grow(owned_size(additional, std::mem::size_of::<T>())?)?;
        values.try_reserve_exact(additional).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    }
    values.push(value);
    Ok(())
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_namespace_string(path: &crate::kernel::NamespacePath, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
    let mut length = path.components().len().saturating_sub(1);
    for component in path.components() {
        work.charge(crate::WorkCounters { items_examined: component.as_bytes().len() as u64 + 1, ..crate::WorkCounters::default() })?;
        let Some(bytes) = component.unicode_utf8_bytes() else {
            return Err(native_diagnostic(work, format_args!("{path:?}"), NativeTreeMaterializationError::InvalidPath)?);
        };
        length = length.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
    }
    let custody = work.claim(length as u64)?;
    let mut output = String::new();
    output.try_reserve_exact(length).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    work.admit(crate::WorkCounters { bytes_copied: (length - path.components().len().saturating_sub(1)) as u64, ..crate::WorkCounters::default() })?;
    for component in path.components() {
        work.charge(crate::WorkCounters { items_examined: component.as_bytes().len() as u64 + 1, ..crate::WorkCounters::default() })?;
        if !output.is_empty() { output.push('/'); }
        let before = output.len();
        if component.append_unicode(&mut output).is_err() {
            return Err(native_diagnostic(work, format_args!("{path:?}"), NativeTreeMaterializationError::InvalidPath)?);
        }
        work.spent(crate::WorkCounters { bytes_copied: (output.len() - before) as u64, ..crate::WorkCounters::default() })?;
    }
    Ok((output, custody))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_text_compare(left: &str, right: &str, work: &NativeMaterializationWork) -> Result<std::cmp::Ordering, NativeTreeMaterializationError> {
    let mut left = left.bytes();
    let mut right = right.bytes();
    loop {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
        match (left.next(), right.next()) {
            (Some(a), Some(b)) if a == b => continue,
            (Some(a), Some(b)) => return Ok(a.cmp(&b)),
            (None, Some(_)) => return Ok(std::cmp::Ordering::Less),
            (Some(_), None) => return Ok(std::cmp::Ordering::Greater),
            (None, None) => return Ok(std::cmp::Ordering::Equal),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_path_within(path: &str, parent: &str, allow_equal: bool, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
    let mut path = path.bytes();
    for expected in parent.bytes() {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
        if path.next() != Some(expected) { return Ok(false); }
    }
    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
    Ok(path.next().map_or(allow_equal, |byte| byte == b'/'))
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_install_paths<'a>(paths: impl Iterator<Item = &'a str>, work: &NativeMaterializationWork) -> Result<(Vec<String>, NativeAllocation), NativeTreeMaterializationError> {
    let mut custody = work.claim(0)?;
    let mut output = Vec::new();
    for path in paths {
        let mut owned = native_absolute_string(path, work)?;
        custody.absorb(&mut owned.1)?;
        native_vec_push(&mut output, owned.0, &mut custody)?;
    }
    Ok((output, custody))
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_absolute_string(path: &str, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
    let length = path.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
    let allocation = work.claim(length as u64)?;
    let copy = crate::WorkCounters { bytes_copied: path.len() as u64, ..crate::WorkCounters::default() };
    work.admit(copy)?;
    let mut absolute = String::new();
    absolute.try_reserve_exact(length).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    absolute.push('/');
    absolute.push_str(path);
    work.spent(copy)?;
    Ok((absolute, allocation))
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_stage_options(options: &crate::MaterializeOptions, destination: &Path, expected: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> Result<(crate::MaterializeOptions, NativeAllocation), NativeTreeMaterializationError> {
    let (destination, custody) = work.path(destination, None)?;
    Ok((crate::MaterializeOptions {
        destination,
        expected_root_identity: Some(expected),
        maximum_directory_entries: options.maximum_directory_entries,
        maximum_extent_spans: options.maximum_extent_spans,
        transfer_bytes: options.transfer_bytes,
    }, custody))
}

#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_bytes(value: &[u8], work: &NativeMaterializationWork, allocation: &mut NativeAllocation) -> Result<Vec<u8>, NativeTreeMaterializationError> {
    let delta = crate::WorkCounters { bytes_copied: value.len() as u64, ..crate::WorkCounters::default() };
    work.admit(delta)?;
    allocation.grow(value.len() as u64)?;
    let mut output = Vec::new();
    output.try_reserve_exact(value.len()).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    output.extend_from_slice(value);
    work.spent(delta)?;
    Ok(output)
}

#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_edit(edit: &MaterializationEdit, work: &NativeMaterializationWork) -> Result<(MaterializationEdit, NativeAllocation), NativeTreeMaterializationError> {
    let mut allocation = work.claim(0)?;
    let edit = match edit {
        MaterializationEdit::Install { path, image } => MaterializationEdit::Install {
            path: duplicate_native_string(path, work, &mut allocation)?,
            image: duplicate_native_bytes(image, work, &mut allocation)?,
        },
        MaterializationEdit::SetMetadata { path, image } => MaterializationEdit::SetMetadata {
            path: duplicate_native_string(path, work, &mut allocation)?,
            image: duplicate_native_bytes(image, work, &mut allocation)?,
        },
        MaterializationEdit::Remove { path } => MaterializationEdit::Remove {
            path: duplicate_native_string(path, work, &mut allocation)?,
        },
        MaterializationEdit::Rename { from, to } => MaterializationEdit::Rename {
            from: duplicate_native_string(from, work, &mut allocation)?,
            to: duplicate_native_string(to, work, &mut allocation)?,
        },
    };
    Ok((edit, allocation))
}

#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_preimage(preimage: &MaterializationPreimage, work: &NativeMaterializationWork) -> Result<MaterializationPreimage, NativeTreeMaterializationError> {
    let mut allocation = work.claim(0)?;
    let image = duplicate_native_bytes(&preimage.image, work, &mut allocation)?;
    Ok(MaterializationPreimage { image, custody: Some(allocation) })
}

/// Duplicates the actual logical payload while retaining its original ledger.
#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_journal(journal: &MaterializationJournal, work: &NativeMaterializationWork) -> Result<MaterializationJournal, NativeTreeMaterializationError> {
    let mut output = MaterializationJournal {
            version: journal.version,
            revision: journal.revision,
            plan: MaterializationPlan {
                operation_id: journal.plan.operation_id,
                intent: match &journal.plan.intent {
                    MaterializationIntent::GenerationTransition { from, to } => MaterializationIntent::GenerationTransition { from: *from, to: *to },
                    MaterializationIntent::NativePreparation { .. } => MaterializationIntent::NativePreparation { selection: Vec::new(), original: Vec::new() },
                },
                edits: Vec::new(),
            },
            root_identity: journal.root_identity,
            preparation_identity: journal.preparation_identity,
            preparation_progress: journal.preparation_progress,
            work: journal.work,
            preimages: Vec::new(),
            phase: journal.phase,
            applied: journal.applied,
            restored: journal.restored,
        custody: Some(work.claim(0)?),
    };
    let custody = output.custody.as_mut().expect("native journal custody");
    if let (
        MaterializationIntent::NativePreparation { selection, original },
        MaterializationIntent::NativePreparation { selection: copied, original: copied_original },
    ) = (&journal.plan.intent, &mut output.plan.intent) {
        *copied_original = duplicate_native_bytes(original, work, custody)?;
        for selected in selection {
            let provider_identity = duplicate_native_bytes(&selected.provider_identity, work, custody)?;
            native_vec_push(copied, MaterializationPreparationSelection {
                provider_identity,
                volume: selected.volume,
                generation: selected.generation,
            }, custody)?;
        }
    }
    for edit in &journal.plan.edits {
        let (edit, mut allocation) = duplicate_native_edit(edit, work)?;
        if let Err(error) = custody.absorb(&mut allocation) {
            drop(edit);
            return Err(error);
        }
        native_vec_push(&mut output.plan.edits, edit, custody)?;
    }
    for preimage in &journal.preimages {
        let image = duplicate_native_bytes(&preimage.image, work, custody)?;
        native_vec_push(&mut output.preimages, MaterializationPreimage { image, custody: None }, custody)?;
    }
    Ok(output)
}

pub(crate) fn duplicate_journal(journal: &MaterializationJournal) -> Result<MaterializationJournal, MemoryMaterializationJournalStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(custody) = journal.custody.as_ref().or_else(|| journal.preimages.iter().find_map(|preimage| preimage.custody.as_ref())) {
        return duplicate_native_journal(journal, custody.work()).map_err(MemoryMaterializationJournalStoreError::NativeWork);
    }
    Ok(MaterializationJournal {
        version: journal.version, revision: journal.revision, plan: journal.plan.clone(),
        root_identity: journal.root_identity, preparation_identity: journal.preparation_identity,
        preparation_progress: journal.preparation_progress, work: journal.work,
        preimages: journal.preimages.iter().map(|preimage| MaterializationPreimage { image: preimage.image.clone(), #[cfg(not(target_arch = "wasm32"))] custody: None }).collect(), phase: journal.phase,
        applied: journal.applied, restored: journal.restored,
        #[cfg(not(target_arch = "wasm32"))] custody: None,
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: MaterializationJournalStore> JournaledMaterializer<S, NativeTreeMaterializationBackend> {
    /// Stages immutable generation selections through this same owned journal.
    #[cfg(feature = "native-mount")]
    pub async fn stage_generation<A, O, E, F, Fut>(
        &self,
        generation: &crate::Generation<A, O>,
        relative_selections: &[String],
        options: &crate::MaterializeOptions,
        journal: MaterializationJournal,
        physical_work: &crate::NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        A: crate::AsyncAuthorityStore,
        O: crate::AsyncObjectStore,
        E: From<crate::MaterializeError> + From<crate::WorkspaceError>
            + From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>> + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let original = physical_work.clone();
        // This prepaid cause slot outlives every physically owned frame.
        let failure_slot = NativeStageUncertainty::<E>::new(&original)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        let state = std::sync::Mutex::new(Some(NativeStageJournal::<S::ActionCompletion<'_>>::Journal(journal)));
        let reader = generation.workspace().readonly_pinned_reader_with_native_work(
            generation.id(), &original, original.cancellation(),
        ).await.map_err(E::from)?;
        let mut control = |phase| {
            let owner = match phase {
                crate::native_mount::NativeMaterializationActionPhase::Before => Some(before_effect()),
                crate::native_mount::NativeMaterializationActionPhase::Completed => None,
            };
            let owned = match state.lock() {
                Ok(mut state) => state.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            let state = &state;
            async move {
                let next = match (phase, owned) {
                    (crate::native_mount::NativeMaterializationActionPhase::Before,
                        Some(NativeStageJournal::Journal(journal))) => {
                        owner.expect("before action owns its original guard future").await?;
                        self.verify_allocation(&journal).await.map_err(E::from)?;
                        let last_durable_work = journal.work;
                        let completion = self.store.prepare_action(journal).await
                            .map_err(|error| E::from(MaterializationError::Store(error)))?;
                        NativeStageJournal::Accepted { completion, last_durable_work }
                    }
                    (crate::native_mount::NativeMaterializationActionPhase::Completed,
                        Some(NativeStageJournal::Accepted { completion, last_durable_work })) => {
                        match state.lock() {
                            Ok(mut state) => *state = Some(NativeStageJournal::Completing { last_durable_work }),
                            Err(poisoned) => *poisoned.into_inner() = Some(NativeStageJournal::Completing { last_durable_work }),
                        }
                        let journal = completion.await
                            .map_err(|error| E::from(MaterializationError::Store(error)))?;
                        NativeStageJournal::Journal(journal)
                    }
                    _ => return Err(E::from(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout))),
                };
                match state.lock() {
                    Ok(mut state) => *state = Some(next),
                    Err(poisoned) => *poisoned.into_inner() = Some(next),
                }
                Ok(())
            }
        };
        let result = crate::native_mount::materialize_checkout_host_paths_with_work_guarded(
            &reader, relative_selections, options, physical_work, &mut control,
        ).await;
        drop(control);
        let state = match state.into_inner() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        match (result, state) {
            (Ok(_), Some(NativeStageJournal::Journal(journal))) => Ok(journal),
            (Err(cause), Some(NativeStageJournal::Accepted { completion, last_durable_work })) => {
                // An unpolled accepted frame is retired, never new cleanup IO.
                drop(completion);
                Err(E::from(NativeStageUncertainty::finish(failure_slot, cause, last_durable_work, original.receipt())))
            }
            (Err(cause), Some(NativeStageJournal::Completing { last_durable_work })) =>
                Err(E::from(NativeStageUncertainty::finish(failure_slot, cause, last_durable_work, original.receipt()))),
            (Err(cause), _) => Err(cause),
            _ => Err(E::from(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout))),
        }
    }

    #[cfg(feature = "native-mount")]
    async fn reset_stage<E, F, Fut>(
        &self, journal: MaterializationJournal, relative: &str, before_effect: &mut F,
    ) -> Result<(MaterializationJournal, crate::NativeRootIdentity), E>
    where
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let original = &self.backend.work;
        let failure_slot = NativeStageUncertainty::<E>::new(original)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        before_effect().await?;
        self.verify_allocation(&journal).await.map_err(E::from)?;
        let last_durable_work = journal.work;
        let completion = self.store.prepare_action(journal).await
            .map_err(|error| E::from(MaterializationError::Store(error)))?;
        let identity = match self.backend.reset_target_directory(relative).await {
            Ok(identity) => identity,
            Err(error) => {
                drop(completion);
                let cause = E::from(MaterializationError::Backend(error));
                return Err(E::from(NativeStageUncertainty::finish(failure_slot, cause, last_durable_work, original.receipt())));
            }
        };
        let journal = completion.await.map_err(|error| {
            let cause = E::from(MaterializationError::Store(error));
            E::from(NativeStageUncertainty::finish(failure_slot, cause, last_durable_work, original.receipt()))
        })?;
        Ok((journal, identity))
    }


}

/// Native same-volume tree publisher used for root checkout application.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeTreeMaterializationBackend {
    root: std::sync::Arc<PathBuf>,
    target: std::sync::Arc<PathBuf>,
    backup: std::sync::Arc<PathBuf>,
    preparation_identity: MaterializationPreparationIdentity,
    work: NativeMaterializationWork,
    held_root: std::sync::Arc<crate::native_host::HostRoot>,
    held_target: std::sync::Arc<crate::native_host::HostRoot>,
    held_backup: std::sync::Arc<crate::native_host::HostRoot>,
    operation_directory: std::sync::Arc<PathBuf>,
    _allocation: std::sync::Arc<NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeEntry<'a> {
    root: &'a crate::native_host::HostRoot,
    path: &'a Path,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTreeMaterializationBackend {
    async fn prepare_allocations<S, F, Fut, E>(
        store: &S, mut journal: MaterializationJournal, root: &Path,
        operation_directory: &Path, work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        S: MaterializationJournalStore,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>,
    {
        let backend_error = MaterializationError::<S::Error, NativeTreeMaterializationError>::Backend;
        let store_error = MaterializationError::<S::Error, NativeTreeMaterializationError>::Store;
        let incompatible = || MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal;
        let expected_root = crate::NativeRootIdentity::from_bytes(journal.root_identity.ok_or_else(incompatible)?);
        let mut progress = journal.preparation_progress.ok_or_else(incompatible)?;
        if !valid_journal(&journal, journal.plan.operation_id) || !native_check_plan(&journal.plan, work).map_err(backend_error)? { return Err(incompatible().into()); }
        if progress.pending.is_some() { return Err(backend_error(NativeTreeMaterializationError::IndeterminatePreparation).into()); }
        for step in [MaterializationPreparationAllocation::Operation, MaterializationPreparationAllocation::Target, MaterializationPreparationAllocation::Backup] {
            let known = match step {
                MaterializationPreparationAllocation::Operation => progress.operation,
                MaterializationPreparationAllocation::Target => progress.target,
                MaterializationPreparationAllocation::Backup => progress.backup,
            };
            if known.is_some() { continue; }
            before_effect().await?;
            Self::verify_recorded_preparation(root, expected_root, operation_directory, progress, work).map_err(backend_error)?;
            let current = store.load(journal.plan.operation_id).await.map_err(store_error)?.ok_or_else(incompatible)?;
            if current != journal { return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict.into()); }
            progress.pending = Some(step);
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(backend_error)?;
            let (stored, pending) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = pending;
            if !stored { return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended.into()); }
            let current = store.load(journal.plan.operation_id).await.map_err(store_error)?.ok_or_else(incompatible)?;
            if current != journal { return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict.into()); }
            let (root_copy, root_custody) = work.path(root, None).map_err(backend_error)?;
            let (operation_copy, operation_custody) = work.path(operation_directory, None).map_err(backend_error)?;
            let actual = acyclic_native_runtime::run_blocking_io({
                let work = work.clone();
                move || {
                    let _paths = (root_custody, operation_custody);
                    let source = crate::native_host::HostRoot::open(&root_copy, &mut &work)?;
                    if source.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: source.identity() }); }
                    let parent = crate::native_host::HostRoot::open(operation_copy.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut &work)?;
                    let expected_parent = crate::NativeRootIdentity::from_bytes(progress.parent);
                    if parent.identity() != expected_parent { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual: parent.identity() }); }
                    let parent = parent.open_dir_held(Path::new(""), &mut &work)?;
                    let leaf = Path::new(operation_copy.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
                    let allocation = match step {
                        MaterializationPreparationAllocation::Operation => crate::native_host::create_new_directory_held_with_work(&parent, leaf, &mut &work)?,
                        MaterializationPreparationAllocation::Target | MaterializationPreparationAllocation::Backup => {
                            let expected = crate::NativeRootIdentity::from_bytes(progress.operation.ok_or(NativeTreeMaterializationError::InvalidLayout)?);
                            let operation = with_native_walk_leaf(leaf, &work, |name| open_native_walk_directory(&parent, name, &work))?;
                            let actual = crate::native_host::directory_identity(&operation, &mut &work)?;
                            if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
                            if let Some(expected) = progress.target { Self::recorded_preparation_child(&operation, Path::new("target"), expected, &work)?; }
                            crate::native_host::create_new_directory_held_with_work(&operation, match step { MaterializationPreparationAllocation::Target => Path::new("target"), _ => Path::new("backup") }, &mut &work)?
                        }
                    };
                    crate::native_host::directory_identity(&allocation, &mut &work).map(|identity| identity.to_bytes())
                }
            }, None).expect("unmetered task construction cannot fail").await.map_err(|error| backend_error(NativeTreeMaterializationError::Io(error)))?.map_err(backend_error)?;
            match step {
                MaterializationPreparationAllocation::Operation => progress.operation = Some(actual),
                MaterializationPreparationAllocation::Target => progress.target = Some(actual),
                MaterializationPreparationAllocation::Backup => progress.backup = Some(actual),
            }
            progress.pending = None;
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(backend_error)?;
            let (stored, completed) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = completed;
            if !stored { return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended.into()); }
            before_effect().await?;
            Self::verify_recorded_preparation(root, expected_root, operation_directory, progress, work).map_err(backend_error)?;
        }
        before_effect().await?;
        Self::verify_recorded_preparation(root, expected_root, operation_directory, progress, work).map_err(backend_error)?;
        journal.preparation_identity = Some(MaterializationPreparationIdentity {
            operation: progress.operation.ok_or_else(incompatible)?,
            target: progress.target.ok_or_else(incompatible)?,
            backup: progress.backup.ok_or_else(incompatible)?,
        });
        journal.preparation_progress = None;
        journal.work = work.journal_work().map_err(backend_error)?;
        let (stored, journal) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
        if !stored { return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended.into()); }
        Ok(journal)
    }

    fn recorded_preparation_child(parent: &cap_std::fs::Dir, leaf: &Path, expected: [u8; 16], work: &NativeMaterializationWork) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
        let directory = with_native_walk_leaf(leaf, work, |name| open_native_walk_directory(parent, name, work))?;
        let actual = crate::native_host::directory_identity(&directory, &mut &*work)?;
        let expected = crate::NativeRootIdentity::from_bytes(expected);
        if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
        Ok(directory)
    }

    fn verify_recorded_preparation(root: &Path, expected_root: crate::NativeRootIdentity, operation_directory: &Path, progress: MaterializationPreparationProgress, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        let source = crate::native_host::HostRoot::open(root, &mut &*work)?;
        if source.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: source.identity() }); }
        let parent = crate::native_host::HostRoot::open(operation_directory.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut &*work)?;
        let expected_parent = crate::NativeRootIdentity::from_bytes(progress.parent);
        if parent.identity() != expected_parent { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual: parent.identity() }); }
        let parent = parent.open_dir_held(Path::new(""), &mut &*work)?;
        if let Some(expected) = progress.operation {
            let operation = Self::recorded_preparation_child(&parent, Path::new(operation_directory.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?), expected, work)?;
            if let Some(expected) = progress.target { Self::recorded_preparation_child(&operation, Path::new("target"), expected, work)?; }
            if let Some(expected) = progress.backup { Self::recorded_preparation_child(&operation, Path::new("backup"), expected, work)?; }
        }
        Ok(())
    }

    /// Binds native allocations, optionally creating the backup directory.
    pub fn new(root: impl Into<PathBuf>, operation_directory: impl Into<PathBuf>, custody: Option<NativeAllocation>, work: NativeMaterializationWork, create_backup: bool) -> Result<Self, NativeTreeMaterializationError> {
        let root = root.into();
        let operation_directory = operation_directory.into();
        let mut custody = match custody {
            Some(custody) => custody,
            None => work.retain_owned(root.capacity().checked_add(operation_directory.capacity()).ok_or(crate::WorkError::Overflow)? as u64)?,
        };
        let root = root;
        let operation_directory = operation_directory;
        if !work.shares_ledger(custody.work()) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let operation_root = crate::native_host::HostRoot::open(&operation_directory, &mut &work)?;
        let held_root = native_owned_arc(crate::native_host::HostRoot::open(&root, &mut &work)?, &work, &mut custody)?;
        let root_identity = held_root.identity();
        if root_identity.device != operation_root.identity().device {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let operation_directory_cap = operation_root.open_dir_held(Path::new(""), &mut &work)?;
        let root_directory_cap = held_root.open_dir_held(Path::new(""), &mut &work)?;
        #[cfg(unix)]
        let nested = crate::native_host::directory_has_ancestor_with_work(&operation_directory_cap, root_identity, &mut &work)?
            || crate::native_host::directory_has_ancestor_with_work(&root_directory_cap, operation_root.identity(), &mut &work)?;
        #[cfg(windows)]
        let nested = native_directory_contains(&root_directory_cap, operation_root.identity(), &work)?
            || native_directory_contains(&operation_directory_cap, root_identity, &work)?;
        if nested { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let mut target_owned = work.path(&operation_directory, Some(Path::new("target")))?;
        custody.absorb(&mut target_owned.1)?;
        let target = target_owned.0;
        let mut backup_owned = work.path(&operation_directory, Some(Path::new("backup")))?;
        custody.absorb(&mut backup_owned.1)?;
        let backup = backup_owned.0;
        let operation_identity = operation_root.identity().to_bytes();
        if create_backup {
            let directory = operation_root.open_dir_held(Path::new(""), &mut &work)?;
            crate::native_host::create_directories_held(&directory, Path::new("backup"), &mut &work)?;
        }
        let target_directory = operation_root.open_dir_held(Path::new("target"), &mut &work)?;
        let target_identity = crate::native_host::directory_identity(&target_directory, &mut &work)?;
        let held_target = native_owned_arc(crate::native_host::HostRoot::from_held_dir_with_identity(target_directory, target_identity), &work, &mut custody)?;
        let backup_directory = operation_root.open_dir_held(Path::new("backup"), &mut &work)?;
        let backup_identity = crate::native_host::directory_identity(&backup_directory, &mut &work)?;
        let held_backup = native_owned_arc(crate::native_host::HostRoot::from_held_dir_with_identity(backup_directory, backup_identity), &work, &mut custody)?;
        let preparation_identity = MaterializationPreparationIdentity {
            operation: operation_identity,
            target: held_target.identity().to_bytes(),
            backup: held_backup.identity().to_bytes(),
        };
        Ok(Self {
            root: native_owned_arc(root, &work, &mut custody)?,
            target: native_owned_arc(target, &work, &mut custody)?,
            backup: native_owned_arc(backup, &work, &mut custody)?,
            operation_directory: native_owned_arc(operation_directory, &work, &mut custody)?,
            _allocation: {
                let mut header = work.claim(std::mem::size_of::<NativeAllocation>() as u64)?;
                custody.absorb(&mut header)?;
                std::sync::Arc::new(custody)
            },
            held_root,
            held_target,
            held_backup,
            work,
            preparation_identity,
        })
    }

    /// Shares the native boundary with a receipt-bearing durable store adapter.
    pub fn work(&self) -> NativeMaterializationWork { self.work.clone() }

    /// Borrows the original root path without canonicalization or copying.
    pub fn root_path(&self) -> &Path { self.root.as_path() }

    /// Borrows the original operation-directory path.
    pub fn operation_directory_path(&self) -> &Path { self.operation_directory.as_path() }

    /// Opens a mapping through the held root without following symlinks.
    pub fn mapping_root_identity(&self, path: &str) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::InvalidPath)?);
        }
        let actual = self.work.root_identity(self.root_path())?;
        if actual != self.held_root.identity() {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected: self.held_root.identity(), actual });
        }
        let directory = self.held_root.open_dir_held(Path::new(path), &mut &self.work)?;
        crate::native_host::directory_identity(&directory, &mut &self.work)
    }

    /// Allocates a lexical staging path with retained capacity ownership.
    pub fn staging_path(&self, path: &str) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::InvalidPath)?);
        }
        self.work.path(self.target.as_path(), Some(Path::new(path)))
    }

    /// Checks each actual held initial allocation before staging can write.
    pub fn require_initial_empty(&self) -> Result<(), NativeTreeMaterializationError> {
        for (root, path) in [
            (&self.held_root, self.root_path()),
            (&self.held_target, self.target.as_path()),
            (&self.held_backup, self.backup.as_path()),
        ] {
            let directory = root.open_dir_held(Path::new(""), &mut &self.work)?;
            let (mut reader, _cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &self.work)?;
            if next_native_walk_name(&mut reader, &self.work)?.is_some() {
                return Err(native_diagnostic(&self.work, path.display(), NativeTreeMaterializationError::ExternalMutation)?);
            }
        }
        Ok(())
    }

    /// Checks root entries against install paths and preserved names.
    pub fn require_selected_root(&self, edits: &[MaterializationEdit], preserved: &[&str]) -> Result<(), NativeTreeMaterializationError> {
        for path in preserved {
            if !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            }))? {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let directory = self.held_root.open_dir_held(Path::new(""), &mut &self.work)?;
        let (mut reader, _cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &self.work)?;
        while let Some((name, _allocation)) = next_native_walk_name(&mut reader, &self.work)? {
            let mut selected = false;
            let paths = edits.iter().map(|edit| match edit {
                MaterializationEdit::Install { path, .. } => Some(path.as_str()),
                _ => None,
            }).chain(preserved.iter().copied().map(Some));
            for path in paths {
                self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
                let Some(path) = path else { continue; };
                let top = path.split('/').next().unwrap_or_default();
                #[cfg(unix)]
                let equal = {
                    use std::os::unix::ffi::OsStrExt as _;
                    self.work.charge(crate::WorkCounters { items_examined: name.as_bytes().len().min(top.len()) as u64, ..crate::WorkCounters::default() })?;
                    name.as_bytes() == top.as_bytes()
                };
                #[cfg(windows)]
                let equal = {
                    self.work.charge(crate::WorkCounters { items_examined: name.len().min(top.len()) as u64, ..crate::WorkCounters::default() })?;
                    name.iter().copied().eq(top.encode_utf16())
                };
                if equal { selected = true; break; }
            }
            if !selected {
                return Err(native_diagnostic(&self.work, format_args!("{}: {name:?}", self.root_path().display()), NativeTreeMaterializationError::ExternalMutation)?);
            }
        }
        Ok(())
    }

    /// Clears a staged directory while retaining its allocation witness.
    pub async fn reset_target_directory(&self, path: &str) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::InvalidPath)?);
        }
        if self.observe_allocation_identity().await? != self.allocation_identity()
            || self.observe_preparation_identity().await? != self.preparation_identity() {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::ExternalMutation)?);
        }
        let backend = self.clone();
        let mut path_allocation = self.work.claim(0)?;
        let path = duplicate_native_string(path, &self.work, &mut path_allocation)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _path_allocation = path_allocation;
            let directory = backend.held_target.open_dir_held(Path::new(""), &mut &backend.work)?;
            let identity = if path.is_empty() {
                let (mut reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &backend.work)?;
                while let Some((name, allocation)) = next_native_walk_name(&mut reader, &backend.work)? {
                    remove_native_walk(&directory, &name, &backend.work)?;
                    drop(name);
                    drop(allocation);
                }
                drop(reader);
                drop(cursor);
                backend.held_target.identity()
            } else {
                let relative = Path::new(&path);
                backend.create_parents(&NativeEntry { root: &backend.held_target, path: relative })?;
                let parent = backend.held_target.open_dir_held(relative.parent().unwrap_or_else(|| Path::new("")), &mut &backend.work)?;
                let leaf = relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                remove_native_held(&parent, Path::new(leaf), &backend.work)?;
                let child = crate::native_host::create_new_directory_held_with_work(&parent, Path::new(leaf), &mut &backend.work)?;
                let identity = crate::native_host::directory_identity(&child, &mut &backend.work)?;
                identity
            };
            crate::native_host::sync_directory_held(&directory, &mut &backend.work)?;
            Ok(identity)
        }, None).expect("unmetered task construction cannot fail").await?
    }

    fn exists(&self, path: &NativeEntry<'_>) -> Result<bool, NativeTreeMaterializationError> {
        let (parent, leaf) = match self.parent(path) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        self.work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::default() })?;
        match parent.symlink_metadata(leaf) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn create_parents(&self, path: &NativeEntry<'_>) -> Result<(), NativeTreeMaterializationError> {
        let directory = path.root.open_dir_held(Path::new(""), &mut &self.work)?;
        crate::native_host::create_directories_held(&directory, path.path.parent().unwrap_or_else(|| Path::new("")), &mut &self.work)?;
        Ok(())
    }

    fn rename(&self, from: &NativeEntry<'_>, to: &NativeEntry<'_>) -> Result<(), NativeTreeMaterializationError> {
        let (source, from_name) = self.parent(from)?;
        let (destination, to_name) = self.parent(to)?;
        self.work.admit(crate::WorkCounters { materializations: 1, ..crate::WorkCounters::default() })?;
        crate::native_host::rename_no_replace_held(&source, &from_name, &destination, &to_name, &mut &self.work)?;
        self.work.spent(crate::WorkCounters { materializations: 1, ..crate::WorkCounters::default() })?;
        let source = crate::native_host::sync_directory_held(&source, &mut &self.work);
        let destination = crate::native_host::sync_directory_held(&destination, &mut &self.work);
        source.and(destination)?;
        Ok(())
    }

    fn remove(&self, path: &NativeEntry<'_>) -> Result<(), NativeTreeMaterializationError> {
        let (parent, leaf) = match self.parent(path) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        remove_native_held(&parent, &leaf, &self.work)?;
        crate::native_host::sync_directory_held(&parent, &mut &self.work)?;
        Ok(())
    }

    fn metadata_file(&self, path: &NativeEntry<'_>) -> Result<(std::fs::File, crate::native_host::HostStat), NativeTreeMaterializationError> {
        let (parent, leaf) = self.parent(path)?;
        let expected = native_stat_held(&parent, &leaf, &self.work)?;
        let file = with_native_walk_leaf(&leaf, &self.work, |name| {
            if expected.is_dir() { Ok(open_native_walk_directory(&parent, name, &self.work)?.into_std_file()) }
            else { open_native_walk_file(&parent, name, &self.work) }
        })?;
        let actual = crate::native_host::stat_file_with_work(&file, &mut &self.work)?;
        if native_stat_identity(&actual) != native_stat_identity(&expected) {
            return Err(native_diagnostic(&self.work, path.path.display(), NativeTreeMaterializationError::ExternalMutation)?);
        }
        Ok((file, actual))
    }

    fn set_metadata(&self, path: &NativeEntry<'_>, desired: &NativeMetadataImage, expected: Option<[u8; 32]>) -> Result<(), NativeTreeMaterializationError> {
        let (_file, metadata) = self.metadata_file(path)?;
        if Some(native_metadata_fingerprint(&metadata, None, &self.work)?) != expected {
            return Err(native_diagnostic(&self.work, path.path.display(), NativeTreeMaterializationError::ExternalMutation)?);
        }
        #[cfg(windows)]
        {
            let (parent, leaf) = self.parent(path)?;
            let attributes = desired.windows_attributes.unwrap_or_else(|| {
                if desired.readonly { metadata.file_attributes() | 1 }
                else { metadata.file_attributes() & !1 }
            });
            crate::native_host::set_file_attributes_held_measured(&parent, &leaf, attributes, Some(native_stat_identity(&metadata)), &self.work)?;
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            let Some(mode) = desired.posix_mode else { return Ok(()); };
            loop {
                self.work.charge(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::default() })?;
                if unsafe { libc::fchmod(_file.as_raw_fd(), mode as libc::mode_t) } == 0 { break; }
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted { return Err(error.into()); }
            }
            crate::native_host::sync_file_held_with_work(&_file, &mut &self.work)?;
        }
        Ok(())
    }

    fn paths<'a>(&'a self, path: &'a str) -> (NativeEntry<'a>, NativeEntry<'a>, NativeEntry<'a>) {
        let path = Path::new(path);
        (
            NativeEntry { root: &self.held_root, path },
            NativeEntry { root: &self.held_target, path },
            NativeEntry { root: &self.held_backup, path },
        )
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_owned_arc<T>(value: T, work: &NativeMaterializationWork, custody: &mut NativeAllocation) -> Result<std::sync::Arc<T>, NativeTreeMaterializationError> {
    let bytes = std::mem::size_of::<T>();
    let mut header = work.claim(bytes as u64)?;
    custody.absorb(&mut header)?;
    Ok(std::sync::Arc::new(value))
}

/// Checks preparation layout through held directory identities.
#[cfg(not(target_arch = "wasm32"))]
pub fn validate_native_preparation_location(
    root: &Path,
    operation_directory: &Path,
    work: &NativeMaterializationWork,
) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
    let root = crate::native_host::HostRoot::open(root, &mut &*work)?;
    let mut existing_path = operation_directory;
    let operation = loop {
        match crate::native_host::HostRoot::open(existing_path, &mut &*work) {
            Ok(held) => break held,
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                existing_path = existing_path.parent().ok_or(error)?;
            }
            Err(error) => return Err(error),
        }
    };
    for component in operation_directory.strip_prefix(existing_path).map_err(|_| NativeTreeMaterializationError::InvalidLayout)?.components() {
        work.charge(crate::WorkCounters { source_entries_visited: 1, ..crate::WorkCounters::default() })?;
        if !matches!(component, std::path::Component::Normal(_)) { return Err(NativeTreeMaterializationError::InvalidLayout); }
    }
    if root.identity().device != operation.identity().device { return Err(NativeTreeMaterializationError::InvalidLayout); }
    let root_directory = root.open_dir_held(Path::new(""), &mut &*work)?;
    let operation_directory_cap = operation.open_dir_held(Path::new(""), &mut &*work)?;
    let exists = existing_path == operation_directory;
    #[cfg(unix)]
    let nested = crate::native_host::directory_has_ancestor_with_work(&operation_directory_cap, root.identity(), &mut &*work)?
        || (exists && crate::native_host::directory_has_ancestor_with_work(&root_directory, operation.identity(), &mut &*work)?);
    #[cfg(windows)]
    let nested = native_directory_contains(&root_directory, operation.identity(), work)?
        || (exists && native_directory_contains(&operation_directory_cap, root.identity(), work)?);
    if nested { return Err(NativeTreeMaterializationError::InvalidLayout); }
    Ok(root.identity())
}

#[cfg(not(target_arch = "wasm32"))]
impl MaterializationBackend for NativeTreeMaterializationBackend {
    type Error = NativeTreeMaterializationError;

    fn allocation_identity(&self) -> Option<[u8; 16]> {
        Some(self.held_root.identity().to_bytes())
    }

    fn preparation_identity(&self) -> Option<MaterializationPreparationIdentity> {
        Some(self.preparation_identity)
    }

    fn observed_work(&self) -> Result<Option<crate::WorkCounters>, Self::Error> {
        self.work.journal_work().map(Some)
    }

    fn retain_original_work(&self, work: crate::WorkCounters) {
        self.work.set_original_work(work);
    }

    fn check_plan(&self, plan: &MaterializationPlan) -> Result<bool, Self::Error> {
        native_check_plan(plan, &self.work)
    }

    fn observation_interrupted(&self, error: &Self::Error) -> bool {
        matches!(error, NativeTreeMaterializationError::Work(_) | NativeTreeMaterializationError::Cancelled | NativeTreeMaterializationError::AllocationFailed(_))
    }

    async fn settle_observed(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        let backend = self.clone();
        let (edit, allocation) = duplicate_native_edit(edit, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _allocation = allocation;
            let path = match &edit {
                MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path }
                | MaterializationEdit::SetMetadata { path, .. } => path,
                MaterializationEdit::Rename { .. } => return Err(NativeTreeMaterializationError::UnsupportedRename),
            };
            for root in [&backend.held_root, &backend.held_target, &backend.held_backup] {
                let parent = match root.open_dir_held(Path::new(path).parent().unwrap_or_else(|| Path::new("")), &mut &backend.work) {
                    Ok(parent) => parent,
                    Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                crate::native_host::sync_directory_held(&parent, &mut &backend.work)?;
            }
            if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                let (live, _, _) = backend.paths(path);
                let (parent, leaf) = backend.parent(&live)?;
                crate::native_host::sync_metadata_held_with_work(&parent, leaf, &mut &backend.work)?;
            }
            Ok(())
        }, None).expect("unmetered task construction cannot fail").await?
    }

    fn retain_plan(&self, plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.retain_owned(plan_owned_bytes(plan)?).map(Some)
    }

    fn check_custody(&self, journal: &mut MaterializationJournal, incoming: &mut [Option<NativeAllocation>]) -> Result<(), Self::Error> {
        let bytes = journal_owned_bytes(journal)?;
        let custody = journal.custody.as_mut().ok_or(NativeTreeMaterializationError::MissingCustody)?;
        if !self.work.shares_ledger(custody.work()) || custody.bytes() < bytes {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        for incoming in incoming.iter_mut().flatten() { custody.absorb(incoming)?; }
        Ok(())
    }

    fn describe_observation_error(&self, error: Self::Error) -> Result<(String, Option<NativeAllocation>), Self::Error> {
        match self.work.display(&error) {
            Ok((text, custody)) => Ok((text, Some(custody))),
            Err(_format_failure) => Err(error),
        }
    }

    fn reserve_observations(
        &self,
        observations: &mut Vec<MaterializationObservation>,
        count: usize,
        allocation: &mut Option<NativeAllocation>,
    ) -> Result<(), Self::Error> {
        let bytes = owned_size(count, std::mem::size_of::<MaterializationObservation>())?;
        *allocation = Some(self.work.claim_allocation(bytes, u64::from(count != 0), crate::WorkCounters::UNCHARGED)?);
        let allocation = allocation.as_mut().expect("admitted observations");
        observations.try_reserve_exact(count).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        allocation.reconcile_capacity(owned_size(observations.capacity(), std::mem::size_of::<MaterializationObservation>())?)
    }

    fn append_preimage(&self, journal: &mut MaterializationJournal, mut preimage: MaterializationPreimage) -> Result<(), Self::Error> {
        let mut image_custody = preimage.custody.take().ok_or(NativeTreeMaterializationError::MissingCustody)?;
        let preimage = preimage; // Drop bytes before their detached guard on rejection.
        let custody = journal.custody.as_mut().ok_or(NativeTreeMaterializationError::MissingCustody)?;
        if !self.work.shares_ledger(custody.work()) || !custody.shares_ledger(&image_custody) {
            return Err(NativeTreeMaterializationError::MissingCustody);
        }
        custody.absorb(&mut image_custody)?;
        let capacity = journal.preimages.capacity();
        if journal.preimages.len() == capacity {
            let next = capacity.checked_mul(2).ok_or(crate::WorkError::Overflow)?.max(4);
            let mut growth = self.work.claim(owned_size(next - capacity, std::mem::size_of::<MaterializationPreimage>())?)?;
            journal.preimages.try_reserve_exact(next - journal.preimages.len())
                .map_err(NativeTreeMaterializationError::AllocationFailed)?;
            let reconciled = growth.reconcile_capacity(owned_size(journal.preimages.capacity() - capacity, std::mem::size_of::<MaterializationPreimage>())?);
            custody.absorb(&mut growth)?;
            reconciled?;
        }
        journal.preimages.push(preimage);
        Ok(())
    }

    async fn observe_preparation_identity(&self) -> Result<Option<MaterializationPreparationIdentity>, Self::Error> {
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            let mut current = backend.preparation_identity;
            for (path, slot) in [
                (backend.target.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut current.operation),
                (backend.target.as_path(), &mut current.target),
                (backend.backup.as_path(), &mut current.backup),
            ] {
                *slot = crate::native_host::HostRoot::open(path, &mut &backend.work)?.identity().to_bytes();
            }
            Ok(Some(current))
        }, None).expect("unmetered task construction cannot fail").await?
    }

    async fn observe_allocation_identity(&self) -> Result<Option<[u8; 16]>, Self::Error> {
        let root = self.root.clone();
        let work = self.work.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            crate::native_host::HostRoot::open(root.as_path(), &mut &work)
                .map(|held| Some(held.identity().to_bytes())).map_err(Self::Error::from)
        }, None).expect("unmetered task construction cannot fail").await?
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _edit_allocation = edit_allocation;
            let edit = &edit;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            if backend.exists(&backup)? {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::UnexpectedBackup)?);
            }
            if let MaterializationEdit::Install { image, .. } = edit
                && image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)
                && backend.exists(&live)? {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            if let MaterializationEdit::SetMetadata { image, .. } = edit {
                let desired = decode_native_metadata(image)?;
                let (_file, metadata) = backend.metadata_file(&live)?;
                let before = native_metadata_fingerprint(&metadata, None, &backend.work)?;
                let after = native_metadata_fingerprint(&metadata, Some(&desired), &backend.work)?;
                let (image, custody) = encode_native_witness(Some(before), Some(after), Some(native_metadata_from_stat(&metadata)), &backend.work)?;
                return Ok(MaterializationPreimage { image, custody: Some(custody) });
            }
            let before = backend.fingerprint(&live, !matches!(edit, MaterializationEdit::SetMetadata { .. }))?;
            let after = match edit {
                MaterializationEdit::Install { .. } => match backend.fingerprint(&target, true)? {
                    Some(image) => Some(image),
                    None => return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::MissingTarget)?),
                },
                MaterializationEdit::Remove { .. } => None,
                MaterializationEdit::SetMetadata { .. } => unreachable!("metadata captured from its held allocation"),
                MaterializationEdit::Rename { .. } => {
                    return Err(NativeTreeMaterializationError::UnsupportedRename);
                }
            };
            let (image, custody) = encode_native_witness(before, after, None, &backend.work)?;
            Ok(MaterializationPreimage { image, custody: Some(custody) })
        }, None).expect("unmetered task construction cannot fail")
        .await?
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let preimage = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _edit_allocation = edit_allocation;
            let edit = &edit;
            let preimage = &preimage;
            let (before, after) = decode_native_witness(&preimage.image)?;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            let current = backend.fingerprint(&live, !matches!(edit, MaterializationEdit::SetMetadata { .. }))?;
            let target_current = if matches!(edit, MaterializationEdit::Install { .. }) {
                backend.fingerprint(&target, true)?
            } else {
                None
            };
            let backup_current = backend.fingerprint(&backup, true)?;
            let backup_exists = backup_current.is_some();
            if current == before && current == after && target_current.is_none() && backup_exists {
                return Ok(MaterializationObservation::Postimage);
            }
            if current == before {
                if matches!(edit, MaterializationEdit::Install { .. }) && target_current != after {
                    return Ok(MaterializationObservation::Diverged);
                }
                return Ok(MaterializationObservation::Preimage);
            }
            if current == after {
                if target_current.is_some() && target_current != after {
                    return Ok(MaterializationObservation::Diverged);
                }
                return Ok(MaterializationObservation::Postimage);
            }

            if current.is_none()
                && backup_current == before
                && matches!(edit, MaterializationEdit::Install { .. })
            {
                return if target_current.is_none() || target_current == after {
                    Ok(MaterializationObservation::Interrupted)
                } else {
                    Ok(MaterializationObservation::Diverged)
                };
            }
            Ok(MaterializationObservation::Diverged)
        }, None).expect("unmetered task construction cannot fail")
        .await?
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let preimage = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _edit_allocation = edit_allocation;
            let edit = &edit;
            let preimage = &preimage;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            let (before, after) = decode_native_witness(&preimage.image)?;
            if let MaterializationEdit::SetMetadata { image, .. } = edit {
                let current = backend.fingerprint(&live, !matches!(edit, MaterializationEdit::SetMetadata { .. }))?;
                if current == after {
                    return Ok(());
                }
                if current != before {
                    return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
                }
                return backend.set_metadata(&live, &decode_native_metadata(image)?, before);
            }
            let target_expected = matches!(edit, MaterializationEdit::Install { .. });
            let backup_exists = backend.exists(&backup)?;
            let target_fingerprint = backend.fingerprint(&target, true)?;
            let target_exists = target_fingerprint.is_some();
            let live_fingerprint = backend.fingerprint(&live, true)?;
            let live_exists = live_fingerprint.is_some();
            if !backup_exists && !target_exists && live_fingerprint == after {
                return Ok(());
            }
            if backup_exists && !target_exists && live_fingerprint == after {
                return Ok(());
            }
            if backup_exists && live_fingerprint.is_some() && live_fingerprint != after {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            if !backup_exists && live_fingerprint != before {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            if target_expected && target_fingerprint != after {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            if backup_exists {
                if target_expected && target_exists {
                    backend.remove(&live)?;
                    backend.rename(&target, &live)?;
                } else if target_expected && !live_exists {
                    return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::MissingTarget)?);
                } else if !target_expected {
                    backend.remove(&live)?;
                }
                let (parent, _) = backend.parent(&backup)?;
                crate::native_host::sync_directory_held(&parent, &mut &backend.work)?;
                return Ok(());
            }
            if live_exists {
                backend.create_parents(&backup)?;
                backend.rename(&live, &backup)?;
            }
            if target_expected {
                if !target_exists {
                    return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::MissingTarget)?);
                }
                backend.create_parents(&live)?;
                backend.rename(&target, &live)?;
            }
            Ok(())
        }, None).expect("unmetered task construction cannot fail")
        .await?
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let preimage = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _edit_allocation = edit_allocation;
            let edit = &edit;
            let preimage = &preimage;
            let path = edit_path(edit);
            let (live, _, backup) = backend.paths(path);
            let (before, after) = decode_native_witness(&preimage.image)?;
            let current = backend.fingerprint(&live, !matches!(edit, MaterializationEdit::SetMetadata { .. }))?;
            if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                if current == before {
                    return Ok(());
                }
                if current != after {
                    return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
                }
                let metadata = decode_native_preimage_metadata(&preimage.image)?
                    .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
                return backend.set_metadata(&live, &metadata, after);
            }
            let backup_current = backend.fingerprint(&backup, true)?;
            let backup_exists = backup_current.is_some();
            if backup_exists && backup_current != before {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            if current == before && !backup_exists {
                return Ok(());
            }
            if current != after && !(current.is_none() && backup_exists) {
                return Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::ExternalMutation)?);
            }
            match before {
                None => backend.remove(&live),
                Some(_) if backup_exists => {
                    backend.remove(&live)?;
                    backend.rename(&backup, &live)
                }
                Some(expected) if current == Some(expected) => Ok(()),
                Some(_) => Err(native_diagnostic(&backend.work, path, NativeTreeMaterializationError::MissingPreimage)?),
            }
        }, None).expect("unmetered task construction cannot fail")
        .await?
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_native_held(parent: &cap_std::fs::Dir, leaf: &Path, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    with_native_walk_leaf(leaf, work, |name| remove_native_walk(parent, name, work))
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_native_walk_leaf(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, directory: bool, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    {
        if directory {
            crate::native_host::remove_empty_directory_held_with_work(parent, Path::new(name), &mut &*work)
        } else {
            crate::native_host::remove_leaf_with_work(parent, Path::new(name), &mut &*work)
        }
    }
    #[cfg(windows)]
    { crate::native_host::remove_windows_leaf_with_work(parent, name, directory, &mut &*work) }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeRemoveFrame {
    directory: cap_std::fs::Dir,
    reader: crate::native_host::HostStatReader,
    cursor: NativeAllocation,
    name: Option<(NativeWalkName, NativeAllocation)>,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_remove_frame(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, metadata: &crate::native_host::HostStat, work: &NativeMaterializationWork) -> Result<NativeRemoveFrame, NativeTreeMaterializationError> {
    let directory = open_native_walk_directory(parent, name, work)?;
    let actual = crate::native_host::directory_identity(&directory, &mut &*work)?;
    if actual != native_stat_identity(metadata) {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected: native_stat_identity(metadata), actual });
    }
    let (reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    Ok(NativeRemoveFrame { directory, reader, cursor, name: None })
}

#[cfg(not(target_arch = "wasm32"))]
fn remove_native_walk(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    let metadata = match stat_native_walk_name(parent, name, work) {
        Ok(value) => value,
        Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return remove_native_walk_leaf(parent, name, false, work);
    }
    let root = native_remove_frame(parent, name, &metadata, work)?;
    let mut storage = work.claim(std::mem::size_of::<NativeRemoveFrame>() as u64)?;
    let mut frames = Vec::new();
    frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    frames.push(root);
    while let Some(frame) = frames.last_mut() {
        if let Some((child, allocation)) = next_native_walk_name(&mut frame.reader, work)? {
            let metadata = stat_native_walk_name(&frame.directory, &child, work)?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                let mut next = native_remove_frame(&frame.directory, &child, &metadata, work)?;
                next.name = Some((child, allocation));
                if frames.len() == frames.capacity() {
                    storage.grow(std::mem::size_of::<NativeRemoveFrame>() as u64)?;
                    frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
                }
                frames.push(next);
            } else {
                remove_native_walk_leaf(&frame.directory, &child, false, work)?;
                drop(child);
                drop(allocation);
            }
        } else {
            let frame = frames.pop().expect("current removal frame");
            crate::native_host::sync_directory_held(&frame.directory, &mut &*work)?;
            let NativeRemoveFrame { directory, reader, cursor, name: child } = frame;
            drop(reader);
            drop(cursor);
            drop(directory);
            let parent = frames.last().map_or(parent, |frame| &frame.directory);
            if let Some((child, allocation)) = child {
                remove_native_walk_leaf(parent, &child, true, work)?;
                drop(child);
                drop(allocation);
            } else {
                remove_native_walk_leaf(parent, name, true, work)?;
            }
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. }
        | MaterializationEdit::Remove { path }
        | MaterializationEdit::SetMetadata { path, .. } => path,
        MaterializationEdit::Rename { from, .. } => from,
    }
}

#[cfg(not(target_arch = "wasm32"))]
const NATIVE_WITNESS_VERSION: u8 = 2;

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Debug)]
struct NativeMetadataImage {
    readonly: bool,
    posix_mode: Option<u32>,
    windows_attributes: Option<u32>,
}

#[cfg(not(target_arch = "wasm32"))]
type NativeFingerprint = [u8; 32];

#[cfg(not(target_arch = "wasm32"))]
type NativeWitness = (Option<NativeFingerprint>, Option<NativeFingerprint>);

#[cfg(not(target_arch = "wasm32"))]
fn encode_native_witness(
    before: Option<[u8; 32]>,
    after: Option<[u8; 32]>,
    metadata: Option<NativeMetadataImage>,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    use std::io::Write as _;
    let mut writer = NativeImageWriter::new(work, true)?;
    let mut header = [0; 67];
    header[0] = NATIVE_WITNESS_VERSION;
    for (offset, fingerprint) in [(1, before), (34, after)] {
        header[offset] = u8::from(fingerprint.is_some());
        header[offset + 1..offset + 33].copy_from_slice(&fingerprint.unwrap_or([0; 32]));
    }
    if let Err(error) = writer.write_all(&header) {
        return Err(writer.failure.take().unwrap_or_else(|| error.into()));
    }
    if let Some(metadata) = metadata {
        if let Err(error) = writer.write_all(&native_metadata_bytes(&metadata)) {
            return Err(writer.failure.take().unwrap_or_else(|| error.into()));
        }
    }
    Ok((writer.image, writer.allocation))
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeImageWriter<'a> {
    image: Vec<u8>,
    work: &'a NativeMaterializationWork,
    encoded: bool,
    failure: Option<NativeTreeMaterializationError>,
    allocation: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> NativeImageWriter<'a> {
    fn new(work: &'a NativeMaterializationWork, encoded: bool) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self { image: Vec::new(), work, encoded, failure: None, allocation: work.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED)? })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<usize, NativeTreeMaterializationError> {
        let remaining = self.work.remaining()?;
        let available = if self.encoded { remaining.bytes_encoded.min(remaining.bytes_copied) }
            else { remaining.bytes_copied };
        let length = bytes.len().min(usize::try_from(available).unwrap_or(usize::MAX));
        let delta = crate::WorkCounters { bytes_encoded: if self.encoded { length as u64 } else { 0 },
            bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
        if length == 0 && !bytes.is_empty() {
            self.work.admit(crate::WorkCounters { bytes_encoded: u64::from(self.encoded),
                bytes_copied: 1, ..crate::WorkCounters::UNCHARGED })?;
        }
        let mut copy = self.work.reserve_work(delta)?;
        let end = self.image.len().checked_add(length).ok_or(crate::WorkError::Overflow)?;
        if end > self.image.capacity() {
            let next = self.image.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?.max(8).max(end);
            self.allocation.grow(owned_size(next - self.image.capacity(), 1)?)?;
            self.image.try_reserve_exact(next - self.image.len())
                .map_err(NativeTreeMaterializationError::AllocationFailed)?;
            self.allocation.resize_retained(self.image.capacity() as u64)?;
        }
        self.image.extend_from_slice(&bytes[..length]);
        copy.record_completed(delta)?;
        Ok(length)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::io::Write for NativeImageWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes).map_err(|error| {
            self.failure = Some(error);
            std::io::ErrorKind::Other.into()
        })
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_witness(image: &[u8]) -> Result<NativeWitness, NativeTreeMaterializationError> {
    let (witness, _) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let image: &[u8; 67] = witness
        .try_into()
        .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
    let (version, fields) = image
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    if *version != NATIVE_WITNESS_VERSION {
        return Err(NativeTreeMaterializationError::InvalidPreimage);
    }
    let decode = |present: u8, bytes: &[u8]| {
        let fingerprint: [u8; 32] = bytes
            .try_into()
            .map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
        match present {
            0 if fingerprint == [0; 32] => Ok(None),
            1 => Ok(Some(fingerprint)),
            _ => Err(NativeTreeMaterializationError::InvalidPreimage),
        }
    };
    let (before, after) = fields
        .split_at_checked(33)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let (before_present, before_bytes) = before
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    let (after_present, after_bytes) = after
        .split_first()
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    Ok((
        decode(*before_present, before_bytes)?,
        decode(*after_present, after_bytes)?,
    ))
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_preimage_metadata(
    image: &[u8],
) -> Result<Option<NativeMetadataImage>, NativeTreeMaterializationError> {
    let (_, metadata) = image
        .split_at_checked(67)
        .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
    if metadata.is_empty() {
        Ok(None)
    } else {
        decode_native_metadata(metadata).map(Some)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn decode_native_metadata(
    image: &[u8],
) -> Result<NativeMetadataImage, NativeTreeMaterializationError> {
    let [readonly, posix, p0, p1, p2, p3, windows, w0, w1, w2, w3] = image else {
        return Err(NativeTreeMaterializationError::InvalidPreimage);
    };
    if *readonly > 1 || *posix > 1 || *windows > 1
        || (*posix == 0 && [*p0, *p1, *p2, *p3] != [0; 4])
        || (*windows == 0 && [*w0, *w1, *w2, *w3] != [0; 4]) {
        return Err(NativeTreeMaterializationError::InvalidPreimage);
    }
    Ok(NativeMetadataImage {
        readonly: *readonly != 0,
        posix_mode: (*posix != 0).then(|| u32::from_le_bytes([*p0, *p1, *p2, *p3])),
        windows_attributes: (*windows != 0).then(|| u32::from_le_bytes([*w0, *w1, *w2, *w3])),
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_bytes(metadata: &NativeMetadataImage) -> [u8; 11] {
    let mut bytes = [0; 11];
    bytes[0] = u8::from(metadata.readonly);
    bytes[1] = u8::from(metadata.posix_mode.is_some());
    bytes[2..6].copy_from_slice(&metadata.posix_mode.unwrap_or(0).to_le_bytes());
    bytes[6] = u8::from(metadata.windows_attributes.is_some());
    bytes[7..11].copy_from_slice(&metadata.windows_attributes.unwrap_or(0).to_le_bytes());
    bytes
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn native_stat_held(parent: &cap_std::fs::Dir, leaf: &Path, work: &NativeMaterializationWork) -> Result<crate::native_host::HostStat, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { crate::native_host::stat_leaf_with_work(parent, leaf.as_os_str(), &mut &*work) }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        let mut count = 0_usize;
        for _unit in leaf.as_os_str().encode_wide() {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        }
        let copied = crate::WorkCounters { bytes_copied: owned_size(count, std::mem::size_of::<u16>())?, ..crate::WorkCounters::default() };
        work.admit(copied)?;
        let _allocation = work.claim(copied.bytes_copied)?;
        let mut wide = Vec::new();
        wide.try_reserve_exact(count).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        wide.extend(leaf.as_os_str().encode_wide());
        work.spent(copied)?;
        crate::native_host::stat_windows_leaf_with_work(parent, &wide, &mut &*work)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_stat_identity(metadata: &crate::native_host::HostStat) -> crate::NativeRootIdentity {
    #[cfg(unix)]
    { crate::NativeRootIdentity { device: metadata.dev(), object: metadata.ino() } }
    #[cfg(windows)]
    { crate::NativeRootIdentity { device: u64::from(metadata.volume_serial_number()), object: metadata.file_index() } }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_stat_readonly(metadata: &crate::native_host::HostStat) -> bool {
    #[cfg(unix)]
    { metadata.mode() & 0o222 == 0 }
    #[cfg(windows)]
    { metadata.file_attributes() & 1 != 0 }
}
#[cfg(not(target_arch = "wasm32"))]
impl NativeTreeMaterializationBackend {
    fn parent<'a>(&self, path: &NativeEntry<'a>) -> Result<(cap_std::fs::Dir, &'a Path), NativeTreeMaterializationError> {
        self.work.charge(crate::WorkCounters { source_path_components: 1, ..crate::WorkCounters::default() })?;
        let parent = path.root.open_dir_held(path.path.parent().unwrap_or_else(|| Path::new("")), &mut &self.work)?;
        let leaf = path.path.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        Ok((parent, Path::new(leaf)))
    }

    fn fingerprint(&self, path: &NativeEntry<'_>, contents: bool) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
        let (parent, leaf) = match self.parent(path) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let metadata = match native_stat_held(&parent, leaf, &self.work) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut hasher = blake3::Hasher::new();
        hash_native_entry(self, &parent, &leaf, &metadata, &mut hasher, contents)?;
        Ok(Some(*hasher.finalize().as_bytes()))
    }
}

#[cfg(windows)]
fn native_directory_contains(directory: &cap_std::fs::Dir, wanted: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
    if crate::native_host::directory_identity(directory, &mut &*work)? == wanted { return Ok(true); }
    let (reader, cursor) = crate::native_host::read_dir_held_with_work(directory, &mut &*work)?;
    let mut frames = Vec::new();
    let mut storage = work.claim(std::mem::size_of::<(Option<cap_std::fs::Dir>, crate::native_host::HostStatReader, NativeAllocation)>() as u64)?;
    frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    frames.push((None, reader, cursor));
    while let Some((held, reader, _cursor)) = frames.last_mut() {
        let parent = held.as_ref().unwrap_or(directory);
        let Some((name, allocation)) = next_native_walk_name(reader, work)? else { frames.pop(); continue; };
        let metadata = stat_native_walk_name(parent, &name, work)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            let child = open_native_walk_directory(parent, &name, work)?;
            let actual = crate::native_host::directory_identity(&child, &mut &*work)?;
            if actual != native_stat_identity(&metadata) { return Err(NativeTreeMaterializationError::AllocationChanged { expected: native_stat_identity(&metadata), actual }); }
            if actual == wanted { return Ok(true); }
            let (reader, cursor) = crate::native_host::read_dir_held_with_work(&child, &mut &*work)?;
            if frames.len() == frames.capacity() {
                storage.grow(std::mem::size_of::<(Option<cap_std::fs::Dir>, crate::native_host::HostStatReader, NativeAllocation)>() as u64)?;
                frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
            }
            frames.push((Some(child), reader, cursor));
        }
        drop(name);
        drop(allocation);
    }
    Ok(false)
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_metadata_edit(
    metadata: &crate::native_host::HostStat,
    desired: Option<&NativeMetadataImage>,
    hasher: &mut blake3::Hasher,
) {
    let file_type = metadata.file_type();
    hasher.update(if file_type.is_symlink() {
        b"link"
    } else if file_type.is_dir() {
        b"directory"
    } else if file_type.is_file() {
        b"file"
    } else {
        b"special"
    });
    #[cfg(unix)]
    {
        for value in [metadata.dev(), metadata.ino()] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(
            &u64::from(
                desired
                    .and_then(|value| value.posix_mode)
                    .unwrap_or_else(|| metadata.mode()),
            )
            .to_le_bytes(),
        );
    }
    #[cfg(windows)]
    {
        let attributes = desired
            .and_then(|value| value.windows_attributes)
            .unwrap_or_else(|| {
                let attributes = metadata.file_attributes();
                match desired {
                    Some(value) if value.readonly => attributes | 1,
                    Some(_) => attributes & !1,
                    None => attributes,
                }
            });
        hasher.update(&attributes.to_le_bytes());
    }
    #[cfg(not(any(unix, windows)))]
    hasher.update(&[u8::from(desired.map_or_else(
        || metadata.permissions().readonly(),
        |value| value.readonly,
    ))]);
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_fingerprint(metadata: &crate::native_host::HostStat, desired: Option<&NativeMetadataImage>, work: &NativeMaterializationWork) -> Result<[u8; 32], NativeTreeMaterializationError> {
    let kind: &[u8] = if metadata.file_type().is_symlink() { b"link" } else if metadata.is_dir() { b"directory" } else if metadata.file_type().is_file() { b"file" } else { b"special" };
    #[cfg(unix)]
    let metadata_bytes = 24;
    #[cfg(windows)]
    let metadata_bytes = if metadata.is_dir() || metadata.file_type().is_file() { 20 } else { 4 };
    work.charge(crate::WorkCounters { source_entries_visited: 1, bytes_hashed: kind.len() as u64 + metadata_bytes, ..crate::WorkCounters::default() })?;
    let mut hasher = blake3::Hasher::new();
    hash_native_metadata_edit(metadata, desired, &mut hasher);
    #[cfg(windows)]
    if metadata.is_dir() || metadata.file_type().is_file() { hasher.update(&native_stat_identity(metadata).to_bytes()); }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
type NativeWalkName = std::ffi::OsString;
#[cfg(windows)]
type NativeWalkName = Vec<u16>;
#[cfg(unix)]
type NativeWalkNameRef = std::ffi::OsStr;
#[cfg(windows)]
type NativeWalkNameRef = [u16];

#[cfg(not(target_arch = "wasm32"))]
fn with_native_walk_leaf<T>(leaf: &Path, work: &NativeMaterializationWork, action: impl FnOnce(&NativeWalkNameRef) -> Result<T, NativeTreeMaterializationError>) -> Result<T, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { let _ = work; action(leaf.as_os_str()) }
    #[cfg(windows)]
    { crate::native_host::with_wide_path_with_work(leaf, &mut &*work, |name, _| action(name)) }
}

#[cfg(not(target_arch = "wasm32"))]
fn next_native_walk_name(reader: &mut crate::native_host::HostStatReader, work: &NativeMaterializationWork) -> Result<Option<(NativeWalkName, NativeAllocation)>, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { reader.next_with_work(&mut &*work).map(|entry| entry.map(|(entry, allocation)| (entry.name, allocation))) }
    #[cfg(windows)]
    { reader.next_wide_with_work(&mut &*work).map(|entry| entry.map(|(entry, allocation)| (entry.name, allocation))) }
}

#[cfg(not(target_arch = "wasm32"))]
fn stat_native_walk_name(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, work: &NativeMaterializationWork) -> Result<crate::native_host::HostStat, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { crate::native_host::stat_leaf_with_work(parent, name, &mut &*work) }
    #[cfg(windows)]
    { crate::native_host::stat_windows_leaf_with_work(parent, name, &mut &*work) }
}

#[cfg(not(target_arch = "wasm32"))]
fn open_native_walk_directory(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, work: &NativeMaterializationWork) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { crate::native_host::open_unix_directory_with_work(parent, name, false, &mut &*work) }
    #[cfg(windows)]
    {
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        crate::native_host::open_windows_leaf_handle_with_work(parent, name, FILE_GENERIC_READ, Some(true), &mut &*work)
            .map(cap_std::fs::Dir::from_std_file)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn open_native_walk_file(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, work: &NativeMaterializationWork) -> Result<std::fs::File, NativeTreeMaterializationError> {
    #[cfg(unix)]
    { crate::native_host::open_leaf_with_work(parent, Path::new(name), &crate::native_host::NativeOpenOptions::Read, &mut &*work) }
    #[cfg(windows)]
    {
        use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
        crate::native_host::open_windows_leaf_handle_with_work(parent, name, FILE_GENERIC_READ, Some(false), &mut &*work)
    }
}

#[cfg(windows)]
fn native_windows_name_bytes(name: &[u16]) -> impl Iterator<Item = u8> + '_ {
    native_utf16_name_bytes(name.iter().copied())
}

#[cfg(all(not(target_arch = "wasm32"), any(windows, all(feature = "local", feature = "native-mount"))))]
fn native_utf16_name_bytes(units: impl Iterator<Item = u16>) -> impl Iterator<Item = u8> {
    std::char::decode_utf16(units).flat_map(|decoded| {
        let value = match decoded { Ok(value) => u32::from(value), Err(error) => u32::from(error.unpaired_surrogate()) };
        let mut bytes = [0; 4];
        let length = if value < 0x80 {
            bytes[0] = value as u8; 1
        } else if value < 0x800 {
            bytes[0] = 0xc0 | (value >> 6) as u8;
            bytes[1] = 0x80 | (value & 63) as u8; 2
        } else if value < 0x10000 {
            bytes[0] = 0xe0 | (value >> 12) as u8;
            bytes[1] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[2] = 0x80 | (value & 63) as u8; 3
        } else {
            bytes[0] = 0xf0 | (value >> 18) as u8;
            bytes[1] = 0x80 | ((value >> 12) & 63) as u8;
            bytes[2] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[3] = 0x80 | (value & 63) as u8; 4
        };
        bytes.into_iter().take(length)
    })
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_host_name_matches(host: &NativeWalkNameRef, logical: &crate::kernel::LogicalName, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
    fn equal<T: Eq>(mut left: impl Iterator<Item = T>, mut right: impl Iterator<Item = T>, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
        loop {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            match (left.next(), right.next()) {
                (Some(a), Some(b)) if a == b => {}
                (None, None) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }
    match logical.encoding() {
        crate::kernel::NameEncoding::WindowsUtf16Le => {
            if logical.as_bytes().len() % 2 != 0 { return Ok(false); }
            let units = logical.as_bytes().chunks_exact(2).map(|pair| u16::from_le_bytes([pair[0], pair[1]]));
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt as _;
                equal(host.as_bytes().iter().copied(), native_utf16_name_bytes(units), work)
            }
            #[cfg(windows)]
            { equal(host.iter().copied(), units, work) }
        }
        crate::kernel::NameEncoding::Utf8 | crate::kernel::NameEncoding::PosixBytes => {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt as _;
                equal(host.as_bytes().iter().copied(), logical.as_bytes().iter().copied(), work)
            }
            #[cfg(windows)]
            {
                work.charge(crate::WorkCounters { items_examined: logical.as_bytes().len() as u64, ..crate::WorkCounters::default() })?;
                let Ok(text) = std::str::from_utf8(logical.as_bytes()) else { return Ok(false); };
                equal(host.iter().copied(), text.encode_utf16(), work)
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_walk_name(work: &NativeMaterializationWork, name: &NativeWalkNameRef, hasher: &mut blake3::Hasher) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    { hash_native_name(work, name, hasher) }
    #[cfg(windows)]
    {
        work.charge(crate::WorkCounters { bytes_hashed: owned_size(name.len(), 2)?, ..crate::WorkCounters::default() })?;
        for unit in name { hasher.update(&unit.to_le_bytes()); }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeHashFrame {
    directory: cap_std::fs::Dir,
    names: Vec<(NativeWalkName, NativeAllocation)>,
    _storage: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_hash_frame(directory: cap_std::fs::Dir, work: &NativeMaterializationWork) -> Result<NativeHashFrame, NativeTreeMaterializationError> {
    let (mut reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    let mut names = Vec::new();
    let mut storage = work.claim(0)?;
    while let Some(name) = next_native_walk_name(&mut reader, work)? {
        if names.len() == names.capacity() {
            let additional = names.capacity().max(1);
            let bytes = additional.checked_mul(std::mem::size_of::<(NativeWalkName, NativeAllocation)>()).ok_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
            storage.grow(bytes as u64)?;
            names.try_reserve_exact(additional).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        }
        names.push(name);
    }
    drop(reader);
    drop(cursor);
    native_sort_walk_names(&mut names, work)?;
    Ok(NativeHashFrame { directory, names, _storage: storage })
}

#[cfg(not(target_arch = "wasm32"))]
fn native_sort_walk_names(names: &mut [(NativeWalkName, NativeAllocation)], work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
    fn less(left: &NativeWalkName, right: &NativeWalkName, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
        #[cfg(unix)]
        let (mut left, mut right) = {
            use std::os::unix::ffi::OsStrExt as _;
            (left.as_bytes().iter().copied(), right.as_bytes().iter().copied())
        };
        #[cfg(windows)]
        let (mut left, mut right) = (native_windows_name_bytes(left), native_windows_name_bytes(right));
        loop {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            match (left.next(), right.next()) {
                (Some(a), Some(b)) if a == b => continue,
                (Some(a), Some(b)) => return Ok(a < b),
                (None, Some(_)) => return Ok(true),
                _ => return Ok(false),
            }
        }
    }
    native_sort_by(names, work, |left, right| less(&left.0, &right.0, work))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_sort_by<T>(values: &mut [T], work: &NativeMaterializationWork, mut less: impl FnMut(&T, &T) -> Result<bool, NativeTreeMaterializationError>) -> Result<(), NativeTreeMaterializationError> {
    fn sift<T>(values: &mut [T], mut root: usize, end: usize, work: &NativeMaterializationWork, less: &mut impl FnMut(&T, &T) -> Result<bool, NativeTreeMaterializationError>) -> Result<(), NativeTreeMaterializationError> {
        while let Some(child) = root.checked_mul(2).and_then(|value| value.checked_add(1)).filter(|child| *child < end) {
            let mut smallest = child;
            if child + 1 < end && less(&values[child + 1], &values[child])? { smallest = child + 1; }
            if !less(&values[smallest], &values[root])? { break; }
            work.charge(crate::WorkCounters { bytes_copied: owned_size(2, std::mem::size_of::<T>())?, ..crate::WorkCounters::default() })?;
            values.swap(root, smallest);
            root = smallest;
        }
        Ok(())
    }
    let length = values.len();
    for root in (0..length / 2).rev() { sift(values, root, length, work, &mut less)?; }
    for end in (1..length).rev() {
        work.charge(crate::WorkCounters { bytes_copied: owned_size(2, std::mem::size_of::<T>())?, ..crate::WorkCounters::default() })?;
        values.swap(0, end);
        sift(values, 0, end, work, &mut less)?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_entry(backend: &NativeTreeMaterializationBackend, parent: &cap_std::fs::Dir, leaf: &Path, metadata: &crate::native_host::HostStat, hasher: &mut blake3::Hasher, contents: bool) -> Result<(), NativeTreeMaterializationError> {
    let mut frames = Vec::new();
    let mut storage = backend.work.claim(0)?;
    let first = with_native_walk_leaf(leaf, &backend.work, |name| hash_native_entry_shallow(backend, parent, name, metadata, hasher, contents))?;
    if let Some(frame) = first {
        storage.grow(std::mem::size_of::<NativeHashFrame>() as u64)?;
        frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        frames.push(frame);
    }
    while let Some(frame) = frames.last_mut() {
        let Some((name, allocation)) = frame.names.pop() else {
            frames.pop();
            continue;
        };
        hash_native_walk_name(&backend.work, &name, hasher)?;
        let child = stat_native_walk_name(&frame.directory, &name, &backend.work)?;
        let next = hash_native_entry_shallow(backend, &frame.directory, &name, &child, hasher, true)?;
        drop(name);
        drop(allocation);
        if let Some(next) = next {
            if frames.len() == frames.capacity() {
                storage.grow(std::mem::size_of::<NativeHashFrame>() as u64)?;
                frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
            }
            frames.push(next);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_entry_shallow(
    backend: &NativeTreeMaterializationBackend,
    parent: &cap_std::fs::Dir,
    leaf: &NativeWalkNameRef,
    metadata: &crate::native_host::HostStat,
    hasher: &mut blake3::Hasher,
    contents: bool,
) -> Result<Option<NativeHashFrame>, NativeTreeMaterializationError> {
    use std::io::Read as _;
    let kind: &[u8] = if metadata.file_type().is_symlink() { b"link" } else if metadata.is_dir() { b"directory" } else if metadata.file_type().is_file() { b"file" } else { b"special" };
    #[cfg(unix)]
    let metadata_bytes = if contents { 81 } else { 24 };
    #[cfg(windows)]
    let metadata_bytes = if contents { 33 } else if metadata.is_dir() || metadata.file_type().is_file() { 20 } else { 4 };
    #[cfg(not(any(unix, windows)))]
    let metadata_bytes = if contents { 9 } else { 1 };
    backend.work.charge(crate::WorkCounters { source_entries_visited: 1, bytes_hashed: kind.len() as u64 + metadata_bytes, ..crate::WorkCounters::default() })?;
    if !contents {
        hash_native_metadata_edit(metadata, None, hasher);
        #[cfg(windows)]
        if metadata.is_dir() || metadata.file_type().is_file() {
            hasher.update(&native_stat_identity(metadata).to_bytes());
        }
        return Ok(None);
    }
    hasher.update(kind);
    hasher.update(&metadata.len().to_le_bytes());
    hasher.update(&[u8::from(native_stat_readonly(metadata))]);
    hash_native_metadata(metadata, hasher);
    if metadata.file_type().is_symlink() {
        #[cfg(unix)]
        let (target, allocation) = crate::native_host::read_link_held_with_work(parent, Path::new(leaf), backend.work.remaining()?.bytes_hashed, &mut &backend.work)?;
        #[cfg(windows)]
        let (target, allocation) = crate::native_host::read_link_held_wide_with_work(parent, leaf, backend.work.remaining()?.bytes_hashed, &mut &backend.work)?;
        #[cfg(unix)]
        {
            backend.work.charge(crate::WorkCounters { bytes_hashed: target.len() as u64, ..crate::WorkCounters::default() })?;
            hasher.update(&target);
        }
        #[cfg(windows)]
        {
            backend.work.charge(crate::WorkCounters { bytes_hashed: owned_size(target.len(), 2)?, ..crate::WorkCounters::default() })?;
            for unit in &target { hasher.update(&unit.to_le_bytes()); }
        }
        drop(target);
        drop(allocation);
    } else if metadata.is_dir() {
        let directory = open_native_walk_directory(parent, leaf, &backend.work)?;
        let opened = crate::native_host::directory_identity(&directory, &mut &backend.work)?;
        if opened != native_stat_identity(metadata) {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected: native_stat_identity(metadata), actual: opened });
        }
        return native_hash_frame(directory, &backend.work).map(Some);
    } else if metadata.file_type().is_file() {
        let mut file = open_native_walk_file(parent, leaf, &backend.work)?;
        let opened = crate::native_host::stat_file_with_work(&file, &mut &backend.work)?;
        if native_stat_identity(&opened) != native_stat_identity(metadata) || opened.len() != metadata.len() {
            return Err(native_diagnostic(&backend.work, format_args!("{leaf:?}"), NativeTreeMaterializationError::ExternalMutation)?);
        }
        let mut buffer = [0; 64 * 1024];
        let mut offset = 0;
        while offset < metadata.len() {
            let remaining = backend.work.remaining()?;
            let length = (metadata.len() - offset).min(buffer.len() as u64).min(remaining.source_bytes_read).min(remaining.bytes_hashed) as usize;
            if length == 0 {
                backend.work.charge(crate::WorkCounters { source_bytes_read: 1, bytes_hashed: 1, ..crate::WorkCounters::default() })?;
            }
            backend.work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::default() })?;
            let read = file.read(buffer.get_mut(..length).ok_or(NativeTreeMaterializationError::InvalidPreimage)?)?;
            if read == 0 { return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "fingerprint file became shorter").into()); }
            backend.work.spent(crate::WorkCounters { source_bytes_read: read as u64, bytes_hashed: read as u64, ..crate::WorkCounters::default() })?;
            hasher.update(buffer.get(..read).ok_or(NativeTreeMaterializationError::InvalidPreimage)?);
            offset += read as u64;
        }
        let finished = crate::native_host::stat_file_with_work(&file, &mut &backend.work)?;
        if native_stat_identity(&finished) != native_stat_identity(metadata) || finished.len() != metadata.len() {
            return Err(native_diagnostic(&backend.work, format_args!("{leaf:?}"), NativeTreeMaterializationError::ExternalMutation)?);
        }
    }
    Ok(None)
}

#[cfg(all(not(windows), not(target_arch = "wasm32")))]
fn hash_native_name(work: &NativeMaterializationWork, name: &std::ffi::OsStr, hasher: &mut blake3::Hasher) -> Result<(), NativeTreeMaterializationError> {
    let length = name.as_encoded_bytes().len() as u64;
    work.charge(crate::WorkCounters { bytes_hashed: length, ..crate::WorkCounters::default() })?;
    hash_native_os_str(name, hasher);
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &crate::native_host::HostStat,
    hasher: &mut blake3::Hasher,
) {
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(metadata.mode()),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    {
        for value in [
            metadata.nlink(),
            u64::from(metadata.uid()),
            u64::from(metadata.gid()),
            metadata.rdev(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(&metadata.mtime().to_le_bytes());
        hasher.update(&metadata.mtime_nsec().to_le_bytes());
    }
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &crate::native_host::HostStat,
    hasher: &mut blake3::Hasher,
) {
    hasher.update(&u64::from(metadata.file_attributes()).to_le_bytes());
    {
        for value in [metadata.last_write_time(), metadata.len()] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_metadata(
    _metadata: &cap_std::fs::Metadata,
    _hasher: &mut blake3::Hasher,
) {
}

#[cfg(all(test, not(target_arch = "wasm32")))]
fn native_metadata(metadata: &std::fs::Metadata) -> NativeMetadataImage {
    NativeMetadataImage {
        readonly: metadata.permissions().readonly(),
        #[cfg(unix)]
        posix_mode: {
            use std::os::unix::fs::MetadataExt as _;
            Some(metadata.mode())
        },
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: {
            use std::os::windows::fs::MetadataExt as _;
            Some(metadata.file_attributes())
        },
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_metadata_from_stat(metadata: &crate::native_host::HostStat) -> NativeMetadataImage {
    NativeMetadataImage {
        readonly: native_stat_readonly(metadata),
        #[cfg(unix)]
        posix_mode: Some(metadata.mode()),
        #[cfg(not(unix))]
        posix_mode: None,
        #[cfg(windows)]
        windows_attributes: Some(metadata.file_attributes()),
        #[cfg(not(windows))]
        windows_attributes: None,
    }
}

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
fn encode_native_metadata(
    metadata: &crate::WorkspaceMetadata,
    work: &NativeMaterializationWork,
) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
    let readonly = metadata
        .windows_attributes
        .is_some_and(|attributes| attributes & 1 != 0)
        || metadata.posix_mode.is_some_and(|mode| mode & 0o222 == 0);
    let mut writer = NativeImageWriter::new(work, true)?;
    use std::io::Write as _;
    if let Err(error) = writer.write_all(&native_metadata_bytes(&NativeMetadataImage {
        readonly,
        posix_mode: metadata.posix_mode,
        windows_attributes: metadata.windows_attributes,
    })) {
        return Err(writer.failure.take().unwrap_or_else(|| error.into()));
    }
    Ok((writer.image, writer.allocation))
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::unix::ffi::OsStrExt as _;
    hasher.update(value.as_bytes());
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    hasher.update(value.to_string_lossy().as_bytes());
}

/// Native tree publication failure.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Error)]
pub enum NativeTreeMaterializationError {
    /// Root, target, or backup layout is not same-root and operation-scoped.
    #[error("native materialization layout is invalid")]
    InvalidLayout,
    /// The exact target binding disappeared before application.
    #[error("native materialization target is missing for '{0}'")]
    MissingTarget(String, Option<NativeAllocation>),
    /// A physical endpoint changed after its durable witness was captured.
    #[error("native materialization observed an external mutation at '{0}'")]
    ExternalMutation(String, Option<NativeAllocation>),
    /// A durable preimage references a missing backup binding.
    #[error("native materialization preimage is missing for '{0}'")]
    MissingPreimage(String, Option<NativeAllocation>),
    /// Persisted preimage bytes are malformed.
    #[error("native materialization preimage is invalid")]
    InvalidPreimage,
    /// A backup exists before its operation journal has been created.
    #[error("native materialization found an unexpected backup for '{0}'")]
    UnexpectedBackup(String, Option<NativeAllocation>),
    /// Native path exchange does not yet expose a rename edit directly.
    #[error("native materialization rename edits are unsupported")]
    UnsupportedRename,
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A changed namespace path cannot be represented by this host publisher.
    #[error("native materialization path is invalid: '{0}'")]
    InvalidPath(String, Option<NativeAllocation>),
    /// Cooperative cancellation at a host or journal boundary.
    #[error("native materialization cancelled")]
    Cancelled,
    /// The original accounting state panicked; only physical retirement remains.
    #[error("original native work state is poisoned")]
    OriginalWorkPoisoned,
    /// A finite work allowance was exhausted before the next operation.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// The original fallible owned buffer reservation failed.
    #[error("native materialization allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// Source metadata requires observations this native publisher cannot prove.
    #[error("native materialization cannot verify complete source metadata at '{0}'")]
    UnverifiedSourceMetadata(String, Option<NativeAllocation>),
    /// The actual held checkout allocation differs from the original owner.
    #[error("native materialization root allocation changed: expected {expected:?}, observed {actual:?}")]
    AllocationChanged {
        /// Recorded allocation identity.
        expected: crate::NativeRootIdentity,
        /// Observed allocation identity.
        actual: crate::NativeRootIdentity,
    },
    /// An entire native builtin payload exceeds its admitted byte cap.
    #[error("native builtin payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge {
        /// Permitted payload length.
        maximum: u64,
        /// Observed payload length.
        actual: u64,
    },
    /// The exact current held-leaf preimage differs from the edit's read phase.
    #[error("native builtin edit preimage conflicts with the current file")]
    PreimageConflict,
    /// A durable mkdir intent lacks a confirmed created allocation identity.
    #[error("native preparation has an indeterminate allocation; retain the original pending journal without adopting the current path")]
    IndeterminatePreparation,
    /// The original preparation returned without its admitted native owner.
    #[error("preparation journal lost its original native custody")]
    MissingCustody,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTreeMaterializationError {
    /// Moves original diagnostic custody without allocating or checking work.
    #[must_use]
    pub fn into_native_failure(mut self) -> (Self, Option<NativeAllocation>) {
        let allocation = self.take_native_failure_allocation();
        (self, allocation)
    }

    pub(crate) fn take_native_failure_allocation(&mut self) -> Option<NativeAllocation> {
        self.native_failure_allocation_mut().and_then(Option::take)
    }

    pub(crate) fn native_failure_allocation_mut(&mut self) -> Option<&mut Option<NativeAllocation>> {
        match self {
            Self::MissingTarget(_, allocation)
            | Self::ExternalMutation(_, allocation)
            | Self::MissingPreimage(_, allocation)
            | Self::UnexpectedBackup(_, allocation)
            | Self::InvalidPath(_, allocation)
            | Self::UnverifiedSourceMetadata(_, allocation) => Some(allocation),
            _ => None,
        }
    }

    /// Recovers the original runtime meter failure.
    #[must_use]
    pub fn from_runtime_error(error: std::io::Error) -> Self {
        if error.get_ref().and_then(|cause| cause.downcast_ref::<crate::DemandError>())
            .is_some_and(|cause| matches!(cause, crate::DemandError::Work(_) | crate::DemandError::Cancelled | crate::DemandError::Allocation(_) | crate::DemandError::OriginalWorkPoisoned)) {
            native_materialization_work_error(crate::demand::native_demand_error(error))
        } else {
            Self::Io(error)
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_diagnostic(
    work: &NativeMaterializationWork,
    value: impl std::fmt::Display,
    cause: fn(String, Option<NativeAllocation>) -> NativeTreeMaterializationError,
) -> Result<NativeTreeMaterializationError, NativeTreeMaterializationError> {
    let (text, allocation) = work.display(&value)?;
    Ok(cause(text, Some(allocation)))
}

#[cfg(not(target_arch = "wasm32"))]
fn native_check_plan(plan: &MaterializationPlan, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
    let _scratch = work.claim(owned_size(plan.edits.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?, std::mem::size_of::<&str>())?)?;
    validate_plan_steps(
        plan,
        || work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }),
        |capacity| {
            let mut paths = Vec::new();
            paths.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed)?;
            Ok(paths)
        },
        |path| crate::path::is_canonical_relative_with_step(path, || work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })),
        |paths| native_sort_by(paths, work, |left, right| Ok(native_text_compare(left, right, work)? == std::cmp::Ordering::Greater)),
        |left, right| native_path_within(right, left, true, work),
    )
}

/// A bounded publication request for an authenticated workspace generation.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub struct NativeWorkspacePublication<'a> {
    /// Physical checkout root.
    pub root: &'a Path,
    /// Checkout allocation identity captured by the owner.
    pub source_root_identity: crate::NativeRootIdentity,
    /// Service-owned staging and recovery directory.
    pub operation_directory: &'a Path,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Expected physical/logical baseline.
    pub from: GenerationId,
    /// Exact target generation.
    pub to: GenerationId,
    /// Root-level names never touched by publication.
    pub excluded_names: &'a [&'a str],
    /// Native staging policy.
    pub options: &'a crate::MaterializeOptions,
    /// Cumulative planning, host, staging, and durable journal budget.
    pub budget: crate::WorkBudget,
    /// Shared cancellation boundary.
    pub cancellation: &'a crate::CancellationToken,
}

/// Stages and captures a journal before applying checkout edits.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn prepare_native_generation_transition<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
)-> Result<crate::OperationReceipt<MaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)> {
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|(error, work)| (NativeWorkspacePublicationError::Native(error), work))?;
    let mut before_effect = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), true, &mut before_effect).await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

/// Loads a journal through bounded IO without touching the checkout.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn load_native_materialization_journal(
    state: &crate::LocalCoreStateStore,
    operation_id: OperationId,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
)-> Result<crate::OperationReceipt<Option<MaterializationJournal>>, (NativeWorkspacePublicationError, crate::WorkCounters)> {
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| (NativeWorkspacePublicationError::Native(error), work))?;
    let result = async {
        let journal = MaterializationJournalStore::load(&state.materialization_with_work(work.clone()), operation_id).await?;
        if let Some(journal) = &journal {
            if !valid_journal(journal, operation_id) || !native_check_plan(&journal.plan, &work)? {
                return Err(NativeWorkspacePublicationError::MismatchedJournal);
            }
        }
        Ok(journal)
    }.await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

/// Publishes a generation transition with authorization before each action.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn publish_native_generation_transition<A, O, F, Fut>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    mut before_effect: F,
)-> Result<crate::OperationReceipt<MaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), false, &mut before_effect).await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn publish_native_transition_inner<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore, F, Fut>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    prepare_only: bool,
    before_effect: &mut F,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
where
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    before_effect().await?;
    if work.budget() != publication.budget {
        return Err(NativeTreeMaterializationError::InvalidLayout.into());
    }
    let store = state.materialization_with_work(work.clone());
    let NativeWorkspacePublication {
        root,
        source_root_identity,
        operation_directory,
        operation_id,
        from,
        to,
        excluded_names,
        options,
        budget: _,
        cancellation,
    } = publication;
    if from_generation.id() != from || to_generation.id() != to {
        return Err(NativeWorkspacePublicationError::MismatchedJournal);
    }
    let mut root_owned = work.path(root, None)?;
    let mut operation_owned = work.path(operation_directory, None)?;
    root_owned.1.absorb(&mut operation_owned.1)?;
    let path_custody = root_owned.1;
    let root = root_owned.0;
    let operation_directory = operation_owned.0;
    let (root, operation_directory, path_custody) = acyclic_native_runtime::run_blocking_io({
        let work = work.clone();
        move || {
            let actual = validate_native_preparation_location(&root, &operation_directory, &work)?;
            if actual != source_root_identity { return Err(NativeTreeMaterializationError::AllocationChanged { expected: source_root_identity, actual }); }
            Ok::<_, NativeTreeMaterializationError>((root, operation_directory, path_custody))
        }
    }, None).expect("unmetered task construction cannot fail").await??;
    if let Some(mut existing) = MaterializationJournalStore::load(&store, operation_id).await? {
        if existing.root_identity != Some(source_root_identity.to_bytes()) {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if existing.plan.intent != (MaterializationIntent::GenerationTransition { from, to }) {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if !valid_journal(&existing, operation_id) || !native_check_plan(&existing.plan, &work)? {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        work.set_original_work(existing.work);
        if existing.preparation_progress.is_some() {
            existing = NativeTreeMaterializationBackend::prepare_allocations(&store, existing, &root, &operation_directory, &work, before_effect).await?;
        }
        let backend = acyclic_native_runtime::run_blocking_io({
            let work = work.clone();
            move || NativeTreeMaterializationBackend::new(root, operation_directory, Some(path_custody), work, false)
        }, None).expect("unmetered task construction cannot fail")
        .await??;
        if backend.held_root.identity() != source_root_identity {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected: source_root_identity, actual: backend.held_root.identity() }.into());
        }
        let materializer = JournaledMaterializer::new(store, backend);
        if prepare_only && !matches!(existing.phase, MaterializationPhase::Preparing | MaterializationPhase::Prepared) {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if matches!(existing.phase, MaterializationPhase::Preparing | MaterializationPhase::Prepared) {
            materializer.verify_allocation(&existing).await?;
            native_preflight_plan(&materializer.backend().held_root, &existing.plan, from_generation, &work, cancellation).await?;
        }
        if existing.phase == MaterializationPhase::Preparing && existing.preimages.is_empty() {
            let (reset, stage_identity) = materializer.reset_stage(
                existing, "", before_effect,
            ).await?;
            existing = reset;
            before_effect().await?;
            materializer.verify_allocation(&existing).await?;
            let (install_paths, _install_custody) = native_install_paths(existing.plan.edits.iter().filter_map(|edit| match edit {
                MaterializationEdit::Install { path, .. } => Some(path.as_str()),
                _ => None,
            }), &work)?;
            let (path_options, _options_custody) = native_stage_options(options, materializer.backend().target.as_path(), stage_identity, &work)?;
            if !install_paths.is_empty() {
                before_effect().await?;
                materializer.verify_allocation(&existing).await?;
                existing = materializer.stage_generation(
                    to_generation, &install_paths, &path_options, existing,
                    &work, before_effect,
                ).await?;
                before_effect().await?;
                materializer.verify_allocation(&existing).await?;
            }
        }
        let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
        if prepare_only {
            if existing.phase == MaterializationPhase::Prepared { return Ok(existing); }
            return materializer.capture_preimages(existing, &mut guarded).await;
        }
        return materializer
            .replay(existing, MaterializationRecovery::Complete, &mut guarded)
            .await;
    }

    let (changes, changes_custody) = from_generation
        .diff_to_with_native_work(to_generation, u32::MAX, &work, cancellation)
        .await?;
    let (changed, mut changed_custody) = changes
        .changed_paths_with_native_work(u32::MAX, &work, cancellation)
        .await?;
    drop(changes);
    drop(changes_custody);
    let held_root = crate::native_host::HostRoot::open(&root, &mut &work)?;
    if held_root.identity() != source_root_identity {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected: source_root_identity, actual: held_root.identity() }.into());
    }
    let mut paths = Vec::new();
    let mut paths_storage = work.claim(0)?;
    let mut structural_directories = Vec::new();
    let mut structural_storage = work.claim(0)?;
    let mut metadata_edits = Vec::new();
    let mut metadata_storage = work.claim(0)?;
    for change in changed {
        let namespace_bytes = change.path.retained_bytes();
        let before_directory = change.before.is_some_and(|record| record.kind == crate::kernel::FileKind::Directory);
        let after_directory = change.after.is_some_and(|record| record.kind == crate::kernel::FileKind::Directory);
        let after_present = change.after.is_some();
        let (path, mut path_allocation) = native_namespace_string(&change.path, &work)?;
        drop(change);
        changed_custody.release(namespace_bytes)?;
        let top = path.split('/').next().unwrap_or_default();
        let mut excluded = false;
        for name in excluded_names {
            if native_text_compare(top, name, &work)? == std::cmp::Ordering::Equal { excluded = true; break; }
        }
        if excluded { continue; }
        if before_directory && after_directory {
            let (absolute, _absolute_custody) = native_absolute_string(&path, &work)?;
            let stat = to_generation.stat_with_native_work(&absolute, &work, cancellation).await?;
            let (image, mut image_custody) = encode_native_metadata(&stat.metadata, &work)?;
            path_allocation.absorb(&mut image_custody)?;
            native_vec_push(&mut metadata_edits, (MaterializationEdit::SetMetadata { path, image }, path_allocation), &mut metadata_storage)?;
            continue;
        }
        if before_directory || after_directory {
            let mut allocation = work.claim(0)?;
            let directory = duplicate_native_string(&path, &work, &mut allocation)?;
            native_vec_push(&mut structural_directories, (directory, allocation), &mut structural_storage)?;
        }
        native_vec_push(&mut paths, (path, after_present, path_allocation), &mut paths_storage)?;
    }
    drop(changed_custody);
    let mut index = 0;
    while index < paths.len() {
        let mut descendant = false;
        for (directory, _) in &structural_directories {
            if native_path_within(&paths[index].0, directory, false, &work)? { descendant = true; break; }
        }
        if descendant { paths.swap_remove(index); } else { index += 1; }
    }
    drop(structural_directories);
    drop(structural_storage);
    for (path, _, _) in &paths {
        native_preflight_path(&held_root, path, from_generation, true, true, &work, cancellation).await?;
    }
    for (edit, _) in &metadata_edits {
        native_preflight_path(&held_root, edit_path(edit), from_generation, false, true, &work, cancellation).await?;
    }
    native_sort_by(&mut paths, &work, |left, right| Ok(native_text_compare(&left.0, &right.0, &work)? == std::cmp::Ordering::Greater))?;
    let (install_paths, _install_custody) = native_install_paths(paths.iter().filter(|(_, install, _)| *install).map(|(path, _, _)| path.as_str()), &work)?;
    let mut edits = Vec::new();
    let mut plan_custody = work.claim(0)?;
    for (path, install, mut allocation) in paths {
        let edit = if install {
            allocation.grow(1)?;
            let mut image = Vec::new();
            image.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
            image.push(1);
            MaterializationEdit::Install { path, image }
        } else { MaterializationEdit::Remove { path } };
        plan_custody.absorb(&mut allocation)?;
        native_vec_push(&mut edits, edit, &mut plan_custody)?;
    }
    drop(paths_storage);
    for (edit, mut allocation) in metadata_edits {
        plan_custody.absorb(&mut allocation)?;
        native_vec_push(&mut edits, edit, &mut plan_custody)?;
    }
    drop(metadata_storage);
    let plan = MaterializationPlan { operation_id, intent: MaterializationIntent::GenerationTransition { from, to }, edits };
    if !native_check_plan(&plan, &work)? { return Err(NativeWorkspacePublicationError::MismatchedJournal); }
    work.set_original_work(crate::WorkCounters::UNCHARGED);
    let parent = crate::native_host::HostRoot::open(operation_directory.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &mut &work)?;
    let mut journal = MaterializationJournal {
        #[cfg(not(target_arch = "wasm32"))] custody: None,
        version: JOURNAL_VERSION, revision: 0, plan,
        root_identity: Some(source_root_identity.to_bytes()), preparation_identity: None,
        preparation_progress: Some(MaterializationPreparationProgress { parent: parent.identity().to_bytes(), operation: None, target: None, backup: None, pending: None }),
        work: work.journal_work()?, preimages: Vec::new(),
        phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
    };
    journal.custody = Some(plan_custody);
    drop(parent);
    let (stored, original) = compare_next_journal::<_, NativeTreeMaterializationError>(&store, journal).await?;
    if !stored { return Err(NativeWorkspacePublicationError::MismatchedJournal); }
    let mut prepared = NativeTreeMaterializationBackend::prepare_allocations(&store, original, &root, &operation_directory, &work, before_effect).await?;
    let backend = acyclic_native_runtime::run_blocking_io({
        let work = work.clone();
        move || NativeTreeMaterializationBackend::new(root, operation_directory, Some(path_custody), work, false)
    }, None).expect("unmetered task construction cannot fail").await??;
    let materializer = JournaledMaterializer::new(store, backend);
    materializer.verify_allocation(&prepared).await?;
    let allocated_target = crate::NativeRootIdentity::from_bytes(prepared.preparation_identity.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?.target);
    let (path_options, _options_custody) = native_stage_options(options, materializer.backend().target.as_path(), allocated_target, &work)?;
    if !install_paths.is_empty() {
        before_effect().await?;
        materializer.verify_allocation(&prepared).await?;
        prepared = materializer.stage_generation(
            to_generation, &install_paths, &path_options, prepared,
            &work, before_effect,
        ).await?;
        before_effect().await?;
        materializer.verify_allocation(&prepared).await?;
    }
    let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
    if prepare_only {
        return materializer.capture_preimages(prepared, &mut guarded).await;
    }
    materializer
        .replay(prepared, MaterializationRecovery::Complete, &mut guarded)
        .await
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn native_before_effect<A, O, S, Fut>(
    owner: Fut, verify_original_source: bool, store: &S,
    root: &crate::native_host::HostRoot, operation: OperationId,
    generation: &crate::Generation<A, O>, work: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<(), NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore,
    S: MaterializationJournalStore<Error = crate::LocalCoreStateStoreError>,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    owner.await?;
    if verify_original_source {
        let journal = store.load(operation).await?.ok_or(NativeWorkspacePublicationError::MismatchedJournal)?;
        if !valid_journal(&journal, operation) || !native_check_plan(&journal.plan, work)? {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        native_preflight_plan(root, &journal.plan, generation, work, cancellation).await?;
    }
    Ok(())
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn native_preflight_plan<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    root: &crate::native_host::HostRoot, plan: &MaterializationPlan,
    generation: &crate::Generation<A, O>, work: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<(), NativeWorkspacePublicationError> {
    for edit in &plan.edits {
        native_preflight_path(root, edit_path(edit), generation, !matches!(edit, MaterializationEdit::SetMetadata { .. }), false, work, cancellation).await?;
    }
    Ok(())
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn native_preflight_path<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    root: &crate::native_host::HostRoot, path: &str, generation: &crate::Generation<A, O>,
    walk_children: bool, verify_access_time: bool, work: &NativeMaterializationWork, cancellation: &crate::CancellationToken,
) -> Result<(), NativeWorkspacePublicationError> {
    let mut storage = work.claim(0)?;
    let mut pending = Vec::new();
    let mut custody = work.claim(0)?;
    let path = duplicate_native_string(path, work, &mut custody)?;
    native_vec_push(&mut pending, (path, walk_children, custody), &mut storage)?;
    while let Some((path, walk_children, _custody)) = pending.pop() {
        let (absolute, _absolute_custody) = native_absolute_string(&path, work)?;
        let source = match generation.stat_with_native_work(&absolute, work, cancellation).await {
            Ok(source) => Some(source),
            Err((crate::WorkspaceError::NotFound, allocation)) => { drop(allocation); None }
            Err(cause) => return Err(cause.into()),
        };
        let host_path = Path::new(&path);
        let parent = match root.open_dir_held(host_path.parent().unwrap_or_else(|| Path::new("")), &mut &*work) {
            Ok(parent) => Some(parent),
            Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let leaf = Path::new(host_path.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
        let observed = match &parent {
            Some(parent) => match native_stat_held(parent, leaf, work) {
                Ok(metadata) => Some(metadata),
                Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            },
            None => None,
        };
        let matches = match (source, observed, parent) {
            (None, None, _) => true,
            (Some(source), Some(observed), Some(parent)) => {
                if !native_source_metadata_matches(&observed, &source.metadata, &path, work, verify_access_time)? { false }
                else {
                    match source.kind {
                        crate::kernel::FileKind::Regular if observed.file_type().is_file() =>
                            host_holds_generation_file(&parent, &path, generation, &source, &observed, work, cancellation).await?,
                        crate::kernel::FileKind::Directory if observed.is_dir() => {
                            !walk_children || host_enqueue_generation_children(root, &path, generation, &observed, &mut pending, &mut storage, work, cancellation).await?
                        }
                        crate::kernel::FileKind::SymbolicLink if observed.file_type().is_symlink() => {
                            let published = generation.read_symbolic_link(&absolute, work, cancellation).await.map(|chunk| chunk.bytes).map_err(|error| (error, None))?;
                            let matched = with_native_walk_leaf(leaf, work, |name| {
                                #[cfg(unix)]
                                let (target, _target_custody) = crate::native_host::read_link_held_with_work(&parent, Path::new(name), work.remaining()?.source_bytes_read, &mut &*work)?;
                                #[cfg(windows)]
                                let (target, _target_custody) = crate::native_host::read_link_held_wide_with_work(&parent, name, work.remaining()?.source_bytes_read, &mut &*work)?;
                                #[cfg(unix)]
                                let mut actual = target.iter().copied();
                                #[cfg(windows)]
                                let mut actual = native_utf16_name_bytes(target.iter().copied());
                                let mut expected = published.iter().copied();
                                loop {
                                    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
                                    match (actual.next(), expected.next()) {
                                        (Some(a), Some(b)) if a == b => {}
                                        (None, None) => return Ok(true),
                                        _ => return Ok(false),
                                    }
                                }
                            })?;
                            matched && native_stat_identity(&native_stat_held(&parent, leaf, work)?) == native_stat_identity(&observed)
                        }
                        _ => false,
                    }
                }
            }
            _ => false,
        };
        if !matches {
            return Err(native_diagnostic(work, &path, NativeTreeMaterializationError::ExternalMutation)?.into());
        }
    }
    Ok(())
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_source_metadata_matches(
    metadata: &crate::native_host::HostStat, authored: &crate::WorkspaceMetadata,
    path: &str, work: &NativeMaterializationWork, verify_access_time: bool,
) -> Result<bool, NativeTreeMaterializationError> {
    work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
    if authored.has_named_attributes || authored.has_acl || authored.has_security_descriptor || authored.posix_flags.is_some() {
        return Err(native_diagnostic(work, path, NativeTreeMaterializationError::UnverifiedSourceMetadata)?);
    }
    #[cfg(windows)]
    if authored.posix_mode.is_some() || authored.posix_uid.is_some() || authored.posix_gid.is_some() || authored.changed_ns.is_some() {
        return Err(native_diagnostic(work, path, NativeTreeMaterializationError::UnverifiedSourceMetadata)?);
    }
    #[cfg(unix)]
    if authored.windows_attributes.is_some() {
        return Err(native_diagnostic(work, path, NativeTreeMaterializationError::UnverifiedSourceMetadata)?);
    }
    let times: [(Option<i64>, fn(&crate::native_host::HostStat) -> std::io::Result<cap_std::time::SystemTime>); 3] = [
        (authored.created_ns, crate::native_host::HostStat::created),
        (authored.modified_ns, crate::native_host::HostStat::modified),
        (if verify_access_time { authored.accessed_ns } else { None }, crate::native_host::HostStat::accessed),
    ];
    for (expected, observe) in times {
        if let Some(expected) = expected {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            match crate::native_capture::system_time(observe(metadata)) {
                crate::kernel::MetadataField::Value(actual) if actual == expected => {}
                crate::kernel::MetadataField::Value(_) => return Ok(false),
                crate::kernel::MetadataField::Unavailable => return Err(native_diagnostic(work, path, NativeTreeMaterializationError::UnverifiedSourceMetadata)?),
            }
        }
    }
    #[cfg(unix)]
    if let Some(expected) = authored.changed_ns {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let actual = i128::from(metadata.ctime()) * 1_000_000_000 + i128::from(metadata.ctime_nsec());
        if actual != i128::from(expected) { return Ok(false); }
    }
    #[cfg(unix)]
    {
        for (expected, actual) in [
            (authored.posix_mode.map(|mode| mode & 0o7777), metadata.mode() & 0o7777),
            (authored.posix_uid, metadata.uid()), (authored.posix_gid, metadata.gid()),
        ] {
            if let Some(expected) = expected {
                work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
                if expected != actual { return Ok(false); }
            }
        }
    }
    #[cfg(windows)]
    if let Some(expected) = authored.windows_attributes {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
        const AUTHORED: u32 = 0x1 | 0x2 | 0x4 | 0x20 | 0x100 | 0x2000;
        if expected & AUTHORED != metadata.file_attributes() & AUTHORED { return Ok(false); }
    }
    Ok(true)
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn native_join_generation_child(path: &str, name: &crate::kernel::LogicalName, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
    work.charge(crate::WorkCounters { items_examined: name.as_bytes().len() as u64 + 1, ..crate::WorkCounters::default() })?;
    let Some(name_bytes) = name.unicode_utf8_bytes() else {
        return Err(native_diagnostic(work, format_args!("{path}/{name:?}"), NativeTreeMaterializationError::InvalidPath)?);
    };
    let length = path.len().checked_add(1).and_then(|bytes| bytes.checked_add(name_bytes)).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters { bytes_copied: length as u64 - 1, ..crate::WorkCounters::default() };
    work.admit(copied)?;
    let custody = work.claim(length as u64)?;
    let mut output = String::new();
    output.try_reserve_exact(length).map_err(NativeTreeMaterializationError::AllocationFailed)?;
    output.push_str(path);
    output.push('/');
    if name.append_unicode(&mut output).is_err() {
        return Err(native_diagnostic(work, format_args!("{path}/{name:?}"), NativeTreeMaterializationError::InvalidPath)?);
    }
    work.spent(copied)?;
    Ok((output, custody))
}
/// Compares exact source membership and queues each witnessed child once.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
async fn host_enqueue_generation_children<A, O>(
    root: &crate::native_host::HostRoot,
    path: &str,
    generation: &crate::Generation<A, O>,
    observed: &crate::native_host::HostStat,
    pending: &mut Vec<(String, bool, NativeAllocation)>,
    pending_storage: &mut NativeAllocation,
    work: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<bool, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let directory = match root.open_dir_held(Path::new(path), &mut &*work) {
        Ok(directory) => directory,
        Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if crate::native_host::directory_identity(&directory, &mut &*work)? != native_stat_identity(observed) {
        return Err(native_diagnostic(work, path, NativeTreeMaterializationError::ExternalMutation)?.into());
    }
    let mut host = Vec::new();
    let mut storage = work.claim(0)?;
    let (mut reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    while let Some(name) = next_native_walk_name(&mut reader, work)? {
        native_vec_push(&mut host, name, &mut storage)?;
    }
    drop(reader);
    drop(cursor);
    let (directory, _directory_custody) = native_absolute_string(path, work)?;
    let mut after = None;
    let mut after_custody = work.claim(0)?;
    loop {
        let (mut page, mut page_custody) = generation
            .list_after_with_native_work(&directory, after.as_ref(), 1_024, work, cancellation)
            .await?;
        for entry in &page.entries {
            let mut index = 0;
            let mut found = false;
            while index < host.len() {
                if native_host_name_matches(&host[index].0, &entry.name, work)? {
                    host.swap_remove(index);
                    found = true;
                    break;
                }
                index += 1;
            }
            if !found { return Ok(false); }
            let (child, custody) = native_join_generation_child(path, &entry.name, work)?;
            native_vec_push(pending, (child, true, custody), pending_storage)?;
        }
        let has_more = page.has_more;
        let next = if has_more { page.entries.pop().map(|entry| entry.name) } else { None };
        let next_bytes = next.as_ref().map_or(0, |name| name.retained_bytes() as u64);
        drop(page);
        page_custody.resize_retained(next_bytes)?;
        drop(after.take());
        drop(after_custody);
        after = next;
        after_custody = page_custody;
        if !has_more || after.is_none() { break; }
    }
    Ok(host.is_empty())
}

/// Compares all original regular-file bytes without a size-based shortcut.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
async fn host_holds_generation_file<A, O>(
    parent: &cap_std::fs::Dir,
    path: &str,
    generation: &crate::Generation<A, O>,
    stat: &crate::WorkspaceStat,
    metadata: &crate::native_host::HostStat,
    work: &NativeMaterializationWork,
    cancellation: &crate::CancellationToken,
) -> Result<bool, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let leaf = Path::new(path).file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    let (absolute, _absolute_custody) = native_absolute_string(path, work)?;
    if stat.kind != crate::kernel::FileKind::Regular || stat.logical_bytes != Some(metadata.len()) {
        return Ok(false);
    }
    let published = generation.read(&absolute, metadata.len(), work, cancellation).await.map_err(|error| (error, None))?;
    let mut file = crate::native_host::open_leaf_with_work(&parent, Path::new(leaf), &crate::native_host::NativeOpenOptions::Read, &mut &*work)?;
    let opened = crate::native_host::stat_file_with_work(&file, &mut &*work)?;
    if native_stat_identity(&opened) != native_stat_identity(&metadata) || opened.len() != metadata.len() {
        return Err(native_diagnostic(work, path, NativeTreeMaterializationError::ExternalMutation)?.into());
    }
    let mut buffer = [0; 64 * 1024];
    let mut offset = 0;
    use std::io::Read as _;
    while offset < published.len() {
        let length = (published.len() - offset).min(buffer.len()).min(usize::try_from(work.remaining()?.source_bytes_read).unwrap_or(usize::MAX));
        if length == 0 { work.charge(crate::WorkCounters { source_bytes_read: 1, ..crate::WorkCounters::default() })?; }
        work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::default() })?;
        let count = file.read(buffer.get_mut(..length).ok_or(NativeTreeMaterializationError::InvalidPreimage)?)?;
        work.spent(crate::WorkCounters { source_bytes_read: count as u64, ..crate::WorkCounters::default() })?;
        if count == 0 { return Ok(false); }
        let expected = published.get(offset..offset + count).ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
        for (&left, &right) in expected.iter().zip(&buffer[..count]) {
            work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if left != right { return Ok(false); }
        }
        offset += count;
    }
    let finished = crate::native_host::stat_file_with_work(&file, &mut &*work)?;
    let selected = native_stat_held(parent, Path::new(leaf), work)?;
    Ok(native_stat_identity(&finished) == native_stat_identity(metadata)
        && native_stat_identity(&selected) == native_stat_identity(metadata)
        && finished.len() == metadata.len()
        && native_source_metadata_matches(&finished, &stat.metadata, path, work, false)?)
}

/// Core native workspace publication failure.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
#[derive(Debug, Error)]
pub enum NativeWorkspacePublicationError {
    /// Authenticated workspace access or export failed.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Original authenticated source cause and its actual retained native payload.
    #[error("{0}")]
    OriginalWorkspace(#[source] crate::WorkspaceError, Option<NativeAllocation>),
    /// Host tree planning or publication failed.
    #[error(transparent)]
    Native(#[from] NativeTreeMaterializationError),
    /// Original native selected-generation physical producer cause.
    #[error(transparent)]
    NativeMaterialize(#[from] crate::MaterializeError),
    /// Durable state access failed.
    #[error(transparent)]
    State(#[from] crate::LocalCoreStateStoreError),
    /// Journaled publication failed.
    #[error(transparent)]
    Materialization(
        #[from]
        MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>,
    ),
    /// An operation identity was reused for different generations.
    #[error("native workspace publication journal does not match the requested generations")]
    MismatchedJournal,
    /// Native staging failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Publication planning or materialization exceeded its cumulative budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Original typed failure retained in the funded stage slot.
    #[error(transparent)]
    NativeStageUncertain(#[from] NativeStageUncertainty<NativeWorkspacePublicationError>),
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
impl From<(crate::WorkspaceError, Option<NativeAllocation>)> for NativeWorkspacePublicationError {
    fn from((cause, allocation): (crate::WorkspaceError, Option<NativeAllocation>)) -> Self {
        Self::OriginalWorkspace(cause, allocation)
    }
}

/// Process-local materialization journal adapter.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: std::sync::Mutex<std::collections::BTreeMap<OperationId, MaterializationJournal>>,
}

/// A pre-funded completion in the original process-local journal adapter.
#[cfg(not(target_arch = "wasm32"))]
pub struct MemoryMaterializationActionCompletion<'a> {
    store: &'a MemoryMaterializationJournalStore,
    journals: Option<(MaterializationJournal, MaterializationJournal)>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Future for MemoryMaterializationActionCompletion<'_> {
    type Output = Result<MaterializationJournal, MemoryMaterializationJournalStoreError>;

    fn poll(self: std::pin::Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let Some((mut journal, mut replacement)) = this.journals.take() else {
            return std::task::Poll::Ready(Err(MemoryMaterializationJournalStoreError::Unavailable));
        };
        let store = this.store;
        std::task::Poll::Ready((|| {
            let expected_revision = journal.revision - 1;
            if let Some(work) = replacement.custody.as_ref().map(NativeAllocation::work) {
                let completed = match work.journal_work() {
                    Ok(completed) => completed,
                    Err(error) => return Err(MemoryMaterializationJournalStoreError::ActionNativeWork(error, journal)),
                };
                journal.work = completed;
                replacement.work = completed;
            }
            let retired = {
                let mut journals = match store.journals.lock() {
                    Ok(journals) => journals,
                    Err(_) => return Err(MemoryMaterializationJournalStoreError::ActionUnavailable(journal)),
                };
                let Some(stored) = journals.get_mut(&journal.plan.operation_id) else {
                    drop(journals);
                    return Err(MemoryMaterializationJournalStoreError::ActionConflict(journal));
                };
                if stored.revision != expected_revision {
                    drop(journals);
                    return Err(MemoryMaterializationJournalStoreError::ActionConflict(journal));
                }
                // This existing BTree slot needs no new node or payload backing.
                std::mem::replace(stored, replacement)
            };
            // Retire the former payload and its accounting outside the map lock.
            drop(retired);
            Ok(journal)
        })())
    }
}

impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> = MemoryMaterializationActionCompletion<'a> where Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(
        &'a self,
        mut journal: MaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        if let Some(custody) = &journal.custody {
            let bytes = match journal_owned_bytes(&journal) {
                Ok(bytes) => bytes,
                Err(error) => return Err(Self::Error::ActionNativeWork(error, journal)),
            };
            if custody.bytes() < bytes {
                return Err(Self::Error::ActionNativeWork(
                    NativeTreeMaterializationError::MissingCustody, journal,
                ));
            }
        }
        let work = {
            let journals = match self.journals.lock() {
                Ok(journals) => journals,
                Err(_) => return Err(Self::Error::ActionUnavailable(journal)),
            };
            let Some(stored) = journals.get(&journal.plan.operation_id) else {
                drop(journals);
                return Err(Self::Error::ActionConflict(journal));
            };
            if *stored != journal {
                drop(journals);
                return Err(Self::Error::ActionConflict(journal));
            }
            match (&stored.custody, &journal.custody) {
                (Some(stored_custody), Some(input_custody)) => {
                    if !stored_custody.work().shares_allocation_context(input_custody.work()) {
                        drop(journals);
                        return Err(Self::Error::ActionNativeWork(
                            NativeTreeMaterializationError::InvalidLayout, journal,
                        ));
                    }
                    let bytes = match journal_owned_bytes(stored) {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            drop(journals);
                            return Err(Self::Error::ActionNativeWork(error, journal));
                        }
                    };
                    if stored_custody.bytes() < bytes {
                        drop(journals);
                        return Err(Self::Error::ActionNativeWork(
                            NativeTreeMaterializationError::MissingCustody, journal,
                        ));
                    }
                    Some(stored_custody.work().clone())
                }
                (None, None) => None,
                _ => {
                    drop(journals);
                    return Err(Self::Error::ActionNativeWork(
                        NativeTreeMaterializationError::MissingCustody, journal,
                    ));
                }
            }
        };
        let expected_revision = journal.revision;
        let Some(next_revision) = expected_revision.checked_add(1) else {
            return Err(Self::Error::ActionNativeWork(crate::WorkError::Overflow.into(), journal));
        };
        let mut replacement = match &work {
            Some(work) => {
                if let Err(error) = work.admit(crate::WorkCounters::UNCHARGED) {
                    return Err(Self::Error::ActionNativeWork(error, journal));
                }
                match duplicate_native_journal(&journal, work) {
                    Ok(replacement) => replacement,
                    Err(error) => return Err(Self::Error::ActionNativeWork(error, journal)),
                }
            }
            None => duplicate_journal(&journal)?,
        };
        if let Some(work) = &work
            && let Err(error) = work.admit(crate::WorkCounters::UNCHARGED) {
            return Err(Self::Error::ActionNativeWork(error, journal));
        }
        journal.revision = next_revision;
        replacement.revision = next_revision;
        Ok(MemoryMaterializationActionCompletion {
            store: self, journals: Some((journal, replacement)),
        })
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        let journals = self.journals.lock().map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
        journals.get(&operation_id).map(duplicate_journal).transpose()
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let mut journals = self.journals.lock().map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
        let stored = crate::record_store::stored_revision(&journals, &operation_id) == expected_revision;
        if stored {
            let copied = duplicate_journal(&replacement)?;
            journals.insert(operation_id, copied);
        }
        Ok((stored, replacement))
    }
}

/// Process-local synchronization or original native payload work failure.
#[derive(Debug, Error)]
pub enum MemoryMaterializationJournalStoreError {
    /// The backing record mutex was poisoned.
    #[error("materialization memory store is unavailable")]
    Unavailable,
    /// Duplicating the payload exceeded or failed its original native work.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory store native work failed: {0}")]
    NativeWork(#[from] NativeTreeMaterializationError),
    /// The original journal changed before its admitted completion was stored.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory action completion conflicted")]
    ActionConflict(MaterializationJournal),
    /// An admitted action could not access its store; its packet stays owned.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory action completion store is unavailable")]
    ActionUnavailable(MaterializationJournal),
    /// Original preparation or completion work failed with its journal intact.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory action completion native work failed: {0}")]
    ActionNativeWork(#[source] NativeTreeMaterializationError, MaterializationJournal),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Digest;
    use std::sync::Mutex;

    fn test_plan(operation_id: OperationId, edits: Vec<MaterializationEdit>) -> MaterializationPlan {
        MaterializationPlan {
            operation_id, edits,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
        }
    }

    #[derive(Default)]
    struct Backend(Mutex<Vec<Vec<u8>>>);

    #[derive(Debug, Error)]
    enum BackendError {
        #[error("test backend unavailable")]
        Unavailable,
    }

    impl MaterializationBackend for Backend {
        type Error = BackendError;

        fn append_preimage(&self, journal: &mut MaterializationJournal, preimage: MaterializationPreimage) -> Result<(), Self::Error> {
            journal.preimages.push(preimage);
            Ok(())
        }

        async fn capture(
            &self,
            _edit: &MaterializationEdit,
        ) -> Result<MaterializationPreimage, Self::Error> {
            Ok(MaterializationPreimage { image: vec![0], #[cfg(not(target_arch = "wasm32"))] custody: None })
        }

        async fn apply(
            &self,
            edit: &MaterializationEdit,
            _preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            let MaterializationEdit::Install { image, .. } = edit else {
                return Err(BackendError::Unavailable);
            };
            self.0.lock().map_err(|_| BackendError::Unavailable)?.push(image.clone());
            Ok(())
        }

        async fn restore(
            &self,
            _edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            self.0
                .lock()
                .map_err(|_| BackendError::Unavailable)?
                .push(preimage.image.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn obsolete_journal_versions_reject_recovery_without_effects() {
        for version in [0, 1, 3, u32::MAX] {
            let operation_id = OperationId::new();
            let journal = MaterializationJournal {
                #[cfg(not(target_arch = "wasm32"))] custody: None,
                version,
                revision: 1,
                root_identity: None,
                preparation_identity: None,
                preparation_progress: None,
                work: crate::WorkCounters::default(),
                plan: test_plan(operation_id, vec![MaterializationEdit::Install {
                        path: "file".into(),
                        image: vec![9],
                    }]
                ),
                preimages: vec![MaterializationPreimage { image: vec![0], #[cfg(not(target_arch = "wasm32"))] custody: None }],
                phase: MaterializationPhase::Prepared,
                applied: 0,
                restored: 0,
            };
            let store = MemoryMaterializationJournalStore::default();
            store
                .compare_and_swap(operation_id, 0, duplicate_journal(&journal).expect("ordinary journal copy"))
                .await
                .expect("seed obsolete journal");
            let materializer = JournaledMaterializer::new(store, Backend::default());
            assert!(matches!(
                materializer
                    .recover(operation_id, MaterializationRecovery::Complete, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(())))
                    .await,
                Err(MaterializationError::IncompatibleJournal)
            ));
            assert!(materializer.backend.0.lock().expect("backend").is_empty());
            assert_eq!(
                materializer
                    .store
                    .load(operation_id)
                    .await
                    .expect("retained journal"),
                Some(journal)
            );
        }
    }
    #[tokio::test]
    async fn interrupted_rollback_cannot_be_completed_forward() {
        let operation_id = OperationId::new();
        let store = MemoryMaterializationJournalStore::default();
        let plan = test_plan(operation_id, vec![MaterializationEdit::Install {
                path: "file".to_owned(),
                image: vec![9],
            }]
        );
        store
            .compare_and_swap(
                operation_id,
                0,
                MaterializationJournal {
        #[cfg(not(target_arch = "wasm32"))] custody: None,
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: None,
                    preparation_identity: None,
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan,
                    preimages: vec![MaterializationPreimage { image: vec![0], #[cfg(not(target_arch = "wasm32"))] custody: None }],
                    phase: MaterializationPhase::RollingBack,
                    applied: 1,
                    restored: 1,
                },
            )
            .await
            .expect("store journal");
        let materializer = JournaledMaterializer::new(store, Backend::default());
        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::Complete, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(())))
                .await,
            Err(MaterializationError::IncompatibleJournal)
        ));
    }

    #[tokio::test]
    async fn overlapping_or_noncanonical_plans_are_rejected_before_capture() {
        let materializer = JournaledMaterializer::new(
            MemoryMaterializationJournalStore::default(),
            Backend::default(),
        );
        let plan = MaterializationPlan {
            operation_id: OperationId::new(),
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: vec![
                MaterializationEdit::Remove {
                    path: "directory".to_owned(),
                },
                MaterializationEdit::Install {
                    path: "directory/file".to_owned(),
                    image: vec![1],
                },
            ],
        };
        assert!(matches!(
            materializer.apply(plan, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(()))).await,
            Err(MaterializationError::IncompatibleJournal)
        ));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_applies_and_rolls_back_selected_paths() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(root.join(".git")).expect("git directory");
        std::fs::create_dir_all(&target).expect("target directory");
        std::fs::write(root.join("old.txt"), b"old").expect("old file");
        std::fs::write(root.join(".git/config"), b"git").expect("git file");
        std::fs::write(target.join("new.txt"), b"new").expect("new file");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let plan = test_plan(OperationId::new(), vec![MaterializationEdit::Remove { path: "old.txt".into() }, MaterializationEdit::Install { path: "new.txt".into(), image: vec![1] }]);
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(()))).await.expect("apply tree");
        assert!(!root.join("old.txt").exists());
        assert_eq!(std::fs::read(root.join("new.txt")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(root.join(".git/config")).expect("git"),
            b"git"
        );
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(())))
            .await
            .expect("rollback");
        assert_eq!(std::fs::read(root.join("old.txt")).expect("old"), b"old");
        assert!(!root.join("new.txt").exists());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rejects_missing_staged_target_during_capture() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original finite work");
        let backend = NativeTreeMaterializationBackend::new(root.clone(), operation.clone(), None, work.clone(), true).expect("native backend");
        let baseline = work.live_allocation_bytes();
        let edit = MaterializationEdit::Install {
            path: "file.txt".to_owned(),
            image: Vec::new(),
        };

        let error = backend.capture(&edit).await.expect_err("missing actual staged target");
        let retained = work.live_allocation_bytes();
        assert!(retained > baseline, "real native diagnostic remains owned after failure");
        cancellation.cancel();
        let prior = work.receipt();
        let (error, allocation) = error.into_native_failure();
        let allocation = allocation.expect("original display allocation travels with its cause");
        assert!(allocation.work().shares_ledger(&work));
        assert!(matches!(&error, NativeTreeMaterializationError::MissingTarget(path, None) if path == "file.txt"));
        assert_eq!(work.receipt(), prior, "pure extraction never reclaims or checks work after cancellation");
        work.receipt().verify(budget).expect("all native failure prefix work is available");
        drop(error);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("live remains"),
            b"before"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rolls_back_interrupted_install_from_durable_backup() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        let backup = operation.join("backup");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::create_dir_all(&backup).expect("backup");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let plan = test_plan(OperationId::new(), vec![MaterializationEdit::Install { path: "file.txt".into(), image: vec![1] }]);
        let edit = &plan.edits[0];
        let preimage = backend.capture(edit).await.expect("capture");

        std::fs::rename(root.join("file.txt"), backup.join("file.txt"))
            .expect("simulate crash after backup rename");
        assert_eq!(
            backend.observe(edit, &preimage).await.expect("observe"),
            MaterializationObservation::Interrupted
        );
        backend
            .restore(edit, &preimage)
            .await
            .expect("restore interrupted install");
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("restored live"),
            b"before"
        );
        assert!(!backup.join("file.txt").exists());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_revalidates_witnesses_inside_apply() {
        for mutate_target in [false, true] {
            let temporary = tempfile::tempdir().expect("temporary root");
            let root = temporary.path().join("checkout");
            let operation = temporary.path().join("operation");
            let target = operation.join("target");
            std::fs::create_dir_all(&root).expect("root");
            std::fs::create_dir_all(&target).expect("target");
            std::fs::write(root.join("file.txt"), b"before").expect("before");
            std::fs::write(target.join("file.txt"), b"after").expect("after");
            let backend =
                NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
            let plan = test_plan(OperationId::new(), vec![MaterializationEdit::Install { path: "file.txt".into(), image: vec![1] }]);
            let edit = plan.edits.first().expect("install edit");
            let preimage = backend.capture(edit).await.expect("capture");
            assert_eq!(
                backend.observe(edit, &preimage).await.expect("observe"),
                MaterializationObservation::Preimage
            );
            let changed = if mutate_target {
                target.join("file.txt")
            } else {
                root.join("file.txt")
            };
            std::fs::write(&changed, b"external").expect("mutate after observe");
            assert!(matches!(
                backend.apply(edit, &preimage).await,
                Err(NativeTreeMaterializationError::ExternalMutation(path, Some(_))) if path == "file.txt"
            ));
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_refuses_to_roll_back_over_an_external_write() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let plan = test_plan(OperationId::new(), vec![MaterializationEdit::Install { path: "file.txt".into(), image: vec![1] }]);
        let edit = plan.edits.first().expect("install edit");
        let preimage = backend.capture(edit).await.expect("capture");
        backend.apply(edit, &preimage).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external write");

        assert!(matches!(
            backend.restore(edit, &preimage).await,
            Err(NativeTreeMaterializationError::ExternalMutation(path, Some(_))) if path == "file.txt"
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external content survives"),
            b"external"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn metadata_edit_preserves_concurrent_child_content() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(root.join("directory")).expect("checkout directory");
        std::fs::create_dir_all(operation.join("target")).expect("target directory");
        let child = root.join("directory/child.txt");
        std::fs::write(&child, b"before").expect("child file");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let mut desired = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("directory metadata"),
        );
        #[cfg(unix)]
        {
            desired.posix_mode = desired.posix_mode.map(|mode| mode ^ 0o200);
        }
        #[cfg(windows)]
        {
            desired.windows_attributes =
                desired.windows_attributes.map(|attributes| attributes ^ 2);
        }
        let edit = MaterializationEdit::SetMetadata {
            path: "directory".to_owned(),
            image: native_metadata_bytes(&desired).to_vec(),
        };
        let preimage = backend.capture(&edit).await.expect("capture metadata");
        std::fs::write(&child, b"concurrent child write").expect("update child");
        assert_eq!(
            backend.observe(&edit, &preimage).await.expect("observe"),
            MaterializationObservation::Preimage
        );
        backend
            .apply(&edit, &preimage)
            .await
            .expect("apply metadata");
        assert_eq!(
            std::fs::read(&child).expect("read child"),
            b"concurrent child write"
        );
        backend
            .restore(&edit, &preimage)
            .await
            .expect("restore metadata");
        assert_eq!(
            std::fs::read(&child).expect("read restored child"),
            b"concurrent child write"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn metadata_edit_rejects_a_replaced_directory() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let directory = root.join("directory");
        std::fs::create_dir_all(&directory).expect("checkout directory");
        std::fs::create_dir_all(operation.join("target")).expect("target directory");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let edit = MaterializationEdit::SetMetadata {
            path: "directory".to_owned(),
            image: native_metadata_bytes(&native_metadata(
                &std::fs::symlink_metadata(&directory).expect("directory metadata"),
            )).to_vec(),
        };
        let preimage = backend.capture(&edit).await.expect("capture metadata");
        std::fs::rename(&directory, root.join("displaced")).expect("replace binding");
        std::fs::create_dir(&directory).expect("replacement directory");
        assert_eq!(
            backend.observe(&edit, &preimage).await.expect("observe"),
            MaterializationObservation::Diverged
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_tree_backend_preserves_dangling_symlinks_on_rollback() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("checkout directory");
        std::fs::create_dir_all(&target).expect("target directory");
        symlink("missing-target", root.join("link")).expect("dangling symlink");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true).expect("native backend");
        let plan = test_plan(OperationId::new(), vec![MaterializationEdit::Remove { path: "link".into() }]);
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(()))).await.expect("remove link");
        assert!(std::fs::symlink_metadata(root.join("link")).is_err());
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack, &mut |_| std::future::ready(Ok::<(), MaterializationError<_, _>>(())))
            .await
            .expect("restore link");
        assert_eq!(
            std::fs::read_link(root.join("link")).expect("restored symlink"),
            PathBuf::from("missing-target")
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_tree_backend_rejects_operation_directory_inside_checkout() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = root.join("operation");
        let sentinel = operation.join("target/sentinel");
        std::fs::create_dir_all(sentinel.parent().expect("sentinel parent"))
            .expect("target directory");
        std::fs::write(&sentinel, b"keep").expect("sentinel");

        assert!(matches!(
            NativeTreeMaterializationBackend::new(&root, &operation, None, unbounded_native_work(), true),
            Err(NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(std::fs::read(sentinel).expect("sentinel remains"), b"keep");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn completed_work_retains_an_overlarge_copy_before_refusing_it() {
        let work = NativeMaterializationWork::new(
            crate::WorkBudget { bytes_copied: 9, ..crate::WorkBudget::UNBOUNDED },
            &crate::CancellationToken::new(),
        ).expect("copy allowance");
        let mut reservation = work.reserve_work(crate::WorkCounters { bytes_copied: 5, ..Default::default() }).expect("reserved copy");
        assert!(reservation.record_completed(crate::WorkCounters { bytes_copied: 7, ..Default::default() }).is_err());
        drop(reservation);
        assert_eq!(work.receipt().bytes_copied, 7);
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn unbounded_native_work() -> NativeMaterializationWork {
        NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("native work")
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn finite_native_budget() -> crate::WorkBudget {
        let mut value = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("budget encoding");
        for field in value.as_object_mut().expect("budget object").values_mut() {
            *field = serde_json::Value::from(100_000_000_u64);
        }
        serde_json::from_value(value).expect("finite budget")
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_reads_whole_binary_bytes_with_original_capacity_custody() {
        let temporary = tempfile::tempdir().expect("real file root");
        let binary = b"\0\xff\x80\r\nabc";
        std::fs::write(temporary.path().join("file"), binary).expect("binary fixture");
        let mut budget = finite_native_budget();
        budget.source_bytes_read = binary.len() as u64;
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("original work");
        let identity = work.root_identity(temporary.path()).expect("original held root witness");
        let bytes = work.read_working_file(temporary.path(), identity, Path::new("file"),
            binary.len(), || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect("whole binary read");
        assert_eq!(bytes.as_ref(), binary);
        drop(bytes);
        let failure = work.read_working_file(temporary.path(), identity, Path::new("file"),
            binary.len() - 1, || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("no truncated prefix");
        assert!(matches!(failure, NativeTreeMaterializationError::PayloadTooLarge { maximum: 7, actual: 8 }));
        assert_eq!(work.receipt().source_bytes_read, binary.len() as u64);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_rejects_noncanonical_paths_and_over_cap_payload_before_effects() {
        let temporary = tempfile::tempdir().expect("real file root");
        let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original work");
        let identity = work.root_identity(temporary.path()).expect("original witness");
        for path in ["../outside", "a/../file", "./file", "/file", "file/", "a//file", "a\\file"] {
            let denied = work.write_working_file(temporary.path(), identity, Path::new(path), "payload", true, None,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("noncanonical relative path");
            assert!(matches!(denied, NativeTreeMaterializationError::InvalidLayout));
        }
        let oversized = "x".repeat(NativeMaterializationWork::MAXIMUM_BUILTIN_BYTES + 1);
        let denied = work.write_working_file(temporary.path(), identity, Path::new("new/file"), &oversized, true, None,
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("UTF-8 cap precedes mkdir");
        assert!(matches!(denied, NativeTreeMaterializationError::PayloadTooLarge { maximum: 262_144, actual: 262_145 }));
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(!temporary.path().join("new").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn native_builtin_never_follows_leaf_or_parent_symlinks() {
        let temporary = tempfile::tempdir().expect("real protected root");
        let root = temporary.path().join("root");
        let external = temporary.path().join("external");
        std::fs::create_dir(&root).expect("root");
        std::fs::create_dir(&external).expect("external");
        std::fs::write(external.join("file"), b"keep").expect("external data");
        std::os::unix::fs::symlink(external.join("file"), root.join("leaf")).expect("leaf link");
        std::os::unix::fs::symlink(&external, root.join("parent")).expect("parent link");
        let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original work");
        let identity = work.root_identity(&root).expect("original root witness");
        for path in ["leaf", "parent/file"] {
            assert!(work.read_working_file(&root, identity, Path::new(path), 32,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.is_err());
            assert!(work.write_working_file(&root, identity, Path::new(path), "overwrite", true, None,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.is_err());
        }
        assert_eq!(std::fs::read(external.join("file")).expect("external remains"), b"keep");
        assert_eq!(work.receipt().output_bytes, 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_edit_compares_exact_preimage_and_creates_held_parent_directories() {
        use sha2::{Digest as _, Sha256};
        let temporary = tempfile::tempdir().expect("real root");
        std::fs::write(temporary.path().join("file"), b"before").expect("preimage");
        let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original work");
        let identity = work.root_identity(temporary.path()).expect("original witness");
        let denied = work.write_working_file(temporary.path(), identity, Path::new("file"), "after", false, Some([0; 32]),
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("wrong exact preimage digest");
        assert!(matches!(denied, NativeTreeMaterializationError::PreimageConflict));
        assert_eq!(std::fs::read(temporary.path().join("file")).expect("no mismatch mutation"), b"before");
        assert_eq!(work.receipt().output_bytes, 0);
        let digest: [u8; 32] = Sha256::digest(b"before").into();
        work.write_working_file(temporary.path(), identity, Path::new("file"), "", false, Some(digest),
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect("same held preimage edit can truncate to empty");
        assert_eq!(std::fs::read(temporary.path().join("file")).expect("empty postimage"), b"");
        work.write_working_file(temporary.path(), identity, Path::new("new/nested/file"), "héllo", true, None,
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect("held parent directory creation");
        assert_eq!(std::fs::read(temporary.path().join("new/nested/file")).expect("whole UTF-8 postimage"), "héllo".as_bytes());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_late_cancellation_retains_actual_read_and_write_prefixes() {
        for writing in [false, true] {
            let temporary = tempfile::tempdir().expect("real root");
            std::fs::write(temporary.path().join("file"), b"before").expect("file fixture");
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("original work");
            let identity = work.root_identity(temporary.path()).expect("original witness");
            let guard = || {
                let actual = work.receipt();
                if if writing { actual.output_bytes > 0 } else { actual.source_bytes_read > 0 } {
                    cancellation.cancel();
                }
                std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))
            };
            let failed = if writing {
                work.write_working_file(temporary.path(), identity, Path::new("file"), "changed", false, None, guard).await
            } else {
                work.read_working_file(temporary.path(), identity, Path::new("file"), 32, guard).await.map(drop)
            }.expect_err("late cancellation cannot acknowledge success");
            assert!(matches!(failed, NativeTreeMaterializationError::Cancelled));
            let actual = work.receipt();
            assert_eq!(if writing { actual.output_bytes } else { actual.source_bytes_read }, if writing { 7 } else { 6 });
            assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
            assert_eq!(std::fs::read(temporary.path().join("file")).expect("actual physical prefix"), if writing { b"changed".as_slice() } else { b"before".as_slice() });
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_readonly_metadata_without_explicit_attributes_matches_its_durable_witness() {
        let temporary = tempfile::tempdir().expect("temporary operation");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("root fixture");
        std::fs::create_dir_all(operation.join("target")).expect("target fixture");
        let file = root.join("file");
        std::fs::write(&file, b"owned").expect("file fixture");
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        let backend = NativeTreeMaterializationBackend::new(root, operation, None, work.clone(), true).expect("held roots");
        let edit = MaterializationEdit::SetMetadata {
            path: "file".to_owned(),
            image: native_metadata_bytes(&NativeMetadataImage { readonly: true, posix_mode: None, windows_attributes: None }).to_vec(),
        };
        let preimage = backend.capture(&edit).await.expect("durable metadata witness");
        backend.apply(&edit, &preimage).await.expect("real readonly mutation");
        assert!(std::fs::metadata(&file).expect("physical metadata").permissions().readonly());
        assert_eq!(backend.observe(&edit, &preimage).await.expect("actual postimage"), MaterializationObservation::Postimage);
        backend.restore(&edit, &preimage).await.expect("restore witnessed attributes");
        assert!(!std::fs::metadata(file).expect("restored metadata").permissions().readonly());
        work.receipt().verify(budget).expect("actual metadata work");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_staging_cleanup_is_bounded_and_never_adopts_replacement() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("root");
        let operation = directory.path().join("operation");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(operation.join("target/partial"), b"partial").expect("partial stage");
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("native meter");
        let backend = NativeTreeMaterializationBackend::new(root, operation.clone(), None, work.clone(), true).expect("backend");
        cancellation.cancel();
        assert!(matches!(backend.reset_target_directory("").await, Err(NativeTreeMaterializationError::Cancelled)));
        assert!(operation.join("target/partial").exists());
        let original = operation.join("target-original");
        std::fs::rename(operation.join("target"), &original).expect("displace original stage");
        std::fs::create_dir(operation.join("target")).expect("replacement stage");
        std::fs::write(operation.join("target/user"), b"keep").expect("user data");
        let replacement = NativeTreeMaterializationBackend::new(backend.root.as_ref(), &operation, None, unbounded_native_work(), true).expect("reopened backend");
        assert_ne!(replacement.preparation_identity(), backend.preparation_identity());
        assert_eq!(std::fs::read(operation.join("target/user")).expect("user data retained"), b"keep");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct FaultingLocalJournal<'a> {
        store: crate::core_state::MeasuredMaterializationStore<'a>,
        fail_revision: u64,
        lose_ack: bool,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl<'store> MaterializationJournalStore for FaultingLocalJournal<'store> {
        type Error = crate::LocalCoreStateStoreError;

        type ActionCompletion<'a> = <crate::core_state::MeasuredMaterializationStore<'store> as MaterializationJournalStore>::ActionCompletion<'a>
        where Self: 'a;

        async fn prepare_action<'a>(&'a self, journal: MaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
            self.store.prepare_action(journal).await
        }

        async fn load(&self, operation_id: OperationId) -> Result<Option<MaterializationJournal>, Self::Error> {
            self.store.load(operation_id).await
        }

        async fn compare_and_swap(&self, operation_id: OperationId, expected_revision: u64, replacement: MaterializationJournal) -> Result<(bool, MaterializationJournal), Self::Error> {
            let fail = replacement.revision == self.fail_revision;
            if fail && !self.lose_ack {
                return Err(std::io::Error::other("injected pre-journal failure").into());
            }
            let stored = self.store.compare_and_swap(operation_id, expected_revision, replacement).await?;
            if fail && stored.0 {
                return Err(std::io::Error::other("injected lost journal acknowledgement").into());
            }
            Ok(stored)
        }
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_prepare_only_restart_and_guarded_partial_apply_keep_original_intent() {
        let temporary = tempfile::tempdir().expect("temporary publication");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        let state_path = temporary.path().join("state");
        std::fs::create_dir(&root).expect("checkout");
        for name in ["one", "two"] { std::fs::write(root.join(name), b"before").expect("baseline"); }
        std::fs::write(root.join("untouched"), b"dirty user data").expect("untouched user data");
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("native-publication", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace");
        for name in ["/one", "/two", "/untouched"] { workspace.write_text(name, "before").await.expect("logical baseline"); }
        let from = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("original generation");
        for name in ["/one", "/two"] { workspace.write_text(name, "after").await.expect("logical target"); }
        let to = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("target generation");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let identity_work = NativeMaterializationWork::new(budget, &cancellation).expect("identity meter");
        let identity = identity_work.root_identity(&root).expect("original allocation");
        drop(identity_work);
        let options = crate::MaterializeOptions::native(&root);
        let operation_id = OperationId::new();
        let publication = || NativeWorkspacePublication {
            root: &root, source_root_identity: identity, operation_directory: &operation,
            operation_id, from: from.id(), to: to.id(), excluded_names: &[],
            options: &options, budget, cancellation: &cancellation,
        };
        let prepared = prepare_native_generation_transition(&from, &to, &crate::LocalCoreStateStore::new(&state_path), publication()).await.expect("prepare without apply");
        assert_eq!(prepared.value.phase, MaterializationPhase::Prepared);
        assert_eq!(prepared.value.preimages.len(), prepared.value.plan.edits.len());
        assert_eq!(std::fs::read(root.join("one")).expect("unchanged checkout"), b"before");
        prepared.work.verify(budget).expect("finite preparation receipt");
        let moved_root = temporary.path().join("offline-root");
        let moved_operation = temporary.path().join("offline-operation");
        std::fs::rename(&root, &moved_root).expect("unavailable checkout");
        std::fs::rename(&operation, &moved_operation).expect("unavailable staging");
        let loaded = load_native_materialization_journal(&crate::LocalCoreStateStore::new(&state_path), operation_id, budget, &cancellation).await.expect("root-independent restart query");
        assert_eq!(loaded.value.as_ref(), Some(&prepared.value));
        std::fs::rename(moved_root, &root).expect("original allocation returns");
        std::fs::rename(moved_operation, &operation).expect("original staging returns");
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("apply meter");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let failure = publish_native_generation_transition(
            &from, &to, &crate::LocalCoreStateStore::new(&state_path), publication(), work,
            || std::future::ready(if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(NativeWorkspacePublicationError::Native(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()))
            }),
        ).await.expect_err("second original action loses owner authorization");
        failure.1.verify(budget).expect("all partial invocation work retained");
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        let loaded = load_native_materialization_journal(&crate::LocalCoreStateStore::new(&state_path), operation_id, budget, &cancellation).await.expect("incomplete original operation").value.expect("original intent");
        assert_eq!(loaded.phase, MaterializationPhase::Applying);
        assert_eq!(loaded.applied, 1);
        assert_eq!(loaded.plan, prepared.value.plan);
        assert_eq!(loaded.root_identity, prepared.value.root_identity);
        assert_eq!(loaded.preparation_identity, prepared.value.preparation_identity);
        assert_eq!(std::fs::read(root.join("one")).expect("first effect"), b"after");
        assert_eq!(std::fs::read(root.join("two")).expect("denied effect"), b"before");
        assert_eq!(std::fs::read(root.join("untouched")).expect("untouched"), b"dirty user data");
        let resumed = publish_native_generation_transition(
            &from, &to, &crate::LocalCoreStateStore::new(&state_path), publication(),
            NativeMaterializationWork::new(budget, &cancellation).expect("restart meter"),
            || std::future::ready(Ok(())),
        ).await.expect("resume same original operation");
        assert_eq!(resumed.value.phase, MaterializationPhase::Applied);
        assert_eq!(std::fs::read(root.join("two")).expect("resumed effect"), b"after");
        assert_eq!(std::fs::read(root.join("untouched")).expect("untouched survives"), b"dirty user data");
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_preflight_does_not_adopt_existing_before_absent_user_file() {
        let temporary = tempfile::tempdir().expect("temporary publication");
        let root = temporary.path().join("root");
        std::fs::create_dir(&root).expect("checkout");
        std::fs::write(root.join("new"), b"after").expect("matching but unauthenticated user file");
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("before-absent", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace");
        let from = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("empty baseline");
        workspace.write_text("/new", "after").await.expect("target");
        let to = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("target generation");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("identity work");
        let identity = work.root_identity(&root).expect("actual allocation");
        let operation = temporary.path().join("operation");
        let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
        let options = crate::MaterializeOptions::native(&root);
        let operation_id = OperationId::new();
        let failure = prepare_native_generation_transition(&from, &to, &state, NativeWorkspacePublication {
            root: &root, source_root_identity: identity, operation_directory: &operation,
            operation_id, from: from.id(), to: to.id(), excluded_names: &[],
            options: &options, budget, cancellation: &cancellation,
        }).await.expect_err("before-absent cannot authorize an existing path");
        assert!(matches!(failure.0, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_, Some(_)))));
        failure.1.verify(budget).expect("preflight failure work");
        assert_eq!(std::fs::read(root.join("new")).expect("user file"), b"after");
        assert!(!operation.exists());
        assert!(load_native_materialization_journal(&state, operation_id, budget, &cancellation).await.expect("original journal query").value.is_none());
        std::fs::remove_file(root.join("new")).expect("remove fixture user file");
        let publication = || NativeWorkspacePublication {
            root: &root, source_root_identity: identity, operation_directory: &operation,
            operation_id, from: from.id(), to: to.id(), excluded_names: &[],
            options: &options, budget, cancellation: &cancellation,
        };
        let prepared = prepare_native_generation_transition(&from, &to, &state, publication()).await.expect("authenticated empty baseline");
        let original_root = temporary.path().join("original-root");
        let failure = publish_native_generation_transition(&from, &to, &state, publication(),
            NativeMaterializationWork::new(budget, &cancellation).expect("guarded meter"), || {
                std::fs::rename(&root, &original_root).expect("root replacement fault after authorization await");
                std::fs::create_dir(&root).expect("replacement user root");
                std::fs::write(root.join("user"), b"replacement").expect("replacement user data");
                std::future::ready(Ok(()))
            },
        ).await.expect_err("actual held/original root is rechecked after owner callback");
        failure.1.verify(budget).expect("post-guard root failure work");
        assert_eq!(std::fs::read(root.join("user")).expect("replacement preserved"), b"replacement");
        assert!(!root.join("new").exists());
        assert!(!original_root.join("new").exists());
        let retained = load_native_materialization_journal(&state, operation_id, budget, &cancellation).await.expect("query incomplete despite diverged root").value.expect("original durable decision");
        assert_eq!(retained.plan, prepared.value.plan);
        assert_eq!(retained.root_identity, Some(identity.to_bytes()));
        assert_eq!(retained.preparation_identity, prepared.value.preparation_identity);
        assert_eq!(retained.phase, MaterializationPhase::Applying);
        assert_eq!(retained.applied, 0);
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_source_preflight_distinguishes_directory_metadata_from_structural_consumption() {
        let temporary = tempfile::tempdir().expect("source preflight");
        let root = temporary.path().join("root");
        std::fs::create_dir(&root).expect("checkout");
        std::fs::create_dir(root.join("tree")).expect("directory");
        std::fs::write(root.join("tree/file"), b"dirty untouched").expect("dirty descendant");
        std::fs::write(root.join("tree/user"), b"unknown untouched").expect("unknown descendant");
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("structural-source", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace");
        let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("logical transaction");
        transaction.create_directory("/tree", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("logical directory");
        transaction.write_text("/tree/file", "before").await.expect("logical descendant");
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("logical publication");
        let generation = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("baseline");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("meter");
        let held = crate::native_host::HostRoot::open(&root, &mut &work).expect("held checkout");
        native_preflight_path(&held, "tree", &generation, false, true, &work, &cancellation).await.expect("metadata-only action does not consume descendants");
        let error = native_preflight_path(&held, "tree", &generation, true, true, &work, &cancellation).await.expect_err("structural removal must authenticate the entire consumed subtree");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_, Some(_)))));
        assert_eq!(std::fs::read(root.join("tree/file")).expect("dirty child untouched"), b"dirty untouched");
        assert_eq!(std::fs::read(root.join("tree/user")).expect("unknown child untouched"), b"unknown untouched");
        std::fs::remove_file(root.join("tree/user")).expect("remove fixture unknown child");
        let error = native_preflight_path(&held, "tree", &generation, true, true, &work, &cancellation).await.expect_err("matching names alone cannot authorize changed descendant bytes");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_, Some(_)))));
        work.receipt().verify(budget).expect("all source membership and content prefix work retained");
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_allocation_intent_local_restart_never_adopts_unknown_mkdir_ack() {
        for fail_revision in 2..=8 {
            for lose_ack in [false, true] {
                let temporary = tempfile::tempdir().expect("allocation fixture");
                let root = temporary.path().join("root");
                let operation = temporary.path().join("operation");
                let state_path = temporary.path().join("state");
                std::fs::create_dir(&root).expect("source root");
                std::fs::write(root.join("file"), b"source remains").expect("source data");
                let budget = finite_native_budget();
                let cancellation = crate::CancellationToken::new();
                let work = NativeMaterializationWork::new(budget, &cancellation).expect("meter");
                let source = work.root_identity(&root).expect("original source allocation");
                let parent = work.root_identity(temporary.path()).expect("original operation parent");
                let operation_id = OperationId::new();
                let plan = test_plan(operation_id, vec![MaterializationEdit::Remove { path: "file".to_owned() }]
                );
                let state = crate::LocalCoreStateStore::new(&state_path);
                let mut journal = MaterializationJournal {
        #[cfg(not(target_arch = "wasm32"))] custody: None,
                    version: JOURNAL_VERSION, revision: 0, plan,
                    root_identity: Some(source.to_bytes()), preparation_identity: None,
                    preparation_progress: Some(MaterializationPreparationProgress {
                        parent: parent.to_bytes(), operation: None, target: None, backup: None, pending: None,
                    }),
                    work: work.journal_work().expect("original prefix"), preimages: Vec::new(),
                    phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
                };
                let custody = work.retain_owned(journal_owned_bytes(&journal).expect("actual fixture capacity")).expect("original fixture admission");
                journal.custody = Some(custody);
                let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), journal).await.expect("first original intent").1;
                assert!(!operation.exists());
                let fault = FaultingLocalJournal { store: state.materialization_with_work(work.clone()), fail_revision, lose_ack };
                let error = NativeTreeMaterializationBackend::prepare_allocations(&fault, initial, &root, &operation, &work, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect_err("original allocation checkpoint failure");
                assert!(matches!(error, MaterializationError::Store(_)));
                work.receipt().verify(budget).expect("all native and failed journal work retained");
                assert_eq!(std::fs::read(root.join("file")).expect("source unchanged"), b"source remains");
                drop(fault);
                drop(state);
                let restarted = crate::LocalCoreStateStore::new(&state_path);
                let resumed_work = NativeMaterializationWork::new(budget, &cancellation).expect("restart meter");
                let current = restarted.materialization_with_work(resumed_work.clone()).load(operation_id).await.expect("root independent original progress query").expect("retained original intent");
                assert_eq!(current.root_identity, Some(source.to_bytes()));
                assert_eq!(current.phase, MaterializationPhase::Preparing);
                assert!(current.preimages.is_empty());
                assert!(current.is_valid_for(operation_id));
                let unknown = current.preparation_progress.is_some_and(|progress| progress.pending.is_some());
                if unknown {
                    let progress = current.preparation_progress.expect("indeterminate allocation prefix");
                    let before = std::fs::read_dir(temporary.path()).expect("fixture names").count();
                    let error = NativeTreeMaterializationBackend::prepare_allocations(&restarted.materialization_with_work(resumed_work.clone()), current, &root, &operation, &resumed_work, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect_err("unknown mkdir cannot be adopted from a current path");
                    assert!(matches!(error, MaterializationError::Backend(NativeTreeMaterializationError::IndeterminatePreparation)));
                    assert_eq!(std::fs::read_dir(temporary.path()).expect("no repair or adoption").count(), before);
                    let retained = load_native_materialization_journal(&restarted, operation_id, budget, &cancellation).await.expect("still original pending decision").value.expect("retained");
                    assert_eq!(retained.preparation_progress, Some(progress));
                    assert!(retained.preparation_identity.is_none());
                } else if current.preparation_progress.is_some() {
                    let completed = NativeTreeMaterializationBackend::prepare_allocations(&restarted.materialization_with_work(resumed_work.clone()), current, &root, &operation, &resumed_work, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect("continue only recorded known original allocations");
                    assert!(completed.preparation_progress.is_none());
                    let ready = completed.preparation_identity.expect("all three actual IDs");
                    assert_eq!(resumed_work.root_identity(&operation).expect("actual operation").to_bytes(), ready.operation);
                    assert_eq!(resumed_work.root_identity(&operation.join("target")).expect("actual target").to_bytes(), ready.target);
                    assert_eq!(resumed_work.root_identity(&operation.join("backup")).expect("actual backup").to_bytes(), ready.backup);
                } else {
                    assert!(current.preparation_identity.is_some());
                }
                resumed_work.receipt().verify(budget).expect("restart work remains bounded");
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct NativeCaptureBarrier {
        inner: NativeTreeMaterializationBackend,
        file: PathBuf,
        armed: std::sync::atomic::AtomicBool,
        effects: std::sync::atomic::AtomicUsize,
    }
    #[cfg(not(target_arch = "wasm32"))]
    impl MaterializationBackend for NativeCaptureBarrier {
        type Error = NativeTreeMaterializationError;
        fn allocation_identity(&self) -> Option<[u8; 16]> { self.inner.allocation_identity() }
        fn preparation_identity(&self) -> Option<MaterializationPreparationIdentity> { self.inner.preparation_identity() }
        fn observed_work(&self) -> Result<Option<crate::WorkCounters>, Self::Error> { self.inner.observed_work() }
        fn retain_original_work(&self, work: crate::WorkCounters) { self.inner.retain_original_work(work); }
        fn check_plan(&self, plan: &MaterializationPlan) -> Result<bool, Self::Error> { self.inner.check_plan(plan) }
        fn retain_plan(&self, plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> { MaterializationBackend::retain_plan(&self.inner, plan) }
        fn check_custody(&self, journal: &mut MaterializationJournal, incoming: &mut [Option<NativeAllocation>]) -> Result<(), Self::Error> { self.inner.check_custody(journal, incoming) }
        fn append_preimage(&self, journal: &mut MaterializationJournal, preimage: MaterializationPreimage) -> Result<(), Self::Error> { self.inner.append_preimage(journal, preimage) }
        async fn observe_allocation_identity(&self) -> Result<Option<[u8; 16]>, Self::Error> { self.inner.observe_allocation_identity().await }
        async fn observe_preparation_identity(&self) -> Result<Option<MaterializationPreparationIdentity>, Self::Error> { self.inner.observe_preparation_identity().await }
        async fn observe(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<MaterializationObservation, Self::Error> { self.inner.observe(edit, preimage).await }
        async fn capture(&self, edit: &MaterializationEdit) -> Result<MaterializationPreimage, Self::Error> {
            if self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
                // Independent user-actor fault: this is not SDK-accounted IO.
                std::fs::write(&self.file, b"user change during capture")?;
            }
            self.inner.capture(edit).await
        }
        async fn apply(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<(), Self::Error> {
            self.effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.apply(edit, preimage).await
        }
        async fn restore(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<(), Self::Error> { self.inner.restore(edit, preimage).await }
        async fn settle_observed(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> { self.inner.settle_observed(edit).await }
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn real_native_first_effect_reauthenticates_original_generation_after_capture_barrier() {
        let temporary = tempfile::tempdir().expect("real native source barrier");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        let state_path = temporary.path().join("state");
        std::fs::create_dir(&root).expect("checkout");
        std::fs::create_dir_all(operation.join("target")).expect("owned fixture target");
        std::fs::write(root.join("file"), b"before").expect("original source");
        std::fs::write(operation.join("target/file"), b"after").expect("actual stage");
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("native-capture-barrier", crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("workspace");
        workspace.write_text("/file", "before").await.expect("original authored source");
        let from = workspace.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).expect("immutable original generation");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("same actual native meter");
        let backend = NativeTreeMaterializationBackend::new(root.clone(), operation.clone(), None, work.clone(), true).expect("actual native capabilities");
        let plan = MaterializationPlan {
            operation_id: OperationId::new(),
            intent: MaterializationIntent::GenerationTransition { from: from.id(), to: GenerationId::new(Digest::from_bytes([2; 32])) },
            edits: vec![MaterializationEdit::Install { path: "file".into(), image: vec![1] }],
        };
        let operation_id = plan.operation_id;
        native_preflight_plan(&backend.held_root, &plan, &from, &work, &cancellation).await.expect("source really matches before capture");
        let source = backend.allocation_identity();
        let preparation = backend.preparation_identity();
        let state = crate::LocalCoreStateStore::new(&state_path);
        let engine = JournaledMaterializer::new(state.materialization_with_work(work.clone()), NativeCaptureBarrier {
            inner: backend, file: root.join("file"), armed: std::sync::atomic::AtomicBool::new(true),
            effects: std::sync::atomic::AtomicUsize::new(0),
        });
        let mut guarded = |verify| {
            native_before_effect(std::future::ready(Ok(())), verify, &engine.store, &engine.backend().inner.held_root, operation_id, &from, &work, &cancellation)
        };
        let error = engine.apply(plan, &mut guarded).await.expect_err("fresh original-source verification rejects the captured user change");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_, Some(_)))));
        assert_eq!(engine.backend().effects.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(std::fs::read(root.join("file")).expect("independent physical read"), b"user change during capture");
        assert_eq!(std::fs::read(operation.join("target/file")).expect("stage untouched"), b"after");
        work.receipt().verify(budget).expect("actual native and immutable source failure prefix");
        let journal = load_native_materialization_journal(&crate::LocalCoreStateStore::new(&state_path), operation_id, budget, &cancellation).await.expect("actual restart query").value.expect("retained original operation");
        assert_eq!(journal.root_identity, source);
        assert_eq!(journal.preparation_identity, preparation);
        assert_eq!(journal.phase, MaterializationPhase::Applying);
        assert_eq!(journal.applied, 0);
        assert_eq!(journal.preimages.len(), 1);
    }

}
