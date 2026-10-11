//! Journaled pathwise application and crash recovery.
//!
//! Merge publication changes an authenticated workspace head atomically. A
//! native checkout still needs pathwise host operations, which cannot be made
//! atomic by an operating system. This module centralizes the durable protocol:
//! capture every preimage before the first mutation, record progress after each
//! operation, and complete or roll back deterministically after interruption.

use crate::record_store::{MAXIMUM_CAS_ATTEMPTS, MemoryRecords, next_revision};
use crate::{GenerationId, OperationId};
use serde::{Deserialize, Serialize};
use std::future::Future;
#[cfg(not(target_arch = "wasm32"))]
use std::path::{Path, PathBuf};
use thiserror::Error;

const JOURNAL_VERSION: u32 = 2;

/// Native preparation installs carrying this prefix must start at an absent
/// live binding; user data can never become an adopted preparation preimage.
#[cfg(not(target_arch = "wasm32"))]
pub const NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX: &[u8] = b"acyclic.native.prepare-absent/v1\0";

/// A funded invocation-local cancellation view of the same original engine.
/// Its parent work predates this view, so its intrinsic Self guard cannot
/// point back to the view. Opaque standard-library Arc backing is excluded.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeInvocationView {
    work: NativeMaterializationWork,
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
        match (self.work.resource.as_ref(), resource) {
            (None, None) => true,
            (Some(left), Some(right)) => std::sync::Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        let mut original = self.work.state();
        original.with_invocation_scope(Some(self.self_ref.clone()), |work| {
            work.with_resource_scope(self.work.resource.as_ref().map(std::sync::Arc::downgrade), operation)
        })
    }

    fn with_io<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> Result<T, crate::DemandError>) -> std::io::Result<T> {
        self.with_original_leaf(|original| {
            operation(original).map_err(|error| original.record_native_io_failure(error))
        })
    }
}

/// A resource allowance is a view of the original engine, never another
/// allocation ledger. Its own logical body is funded by the root-only guard
/// last; opaque standard-library Arc/Weak backing is not a payload claim.
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
    poisoned: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy)]
pub(crate) enum NativeResourceCredit {
    Items, ReturnedItems, Copy, ReadOperations, WriteOperations, DurabilityOperations, ReadBytes, WriteBytes,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceCredit {
    fn counter(self, work: &mut crate::WorkCounters) -> &mut u64 {
        match self {
            Self::Items => &mut work.items_examined,
            Self::ReturnedItems => &mut work.items_returned,
            Self::Copy => &mut work.bytes_copied,
            Self::ReadOperations => &mut work.backend_read_operations,
            Self::WriteOperations => &mut work.backend_write_operations,
            Self::DurabilityOperations => &mut work.durability_operations,
            Self::ReadBytes => &mut work.source_bytes_read,
            Self::WriteBytes => &mut work.output_bytes,
        }
    }

    fn delta(self, amount: u64) -> crate::WorkCounters {
        let mut delta = crate::WorkCounters::UNCHARGED;
        *self.counter(&mut delta) = amount;
        delta
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceState {
    fn effective_budget(&self) -> Result<crate::WorkBudget, crate::DemandError> {
        self.pending.remaining(self.budget).map_err(crate::DemandError::from)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeResourceAllowance {
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, NativeResourceState>, crate::DemandError> {
        let state = self.state.lock().map_err(|_| crate::DemandError::OriginalWorkPoisoned)?;
        if state.poisoned { return Err(crate::DemandError::OriginalWorkPoisoned); }
        Ok(state)
    }

    pub(crate) fn reserve_work<T>(&self, maximum: crate::WorkCounters, operation: impl FnOnce() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut operation = Some(operation);
        self.reserve_work_chain(maximum, &mut || operation.take().expect("one original work reservation")())
    }

    fn reserve_work_chain<T>(&self, maximum: crate::WorkCounters, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        if let Some(parent) = &self.parent {
            return parent.reserve_work_chain(maximum, &mut || self.own_reserve_work(maximum, operation));
        }
        self.own_reserve_work(maximum, operation)
    }

    fn own_reserve_work<T>(&self, maximum: crate::WorkCounters, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut state = self.lock()?;
        state.work.admit(&maximum, &state.effective_budget()?)?;
        let pending = state.pending.checked_add(maximum)?;
        let value = operation()?;
        state.pending = pending;
        Ok(value)
    }

    pub(crate) fn refund_work(&self, unused: crate::WorkCounters) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.refund_work(unused));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => { let mut state = poisoned.into_inner(); state.poisoned = true; state }
        };
        match unused.remaining(state.pending) {
            Ok(pending) => state.pending = pending,
            Err(error) => { state.poisoned = true; return Err(error.into()); }
        }
        parent
    }

    pub(crate) fn complete_work(&self, actual: crate::WorkCounters) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.complete_work(actual));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => { let mut state = poisoned.into_inner(); state.poisoned = true; state }
        };
        let pending = actual.remaining(state.pending);
        state.work.try_add_assign(&actual)?;
        match pending {
            Ok(pending) => state.pending = pending,
            Err(error) => { state.poisoned = true; return Err(error.into()); }
        }
        let completed = if state.poisoned {
            Err(crate::DemandError::OriginalWorkPoisoned)
        } else {
            state.work.verify(state.effective_budget()?).map_err(crate::DemandError::from)
        };
        parent.and(completed)
    }

    fn local_delta(state: &NativeResourceState, mut delta: crate::WorkCounters, bytes: u64) -> Result<crate::WorkCounters, crate::DemandError> {
        // A global receipt's peak never attributes resource ownership.
        delta.peak_allocation_bytes = state.live.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        Ok(delta)
    }

    pub(crate) fn admit(&self, delta: crate::WorkCounters, bytes: u64) -> Result<(), crate::DemandError> {
        if let Some(parent) = &self.parent { parent.admit(delta, bytes)?; }
        let state = self.lock()?;
        state.work.admit(&Self::local_delta(&state, delta, bytes)?, &state.effective_budget()?)?;
        Ok(())
    }

    pub(crate) fn admission<T>(&self, delta: crate::WorkCounters, bytes: u64, operation: impl FnOnce() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut operation = Some(operation);
        self.admission_chain(delta, bytes, &mut || operation.take().expect("one original admission")())
    }

    fn admission_chain<T>(&self, delta: crate::WorkCounters, bytes: u64, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        if let Some(parent) = &self.parent {
            return parent.admission_chain(delta, bytes, &mut || self.own_admission(delta, bytes, operation));
        }
        self.own_admission(delta, bytes, operation)
    }

    fn own_admission<T>(&self, delta: crate::WorkCounters, bytes: u64, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut state = self.lock()?;
        let local = Self::local_delta(&state, delta, bytes)?;
        state.work.admit(&local, &state.effective_budget()?)?;
        let value = operation()?;
        state.work.try_add_assign(&local)?;
        state.live += bytes;
        Ok(value)
    }

    pub(crate) fn completed(&self, delta: crate::WorkCounters, bytes: u64) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.completed(delta, bytes));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.poisoned = true;
                state
            }
        };
        let local = Self::local_delta(&state, delta, bytes)?;
        state.work.try_add_assign(&local)?;
        state.live += bytes;
        let result = if state.poisoned {
            Err(crate::DemandError::OriginalWorkPoisoned)
        } else {
            state.work.verify(state.effective_budget()?).map_err(crate::DemandError::from)
        };
        parent.and(result)
    }

    pub(crate) fn retire(&self, bytes: u64) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.retire(bytes));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.poisoned = true;
                state
            }
        };
        let Some(live) = state.live.checked_sub(bytes) else {
            state.poisoned = true;
            return Err(crate::DemandError::OriginalWorkPoisoned);
        };
        state.live = live;
        parent
    }

    pub(crate) fn remaining(&self) -> Result<crate::WorkBudget, crate::DemandError> {
        let state = self.lock()?;
        let mut remaining = state.work.remaining(state.effective_budget()?)?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(state.live).ok_or(crate::WorkError::Overflow)?;
        drop(state);
        if let Some(parent) = &self.parent { remaining = remaining.intersect_budget(parent.remaining()?); }
        Ok(remaining)
    }

    pub(crate) fn reserve_credit<T>(&self, kind: NativeResourceCredit, count: u64, operation: impl FnOnce() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut operation = Some(operation);
        self.reserve_credit_chain(kind, count, None, &mut || operation.take().expect("one original reservation")())
    }

    fn contains_scope(&self, scope: &Self) -> bool {
        std::ptr::eq(self, scope) || self.parent.as_ref().is_some_and(|parent| parent.contains_scope(scope))
    }

    fn reserve_credit_chain<T>(
        &self, kind: NativeResourceCredit, count: u64, excluded: Option<&Self>,
        operation: &mut dyn FnMut() -> Result<T, crate::DemandError>,
    ) -> Result<T, crate::DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) { return operation(); }
        if let Some(parent) = &self.parent {
            return parent.reserve_credit_chain(kind, count, excluded, &mut || self.own_reserve_credit(kind, count, operation));
        }
        self.own_reserve_credit(kind, count, operation)
    }

    fn reserve_credit_excluding(&self, kind: NativeResourceCredit, count: u64, excluded: Option<&Self>) -> Result<(), crate::DemandError> {
        self.reserve_credit_chain(kind, count, excluded, &mut || Ok(()))
    }

    fn own_reserve_credit<T>(&self, kind: NativeResourceCredit, count: u64, operation: &mut dyn FnMut() -> Result<T, crate::DemandError>) -> Result<T, crate::DemandError> {
        let mut state = self.lock()?;
        state.work.admit(&kind.delta(count), &state.effective_budget()?)?;
        let pending = (*kind.counter(&mut state.pending)).checked_add(count).ok_or(crate::WorkError::Overflow)?;
        let value = operation()?;
        *kind.counter(&mut state.pending) = pending;
        Ok(value)
    }

    pub(crate) fn refund_credit(&self, kind: NativeResourceCredit, count: u64) -> Result<(), crate::DemandError> {
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.refund_credit(kind, count));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.poisoned = true;
                state
            }
        };
        let pending = kind.counter(&mut state.pending);
        let Some(remaining) = pending.checked_sub(count) else {
            state.poisoned = true;
            return Err(crate::DemandError::OriginalWorkPoisoned);
        };
        *pending = remaining;
        parent
    }

    pub(crate) fn complete_credit(&self, kind: NativeResourceCredit, reserved: u64, actual: u64) -> Result<(), crate::DemandError> {
        let refunded = self.refund_credit(kind, reserved);
        let recorded = self.completed(kind.delta(actual), 0);
        if actual > reserved { return Err(crate::DemandError::InvalidRequest); }
        refunded.and(recorded)
    }

    fn refund_credit_excluding(&self, kind: NativeResourceCredit, count: u64, excluded: Option<&Self>) -> Result<(), crate::DemandError> {
        if excluded.is_some_and(|excluded| excluded.contains_scope(self)) { return Ok(()); }
        let parent = self.parent.as_ref().map_or(Ok(()), |parent| parent.refund_credit_excluding(kind, count, excluded));
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.poisoned = true;
                state
            }
        };
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
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.poisoned = true;
                state
            }
        };
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
            state.work.verify(state.effective_budget()?).map_err(crate::DemandError::from)
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
    ($owner:ty) => {
impl acyclic_native_runtime::NativeReadMeter for $owner {
    fn check(&self) -> std::io::Result<()> { self.with_io(|work| work.check()) }

    fn reserve_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.reserve_native_operations(kind, count))
    }
    fn cancel_reserved_operations(&self, kind: acyclic_native_runtime::NativeOperationKind, count: u64) {
        self.with_original_leaf(|work| work.cancel_reserved_native_operations(kind, count));
    }
    fn record_reserved_operation(&self, kind: acyclic_native_runtime::NativeOperationKind) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.record_reserved_native_operation(kind))
    }
    fn admit_write_bytes(&self, maximum: usize) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.admit_native_write_bytes(maximum))
    }
    fn reserve_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.reserve_native_bytes(kind, maximum))
    }
    fn cancel_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64) {
        self.with_original_leaf(|work| work.cancel_reserved_native_bytes(kind, maximum));
    }
    fn record_reserved_bytes(&self, kind: acyclic_native_runtime::NativeOperationKind, maximum: u64, actual: u64) -> std::io::Result<()> {
        self.with_original_leaf(|work| work.record_reserved_native_bytes(kind, maximum, actual))
    }
    fn allocate(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.claim_native_owned(bytes, u64::from(bytes != 0), &None))
    }
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
        self.with_original_leaf(|work| work.record_native_io_failure(crate::DemandError::Allocation(error)))
    }
    fn allocate_refcount_header(&self) -> std::io::Result<()> {
        self.with_io(|work| work.charge(crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED }))
    }
    fn adopt(&self, bytes: u64) -> std::io::Result<()> {
        self.with_io(|work| work.claim_native_owned(bytes, 0, &None))
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

/// Physical primitive retirement carries the actual allocation's resource
/// attribution, independently of the original ledger's retirement cursor.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeResourceRetirement {
    resource: Option<std::sync::Arc<NativeResourceAllowance>>,
    capture: Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>,
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
    resource: Option<std::sync::Arc<NativeResourceAllowance>>,
    capture_origin: Option<std::sync::Weak<crate::demand::CaptureTransientCustody>>,
    invocation: Option<std::sync::Arc<NativeInvocationView>>,
    state: std::sync::Arc<crate::demand::NativeDemandReadMeter>,
    cancellation: crate::CancellationToken,
}

/// Opening policy for an already-authorized host-private state pathname.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeStateFileMode {
    /// Opens an existing regular file without following its final binding.
    Readonly,
    /// Opens an existing regular read/write file without creating or truncating.
    ReadWriteExisting,
    /// Opens or creates a regular read/write file without truncating it.
    ReadWriteCreate,
}

/// Opaque namespace owner for a trusted host's configured private state root.
///
/// This is distinct from accounting, task grants and project-read authority.
/// Only trusted service bootstrap may mint it from its configured private
/// directory while retaining the host's real configuration/creation authority.
/// Finite request handlers receive this owner, never mint one from a request.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeHostPrivateStateRoot {
    inner: std::sync::Arc<NativeHostPrivateStateRootInner>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeHostPrivateStateRootInner {
    // Only bootstrap's unpublished unique Arc may contain None.
    root: Option<crate::native_host::HostRoot>,
    _allocation: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeHostPrivateStateRootInner {
    fn root(&self) -> &crate::native_host::HostRoot {
        self.root.as_ref().expect("only initialized configured-root owners are published")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeHostPrivateStateRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeHostPrivateStateRoot")
            .field("physical_identity", &self.inner.root().identity()).finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeHostPrivateStateRoot {
    /// Registers and holds the genuine trusted host-configured private root.
    ///
    /// This is a host bootstrap boundary, not a request-path authorizer. The
    /// embedding host MUST already hold its configured-directory creation
    /// authority (and service ownership lock, where required) before entry.
    /// Neither a caller's pathname, a project read grant, an owner-file marker
    /// nor `work` establishes that authority. No public from-parts/raw-directory
    /// conversion exists. The physical root is held, not reopened by requests.
    /// The single Arc's logical body is funded before registration effects;
    /// opaque stdlib refcount and OS bookkeeping are excluded from payload/RSS.
    ///
    /// # Errors
    /// Preserves native I/O, original quota/cancellation and creation prefixes.
    pub fn bootstrap_configured_with_native_work(
        configured_directory: &Path, work: &NativeMaterializationWork,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let allocation = work.claim(std::mem::size_of::<NativeHostPrivateStateRootInner>() as u64)?;
        let mut inner = std::sync::Arc::new(NativeHostPrivateStateRootInner { root: None, _allocation: allocation });
        crate::native_host::create_directories_materialization_measured(configured_directory, work)?;
        let (directory, identity) = crate::native_host::open_root_directory_identity_with_work(
            configured_directory, &mut &*work,
        )?;
        work.admit(crate::WorkCounters::UNCHARGED)?;
        let root = crate::native_host::HostRoot::from_held_dir_with_identity(directory, identity);
        std::sync::Arc::get_mut(&mut inner).expect("bootstrap root owner is still unpublished").root = Some(root);
        Ok(Self { inner })
    }
}
#[cfg(not(target_arch = "wasm32"))]
fn validate_native_state_relative(
    relative: &Path, work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    work.admit(crate::WorkCounters::UNCHARGED)?;
    for component in relative.components() {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
    }
    Ok(())
}

/// Lossless owned metadata from a genuine held private-state directory.
/// These are data facts, not namespace authority or a live iterator capability.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativePrivateStateEntry {
    /// Native lossless child name, without an ambient parent pathname.
    pub name: std::ffi::OsString,
    /// True only for a real directory, not a final symlink/reparse binding.
    pub is_directory: bool,
    /// Actual no-follow native object identity observed for this child.
    pub identity: crate::NativeRootIdentity,
    /// Actual native modification timestamp for this child.
    pub modified: std::time::SystemTime,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativePrivateStateRows {
    rows: Vec<NativePrivateStateEntry>,
    backing: NativeAllocation,
    payloads: NativeAllocation,
}


/// An owned regular state-file descriptor and its exact held parent directory.
///
/// Namespace authority is retained from the trusted configured-root owner;
/// accounting alone cannot authorize opening a state file. No raw descriptor
/// escapes. OS-private bookkeeping is not a payload/RSS claim; the inline
/// logical Self remains funded until descriptors and root authority retire.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeStateFile {
    file: std::fs::File,
    parent: cap_std::fs::Dir,
    work: NativeMaterializationWork,
    _root: NativeHostPrivateStateRoot,
    _allocation: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeStateFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeStateFile").finish_non_exhaustive()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeStateFile {
    /// Validates private owner-file metadata through this exact held file.
    ///
    /// Unix requires the current effective user, mode 0600 and one hard link.
    /// Windows preserves the existing regular/non-reparse metadata policy;
    /// current-user payload encryption remains the caller's DPAPI engine.
    /// No raw File or ambient pathname is exposed.
    ///
    /// # Errors
    /// Preserves typed invalid layout, native I/O and original finite work.
    pub fn validate_private_owner(&self) -> Result<(), NativeTreeMaterializationError> {
        crate::native_host::validate_private_state_file_with_work(&self.file, &mut &self.work)
    }

    /// Reads from the held file's current cursor, retaining bounded bytes and
    /// their original allocation guard. No implicit seek or flush occurs.
    ///
    /// # Errors
    /// Preserves original I/O, quota and cancellation causes and actual work.
    pub fn read_owned(
        &mut self, maximum_bytes: usize,
    ) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        self.work.read_owned_bytes(&mut self.file, maximum_bytes)
    }

    /// Writes borrowed bytes at the held cursor without copying or flushing.
    ///
    /// # Errors
    /// Retains each actually completed partial transfer on the original work.
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), NativeTreeMaterializationError> {
        self.work.write_owned_bytes(&mut self.file, bytes)
    }

    /// Sets the held file's length, normally after a completed replacement
    /// write. Opening and writing never implicitly truncate the old file.
    ///
    /// # Errors
    /// Rejects lengths outside the native range before any OS effect or
    /// dynamically allocated invalid-input error.
    pub fn set_len(&self, length: u64) -> Result<(), NativeTreeMaterializationError> {
        if length > i64::MAX as u64 { return Err(NativeTreeMaterializationError::InvalidLayout); }
        acyclic_native_runtime::set_len_measured(&self.file, length, self.work.runtime_meter())
            .map_err(|error| self.work.runtime_error(error))
    }

    /// Performs a full durability barrier on this exact held file.
    ///
    /// # Errors
    /// Preserves native I/O, quota and cancellation causes on the original work.
    pub fn sync_file(&self) -> Result<(), NativeTreeMaterializationError> {
        acyclic_native_runtime::sync_file_measured(
            &self.file, acyclic_native_runtime::Durability::Full, self.work.runtime_meter(),
        ).map_err(|error| self.work.runtime_error(error))
    }

    /// Syncs the exact parent held at open, never a pathname replacement.
    ///
    /// # Errors
    /// Retains actual native read/durability prefixes on the original work.
    pub fn sync_parent(&self) -> Result<(), NativeTreeMaterializationError> {
        crate::native_host::sync_directory_held_measured(&self.parent, &self.work)?;
        self.work.admit(crate::WorkCounters::UNCHARGED)
    }
}


/// A real held original mutex leaf, never an unlocked work proxy. Only
/// accounting/cancellation/invariants may run before this guard is dropped.
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
    ///
    /// # Errors
    /// Rejects cancellation or insufficient allowance before allocating its
    /// actual mutex/state body. Failure counters stay inline: denied meter
    /// construction never allocates a diagnostic counters box.
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
        })
    }

    /// Borrows this view's linked cancellation; cancelling an invocation-local
    /// token never cancels the immutable original task token.
    pub fn cancellation(&self) -> &crate::CancellationToken {
        self.invocation.as_ref().map_or(&self.cancellation, |view| view.cancellation())
    }

    /// Creates an owned local stop on the same original engine and resource.
    /// The returned token cancels this invocation only; root cancellation and
    /// deadlines still reach its linked child and all native waiters.
    ///
    /// # Errors
    /// Rejects cancellation or insufficient original allowance before the
    /// corresponding known token/view allocation.
    pub fn invocation_view(&self) -> Result<(Self, crate::CancellationToken), NativeTreeMaterializationError> {
        let cancellation = crate::CancellationToken::linked_child_with_native_work(self.cancellation(), self)?;
        let metadata = self.claim_allocation_in_origin(std::mem::size_of::<NativeInvocationView>() as u64, 1, Some(&None))?;
        let view = std::sync::Arc::new_cyclic(|self_ref| NativeInvocationView {
            work: self.clone(), cancellation: cancellation.clone(),
            self_ref: self_ref.clone(), _metadata: metadata,
        });
        let mut work = self.clone();
        work.invocation = Some(view);
        Ok((work, cancellation))
    }

    /// Aliases this complete original invocation for native SDK callbacks.
    /// Existing buffer guards and callbacks continue to use the same live
    /// ledger, cancellation roots, counters, and cleanup reservations.
    #[cfg(feature = "native-mount")]
    #[must_use]
    pub fn native_mount_work(&self) -> crate::native_mount::NativeMountMeterWork {
        crate::native_mount::NativeMountMeterWork::from_native_materialization_work(self)
    }

    /// Tests intrinsic original-ledger identity without locking or admission.
    /// This establishes custody identity only, never task or root authority.
    #[must_use]
    pub fn shares_ledger(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.state, &other.state)
    }

    /// Pure identity of the original ledger and its exact resource attribution.
    /// No allowance, cancellation or filesystem authority is created or reset.
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

    /// Keeps the complete original invocation available after native queue
    /// work, including while intrinsic payloads still retain this same state.
    pub(crate) fn original_demand_work(&self) -> crate::demand::OriginalDemandWork {
        crate::demand::OriginalDemandWork::from_native_materialization_work(self)
    }

    pub(crate) fn from_original_demand_work(work: &crate::demand::OriginalDemandWork) -> Result<Self, crate::DemandError> {
        let cancellation = work.with_original_leaf(|state| state.cancellation().clone())?;
        Ok(Self {
            state: std::sync::Arc::clone(work.meter()), cancellation,
            resource: work.resource_allowance().cloned(), capture_origin: work.capture_origin().cloned(),
            invocation: work.invocation_view_state().cloned(),
        })
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn from_native_mount_meter_work(work: &crate::native_mount::NativeMountMeterWork) -> Self {
        let state = std::sync::Arc::clone(work.meter());
        let cancellation = state.lock().cancellation().clone();
        Self { state, cancellation, resource: work.resource_allowance().cloned(), capture_origin: work.capture_origin().cloned(), invocation: work.invocation_view_state().cloned() }
    }

    fn state(&self) -> NativeMaterializationLeaf<'_> {
        let mut original = self.state.lock();
        let previous = original.enter_materialization_leaf(self.invocation.as_ref().map(std::sync::Arc::downgrade));
        NativeMaterializationLeaf { original, previous: Some(previous) }
    }

    /// Borrows one allocator-free accounting/cancellation/invariant leaf of
    /// this complete original view. Never run allocation, OS, serialization,
    /// SDK/user callbacks or awaits while this real mutex guard is held.
    pub(crate) fn with_original_leaf<T>(&self, operation: impl FnOnce(&mut crate::demand::DemandWork) -> T) -> T {
        let mut original = self.state();
        original.with_resource_scope(self.resource.as_ref().map(std::sync::Arc::downgrade), |work| {
            work.with_native_capture_scope(self.capture_origin.clone(), operation)
        })
    }

    /// Borrows the existing runtime callback meter; no lock or new meter is
    /// retained across native I/O, reservation completion, or SDK callbacks.
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

    /// Associates only an authenticated capture's scratch admissions. The
    /// view itself holds no strong capture alias or additional accounting state.
    pub(crate) fn with_capture_custody(
        &self, custody: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let mut view = self.clone();
        view.capture_origin = Some(std::sync::Arc::downgrade(custody));
        Ok(view)
    }

    /// The authentic original grant, or this resource's immutable ceiling.
    /// This snapshot does not grant admission or clear original poison.
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

    /// Limits this resource's own work and tagged live storage while preserving
    /// the complete original global ledger, cancellation and cleanup state.
    /// A nested view also retains every ancestor resource ceiling.
    pub fn native_volume_work(&self, budget: crate::WorkBudget) -> Result<Self, NativeTreeMaterializationError> {
        let metadata = self.claim_root_metadata(std::mem::size_of::<NativeResourceAllowance>() as u64)?;
        let resource = std::sync::Arc::new_cyclic(|self_ref| NativeResourceAllowance {
            state: std::sync::Mutex::new(NativeResourceState {
                work: crate::WorkCounters::UNCHARGED, budget,
                pending: crate::WorkCounters::UNCHARGED, live: 0, poisoned: false,
            }),
            self_ref: self_ref.clone(),
            parent: self.resource.clone(),
            _root_only_metadata_guard: metadata,
        });
        Ok(Self {
            state: std::sync::Arc::clone(&self.state), cancellation: self.cancellation.clone(),
            resource: Some(resource), capture_origin: None,
            invocation: self.invocation.clone(),
        })
    }

    /// Borrows the original operation's native runtime admission interface.
    /// Runtime callbacks lock only their individual admission or completion;
    /// existing payload guards retain this exact ledger during native waits.
    #[must_use]
    pub fn runtime_meter(&self) -> &dyn acyclic_native_runtime::NativeReadMeter {
        if let Some(view) = &self.invocation {
            if view.owns_resource(self.resource.as_ref()) { return view.as_ref(); }
        }
        match &self.resource {
            Some(resource) => resource.as_ref(),
            None => self.state.as_ref(),
        }
    }

    /// Pins the same original runtime meter for an asynchronous native queue.
    /// This only clones its existing ownership handle; no accounting state,
    /// allocation ledger, or runtime meter is constructed.
    #[must_use]
    pub fn runtime_meter_owner(&self) -> std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter> {
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

    /// Pins capture custody only for an accepted source job's physical terminal.
    /// Its SDK allocation guards project back to the same unanchored root/view.
    #[cfg(feature = "native-watch")]
    pub(crate) fn runtime_meter_owner_with_capture_custody(
        &self, custody: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>, NativeTreeMaterializationError> {
        crate::native_capture::capture_runtime_meter_owner(self, custody)
    }

    /// Recovers this original runtime's inline typed first failure; otherwise
    /// preserves the actual native I/O error without interpreting its message.
    pub fn runtime_error(&self, error: std::io::Error) -> NativeTreeMaterializationError {
        match self.state().take_native_io_failure() {
            Some(cause) => native_materialization_work_error(cause),
            None => NativeTreeMaterializationError::from_runtime_error(error),
        }
    }

    /// Copies a borrowed payload through the canonical native byte-buffer
    /// engine, retaining the actual capacity on this original resource.
    /// This grants no access to the supplied bytes.
    ///
    /// # Errors
    /// Preserves the real completed copy prefix and allocation/quota cause.
    pub fn copy_bytes(&self, bytes: &[u8]) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        let mut writer = NativeImageWriter::new_bytes(self)?;
        let copied = writer.append(bytes)?;
        if copied != bytes.len() {
            self.admit(crate::WorkCounters { bytes_copied: 1, ..crate::WorkCounters::UNCHARGED })?;
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok((writer.image, writer.allocation))
    }

    /// Reads an already-authorized reader with bounded retained output.
    /// Each SDK reader call and its returned bytes use the original native
    /// operation/byte reservations. Provider-internal work is not observable.
    /// One extra byte detects an oversized payload; no path is opened here.
    ///
    /// # Errors
    /// Returns original I/O, quota or cancellation causes with their completed
    /// prefix retained by this work, including an oversized final probe.
    pub fn read_owned_bytes<R: std::io::Read + ?Sized>(
        &self, input: &mut R, maximum_bytes: usize,
    ) -> Result<(Vec<u8>, NativeAllocation), NativeTreeMaterializationError> {
        use acyclic_native_runtime::{NativeByteReservation, NativeOperationKind, NativeOperationReservation};
        let mut writer = NativeImageWriter::new_bytes(self)?;
        let mut chunk = [0_u8; 4096];
        loop {
            let remaining = maximum_bytes.checked_sub(writer.image.len())
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
            let requested = remaining.saturating_add(1).min(chunk.len());
            let meter = self.runtime_meter();
            let mut operation = NativeOperationReservation::new(meter, NativeOperationKind::Read, 1)
                .map_err(|error| self.runtime_error(error))?;
            let mut transfer = NativeByteReservation::new(meter, NativeOperationKind::Read, requested)
                .map_err(|error| self.runtime_error(error))?;
            let (read, recorded) = operation.attempt(|| input.read(&mut chunk[..requested]));
            let count = match read {
                Ok(count) if count <= requested => count,
                Ok(_) => return Err(NativeTreeMaterializationError::InvalidLayout),
                Err(error) => {
                    recorded.map_err(|error| self.runtime_error(error))?;
                    if error.kind() == std::io::ErrorKind::Interrupted { continue; }
                    return Err(self.runtime_error(error));
                }
            };
            let transferred = transfer.record(count);
            recorded.map_err(|error| self.runtime_error(error))?;
            transferred.map_err(|error| self.runtime_error(error))?;
            if count == 0 { return Ok((writer.image, writer.allocation)); }
            if count > remaining {
                return Err(NativeTreeMaterializationError::PayloadTooLarge {
                    maximum: maximum_bytes as u64,
                    actual: (writer.image.len() as u64).checked_add(count as u64)
                        .ok_or(crate::WorkError::Overflow)?,
                });
            }
            let copied = writer.append(&chunk[..count])?;
            if copied != count {
                self.admit(crate::WorkCounters { bytes_copied: 1, ..crate::WorkCounters::UNCHARGED })?;
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
    }

    /// Writes borrowed bytes through this original work's SDK I/O boundary.
    /// Records each completed partial write before exposing late cancellation
    /// or failure. No input copy, flush, filesystem permission or new ledger is
    /// introduced; the caller remains responsible for authorizing its writer.
    ///
    /// # Errors
    /// Returns the original admission, cancellation or I/O cause, retaining
    /// the actual completed output prefix in this same original work.
    pub fn write_owned_bytes<W: std::io::Write + ?Sized>(
        &self, output: &mut W, bytes: &[u8],
    ) -> Result<(), NativeTreeMaterializationError> {
        use acyclic_native_runtime::{
            NativeOperationKind, NativeOperationReservation, OwnedNativeByteReservation,
        };
        let mut transfer = OwnedNativeByteReservation::new(
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
    /// Opens a relative state pathname under a genuine configured-root owner.
    ///
    /// The caller MUST supply the bootstrap-minted namespace owner and validate
    /// pathname protection and original-source ownership before entry; this
    /// work is never filesystem authorization. Absolute and parent paths are
    /// rejected before host I/O. Existing no-follow native traversal/name
    /// engines retain the regular file, its exact parent and root authority.
    /// Create mode never truncates; no new Arc backing or ledger is allocated.
    ///
    /// # Errors
    /// Preserves original NotFound (including a missing parent), native I/O,
    /// quota and cancellation causes and every actual completed effect.
    pub fn open_state_file_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path, mode: NativeStateFileMode,
    ) -> Result<NativeStateFile, NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        let leaf = relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let parent = relative.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let allocation = self.retain_owned(std::mem::size_of::<NativeStateFile>() as u64)?;
        let parent = root.inner.root().open_dir_held_with_work(parent, &mut &*self)?;
        let options = match mode {
            NativeStateFileMode::Readonly => crate::native_host::NativeOpenOptions::Read,
            NativeStateFileMode::ReadWriteExisting => crate::native_host::NativeOpenOptions::ReadWrite,
            NativeStateFileMode::ReadWriteCreate => crate::native_host::NativeOpenOptions::ReadWriteCreate,
        };
        let file = crate::native_host::open_leaf_with_work(
            &parent, Path::new(leaf), &options, &mut &*self,
        )?;
        self.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(NativeStateFile { file, parent, work: self.clone(), _root: root.clone(), _allocation: allocation })
    }

    /// Registers relative directories under a trusted configured-root owner.
    ///
    /// Namespace/creation authority comes only from that supplied held owner;
    /// accounting never grants it. Uses the existing held native traversal
    /// and creation engine, without reopening an ambient request pathname.
    ///
    /// # Errors
    /// Preserves original native I/O, quota and cancellation failures and
    /// actually completed creation prefixes.
    pub fn create_directories_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<(), NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        root.inner.root().create_directories_for_state_with_work(relative, &mut &*self)?;
        self.admit(crate::WorkCounters::UNCHARGED)
    }

    /// Observes any final entry beneath the genuine held private-state root.
    ///
    /// The final binding is not followed: a directory, file or symlink/reparse
    /// witness is present and must block fresh owner-credential bootstrap.
    /// Only an actual native NotFound is absence; quota, cancellation,
    /// protection and other I/O failures are never interpreted as missing.
    ///
    /// # Errors
    /// Preserves typed invalid relative names and original finite/native causes.
    pub fn state_path_exists_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<bool, NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        let leaf = relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let parent = relative.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let exists = match root.inner.root().open_dir_held_with_work(parent, &mut &*self) {
            Ok(parent) => crate::native_host::entry_identity_with_work(
                &parent, Path::new(leaf), &mut &*self,
            )?.is_some(),
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error),
        };
        self.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(exists)
    }

    /// Reads the complete namespace beneath a genuine configured-root owner.
    ///
    /// Returns lossless names and no-follow metadata using the existing held
    /// native directory engine. Exhaustion or an original finite-work failure
    /// ends the operation; no new entry cap, truncation or successful partial
    /// list is introduced. Known output/header storage is funded before its
    /// allocation/copy boundary, with actual capacities reconciled before late
    /// cancellation. The returned tuple owns data first and its guard last.
    ///
    /// # Errors
    /// Preserves native directory/stat errors and typed original quota,
    /// cancellation, allocation and poisoned-state causes.
    pub fn read_private_state_directory_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<(Vec<NativePrivateStateEntry>, NativeAllocation), NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        let mut result = NativePrivateStateRows {
            rows: Vec::new(),
            backing: self.claim(0)?,
            payloads: self.retain_owned(std::mem::size_of::<NativePrivateStateRows>() as u64)?,
        };
        let directory = root.inner.root().open_dir_held_with_work(relative, &mut &*self)?;
        let reader = crate::native_host::read_dir_held_with_work(&directory, &mut &*self)?;
        // Tuple fields retain the actual directory reader before its buffer
        // guard on success and every early return.
        let mut reader = reader;
        while let Some(mut entry) = crate::native_host::next_private_state_entry_with_work(
            &mut reader.0, &directory, self,
        )? {
            if result.rows.len() == result.rows.capacity() {
                let old = result.rows.capacity();
                let target = old.checked_add(old.max(1)).ok_or(crate::WorkError::Overflow)?;
                let bytes = target.checked_mul(std::mem::size_of::<NativePrivateStateEntry>())
                    .ok_or(crate::WorkError::Overflow)?;
                let old_bytes = old.checked_mul(std::mem::size_of::<NativePrivateStateEntry>())
                    .ok_or(crate::WorkError::Overflow)?;
                result.backing.grow((bytes - old_bytes) as u64)?;
                let reserved = result.rows.try_reserve_exact(target - result.rows.len());
                let actual = result.rows.capacity().checked_mul(std::mem::size_of::<NativePrivateStateEntry>())
                    .ok_or(crate::WorkError::Overflow)?;
                let reconciled = result.backing.reconcile_capacity(actual as u64);
                reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
                reconciled?;
            }
            let stat = entry.0.stat.as_ref().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
            let identity = native_stat_identity(stat);
            let modified = stat.modified()?.into_std();
            let is_directory = stat.file_type().is_dir() && !stat.file_type().is_symlink();
            #[cfg(windows)]
            let is_directory = is_directory && stat.file_attributes()
                & windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT.0 == 0;
            self.admit(crate::WorkCounters { items_returned: 1, ..crate::WorkCounters::UNCHARGED })?;
            result.payloads.absorb(&mut entry.1)?;
            result.rows.push(NativePrivateStateEntry {
                name: entry.0.name, is_directory, identity, modified,
            });
            self.record_admitted(crate::WorkCounters { items_returned: 1, ..crate::WorkCounters::UNCHARGED })?;
        }
        drop(reader);
        drop(directory);
        result.payloads.absorb(&mut result.backing)?;
        self.admit(crate::WorkCounters::UNCHARGED)?;
        Ok((result.rows, result.payloads))
    }

    /// Reads one no-follow native modification timestamp beneath the held
    /// namespace owner. It never performs comparator-time ambient I/O.
    ///
    /// # Errors
    /// Only a genuine native NotFound becomes `None`; ordinary metadata I/O
    /// errors remain available to the caller's existing epoch policy. Native
    /// quota/cancellation/allocation/poison are never converted to absence.
    pub fn state_path_modified_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<Option<std::time::SystemTime>, NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        let result = match root.inner.root().stat_with_work(relative, &mut &*self) {
            Ok(stat) => Some(stat.modified()?.into_std()),
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        self.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(result)
    }

    /// Validates a real private owner-state directory beneath the held root.
    ///
    /// Unix requires the current effective user and no group/other write bits.
    /// Windows requires real directory/non-reparse metadata. The namespace
    /// owner remains separate from all original-work admission.
    ///
    /// # Errors
    /// Preserves native metadata I/O, typed layout and finite-work failures.
    pub fn validate_private_state_directory(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<(), NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        root.inner.root().validate_private_state_directory_with_work(relative, &mut &*self)
    }

    /// Atomically publishes an already-authorized private owner-state payload.
    ///
    /// Creates only an absent temporary regular leaf (Unix mode 0600), validates
    /// its held metadata, writes borrowed parts without an input copy, fully
    /// syncs the file, then performs the existing held NO-REPLACE rename and
    /// parent durability barriers. Prepared temporary cleanup and its original
    /// write reservation exist before creation, and cannot be gated by a fresh
    /// allocation/admission after fatal refusal. Cleanup never deletes a
    /// published or pre-existing destination.
    ///
    /// # Errors
    /// Preserves the original first fatal cause and every completed native
    /// transfer/publication/cleanup prefix, including late cancellation.
    pub fn publish_private_state_file_with_native_work(
        &self, root: &NativeHostPrivateStateRoot, temporary: &Path, destination: &Path,
        parts: &[&[u8]],
    ) -> Result<(), NativeTreeMaterializationError> {
        validate_native_state_relative(temporary, self)?;
        validate_native_state_relative(destination, self)?;
        if temporary == destination { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let temporary_leaf = temporary.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let destination_leaf = destination.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let temporary_parent = temporary.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let destination_parent = destination.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        self.validate_private_state_directory(root, temporary_parent)?;
        if temporary_parent != destination_parent {
            self.validate_private_state_directory(root, destination_parent)?;
        }
        let parent = root.inner.root().open_dir_held_with_work(temporary_parent, &mut &*self)?;
        let other_parent = if temporary_parent == destination_parent {
            None
        } else { Some(root.inner.root().open_dir_held_with_work(destination_parent, &mut &*self)?) };
        let destination_parent = other_parent.as_ref().unwrap_or(&parent);
        crate::native_host::with_private_state_temporary(&parent, Path::new(temporary_leaf), self, |file, published| {
            for bytes in parts { self.write_owned_bytes(file, bytes)?; }
            acyclic_native_runtime::sync_file_measured(
                file, acyclic_native_runtime::Durability::Full, self.runtime_meter(),
            ).map_err(|error| self.runtime_error(error))?;
            crate::native_host::rename_no_replace_held_with_work(
                &parent, Path::new(temporary_leaf), destination_parent, Path::new(destination_leaf), &mut &*self,
            )?;
            // Set immediately after the real rename: even a later cancelled
            // flush must never arm deletion against the destination.
            *published = true;
            let first = crate::native_host::sync_directory_held_measured(&parent, self);
            let second = match &other_parent {
                Some(parent) => crate::native_host::sync_directory_held_measured(parent, self),
                None => Ok(()),
            };
            first.and(second)?;
            self.admit(crate::WorkCounters::UNCHARGED)
        })
    }

    /// Performs a NO-REPLACE rename solely beneath the supplied held owner.
    /// Publication needing mandatory failure cleanup must use the one-shot
    /// private-state publisher instead of a caller-managed temporary sequence.
    ///
    /// # Errors
    /// Preserves original native rename and finite-work prefixes.
    pub fn rename_private_relative(
        &self, root: &NativeHostPrivateStateRoot, from: &Path, to: &Path,
    ) -> Result<(), NativeTreeMaterializationError> {
        validate_native_state_relative(from, self)?;
        validate_native_state_relative(to, self)?;
        let from_leaf = from.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let to_leaf = to.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let from_parent = from.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let to_parent = to.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let parent = root.inner.root().open_dir_held_with_work(from_parent, &mut &*self)?;
        let other = if from_parent == to_parent { None }
            else { Some(root.inner.root().open_dir_held_with_work(to_parent, &mut &*self)?) };
        crate::native_host::rename_no_replace_held_with_work(
            &parent, Path::new(from_leaf), other.as_ref().unwrap_or(&parent), Path::new(to_leaf), &mut &*self,
        )?;
        self.admit(crate::WorkCounters::UNCHARGED)
    }

    /// Removes a relative regular-file binding beneath the genuine held owner.
    /// This is an ordinary admitted removal, not post-failure cleanup custody.
    ///
    /// # Errors
    /// Preserves original native removal and finite-work prefixes.
    pub fn remove_private_relative(
        &self, root: &NativeHostPrivateStateRoot, relative: &Path,
    ) -> Result<(), NativeTreeMaterializationError> {
        validate_native_state_relative(relative, self)?;
        let leaf = relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let parent = relative.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        let parent = root.inner.root().open_dir_held_with_work(parent, &mut &*self)?;
        crate::native_host::remove_leaf_with_work(&parent, Path::new(leaf), &mut &*self)?;
        self.admit(crate::WorkCounters::UNCHARGED)
    }


    /// Withholds additive completion limits on this same original and resource.
    /// No work is reported until a real accepted SDK event completes. This is
    /// quota custody only, never task, filesystem, or append authorization.
    /// Allocation attempts and live bytes must use their real allocation guards.
    ///
    /// # Errors
    /// Rejects cancellation, insufficient limits, or nonzero allocation/peak
    /// dimensions without changing the budget or counters.
    pub fn reserve_work(&self, maximum: crate::WorkCounters) -> Result<NativeWorkReservation, NativeTreeMaterializationError> {
        if maximum.allocation_operations != 0 || maximum.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let mut state = self.state();
        let mut reserve = || state.reserve_native_work(maximum);
        match &self.resource {
            Some(resource) => resource.reserve_work(maximum, reserve),
            None => reserve(),
        }.map_err(native_materialization_work_error)?;
        Ok(NativeWorkReservation { meter: self.clone(), remaining: maximum })
    }

    /// Admits and charges one actually attempted operation before its boundary.
    pub fn charge(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = match &self.resource {
            Some(resource) => resource.admission(delta, 0, || state.charge(delta)),
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

    pub(crate) fn with_allocations<T, E: From<NativeTreeMaterializationError>>(&self, operation: impl FnOnce(&mut crate::kernel::allocation::AllocationLedger, &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken) -> Result<T, E>) -> Result<T, E> {
        let mut state = self.state();
        if let Err(error) = state.reconcile_native_controls() {
            drop(state);
            return Err(E::from(NativeTreeMaterializationError::Work(error)));
        }
        let (allocations, work, budget, cancellation) = state.allocation_parts();
        operation(allocations, work, budget, cancellation)
    }

    /// Pure existing-root membership, not permission or retained-byte adoption.
    pub(crate) fn owns_native_checkout_token(
        &self, token: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> bool {
        self.state().owns_native_checkout_token(token)
    }

    /// Obtains immutable registered producer membership without an SDK loan,
    /// allocation, namespace grant or resource permission.
    pub(crate) fn checkout_control_sink(&self, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state.checkout_control_witness(producer).ok_or(NativeTreeMaterializationError::InvalidLayout)
    }

    /// Registers the actual pre-funded constructor token on this original.
    /// Metadata backing allocation and copying run outside its leaf mutex.
    pub(crate) fn register_native_checkout_control(
        &self, producer: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        header_guard: &mut NativeAllocation,
    ) -> Result<crate::demand::NativeCheckoutControlWitness, NativeTreeMaterializationError> {
        self.state.register_native_checkout_control(self, producer, header_guard)
    }


    /// Constructs original-ledger private controls through the existing custody
    /// registry. Only bounded synchronous allocation/accounting may run here.
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
    pub(crate) fn with_owned_allocations<T, E: From<NativeTreeMaterializationError>>(&self, operation: impl FnOnce(&mut crate::kernel::allocation::AllocationLedger, &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken) -> Result<T, E>) -> Result<(T, NativeAllocation), E> {
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
                Ok((value, NativeAllocation { meter: self.clone(), bytes, capture: None }))
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
    /// This neither retains storage nor reports an allocation or completed work.
    pub fn admit_retained(&self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let result = state.admit_retained(bytes).and_then(|()| self.resource.as_ref().map_or(Ok(()), |resource| resource.admit(crate::WorkCounters::UNCHARGED, bytes)));
        drop(state);
        result.map_err(native_materialization_work_error)
    }

    pub(crate) fn reserve_cleanup_write(&self) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        let mut state = self.state();
        let mut reserve = || state.reserve_native_cleanup_write();
        match &self.resource {
            Some(resource) => resource.reserve_credit(NativeResourceCredit::WriteOperations, 1, reserve),
            None => reserve(),
        }.map_err(native_materialization_work_error)?;
        Ok(NativeCleanupWrite { meter: self.clone(), additional_resource: None, reserved: true })
    }

    /// Withholds a concurrent copy's maximum without reporting copied bytes.
    /// Record its actual completed prefix, or drop an unused reservation.
    pub(crate) fn reserve_copy(&self, maximum: u64) -> Result<NativeCopyReservation, NativeTreeMaterializationError> {
        let mut state = self.state();
        let mut reserve = || state.reserve_copy(maximum);
        let result = match &self.resource {
            Some(resource) => resource.reserve_credit(NativeResourceCredit::Copy, maximum, reserve),
            None => reserve(),
        };
        drop(state);
        result.map_err(native_materialization_work_error)?;
        Ok(NativeCopyReservation { meter: self.clone(), reserved: Some(maximum) })
    }

    /// Withholds future visits on the original ledger and every actual
    /// resource view. Reservation is not completed work.
    pub(crate) fn reserve_items(
        &self, count: u64,
    ) -> Result<NativeItemReservation, NativeTreeMaterializationError> {
        let mut state = self.state();
        let mut reserve = || state.reserve_pending_items(count);
        let reserved = match &self.resource {
            Some(resource) => resource.reserve_credit(NativeResourceCredit::Items, count, reserve),
            None => reserve(),
        };
        reserved.map_err(native_materialization_work_error)?;
        drop(state);
        Ok(NativeItemReservation { meter: self.clone(), remaining: count })
    }

    /// Admits an actual allocation before its allocator is called; retain the
    /// returned guard for the complete buffer lifetime.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or the exact exceeded work dimension.
    pub fn claim(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation(bytes, u64::from(bytes != 0))
    }

    /// Claims only the actual allocation boundary; the allocator runs after
    /// this leaf unlocks and the returned original guard is already owned.
    pub(crate) fn claim_allocation(&self, bytes: u64, operations: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation_in_origin(bytes, operations, None)
    }

    fn claim_allocation_in_origin(
        &self, bytes: u64, operations: u64,
        origin: Option<&Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>>,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let mut state = self.state();
        let capture = origin.cloned().unwrap_or(None);
        let delta = crate::WorkCounters { allocation_operations: operations, ..crate::WorkCounters::UNCHARGED };
        // Intrinsic guarded payload is not the capture's unguarded scratch.
        let result = match &self.resource {
            Some(resource) => resource.admission(delta, bytes, || state.claim_native_owned(bytes, operations, &None)),
            None => state.claim_native_owned(bytes, operations, &None),
        };
        drop(state);
        result.map_err(native_materialization_work_error)?;
        Ok(NativeAllocation { meter: self.clone(), bytes, capture })
    }

    fn claim_root_metadata(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let root = Self { state: std::sync::Arc::clone(&self.state), cancellation: self.cancellation.clone(), resource: None, capture_origin: None, invocation: self.invocation.clone() };
        root.claim_allocation_in_origin(bytes, 1, Some(&None))
    }

    /// Funds only the actual capture token's compiled logical Self.
    pub(crate) fn claim_capture_custody_metadata(&self) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation_in_origin(std::mem::size_of::<crate::demand::CaptureTransientCustody>() as u64, 1, Some(&None))
    }

    /// Funds only root-owned registered checkout-token bookkeeping.
    pub(crate) fn claim_checkout_token_metadata(&self) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_root_metadata(std::mem::size_of::<crate::kernel::ProofDataCustody>() as u64)
    }

    pub(crate) fn claim_checkout_registry_header(&self) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_root_metadata(std::mem::size_of::<crate::demand::NativeObserverOverflow>() as u64)
    }

    pub(crate) fn claim_checkout_registry_backing(&self, elements: usize) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let bytes = elements.checked_mul(std::mem::size_of::<crate::demand::DemandObserverCustody>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        self.claim_root_metadata(bytes)
    }

    pub(crate) fn reserve_checkout_registry_copy(&self, entries: usize) -> Result<NativeCopyReservation, NativeTreeMaterializationError> {
        let bytes = entries.checked_mul(std::mem::size_of::<crate::demand::DemandObserverCustody>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
        let root = Self { state: std::sync::Arc::clone(&self.state), cancellation: self.cancellation.clone(), resource: None, capture_origin: None, invocation: self.invocation.clone() };
        root.reserve_copy(bytes)
    }

    /// Transfers this registered capture's exclusive already-paid scratch
    /// claim. No ledger admission, cancellation check, or re-adoption occurs.
    pub(crate) fn transfer_capture_allocation(
        &self, bytes: u64, custody: &std::sync::Arc<crate::demand::CaptureTransientCustody>,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        if !custody.owns_native_work(self) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        custody.transfer_retained_bytes(bytes)?;
        Ok(NativeAllocation { meter: self.clone(), bytes, capture: None })
    }

    /// Atomically funds the known allocation and the copy/items which run
    /// immediately after this numeric permit, outside the original mutex.
    pub(crate) fn claim_allocation_with_work(
        &self, bytes: u64, operations: u64, delta: crate::WorkCounters,
    ) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let combined = delta.checked_add(crate::WorkCounters {
            allocation_operations: operations, ..crate::WorkCounters::UNCHARGED
        })?;
        let mut state = self.state();
        let capture = None;
        let result = match &self.resource {
            Some(resource) => resource.admission(combined, bytes, || {
                state.with_resource_scope(None, |work|
                    work.claim_native_owned_with_work(bytes, operations, delta, &None))
            }),
            None => state.with_resource_scope(None, |work|
                work.claim_native_owned_with_work(bytes, operations, delta, &None)),
        };
        drop(state);
        result.map_err(native_materialization_work_error)?;
        Ok(NativeAllocation { meter: self.clone(), bytes, capture })
    }

    /// Attaches a real completed allocation excess before a late refusal.
    /// The returned guard owns the recorded bytes even when the result fails.
    pub(crate) fn record_completed_allocation(&self, bytes: u64) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        self.record_completed_allocation_in_origin(bytes, None)
    }

    fn record_completed_allocation_in_origin(
        &self, bytes: u64,
        origin: Option<&Option<std::sync::Arc<crate::demand::CaptureTransientCustody>>>,
    ) -> (NativeAllocation, Result<(), NativeTreeMaterializationError>) {
        let mut state = self.state();
        let capture = origin.cloned().unwrap_or(None);
        let (owned, result) = state.record_native_owned_completed(bytes, &None);
        let resource = self.resource.as_ref().map_or(Ok(()), |resource| resource.completed(crate::WorkCounters::UNCHARGED, owned));
        drop(state);
        (NativeAllocation { meter: self.clone(), bytes: owned, capture }, result.and(resource).map_err(native_materialization_work_error))
    }

    /// Takes custody of an already measured owned buffer without charging a
    /// second allocation operation.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient live-byte allowance.
    pub fn retain_owned(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.claim_allocation(bytes, 0)
    }

    /// Returns the actual owned journal control-buffer capacity without root
    /// access or cancellation. Record newly produced output work before a
    /// cancellable custody transfer.
    ///
    /// # Errors
    /// Returns checked capacity arithmetic overflow.
    pub fn journal_control_bytes(journal: &MaterializationJournal) -> Result<u64, NativeTreeMaterializationError> {
        journal_owned_bytes(journal)
    }


    /// Formats actionable text into an actually admitted owned UTF-8 buffer.
    ///
    /// # Errors
    /// Returns the original formatting, cancellation, allocation, or finite
    /// encoded/copied-byte boundary failure and retains its actual work.
    pub fn display<T: std::fmt::Display + ?Sized>(&self, value: &T) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
        use std::io::Write as _;
        let mut writer = NativeImageWriter::new(self)?;
        if let Err(error) = write!(&mut writer, "{value}") {
            return Err(writer.failure.take().unwrap_or_else(|| error.into()));
        }
        let text = String::from_utf8(writer.image).map_err(|_| NativeTreeMaterializationError::InvalidPreimage)?;
        Ok((text, writer.allocation))
    }

    /// Actual currently admitted live bytes, rather than historical peak.
    pub fn live_allocation_bytes(&self) -> u64 { self.state().live_allocation_bytes() }

    /// Reads the actual process current-directory spelling through this
    /// original operation's bounded native metadata producer.
    ///
    /// # Errors
    /// Returns the original OS, allocation, quota, or cooperative cancellation
    /// failure. The OS call itself is not forcibly interrupted.
    pub fn current_directory(&self) -> Result<(std::path::PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        #[cfg(unix)]
        {
            // This is the accepted synchronous source operation, not returned
            // buffer custody. Its strong capture anchor ends at handoff.
            let capture = self.capture_origin.as_ref().and_then(std::sync::Weak::upgrade);
            if capture.as_ref().is_some_and(|capture| !capture.owns_native_work(self)) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            let mut source = crate::demand::NativeSourceWork::from_native_materialization_work(self);
            let path = crate::native_host::cwd_path_measured(&mut source)
                .map_err(native_materialization_work_error)?;
            // The source engine already admitted this exact physical buffer on
            // our original ledger. Transfer custody without a second claim.
            let allocation = NativeAllocation { meter: self.clone(), bytes: path.capacity() as u64, capture: None };
            if let Some(capture) = &capture {
                if let Err(error) = capture.transfer_retained_bytes(path.capacity() as u64) {
                    drop(path);
                    drop(allocation);
                    self.state().poison_native_original_work();
                    return Err(error.into());
                }
            }
            if let Err(error) = self.admit(crate::WorkCounters::UNCHARGED) {
                drop(path);
                drop(allocation);
                return Err(error);
            }
            Ok((path, allocation))
        }
        #[cfg(windows)]
        {
            crate::native_host::current_directory_with_work(&mut &*self)
        }
    }

    /// Observes the actual opened root allocation without copying or
    /// canonicalizing its lexical path.
    ///
    /// # Errors
    /// Returns the native open failure or all spent finite-work failures.
    pub fn root_identity(&self, path: &std::path::Path) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open_materialization_measured(path, self)?.identity())
    }

    /// Reads one current-process environment value within its logical native
    /// encoded-byte cap, on this operation's original allocation ledger.
    /// Returned storage must be dropped before its allocation guard. A private
    /// variable name supplies no filesystem-root or SDK-source authority.
    ///
    /// Unix unsafe process-environment mutation must not race this synchronous
    /// read, as required by Rust's `std::env::set_var`/`remove_var` contract.
    /// Windows uses the OS's synchronized live environment and bounded retries.
    /// Native calls are cooperatively checked, not hard-deadline guarantees.
    ///
    /// # Errors
    /// Returns the original quota, cancellation, native I/O or allocator cause.
    /// A NUL-containing name is invalid input; an oversized value is rejected
    /// rather than truncated. Control metadata is not application BODY bytes.
    pub fn environment_variable(
        &self, name: &std::ffi::OsStr, maximum_bytes: usize,
    ) -> Result<Option<(std::ffi::OsString, NativeAllocation)>, NativeTreeMaterializationError> {
        crate::native_host::environment_variable_with_work(name, maximum_bytes, self)
    }

    /// Copies actual lexical bytes into one exactly admitted owned path.
    ///
    /// # Errors
    /// Returns the exact allocation/copy/cancellation failure.
    pub fn copy_path(&self, path: &std::path::Path) -> Result<(std::path::PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        self.owned_path(path, None)
    }

    /// Allocates a joined lexical path. This supplies buffer custody only;
    /// callers must use held-root identity checks for confinement.
    ///
    /// # Errors
    /// Returns the exact allocation/copy/cancellation failure.
    pub fn join_path(&self, root: &std::path::Path, leaf: &std::path::Path) -> Result<(std::path::PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        self.owned_path(root, Some(leaf))
    }

    fn owned_path(&self, root: &std::path::Path, leaf: Option<&std::path::Path>) -> Result<(std::path::PathBuf, NativeAllocation), NativeTreeMaterializationError> {
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
    /// The returned held file, not its lexical path, is the launch capability.
    ///
    /// # Errors
    /// Rejects absolute/parent paths, symlinks and replacement root allocations.
    pub fn open_working_directory(&self, root: &std::path::Path, expected: crate::NativeRootIdentity, relative: &std::path::Path) -> Result<std::fs::File, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            if !matches!(component, std::path::Component::Normal(_) | std::path::Component::CurDir) {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        let held = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
        if held.identity() != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }); }
        let directory = held.open_dir_materialization_measured(relative, self)?;
        let actual = self.root_identity(root)?;
        if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
        Ok(directory.into_std_file())
    }

    /// Maximum whole-file builtin read or UTF-8 write payload.
    pub const MAXIMUM_BUILTIN_BYTES: usize = 262_144;

    /// Reads an entire regular file through the original witnessed root and
    /// held no-follow parent/leaf. No path is handed back for an ambient reopen.
    /// `guard` retains the caller's original owner/grant checks at IO boundaries.
    ///
    /// # Errors
    /// Preserves the exact caller or native cause, including cap, cancellation,
    /// allocation, root/parent/leaf replacement and observed content changes.
    pub async fn read_working_file_guarded<E, F, Fut>(
        &self, root: &Path, expected: crate::NativeRootIdentity, relative: &Path,
        maximum_bytes: usize, mut guard: F,
    ) -> Result<(Vec<u8>, NativeAllocation), E>
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

    /// Writes bounded UTF-8 through one held regular file. An edit hashes the
    /// exact current preimage on that same descriptor before its first write.
    /// This is not an atomic transaction against unrelated external OS writers.
    ///
    /// # Errors
    /// Returns typed preimage conflicts without mutation; partial IO failures
    /// retain actual work and are not reported as successful publication.
    #[allow(clippy::too_many_arguments, reason = "original root, edit preimage and caller guard must remain explicit")]
    pub async fn write_working_file_guarded<E, F, Fut>(
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
            self.charge(crate::WorkCounters { bytes_hashed: preimage.0.len() as u64,
                ..crate::WorkCounters::UNCHARGED }).map_err(E::from)?;
            let actual: [u8; 32] = Sha256::digest(&preimage.0).into();
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

    /// Reads through an already forked original mounted accounting activation.
    /// The caller forks with `try_clone_for_io` before constructing this future;
    /// the fork owns no new grant or meter.
    ///
    /// Native file operations in this leaf are synchronous: every physical call
    /// returns before the next await. Retaining the owned fork through the
    /// child future therefore also retains it through physical completion.
    ///
    /// # Errors
    /// Preserves the same exact caller/native failures as the ordinary held
    /// read. Dropping a suspended child releases its buffers before this scope.
    #[cfg(feature = "native-mount")]
    pub async fn read_working_file_guarded_with_scope<E, F, Fut>(
        &self, scope: crate::native_mount::OwnedNativeMountOriginalWorkScope,
        root: &Path, expected: crate::NativeRootIdentity, relative: &Path,
        maximum_bytes: usize, guard: F,
    ) -> Result<(Vec<u8>, NativeAllocation), E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let operation = self.read_working_file_guarded(root, expected, relative, maximum_bytes, guard);
        let result = operation.await;
        drop(scope);
        result
    }

    /// Writes through the owned original mounted accounting activation, using
    /// the same held-leaf engine and exact preimage comparison as scratch IO.
    /// The activation remains owned until all synchronous physical calls and
    /// the child future finish; it is not a task or volume permission.
    ///
    /// # Errors
    /// Preserves the original guard/native failure and its actual work prefix.
    #[cfg(feature = "native-mount")]
    #[allow(clippy::too_many_arguments, reason = "accepted scope, original root and preimage remain explicit")]
    pub async fn write_working_file_guarded_with_scope<E, F, Fut>(
        &self, scope: crate::native_mount::OwnedNativeMountOriginalWorkScope,
        root: &Path, expected: crate::NativeRootIdentity, relative: &Path,
        content: &str, create_parents: bool, expected_sha256: Option<[u8; 32]>,
        guard: F,
    ) -> Result<(), E>
    where
        E: From<NativeTreeMaterializationError>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let operation = self.write_working_file_guarded(root, expected, relative, content,
            create_parents, expected_sha256, guard);
        let result = operation.await;
        drop(scope);
        result
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
        let held = crate::native_host::HostRoot::open_materialization_measured(root, self).map_err(E::from)?;
        if held.identity() != expected {
            return Err(E::from(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }));
        }
        let mut parent = held.open_dir_materialization_measured(Path::new(""), self).map_err(E::from)?;
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
        let parent_identity = crate::native_host::directory_identity_materialization_measured(&parent, self).map_err(E::from)?;
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
    ) -> Result<(Vec<u8>, NativeAllocation), E>
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
        let initialized = crate::WorkCounters { bytes_copied: capacity as u64, ..crate::WorkCounters::UNCHARGED };
        self.admit(initialized).map_err(E::from)?;
        let mut custody = self.claim(capacity as u64).map_err(E::from)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(capacity).map_err(NativeTreeMaterializationError::AllocationFailed).map_err(E::from)?;
        custody.resize_retained(bytes.capacity() as u64).map_err(E::from)?;
        bytes.resize(capacity, 0);
        self.spent(initialized).map_err(E::from)?;
        let mut filled = 0;
        while filled < bytes.len() {
            guard().await?;
            held.validate(self).map_err(E::from)?;
            let attempted = crate::native_host::read_builtin_file(&held.file, &mut bytes[filled..], self);
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
        bytes.truncate(filled);
        guard().await?;
        Ok((bytes, custody))
    }

    /// Creates or opens a service-owned relative directory through the actual
    /// expected held root. This helper grants no ownership or effect authority.
    ///
    /// # Errors
    /// Rejects non-normal relative paths, replacement roots, finite-work
    /// failures, and an existing leaf when `require_new` is true.
    pub fn create_owned_directory(&self, root: &std::path::Path, expected: crate::NativeRootIdentity, relative: &std::path::Path, require_new: bool) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
            if !matches!(component, std::path::Component::Normal(_)) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        }
        let held = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
        if held.identity() != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: held.identity() }); }
        if relative.as_os_str().is_empty() {
            if require_new { return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into()); }
            return Ok(held.identity());
        }
        let base = held.open_dir_materialization_measured(Path::new(""), self)?;
        let parent = crate::native_host::create_directories_held_with_work(&base, relative.parent().unwrap_or_else(|| Path::new("")), &mut &*self)?;
        let leaf = Path::new(relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
        let directory = if require_new {
            crate::native_host::create_new_directory_held_with_work(&parent, leaf, &mut &*self)?
        } else {
            crate::native_host::create_directories_held_with_work(&parent, leaf, &mut &*self)?
        };
        crate::native_host::directory_identity_materialization_measured(&directory, self)
    }

    /// Releases an exclusively owned private directory, not a project checkout
    /// or user destination. The caller must retain original creator proof,
    /// drained effects, and current task write authority in `before_effect`.
    /// Identities are observations and do not mint that authority.
    ///
    /// # Errors
    /// Preserves all actual work on guard denial, cancellation, partial deletion,
    /// allocation replacement, reparse entries, or mount traversal.
    pub async fn release_owned_directory_guarded<F, Fut, E>(
        &self,
        root: &Path,
        expected_root: crate::NativeRootIdentity,
        relative: &Path,
        expected_directory: crate::NativeRootIdentity,
        mut before_effect: F,
    ) -> Result<(), E>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: From<NativeTreeMaterializationError>,
    {
        if relative.as_os_str().is_empty() { return Err(NativeTreeMaterializationError::InvalidLayout.into()); }
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if !matches!(component, std::path::Component::Normal(_)) { return Err(NativeTreeMaterializationError::InvalidLayout.into()); }
        }
        let held = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
        if held.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: held.identity() }.into()); }
        let parent_path = relative.parent().unwrap_or_else(|| Path::new(""));
        let parent = native_owned_relative_parent(&held, parent_path, self)?;
        let parent_identity = crate::native_host::directory_identity_materialization_measured(&parent, self)?;
        let leaf = Path::new(relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
        #[cfg(unix)]
        let leaf_name = leaf.as_os_str();
        #[cfg(windows)]
        let (leaf_storage, _leaf_custody) = with_native_walk_leaf(leaf, self, |name| {
            let bytes = u64::try_from(name.len()).ok().and_then(|length| length.checked_mul(2)).ok_or(crate::WorkError::Overflow)?;
            let custody = self.claim(bytes)?;
            self.charge(crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::UNCHARGED })?;
            let mut owned = Vec::new();
            owned.try_reserve_exact(name.len()).map_err(NativeTreeMaterializationError::AllocationFailed)?;
            owned.extend_from_slice(name);
            Ok((owned, custody))
        })?;
        #[cfg(windows)]
        let leaf_name = leaf_storage.as_slice();
        let metadata = stat_native_walk_name(&parent, leaf_name, self)?;
        let actual = native_stat_identity(&metadata);
        if actual != expected_directory { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_directory, actual }.into()); }
        if !metadata.is_dir() { return Err(NativeTreeMaterializationError::Io(std::io::Error::from(std::io::ErrorKind::NotADirectory)).into()); }
        let frame = native_owned_remove_frame(&parent, leaf_name, &metadata, self)?;
        let mut walker = NativeRemoveWalker::new(frame, self)?;
        while let Some(step) = walker.next(self, true)? {
            before_effect().await?;
            let current = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
            if current.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: current.identity() }.into()); }
            let current_parent = native_owned_relative_parent(&current, parent_path, self)?;
            let actual = crate::native_host::directory_identity_materialization_measured(&current_parent, self)?;
            if actual != parent_identity { return Err(NativeTreeMaterializationError::AllocationChanged { expected: parent_identity, actual }.into()); }
            let actual = native_stat_identity(&stat_native_walk_name(&current_parent, leaf_name, self)?);
            if actual != expected_directory { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_directory, actual }.into()); }
            walker.verify(&parent, leaf_name, &step, self)?;
            walker.execute(&parent, leaf_name, step, self)?;
        }
        Ok(())
    }

    /// Observes a working-directory binding. This does not authorize a later
    /// ambient-path launch; use `open_working_directory` for its actual handle.
    ///
    /// # Errors
    /// Returns the same bounded binding errors as `open_working_directory`.
    pub fn validate_working_directory(&self, root: &std::path::Path, expected: crate::NativeRootIdentity, relative: &std::path::Path) -> Result<(), NativeTreeMaterializationError> {
        self.open_working_directory(root, expected, relative).map(drop)
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
    let root = crate::native_host::HostRoot::open_materialization_measured(path, work)?;
    if root.identity() != expected {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual: root.identity() });
    }
    let parent = root.open_dir_materialization_measured(relative.parent().unwrap_or_else(|| Path::new("")), work)?;
    let actual = crate::native_host::directory_identity_materialization_measured(&parent, work)?;
    if actual != expected_parent {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual });
    }
    Ok(parent)
}

/// Additive quota custody for an already separately authorized SDK operation.
/// This pins the same original/resource limits, reports no hypothetical work,
/// and refunds only its unused remainder when the accepted operation ends.
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
    /// This pure membership query is not mutation or append authorization.
    #[must_use]
    pub fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.shares_allocation_context(work)
    }

    /// Returns this token's still-owned additive credit, not fresh allowance.
    #[must_use]
    pub const fn remaining_work(&self) -> crate::WorkCounters { self.remaining }

    /// Records a real completed prefix, consuming only its owned credit.
    /// Actual counters remain recordable after late root/local cancellation;
    /// this token grants accounting custody, never permission for a new action.
    ///
    /// # Errors
    /// Rejects a prefix beyond this token, allocation dimensions, poisoned
    /// original state, or accounting overflow without restoring completed credit.
    pub fn record_completed(&mut self, actual: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        if actual.allocation_operations != 0 || actual.peak_allocation_bytes != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let remaining = actual.remaining(self.remaining)?;
        let mut state = self.meter.state();
        self.remaining = remaining;
        let original = state.record_native_work(actual);
        let resource = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.complete_work(actual));
        drop(state);
        original.and(resource).map_err(native_materialization_work_error)
    }

    /// Records an actual read-only SDK receive before rejecting an overlarge
    /// provider prefix. Consumes at most owned credit and creates no allowance
    /// or permission; unknown provider-internal physical I/O is not counted.
    ///
    /// # Errors
    /// Rejects non-read dimensions before mutation. Actual received read work is
    /// retained before an over-credit, overflow or poisoned-state failure.
    pub fn record_received_prefix(
        &mut self, actual: crate::WorkCounters,
    ) -> Result<(), NativeTreeMaterializationError> {
        let allowed = crate::WorkCounters {
            authority_records_read: actual.authority_records_read,
            authority_bytes_read: actual.authority_bytes_read,
            backend_read_operations: actual.backend_read_operations,
            items_examined: actual.items_examined,
            items_returned: actual.items_returned,
            ..crate::WorkCounters::UNCHARGED
        };
        if actual != allowed { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let consumed = crate::WorkCounters {
            authority_records_read: actual.authority_records_read.min(self.remaining.authority_records_read),
            authority_bytes_read: actual.authority_bytes_read.min(self.remaining.authority_bytes_read),
            backend_read_operations: actual.backend_read_operations.min(self.remaining.backend_read_operations),
            items_examined: actual.items_examined.min(self.remaining.items_examined),
            items_returned: actual.items_returned.min(self.remaining.items_returned),
            ..crate::WorkCounters::UNCHARGED
        };
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

/// Custody of actual future item visits; no ledger is moved or reconstructed.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeItemReservation {
    meter: NativeMaterializationWork,
    remaining: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeItemReservation {
    /// Records visits already physically performed under this reservation,
    /// including mandatory cleanup after the original token was cancelled.
    pub(crate) fn consume(&mut self, count: u64) -> Result<(), NativeTreeMaterializationError> {
        self.consume_original(count).map_err(native_materialization_work_error)
    }

    pub(crate) fn consume_original(&mut self, count: u64) -> Result<(), crate::DemandError> {
        let remaining = self.remaining.checked_sub(count)
            .ok_or(crate::WorkError::Overflow)?;
        let mut state = self.meter.state();
        let recorded = state.record_pending_items(count);
        let scoped = self.meter.resource.as_ref().map_or(Ok(()), |resource|
            resource.complete_credit(NativeResourceCredit::Items, count, count));
        self.remaining = remaining;
        recorded.and(scoped)
    }

    /// Drop cannot abandon physical cleanup or panic over a prior quota spill.
    /// Preserve the first typed cause in the existing original failure slot and
    /// stop further admission while already-prepared cleanup keeps running.
    pub(crate) fn consume_deferred(&mut self, count: u64) {
        if let Err(error) = self.consume_original(count) {
            let mut state = self.meter.state();
            state.record_native_io_failure(error);
            state.poison_native_original_work();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeItemReservation {
    fn drop(&mut self) {
        let count = std::mem::take(&mut self.remaining);
        if count == 0 { return; }
        let mut state = self.meter.state();
        state.refund_pending_items(count);
        if let Some(resource) = &self.meter.resource {
            if let Err(error) = resource.refund_credit(NativeResourceCredit::Items, count) {
                state.record_native_io_failure(error);
                state.poison_native_original_work();
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCopyReservation {
    meter: NativeMaterializationWork,
    reserved: Option<u64>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCopyReservation {
    pub(crate) fn record(mut self, actual: u64) -> Result<(), NativeTreeMaterializationError> {
        let maximum = self.reserved.take().expect("owned copy reservation");
        let mut state = self.meter.state();
        let recorded = state.record_reserved_copy(maximum, actual);
        let scoped = self.meter.resource.as_ref().map_or(Ok(()), |resource|
            resource.complete_credit(NativeResourceCredit::Copy, maximum, actual));
        drop(state);
        recorded.and(scoped).map_err(native_materialization_work_error)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeCopyReservation {
    fn drop(&mut self) {
        if let Some(maximum) = self.reserved.take() {
            let mut state = self.meter.state();
            state.cancel_reserved_copy(maximum);
            if let Some(resource) = &self.meter.resource {
                if let Err(error) = resource.refund_credit(NativeResourceCredit::Copy, maximum) {
                    state.record_native_io_failure(error);
                    state.poison_native_original_work();
                }
            }
        }
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
    /// Attributes this already accepted cleanup to the current volume without
    /// refunding its birth constraints or reserving the same root twice.
    /// Original membership is accounting identity, not filesystem authority.
    pub(crate) fn bind_current_work(&mut self, current: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        if !self.reserved || !self.meter.shares_ledger(current) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let Some(resource) = &current.resource else { return Ok(()); };
        if self.additional_resource.as_ref().is_some_and(|bound| std::sync::Arc::ptr_eq(bound, resource)) {
            return Ok(());
        }
        if self.additional_resource.is_some() { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let mut state = self.meter.state();
        state.check().map_err(native_materialization_work_error)?;
        resource.reserve_credit_excluding(NativeResourceCredit::WriteOperations, 1, self.meter.resource.as_deref())
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
            let original = state.record_native_cleanup_write(1);
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
            state.cancel_native_cleanup_write(1);
            let birth = self.meter.resource.as_ref().map_or(Ok(()), |resource| resource.refund_credit(NativeResourceCredit::WriteOperations, 1));
            let current = self.additional_resource.as_ref().map_or(Ok(()), |resource| {
                resource.refund_credit_excluding(NativeResourceCredit::WriteOperations, 1, self.meter.resource.as_deref())
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
    /// Transfers an existing runtime claim without release, readmission or
    /// rebilling. The runtime's allocation owner must be this exact original
    /// root/resource projection; an anchored source job is not adopted by label.
    ///
    /// # Errors
    /// A mismatch preserves the incoming guard intact. Its payload must be
    /// destroyed before either returned guard is retired.
    pub fn from_runtime_allocation(
        original: &NativeMaterializationWork,
        incoming: acyclic_native_runtime::ReadAllocation,
    ) -> Result<Self, (NativeTreeMaterializationError, acyclic_native_runtime::ReadAllocation)> {
        let owner = original.runtime_meter_owner();
        let expected = owner.allocation_owner().unwrap_or(owner);
        match incoming.consume_same_meter_owned_bytes(&expected) {
            Ok(bytes) => Ok(Self {
                meter: original.clone(),
                // These exact SDK runtime owner implementations allocate and
                // release with &None. Source-job allocation_owner projects here
                // before ReadAllocation construction, dropping no payload claim.
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

    /// Moves this actual root bookkeeping claim into its original scalar
    /// owner. The registry is never allowed to own a strong-root guard.
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
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient remaining allowance.
    pub fn grow(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        let retained = self.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        let mut added = self.meter.claim_allocation_in_origin(bytes, u64::from(bytes != 0), Some(&self.capture))?;
        self.absorb(&mut added)?;
        debug_assert_eq!(self.bytes, retained);
        Ok(())
    }

    /// Returns this guard's currently owned byte capacity.
    pub fn bytes(&self) -> u64 { self.bytes }

    /// Borrows this guard's original operation ledger without creating a meter.
    pub const fn work(&self) -> &NativeMaterializationWork { &self.meter }

    /// Moves only this exclusive paid ticket into a physical proof primitive.
    /// Its existing original retirement cursor owns the global release.
    pub(crate) fn transfer_resource_retirement(&mut self, bytes: u64) -> Result<NativeResourceRetirement, NativeTreeMaterializationError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        let retirement = NativeResourceRetirement {
            resource: self.meter.resource.clone(), bytes,
            capture: self.capture.clone(),
            original: std::sync::Arc::clone(&self.meter.state),
        };
        self.bytes = remaining;
        Ok(retirement)
    }

    /// Transfers this concrete already-paid buffer into a registered original
    /// constructor's per-primitive tag. Failure leaves its custody unchanged.
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


    /// Transfers concrete already-paid root data into its registered original
    /// per-primitive tag without another admission or allocation.
    pub(crate) fn transfer_checkout_data(
        &mut self, owner: &std::sync::Arc<crate::kernel::ProofDataCustody>,
        scope: &crate::demand::NativeCheckoutControlWitness,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        self.transfer_checkout_data_bytes(self.bytes, owner, scope)
    }

    /// Transfers only the selected concrete buffer from this exclusive paid
    /// guard. Rejection leaves both its custody and the proof unchanged.
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

    pub(crate) fn transfer_checkout_data_bytes_original(
        &mut self, bytes: u64, owner: &std::sync::Arc<crate::kernel::ProofDataCustody>,
    ) -> Result<crate::kernel::ProofDataClaim, NativeTreeMaterializationError> {
        let membership = self.meter.checkout_control_sink(owner)?;
        self.transfer_checkout_data_bytes(bytes, owner, &membership)
    }

    /// Tests whether both payload guards retain the same original ledger.
    ///
    /// This pure custody identity test grants no resource or mutation authority.
    pub fn shares_ledger(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.meter.state, &other.meter.state)
    }

    /// Tests membership of this actual claim in the original and resource
    /// view. This acquires no lock, allocates nothing, mutates no counters and
    /// grants no filesystem authority or additional allowance.
    #[must_use]
    pub fn matches_work(&self, work: &NativeMaterializationWork) -> bool {
        self.meter.shares_ledger(work) && self.meter.shares_resource_allowance(work)
    }

    /// Transfers existing custody without another allocation.
    ///
    /// # Errors
    /// Rejects distinct meters or byte-count overflow without consuming either
    /// guard or changing its custody.
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
    /// No admission, allocation, cancellation check, or ledger charge occurs.
    ///
    /// # Errors
    /// Rejects more bytes than this guard owns without changing either ledger.
    pub fn split_off(&mut self, bytes: u64) -> Result<Self, NativeTreeMaterializationError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        let split = Self { meter: self.meter.clone(), bytes, capture: self.capture.clone() };
        self.bytes = remaining;
        Ok(split)
    }

    /// Releases capacity only after its buffer has been freed.
    ///
    /// # Errors
    /// Rejects releasing bytes not owned by this guard.
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

    /// Physical cleanup cannot return an error. Keep the actual typed failure
    /// in the existing original slot and deny subsequent work on that engine.
    pub(crate) fn release_deferred(&mut self, bytes: u64) {
        if let Err(error) = self.release_original(bytes) {
            let mut state = self.meter.state();
            state.record_native_io_failure(error);
            state.poison_native_original_work();
        }
    }

    /// Substitutes actual retained bytes for an already admitted opaque quota
    /// without cancellation, another claim, or an unguarded live-byte gap.
    ///
    /// # Errors
    /// Rejects an actual payload larger than its original quota while leaving
    /// the original custody unchanged.
    pub fn substitute(&mut self, retained: u64) -> Result<(), NativeTreeMaterializationError> {
        let released = self.bytes.checked_sub(retained).ok_or(crate::WorkError::Overflow)?;
        self.release(released)
    }

    /// Reconciles this buffer's completed physical capacity, recording excess
    /// and extending its custody before a late budget/cancellation refusal.
    /// Its caller must keep this guard through the buffer's physical drop,
    /// including on error. Shrinking requires the former backing to be gone.
    ///
    /// # Errors
    /// Preserves actual claimed capacity on late original-work rejection.
    pub fn reconcile_capacity(&mut self, actual: u64) -> Result<(), NativeTreeMaterializationError> {
        if actual < self.bytes { self.release(self.bytes - actual)?; }
        let (mut completed, result) = self.meter.record_completed_allocation_in_origin(actual - self.bytes, Some(&self.capture));
        self.absorb(&mut completed)?;
        result
    }

    /// Retains a logical quota or transfers already allocated payload custody.
    ///
    /// This admits live bytes on the same ledger with zero allocation operations;
    /// callers must separately account actual allocation attempts and retire the
    /// old payload before shrinking. It is not an allocator-capacity observation.
    ///
    /// # Errors
    /// Rejects overflow or insufficient remaining logical storage allowance.
    pub fn resize_retained(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        if bytes < self.bytes { return self.release(self.bytes - bytes); }
        let mut state = self.meter.state();
        let added = bytes - self.bytes;
        let result = match &self.meter.resource {
            Some(resource) => resource.admission(crate::WorkCounters::UNCHARGED, added, || state.retain_native_owned(added, &None)),
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
    /// Ordered path edits. Destructive edits do not overlap; metadata edits may
    /// follow edits to descendants so directory identity is preserved.
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
        /// Canonical complete original request, including authority/fence
        /// provenance. The caller authenticates these bytes; they confer no
        /// authority merely by appearing in a recovery journal.
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreimage {
    /// Backend-defined bytes sufficient to restore every representable fact.
    pub image: Vec<u8>,
}

/// One captured preimage and its original allocation ownership.
#[derive(Debug)]
pub struct OwnedMaterializationPreimage {
    /// Actual captured representation.
    pub preimage: MaterializationPreimage,
    /// Native producer ownership; absent only for ordinary adapters.
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: Option<NativeAllocation>,
}

impl OwnedMaterializationPreimage {
    /// Transfers an ordinary adapter's existing administrative preimage.
    pub fn ordinary(preimage: MaterializationPreimage) -> Self {
        Self { preimage, #[cfg(not(target_arch = "wasm32"))] custody: None }
    }
}

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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationJournal {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable plan.
    pub plan: MaterializationPlan,
    /// Actual backend allocation captured when the original intent was created.
    /// `None` is explicitly unwitnessed and does not confer native authority.
    pub root_identity: Option<[u8; 16]>,
    /// Actual operation-owned staging and cleanup allocation identities.
    pub preparation_identity: Option<MaterializationPreparationIdentity>,
    /// Same-operation native allocation progress before the ready triple exists.
    /// A pending step with no actual identity is indeterminate, never adoptable.
    pub preparation_progress: Option<MaterializationPreparationProgress>,
    /// Known lower bound before the last durable journal write, never an exact
    /// cold-recovery total. An interrupted mutation or lost CAS acknowledgement
    /// may have spent additional work; the original live invocation reports its
    /// full observed prefix. Preparing remains explicitly incomplete.
    pub work: crate::WorkCounters,
    /// Preimages in exact edit order.
    pub preimages: Vec<MaterializationPreimage>,
    /// Current recovery phase.
    pub phase: MaterializationPhase,
    /// Number of forward edits known durable.
    pub applied: u32,
    /// Number of applied edits restored from the end.
    pub restored: u32,
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
///
/// The durable preparing record cannot attest work between a physical effect
/// and its acknowledgement. Its counters remain a known lower bound; this
/// live failure retains the observed original prefix and the original cause.
/// The cause slot is funded before effects, and its allocation custody lasts
/// through physical deallocation.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct NativeStageUncertainty<E> {
    primary: Box<Option<E>>,
    /// Last acknowledged original journal work, only a known lower bound.
    pub last_durable_work: crate::WorkCounters,
    /// Actual original work observed by the failing invocation.
    pub observed_work: crate::WorkCounters,
    // Outside and after the box through its physical deallocation.
    custody: NativeAllocation,
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
struct NativeStageFailureSlot<E> {
    primary: Box<Option<E>>,
    custody: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> NativeStageFailureSlot<E> {
    fn new(work: &NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        let custody = work.claim(std::mem::size_of::<Option<E>>() as u64)?;
        let primary = Box::new(None);
        Ok(Self { primary, custody })
    }

    fn finish(mut self, cause: E, last_durable_work: crate::WorkCounters, observed_work: crate::WorkCounters) -> NativeStageUncertainty<E> {
        *self.primary = Some(cause);
        NativeStageUncertainty { primary: self.primary, last_durable_work, observed_work, custody: self.custody }
    }
}

#[cfg(not(target_arch = "wasm32"))]
enum NativeStageJournal<C> {
    Journal(OwnedMaterializationJournal),
    Accepted { completion: C, last_durable_work: crate::WorkCounters },
    Completing { last_durable_work: crate::WorkCounters },
}

/// One metadata completion accepted before its corresponding SDK mutation.
///
/// The provider owns the same journal, original meter and genuine completion
/// reservation. Finishing polls only that previously admitted closed window.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaterializationActionCompletion: Send {
    /// The original provider failure type.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Durably records the actual original work prefix and returns its journal.
    /// No caller-supplied counters, new authority or late allocation are admitted.
    fn complete(self) -> impl Future<Output = Result<OwnedMaterializationJournal, Self::Error>> + Send;
}

/// Durable optimistic-concurrency adapter for materialization journals.
pub trait MaterializationJournalStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Provider-owned completion accepted before one native SDK mutation.
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>: MaterializationActionCompletion<Error = Self::Error>
    where Self: 'a;

    /// Pre-admits the original journal's metadata completion while the current
    /// owner and work/resource fence still authorize this action.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_action<'a>(&'a self, journal: OwnedMaterializationJournal)
        -> impl Future<Output = Result<Self::ActionCompletion<'a>, Self::Error>> + Send;

    /// Loads one journal with its actual decoded or stored payload custody.
    /// Native adapters keep this original guard through every consumer phase.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<OwnedMaterializationJournal>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a journal.
    /// Returns the admitted replacement ownership alongside the CAS decision.
    /// The same packet remains owned during encoding and persistence and is
    /// returned on either CAS decision; only ordinary adapters omit custody.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: OwnedMaterializationJournal,
    ) -> impl Future<Output = Result<(bool, OwnedMaterializationJournal), Self::Error>> + Send;
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

    /// Produces an actionable diagnostic together with its original allocation
    /// custody. Unsupported adapters return the original cause before formatting.
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
    /// Unwitnessed adapters must provide equivalent external durability.
    fn settle_observed(&self, _edit: &MaterializationEdit) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }


    #[cfg(not(target_arch = "wasm32"))]
    fn retain_plan(&self, _plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    #[cfg(not(target_arch = "wasm32"))]
    fn admit_observations(&self, _count: usize) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    /// Transfers captured bytes and their actual ownership into this journal.
    fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error>;

    /// Revalidates the allocation currently named at its physical boundary.
    /// An unwitnessed backend requires equivalent exclusion externally.
    fn observe_allocation_identity(
        &self,
    ) -> impl Future<Output = Result<Option<[u8; 16]>, Self::Error>> + Send {
        async { Ok(self.allocation_identity()) }
    }

    /// Verifies that the checkout still represents the plan's exact source.
    /// Backends without an independently observable generation may retain the
    /// conservative default and provide equivalent fencing externally.
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
    ) -> impl Future<Output = Result<OwnedMaterializationPreimage, Self::Error>> + Send;

    /// Classifies the current path against both durable endpoints.
    ///
    /// Backends that cannot independently witness physical state may retain
    /// the default only when equivalent exclusion is provided externally.
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
    Diverged { original: [u8; 16], current: Option<[u8; 16]> },
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
    Diverged { original: MaterializationPreparationIdentity, current: Option<MaterializationPreparationIdentity> },
    /// A physical observation failed; original witnesses remain retained.
    Unavailable(String),
}

/// Original durable intent and actual, non-authorizing physical observations.
#[derive(Debug)]
pub struct MaterializationInspection {
    /// Original operation, preparation identities and exact recovery progress.
    pub journal: OwnedMaterializationJournal,
    /// Root status even when preimage capture is incomplete.
    pub root: MaterializationRootObservation,
    /// Staging and cleanup status even when capture is incomplete.
    pub preparation: MaterializationPreparationObservation,
    /// Actual observations of captured endpoints, empty if the root is divergent.
    pub observations: Vec<MaterializationObservation>,
    /// Intrinsic ownership of allocated observation and diagnostic buffers.
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: [Option<NativeAllocation>; 4],
}

impl PartialEq for MaterializationInspection {
    fn eq(&self, other: &Self) -> bool {
        self.journal == other.journal && self.root == other.root
            && self.preparation == other.preparation && self.observations == other.observations
    }
}
impl Eq for MaterializationInspection {}

/// Core materializer coordinating one journal store and one host backend.
pub struct JournaledMaterializer<S, B> {
    store: S,
    backend: B,
}

struct MaterializationPreparation {
    plan: MaterializationPlan,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<NativeAllocation>,
}

/// The same durable journal ownership packet in every recovery phase.
#[derive(Debug)]
pub struct OwnedMaterializationJournal {
    /// Original immutable intent, witnesses and durable progress.
    pub journal: MaterializationJournal,
    /// Actual original decoded or constructed buffer ownership.
    #[cfg(not(target_arch = "wasm32"))]
    pub custody: Option<NativeAllocation>,
}

impl OwnedMaterializationJournal {
    /// Transfers an ordinary administrative journal without claiming native work.
    pub fn ordinary(journal: MaterializationJournal) -> Self {
        Self { journal, #[cfg(not(target_arch = "wasm32"))] custody: None }
    }
}

impl std::ops::Deref for OwnedMaterializationJournal {
    type Target = MaterializationJournal;
    fn deref(&self) -> &Self::Target { &self.journal }
}

impl std::ops::DerefMut for OwnedMaterializationJournal {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.journal }
}

impl PartialEq for OwnedMaterializationJournal {
    fn eq(&self, other: &Self) -> bool { self.journal == other.journal }
}

impl Eq for OwnedMaterializationJournal {}

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
    /// Captures preimages, applies the plan, and records every durable boundary.
    pub async fn apply(
        &self,
        plan: MaterializationPlan,
    ) -> Result<OwnedMaterializationJournal, MaterializationError<S::Error, B::Error>> {
        let mut before_effect = |_verify_source| std::future::ready(Ok::<(), MaterializationError<S::Error, B::Error>>(()));
        self.apply_with_guard(plan, &mut before_effect).await
    }

    async fn apply_with_guard<E, F, Fut>(&self, plan: MaterializationPlan, before_effect: &mut F) -> Result<OwnedMaterializationJournal, E>
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
            None => self.prepare_with_custody(MaterializationPreparation {
                plan, #[cfg(not(target_arch = "wasm32"))] custody: input,
            }).await?,
        };
        self.apply_journal_with_guard(journal, before_effect).await
    }

    async fn apply_journal_with_guard<E, F, Fut>(&self, mut journal: OwnedMaterializationJournal, before_effect: &mut F) -> Result<OwnedMaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        if matches!(
            journal.phase,
            MaterializationPhase::RolledBack | MaterializationPhase::RollingBack
        ) {
            return Err(MaterializationError::PlanConflict.into());
        }
        self.verify_allocation(&journal).await?;
        if journal.phase == MaterializationPhase::Preparing {
            journal = self.capture_preimages_with_guard(journal, before_effect).await?;
        }
        if journal.phase == MaterializationPhase::Applied {
            self.repair_applied_with_guard(&journal, before_effect).await?;
            return Ok(journal);
        }
        journal.phase = MaterializationPhase::Applying;
        journal = self.persist(journal).await?;
        while usize::try_from(journal.applied).unwrap_or(usize::MAX) < journal.plan.edits.len() {
            self.verify_allocation(&journal).await?;
            let index = usize::try_from(journal.applied)
                .map_err(|_| MaterializationError::IncompatibleJournal)?;
            let edit = journal
                .plan
                .edits
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal
                .preimages
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                observation @ (MaterializationObservation::Unverified
                | MaterializationObservation::Preimage
                | MaterializationObservation::Interrupted) => {
                    let verify_original_source = journal.applied == 0 && !matches!(observation, MaterializationObservation::Interrupted);
                    before_effect(verify_original_source).await?;
                    if verify_original_source {
                        if let MaterializationIntent::GenerationTransition { from, .. } = &journal.plan.intent
                            && !self.backend.verify_source(*from).await.map_err(MaterializationError::Backend)? {
                            return Err(MaterializationError::PlanConflict.into());
                        }
                    }
                    self.verify_effect_context(&journal).await?;
                    self.backend
                        .apply(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Postimage => {
                    before_effect(false).await?;
                    self.verify_effect_context(&journal).await?;
                    self.backend.settle_observed(edit).await.map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation.into());
                }
            }
            journal.applied = journal.applied.saturating_add(1);
            journal = self.persist(journal).await?;
        }
        journal.phase = MaterializationPhase::Applied;
        self.persist(journal).await.map_err(Into::into)
    }

    /// Recovers an interrupted operation by completing it or restoring preimages.
    pub async fn recover(
        &self,
        operation_id: OperationId,
        recovery: MaterializationRecovery,
    ) -> Result<Option<OwnedMaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let mut before_effect = |_verify_source| std::future::ready(Ok::<(), MaterializationError<S::Error, B::Error>>(()));
        self.recover_with_guard(operation_id, recovery, &mut before_effect).await
    }

    async fn recover_with_guard<E, F, Fut>(&self, operation_id: OperationId, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<Option<OwnedMaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let Some(journal) = self.load(operation_id).await? else {
            return Ok(None);
        };
        self.recover_journal_with_guard(journal, recovery, before_effect).await.map(Some)
    }

    async fn recover_journal_with_guard<E, F, Fut>(&self, journal: OwnedMaterializationJournal, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<OwnedMaterializationJournal, E>
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
                self.apply_journal_with_guard(journal, before_effect).await
            }
            MaterializationRecovery::RollBack => self.rollback_with_guard(journal, before_effect).await,
        }
    }

    /// Retains original immutable intent and allocation witnesses before staging.
    pub async fn prepare(
        &self,
        plan: MaterializationPlan,
    ) -> Result<OwnedMaterializationJournal, MaterializationError<S::Error, B::Error>> {
        self.prepare_with_custody(MaterializationPreparation {
            plan,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }).await
    }

    async fn prepare_with_custody(&self, preparation: MaterializationPreparation) -> Result<OwnedMaterializationJournal, MaterializationError<S::Error, B::Error>> {
        let MaterializationPreparation { plan, #[cfg(not(target_arch = "wasm32"))] custody } = preparation;
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
        // Retain original intent before source verification or capture. An
        // interrupted capture must reopen this operation, not allocate another.
        let journal = MaterializationJournal {
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
        let journal = OwnedMaterializationJournal {
            journal, #[cfg(not(target_arch = "wasm32"))] custody: input,
        };
        let (stored, journal) = self
            .store
            .compare_and_swap(journal.plan.operation_id, 0, journal)
            .await
            .map_err(MaterializationError::Store)?;
        if stored {
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

    async fn capture_preimages_with_guard<E, F, Fut>(
        &self,
        mut journal: OwnedMaterializationJournal,
        before_effect: &mut F,
    ) -> Result<OwnedMaterializationJournal, E>
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

    async fn rollback_with_guard<E, F, Fut>(&self, mut journal: OwnedMaterializationJournal, before_effect: &mut F) -> Result<OwnedMaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        if journal.preparation_progress.is_some() { return Err(MaterializationError::IncompatibleJournal.into()); }
        if journal.phase == MaterializationPhase::RolledBack {
            return Ok(journal);
        }
        if journal.phase == MaterializationPhase::Preparing {
            // No physical edit is permitted until every preimage is retained.
            journal.phase = MaterializationPhase::RolledBack;
            return self.persist(journal).await.map_err(Into::into);
        }
        self.verify_allocation(&journal).await?;
        // An interrupted forward pass may have applied the edit after the
        // last durable one before recording it. Include that edit; the
        // observation below skips it when it is still at its preimage.
        if journal.phase == MaterializationPhase::Applying
            && usize::try_from(journal.applied)
                .is_ok_and(|applied| applied < journal.plan.edits.len())
        {
            journal.applied = journal.applied.saturating_add(1);
        }
        journal.phase = MaterializationPhase::RollingBack;
        journal = self.persist(journal).await?;
        while journal.restored < journal.applied {
            self.verify_allocation(&journal).await?;
            let index = journal
                .applied
                .checked_sub(journal.restored.saturating_add(1))
                .and_then(|value| usize::try_from(value).ok())
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let edit = journal
                .plan
                .edits
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            let preimage = journal
                .preimages
                .get(index)
                .ok_or(MaterializationError::IncompatibleJournal)?;
            match self
                .backend
                .observe(edit, preimage)
                .await
                .map_err(MaterializationError::Backend)?
            {
                MaterializationObservation::Unverified
                | MaterializationObservation::Postimage
                | MaterializationObservation::Interrupted => {
                    before_effect(false).await?;
                    self.verify_effect_context(&journal).await?;
                    self.backend
                        .restore(edit, preimage)
                        .await
                        .map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Preimage => {
                    before_effect(false).await?;
                    self.verify_effect_context(&journal).await?;
                    self.backend.settle_observed(edit).await.map_err(MaterializationError::Backend)?;
                }
                MaterializationObservation::Diverged => {
                    return Err(MaterializationError::ExternalMutation.into());
                }
            }
            journal.restored = journal.restored.saturating_add(1);
            journal = self.persist(journal).await?;
        }
        journal.phase = MaterializationPhase::RolledBack;
        self.persist(journal).await.map_err(Into::into)
    }

    /// Reads authenticated original intent and durable progress without applying
    /// or recapturing physical state. Preparing is recoverable, not publication.
    pub async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OwnedMaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MaterializationError::Store)?;
        if let Some(journal) = &journal {
            if !valid_journal(journal, operation_id)
                || !self.backend.check_plan(&journal.plan).map_err(MaterializationError::Backend)? {
                return Err(MaterializationError::IncompatibleJournal);
            }
        }
        self.backend.retain_original_work(journal.as_ref().map_or_else(crate::WorkCounters::default, |journal| journal.work));
        Ok(journal)
    }

    /// Queries actual endpoints alongside original durable allocation/progress.
    /// A divergent observation is actionable external mutation, not authority to
    /// repair or publish. Uncaptured Preparing edits have no synthetic preimage.
    pub async fn inspect(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationInspection>, MaterializationError<S::Error, B::Error>> {
        self.inspect_with_custody(operation_id).await
    }

    async fn inspect_with_custody(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationInspection>, MaterializationError<S::Error, B::Error>> {
        // Declared before all payloads so failure drops physical buffers first.
        #[cfg(not(target_arch = "wasm32"))]
        let mut custody = [None, None, None, None];
        let Some(journal) = self.load(operation_id).await? else { return Ok(None); };
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
                        custody[1] = allocation;
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
                        custody[2] = allocation;
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
            #[cfg(not(target_arch = "wasm32"))]
            { custody[3] = self.backend.admit_observations(journal.preimages.len()).map_err(MaterializationError::Backend)?; }
            self.backend.reserve_observations(
                &mut observations,
                journal.preimages.len(),
                #[cfg(not(target_arch = "wasm32"))]
                &mut custody[3],
            ).map_err(MaterializationError::Backend)?;
            for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
                observations.push(self.backend.observe(edit, preimage).await.map_err(MaterializationError::Backend)?);
            }
        }
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
        if current.journal != *journal { return Err(MaterializationError::PlanConflict); }
        self.verify_allocation(journal).await
    }

    async fn repair_applied_with_guard<E, F, Fut>(&self, journal: &MaterializationJournal, before_effect: &mut F) -> Result<(), E>
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
        mut journal: OwnedMaterializationJournal,
    ) -> Result<OwnedMaterializationJournal, MaterializationError<S::Error, B::Error>> {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.materializer.persist",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
            })
            .await,
        )
    }
}

async fn compare_next_journal<S: MaterializationJournalStore, B: std::error::Error + 'static>(
    store: &S, mut journal: OwnedMaterializationJournal,
) -> Result<(bool, OwnedMaterializationJournal), MaterializationError<S::Error, B>> {
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
        |path| Ok(validate_materialization_path(path).is_ok()),
        |paths| { paths.sort_unstable(); Ok(()) },
        |left, right| Ok(left == right || right.strip_prefix(left).is_some_and(|suffix| suffix.starts_with('/'))),
    ).map_err(|_| ())
}

enum PlanValidationError<E> { Invalid, Work(E) }

fn validate_plan_steps<'a, E>(
    plan: &'a MaterializationPlan,
    mut step: impl FnMut() -> Result<(), E>,
    allocate: impl FnOnce(usize) -> Result<Vec<&'a str>, E>,
    mut path_valid: impl FnMut(&str) -> Result<bool, E>,
    sort: impl FnOnce(&mut [&str]) -> Result<(), E>,
    mut overlaps: impl FnMut(&str, &str) -> Result<bool, E>,
) -> Result<(), PlanValidationError<E>> {
    use PlanValidationError::Invalid;
    let mut paths = allocate(plan.edits.len().checked_mul(2).ok_or(Invalid)?).map_err(PlanValidationError::Work)?;
    if let MaterializationIntent::NativePreparation { original, .. } = &plan.intent
        && original.is_empty() {
        return Err(Invalid);
    }
    for edit in &plan.edits {
        step().map_err(PlanValidationError::Work)?;
        #[cfg(not(target_arch = "wasm32"))]
        if matches!(plan.intent, MaterializationIntent::NativePreparation { .. })
            && !matches!(edit, MaterializationEdit::Install { image, .. } if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) {
            return Err(Invalid);
        }
        match edit {
            MaterializationEdit::Install { path, .. } | MaterializationEdit::Remove { path } => {
                if !path_valid(path).map_err(PlanValidationError::Work)? { return Err(Invalid); }
                paths.push(path.as_str());
            }
            MaterializationEdit::SetMetadata { path, .. } => {
                if !path_valid(path).map_err(PlanValidationError::Work)? { return Err(Invalid); }
            }
            MaterializationEdit::Rename { from, to } => {
                if !path_valid(from).map_err(PlanValidationError::Work)?
                    || !path_valid(to).map_err(PlanValidationError::Work)? { return Err(Invalid); }
                paths.push(from.as_str());
                paths.push(to.as_str());
            }
        }
    }
    sort(&mut paths).map_err(PlanValidationError::Work)?;
    for pair in paths.windows(2) {
        step().map_err(PlanValidationError::Work)?;
        let [left, right] = pair else { continue };
        if overlaps(left, right).map_err(PlanValidationError::Work)? {
            return Err(Invalid);
        }
    }
    Ok(())
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
pub(crate) fn journal_owned_bytes(journal: &MaterializationJournal) -> Result<u64, NativeTreeMaterializationError> {
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
fn native_namespace_string(path: &crate::NamespacePath, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
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
    let (destination, custody) = work.copy_path(destination)?;
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
fn duplicate_native_preimage(preimage: &MaterializationPreimage, work: &NativeMaterializationWork) -> Result<(MaterializationPreimage, NativeAllocation), NativeTreeMaterializationError> {
    let mut allocation = work.claim(0)?;
    let image = duplicate_native_bytes(&preimage.image, work, &mut allocation)?;
    Ok((MaterializationPreimage { image }, allocation))
}

/// Duplicates the actual logical payload while retaining its original ledger.
#[cfg(not(target_arch = "wasm32"))]
fn duplicate_native_journal(journal: &MaterializationJournal, work: &NativeMaterializationWork) -> Result<OwnedMaterializationJournal, NativeTreeMaterializationError> {
    let mut output = OwnedMaterializationJournal {
        journal: MaterializationJournal {
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
        },
        custody: Some(work.claim(0)?),
    };
    let custody = output.custody.as_mut().expect("native journal custody");
    if let (
        MaterializationIntent::NativePreparation { selection, original },
        MaterializationIntent::NativePreparation { selection: copied, original: copied_original },
    ) = (&journal.plan.intent, &mut output.journal.plan.intent) {
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
        native_vec_push(&mut output.journal.plan.edits, edit, custody)?;
    }
    for preimage in &journal.preimages {
        let (preimage, mut allocation) = duplicate_native_preimage(preimage, work)?;
        if let Err(error) = custody.absorb(&mut allocation) {
            drop(preimage);
            return Err(error);
        }
        native_vec_push(&mut output.journal.preimages, preimage, custody)?;
    }
    Ok(output)
}

fn duplicate_owned_journal(journal: &OwnedMaterializationJournal) -> Result<OwnedMaterializationJournal, MemoryMaterializationJournalStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(custody) = &journal.custody {
        return duplicate_native_journal(&journal.journal, custody.work()).map_err(MemoryMaterializationJournalStoreError::NativeWork);
    }
    Ok(OwnedMaterializationJournal::ordinary(journal.journal.clone()))
}

fn validate_materialization_path(path: &str) -> Result<(), ()> {
    crate::path::is_canonical_relative(path)
        .then_some(())
        .ok_or(())
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: MaterializationJournalStore> JournaledMaterializer<S, NativeTreeMaterializationBackend> {
    /// Stages immutable generation selections through this same owned journal.
    /// Every mutation completes its accepted metadata window before the next
    /// owner check. Partial effects retain their actual typed cause and prefix.
    ///
    /// The physical meter retains its own original resource tag. The store's
    /// completion frame may use a different metadata resource tag belonging to
    /// that same original root; their allocation guards are not merged.
    /// An effect without an acknowledged completion yields
    /// [`NativeStageUncertainty`], leaving the durable preparing record honest
    /// about its known lower-bound work rather than claiming a completed step.
    #[cfg(feature = "native-mount")]
    pub async fn stage_generation_owned_guarded<A, O, E, F, Fut>(
        &self,
        generation: &crate::Generation<A, O>,
        relative_selections: &[String],
        options: &crate::MaterializeOptions,
        journal: OwnedMaterializationJournal,
        physical_work: &crate::native_mount::NativeMountMeterWork,
        before_effect: &mut F,
    ) -> Result<OwnedMaterializationJournal, E>
    where
        A: crate::AsyncAuthorityStore,
        O: crate::AsyncObjectStore,
        E: From<crate::MaterializeError> + From<crate::WorkspaceError>
            + From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>> + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let original = physical_work.native_materialization_work();
        // This prepaid cause slot outlives every physically owned frame.
        let failure_slot = NativeStageFailureSlot::<E>::new(&original)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        let state = std::sync::Mutex::new(Some(NativeStageJournal::Journal(journal)));
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
                        let journal = completion.complete().await
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
                Err(E::from(failure_slot.finish(cause, last_durable_work, original.receipt())))
            }
            (Err(cause), Some(NativeStageJournal::Completing { last_durable_work })) =>
                Err(E::from(failure_slot.finish(cause, last_durable_work, original.receipt()))),
            (Err(cause), _) => Err(cause),
            _ => Err(E::from(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout))),
        }
    }

    #[cfg(feature = "native-mount")]
    async fn reset_stage_owned_guarded<E, F, Fut>(
        &self, journal: OwnedMaterializationJournal, relative: &str, before_effect: &mut F,
    ) -> Result<(OwnedMaterializationJournal, crate::NativeRootIdentity), E>
    where
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let original = &self.backend.work;
        let failure_slot = NativeStageFailureSlot::<E>::new(original)
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
                return Err(E::from(failure_slot.finish(cause, last_durable_work, original.receipt())));
            }
        };
        let journal = completion.complete().await.map_err(|error| {
            let cause = E::from(MaterializationError::Store(error));
            E::from(failure_slot.finish(cause, last_durable_work, original.receipt()))
        })?;
        Ok((journal, identity))
    }

    /// Transfers an already admitted immutable plan into the same durable
    /// Preparing operation, retaining its buffer custody throughout the CAS.
    ///
    /// # Errors
    /// Returns original store/native errors, custody mismatch or contention.
    pub async fn prepare_owned(&self, plan: MaterializationPlan, custody: NativeAllocation) -> Result<OwnedMaterializationJournal, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let custody = custody;
        let plan = plan;
        if !self.backend.work.shares_ledger(custody.work()) {
            return Err(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout));
        }
        if custody.bytes() < plan_owned_bytes(&plan).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout));
        }
        let prepared = self.prepare_with_custody(MaterializationPreparation { plan, custody: Some(custody) }).await?;
        self.verify_owned_journal(&prepared)?;
        Ok(prepared)
    }

    /// Returns the original journal with retained actual output capacities.
    ///
    /// # Errors
    /// Returns original load and finite admission failures.
    pub async fn load_owned(&self, operation_id: OperationId) -> Result<Option<OwnedMaterializationJournal>, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let Some(journal) = self.load(operation_id).await? else { return Ok(None); };
        self.verify_owned_journal(&journal)?;
        Ok(Some(journal))
    }

    fn verify_owned_journal(&self, journal: &OwnedMaterializationJournal) -> Result<(), MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let custody = journal.custody.as_ref().ok_or(MaterializationError::Backend(NativeTreeMaterializationError::MissingCustody))?;
        if !self.backend.work.shares_ledger(custody.work())
            || custody.bytes() < journal_owned_bytes(&journal.journal).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::Backend(NativeTreeMaterializationError::InvalidLayout));
        }
        Ok(())
    }

    /// Recovers the same original owned journal with fresh authorization at
    /// every logical capture, apply or rollback action.
    ///
    /// # Errors
    /// Preserves the original owner, store and native causes without re-adoption.
    pub async fn recover_owned_guarded<E, F, Fut>(&self, operation_id: OperationId, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<Option<OwnedMaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let Some(journal) = self.load_owned(operation_id).await? else { return Ok(None); };
        self.recover_journal_with_guard(journal, recovery, before_effect).await.map(Some)
    }

    /// Inspects original operation/root/cleanup identity with live output
    /// custody, including the real observation and diagnostic buffers.
    /// The journal keeps its own decoded custody. The optional extra guard
    /// covers only actual allocated observation/error buffers and is absent
    /// when those tails own no payload.
    ///
    /// # Errors
    /// Returns original inspection or finite output-admission failures.
    pub async fn inspect_owned(&self, operation_id: OperationId) -> Result<Option<(MaterializationInspection, Option<NativeAllocation>)>, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        // Keep the accumulator before the packet: on a failed absorption the
        // packet's data is dropped before either original custody owner.
        let mut custody: Option<NativeAllocation> = None;
        let Some(mut packet) = self.inspect_with_custody(operation_id).await? else { return Ok(None); };
        for incoming in &mut packet.custody {
            if let Some(retained) = &mut custody {
                if let Some(incoming) = incoming {
                    retained.absorb(incoming).map_err(MaterializationError::Backend)?;
                }
            } else {
                custody = incoming.take();
            }
        }
        Ok(Some((packet, custody)))
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl JournaledMaterializer<crate::LocalCoreStateStore, NativeTreeMaterializationBackend> {
    fn measured_engine(&self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<(JournaledMaterializer<crate::core_state::MeasuredMaterializationStore<'_>, NativeTreeMaterializationBackend>, NativeMaterializationWork), (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(error, work)| (MaterializationError::Backend(error), work))?;
        let mut custody = work.retain_owned(self.backend.allocation.bytes())
            .map_err(|error| (MaterializationError::Backend(error), work.receipt()))?;
        let mut header = work.claim(std::mem::size_of::<NativeAllocation>() as u64)
            .map_err(|error| (MaterializationError::Backend(error), work.receipt()))?;
        custody.absorb(&mut header)
            .map_err(|error| (MaterializationError::Backend(error), work.receipt()))?;
        let mut backend = self.backend.clone();
        backend.work = work.clone();
        backend.allocation = std::sync::Arc::new(custody);
        Ok((JournaledMaterializer::new(self.store.materialization_with_work(work.clone()), backend), work))
    }

    /// Applies through the same engine with bounded native and durable IO.
    pub async fn apply_bounded(&self, plan: MaterializationPlan, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.apply(plan).await, &work)
    }

    /// Retains original Preparing intent before any target writes or capture.
    pub async fn prepare_bounded(&self, plan: MaterializationPlan, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.prepare(plan).await, &work)
    }

    /// Resumes the original durable operation without dropping failure work.
    pub async fn recover_bounded(&self, operation_id: OperationId, recovery: MaterializationRecovery, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<Option<OwnedMaterializationJournal>>, (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.recover(operation_id, recovery).await, &work)
    }

    /// Reads retained intent and durable progress without physical mutation.
    pub async fn load_bounded(&self, operation_id: OperationId, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<Option<OwnedMaterializationJournal>>, (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.load(operation_id).await, &work)
    }

    /// Queries actual root, preparation, and captured endpoints without repair.
    pub async fn inspect_bounded(&self, operation_id: OperationId, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<Option<MaterializationInspection>>, (MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>, crate::WorkCounters)> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.inspect(operation_id).await, &work)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_measured_result<T, E>(result: Result<T, E>, work: &NativeMaterializationWork) -> Result<crate::OperationReceipt<T>, (E, crate::WorkCounters)> {
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

/// Native same-volume tree publisher used for root checkout application.
///
/// Callers first materialize the exact target generation into `target`. The
/// operation directory may be a sibling of the checkout so no repository
/// marker is required, but it must not be the checkout, an ancestor, or a
/// descendant. Each
/// top-level binding is then exchanged with the live root independently. The
/// displaced live binding remains in `backup` until the journal is terminal,
/// preserving metadata, sparse allocation, links, and hard-link identity for
/// rollback without re-encoding host state.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeTreeMaterializationBackend {
    root: std::sync::Arc<PathBuf>,
    target: std::sync::Arc<PathBuf>,
    backup: std::sync::Arc<PathBuf>,
    root_identity: crate::NativeRootIdentity,
    preparation_identity: MaterializationPreparationIdentity,
    work: NativeMaterializationWork,
    held_root: std::sync::Arc<crate::native_host::HostRoot>,
    held_target: std::sync::Arc<crate::native_host::HostRoot>,
    held_backup: std::sync::Arc<crate::native_host::HostRoot>,
    operation_directory: std::sync::Arc<PathBuf>,
    allocation: std::sync::Arc<NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeEntry<'a> {
    root: &'a crate::native_host::HostRoot,
    path: &'a Path,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeTreeMaterializationBackend {
    /// Binds an existing checkout root and same-volume operation directory.
    /// `operation_directory/target` must contain the fully materialized target
    /// tree; `operation_directory/backup` is owned by this backend.
    pub fn new(
        root: impl Into<PathBuf>,
        operation_directory: impl Into<PathBuf>,
    ) -> Result<Self, NativeTreeMaterializationError> {
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|(error, _)| error)?;
        Self::new_with_work(root.into(), operation_directory.into(), work)
    }

    /// Binds native capabilities using the same meter as its durable store.
    pub fn new_with_work(root: PathBuf, operation_directory: PathBuf, work: NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        let custody = work.retain_owned(root.capacity().checked_add(operation_directory.capacity()).ok_or(crate::WorkError::Overflow)? as u64)?;
        Self::bind_with_work(root, operation_directory, work, true, custody)
    }

    /// Opens existing allocations only; inspection never recreates missing data.
    pub fn open_with_work(root: PathBuf, operation_directory: PathBuf, work: NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        let custody = work.retain_owned(root.capacity().checked_add(operation_directory.capacity()).ok_or(crate::WorkError::Overflow)? as u64)?;
        Self::bind_with_work(root, operation_directory, work, false, custody)
    }

    /// Creates preparation capabilities, transferring the caller's actual path
    /// buffer custody without a second claim.
    ///
    /// # Errors
    /// Returns actual layout/open/creation work or an incompatible custody meter.
    pub fn new_owned_with_work(root: PathBuf, operation_directory: PathBuf, custody: NativeAllocation, work: NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        Self::bind_with_work(root, operation_directory, work, true, custody)
    }

    /// Opens existing preparation allocations with moved path-buffer custody.
    ///
    /// # Errors
    /// Returns actual native work without creating missing recovery allocations.
    pub fn open_owned_with_work(root: PathBuf, operation_directory: PathBuf, custody: NativeAllocation, work: NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        Self::bind_with_work(root, operation_directory, work, false, custody)
    }

    async fn prepare_allocations<S, F, Fut, E>(
        store: &S, mut journal: OwnedMaterializationJournal, root: &Path,
        operation_directory: &Path, work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<OwnedMaterializationJournal, E>
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
            let (root_copy, root_custody) = work.copy_path(root).map_err(backend_error)?;
            let (operation_copy, operation_custody) = work.copy_path(operation_directory).map_err(backend_error)?;
            let actual = acyclic_native_runtime::run_blocking_io({
                let work = work.clone();
                move || {
                    let _paths = (root_custody, operation_custody);
                    let source = crate::native_host::HostRoot::open_materialization_measured(&root_copy, &work)?;
                    if source.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: source.identity() }); }
                    let parent = crate::native_host::HostRoot::open_materialization_measured(operation_copy.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &work)?;
                    let expected_parent = crate::NativeRootIdentity::from_bytes(progress.parent);
                    if parent.identity() != expected_parent { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual: parent.identity() }); }
                    let parent = parent.open_dir_materialization_measured(Path::new(""), &work)?;
                    let leaf = Path::new(operation_copy.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?);
                    let allocation = match step {
                        MaterializationPreparationAllocation::Operation => crate::native_host::create_new_directory_held_with_work(&parent, leaf, &mut &work)?,
                        MaterializationPreparationAllocation::Target | MaterializationPreparationAllocation::Backup => {
                            let expected = crate::NativeRootIdentity::from_bytes(progress.operation.ok_or(NativeTreeMaterializationError::InvalidLayout)?);
                            let operation = with_native_walk_leaf(leaf, &work, |name| open_native_walk_directory(&parent, name, &work))?;
                            let actual = crate::native_host::directory_identity_materialization_measured(&operation, &work)?;
                            if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
                            if let Some(expected) = progress.target { Self::recorded_preparation_child(&operation, Path::new("target"), expected, &work)?; }
                            crate::native_host::create_new_directory_held_with_work(&operation, match step { MaterializationPreparationAllocation::Target => Path::new("target"), _ => Path::new("backup") }, &mut &work)?
                        }
                    };
                    crate::native_host::directory_identity_materialization_measured(&allocation, &work).map(|identity| identity.to_bytes())
                }
            }).await.map_err(|error| backend_error(NativeTreeMaterializationError::Io(error)))?.map_err(backend_error)?;
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

    // These checks reopen only recorded allocations; they never infer ownership
    // from an existing path or manufacture a missing witness.
    fn recorded_preparation_child(parent: &cap_std::fs::Dir, leaf: &Path, expected: [u8; 16], work: &NativeMaterializationWork) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
        let directory = with_native_walk_leaf(leaf, work, |name| open_native_walk_directory(parent, name, work))?;
        let actual = crate::native_host::directory_identity_materialization_measured(&directory, work)?;
        let expected = crate::NativeRootIdentity::from_bytes(expected);
        if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
        Ok(directory)
    }

    fn verify_recorded_preparation(root: &Path, expected_root: crate::NativeRootIdentity, operation_directory: &Path, progress: MaterializationPreparationProgress, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        let source = crate::native_host::HostRoot::open_materialization_measured(root, work)?;
        if source.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: source.identity() }); }
        let parent = crate::native_host::HostRoot::open_materialization_measured(operation_directory.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, work)?;
        let expected_parent = crate::NativeRootIdentity::from_bytes(progress.parent);
        if parent.identity() != expected_parent { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_parent, actual: parent.identity() }); }
        let parent = parent.open_dir_materialization_measured(Path::new(""), work)?;
        if let Some(expected) = progress.operation {
            let operation = Self::recorded_preparation_child(&parent, Path::new(operation_directory.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?), expected, work)?;
            if let Some(expected) = progress.target { Self::recorded_preparation_child(&operation, Path::new("target"), expected, work)?; }
            if let Some(expected) = progress.backup { Self::recorded_preparation_child(&operation, Path::new("backup"), expected, work)?; }
        }
        Ok(())
    }

    fn bind_with_work(root: PathBuf, operation_directory: PathBuf, work: NativeMaterializationWork, create_backup: bool, custody: NativeAllocation) -> Result<Self, NativeTreeMaterializationError> {
        let mut custody = custody;
        let root = root;
        let operation_directory = operation_directory;
        if !work.shares_ledger(custody.work()) {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let operation_root = crate::native_host::HostRoot::open_materialization_measured(&operation_directory, &work)?;
        let held_root = native_owned_arc(crate::native_host::HostRoot::open_materialization_measured(&root, &work)?, &work, &mut custody)?;
        let root_identity = held_root.identity();
        if root_identity.device != operation_root.identity().device {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let operation_directory_cap = operation_root.open_dir_materialization_measured(Path::new(""), &work)?;
        let root_directory_cap = held_root.open_dir_materialization_measured(Path::new(""), &work)?;
        #[cfg(unix)]
        let nested = crate::native_host::directory_has_ancestor_with_work(&operation_directory_cap, root_identity, &mut &work)?
            || crate::native_host::directory_has_ancestor_with_work(&root_directory_cap, operation_root.identity(), &mut &work)?;
        #[cfg(windows)]
        let nested = native_directory_contains(&root_directory_cap, operation_root.identity(), &work)?
            || native_directory_contains(&operation_directory_cap, root_identity, &work)?;
        if nested { return Err(NativeTreeMaterializationError::InvalidLayout); }
        let mut target_owned = work.join_path(&operation_directory, Path::new("target"))?;
        custody.absorb(&mut target_owned.1)?;
        let target = target_owned.0;
        let mut backup_owned = work.join_path(&operation_directory, Path::new("backup"))?;
        custody.absorb(&mut backup_owned.1)?;
        let backup = backup_owned.0;
        let operation_identity = operation_root.identity().to_bytes();
        if create_backup {
            let directory = operation_root.open_dir_materialization_measured(Path::new(""), &work)?;
            crate::native_host::create_directories_held_materialization_measured(&directory, Path::new("backup"), &work)?;
        }
        let target_directory = operation_root.open_dir_materialization_measured(Path::new("target"), &work)?;
        let target_identity = crate::native_host::directory_identity_materialization_measured(&target_directory, &work)?;
        let held_target = native_owned_arc(crate::native_host::HostRoot::from_held_dir_with_identity(target_directory, target_identity), &work, &mut custody)?;
        let backup_directory = operation_root.open_dir_materialization_measured(Path::new("backup"), &work)?;
        let backup_identity = crate::native_host::directory_identity_materialization_measured(&backup_directory, &work)?;
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
            allocation: {
                let mut header = work.claim(std::mem::size_of::<NativeAllocation>() as u64)?;
                custody.absorb(&mut header)?;
                std::sync::Arc::new(custody)
            },
            root_identity,
            held_root,
            held_target,
            held_backup,
            work,
            preparation_identity,
        })
    }

    /// Creates an actual bounded native backend and reports all constructor work.
    pub fn new_bounded(root: impl AsRef<Path>, operation_directory: impl AsRef<Path>, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<Self>, (NativeTreeMaterializationError, crate::WorkCounters)> {
        let work = NativeMaterializationWork::new(budget, cancellation)?;
        let value = (|| {
            let mut root_owned = work.copy_path(root.as_ref())?;
            let mut operation_owned = work.copy_path(operation_directory.as_ref())?;
            root_owned.1.absorb(&mut operation_owned.1)?;
            Self::new_owned_with_work(root_owned.0, operation_owned.0, root_owned.1, work.clone())
        })();
        match value {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
            Err(error) => Err((error, work.receipt())),
        }
    }

    /// Shares the native boundary with a receipt-bearing durable store adapter.
    pub fn work(&self) -> NativeMaterializationWork { self.work.clone() }

    /// Borrows the original root path without canonicalization or copying.
    pub fn root_path(&self) -> &Path { self.root.as_path() }

    /// Borrows the original operation-directory path.
    pub fn operation_directory_path(&self) -> &Path { self.operation_directory.as_path() }

    /// Opens a mapping through the original held root, rejecting symlink
    /// components and a replacement of the externally named root.
    ///
    /// # Errors
    /// Returns invalid paths, allocation divergence, or exact native work.
    pub fn mapping_root_identity(&self, path: &str) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::InvalidPath)?);
        }
        let actual = self.work.root_identity(self.root_path())?;
        if actual != self.root_identity {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected: self.root_identity, actual });
        }
        let directory = self.held_root.open_dir_materialization_measured(Path::new(path), &self.work)?;
        crate::native_host::directory_identity_materialization_measured(&directory, &self.work)
    }

    /// Allocates a lexical staging path with actual capacity custody. The
    /// generation writer must still bind its opened root to the identity
    /// returned by `reset_target_directory`.
    ///
    /// # Errors
    /// Rejects invalid paths and insufficient allocation/copy allowance.
    pub fn staging_path(&self, path: &str) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(native_diagnostic(&self.work, path, NativeTreeMaterializationError::InvalidPath)?);
        }
        self.work.join_path(self.target.as_path(), Path::new(path))
    }

    /// Checks each actual held initial allocation before staging can write.
    ///
    /// # Errors
    /// Returns divergence or the actual finite scan/open failure.
    pub fn require_initial_empty(&self) -> Result<(), NativeTreeMaterializationError> {
        for (root, path) in [
            (&self.held_root, self.root_path()),
            (&self.held_target, self.target.as_path()),
            (&self.held_backup, self.backup.as_path()),
        ] {
            let directory = root.open_dir_materialization_measured(Path::new(""), &self.work)?;
            let (mut reader, _cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &self.work)?;
            if next_native_walk_name(&mut reader, &self.work)?.is_some() {
                return Err(native_diagnostic(&self.work, path.display(), NativeTreeMaterializationError::ExternalMutation)?);
            }
        }
        Ok(())
    }

    /// Checks that root entries belong to the original selected install paths.
    /// Missing entries remain permitted during incomplete forward application.
    ///
    /// # Errors
    /// Never adopts unselected user data; returns divergence or spent scan work.
    pub fn require_selected_root(&self, edits: &[MaterializationEdit]) -> Result<(), NativeTreeMaterializationError> {
        let directory = self.held_root.open_dir_materialization_measured(Path::new(""), &self.work)?;
        let (mut reader, _cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &self.work)?;
        while let Some((name, _allocation)) = next_native_walk_name(&mut reader, &self.work)? {
            let mut selected = false;
            for edit in edits {
                self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() })?;
                let MaterializationEdit::Install { path, .. } = edit else { continue; };
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

    /// Clears an operation-owned staged directory while retaining the witnessed
    /// target root allocation. Only a Preparing journal may authorize this.
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
            use cap_fs_ext::DirExt as _;
            let _path_allocation = path_allocation;
            let directory = backend.held_target.open_dir_materialization_measured(Path::new(""), &backend.work)?;
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
                let parent = backend.held_target.open_dir_materialization_measured(relative.parent().unwrap_or_else(|| Path::new("")), &backend.work)?;
                let leaf = relative.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                remove_native_held(&parent, Path::new(leaf), &backend.work)?;
                let child = crate::native_host::create_new_directory_held_with_work(&parent, Path::new(leaf), &mut &backend.work)?;
                let identity = crate::native_host::directory_identity_materialization_measured(&child, &backend.work)?;
                identity
            };
            crate::native_host::sync_directory_held_measured(&directory, &backend.work)?;
            Ok(identity)
        }).await?
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
        let directory = path.root.open_dir_materialization_measured(Path::new(""), &self.work)?;
        crate::native_host::create_directories_held_materialization_measured(&directory, path.path.parent().unwrap_or_else(|| Path::new("")), &self.work)?;
        Ok(())
    }

    fn rename(&self, from: &NativeEntry<'_>, to: &NativeEntry<'_>) -> Result<(), NativeTreeMaterializationError> {
        let (source, from_name) = self.parent(from)?;
        let (destination, to_name) = self.parent(to)?;
        self.work.admit(crate::WorkCounters { materializations: 1, ..crate::WorkCounters::default() })?;
        crate::native_host::rename_no_replace_held_measured(&source, &from_name, &destination, &to_name, &self.work)?;
        self.work.spent(crate::WorkCounters { materializations: 1, ..crate::WorkCounters::default() })?;
        let source = crate::native_host::sync_directory_held_measured(&source, &self.work);
        let destination = crate::native_host::sync_directory_held_measured(&destination, &self.work);
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
        crate::native_host::sync_directory_held_measured(&parent, &self.work)?;
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

    #[cfg(test)]
    fn plan_paths(
        &self,
        operation_id: OperationId,
        from: GenerationId,
        to: GenerationId,
        paths: impl IntoIterator<Item = String>,
    ) -> Result<MaterializationPlan, NativeTreeMaterializationError> {
        let edits = paths
            .into_iter()
            .map(|path| {
                if validate_materialization_path(&path).is_err() {
                    return Err(NativeTreeMaterializationError::InvalidPath(path, None));
                }
                if self.exists(&self.paths(&path).1)? {
                    Ok(MaterializationEdit::Install {
                        path,
                        image: vec![1],
                    })
                } else {
                    Ok(MaterializationEdit::Remove { path })
                }
            })
            .collect::<Result<Vec<_>, NativeTreeMaterializationError>>()?;
        let plan = MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition { from, to },
            edits,
        };
        validate_plan(&plan).map_err(|()| NativeTreeMaterializationError::OverlappingPaths)?;
        Ok(plan)
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

/// Checks a prospective preparation layout using actual held directory
/// allocations, never lexical canonical-prefix ownership.
///
/// # Errors
/// Rejects overlap/device divergence and returns bounded scan/open failures.
#[cfg(not(target_arch = "wasm32"))]
pub fn validate_native_preparation_location(
    root: &Path,
    operation_directory: &Path,
    work: &NativeMaterializationWork,
) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
    let root = crate::native_host::HostRoot::open_materialization_measured(root, work)?;
    let mut existing_path = operation_directory;
    let operation = loop {
        match crate::native_host::HostRoot::open_materialization_measured(existing_path, work) {
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
    let root_directory = root.open_dir_materialization_measured(Path::new(""), work)?;
    let operation_directory_cap = operation.open_dir_materialization_measured(Path::new(""), work)?;
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
        Some(self.root_identity.to_bytes())
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
        matches!(error, NativeTreeMaterializationError::Work(_) | NativeTreeMaterializationError::Cancelled | NativeTreeMaterializationError::Allocation(_, _) | NativeTreeMaterializationError::AllocationFailed(_))
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
                let parent = match root.open_dir_materialization_measured(Path::new(path).parent().unwrap_or_else(|| Path::new("")), &backend.work) {
                    Ok(parent) => parent,
                    Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                crate::native_host::sync_directory_held_measured(&parent, &backend.work)?;
            }
            if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                let (live, _, _) = backend.paths(path);
                let (parent, leaf) = backend.parent(&live)?;
                crate::native_host::sync_metadata_held_with_work(&parent, leaf, &mut &backend.work)?;
            }
            Ok(())
        }).await?
    }


    fn retain_plan(&self, plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.retain_owned(plan_owned_bytes(plan)?).map(Some)
    }

    fn describe_observation_error(&self, error: Self::Error) -> Result<(String, Option<NativeAllocation>), Self::Error> {
        match self.work.display(&error) {
            Ok((text, custody)) => Ok((text, Some(custody))),
            // The shared meter already owns the formatter's spent/fatal prefix.
            // A refused diagnostic must not replace its actual physical cause.
            Err(_format_failure) => Err(error),
        }
    }

    fn reserve_observations(
        &self,
        observations: &mut Vec<MaterializationObservation>,
        count: usize,
        allocation: &mut Option<NativeAllocation>,
    ) -> Result<(), Self::Error> {
        let allocation = allocation.as_mut().ok_or(NativeTreeMaterializationError::MissingCustody)?;
        observations.try_reserve_exact(count).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        allocation.reconcile_capacity(owned_size(observations.capacity(), std::mem::size_of::<MaterializationObservation>())?)
    }

    fn admit_observations(&self, count: usize) -> Result<Option<NativeAllocation>, Self::Error> {
        let bytes = owned_size(count, std::mem::size_of::<MaterializationObservation>())?;
        if count == 0 { self.work.retain_owned(bytes).map(Some) }
        else { self.work.claim(bytes).map(Some) }
    }

    fn append_preimage(&self, packet: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
        let mut image_custody = preimage.custody.ok_or(NativeTreeMaterializationError::MissingCustody)?;
        let preimage = preimage.preimage;
        let OwnedMaterializationJournal { journal, custody } = packet;
        let custody = custody.as_mut().ok_or(NativeTreeMaterializationError::MissingCustody)?;
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
                *slot = crate::native_host::HostRoot::open_materialization_measured(path, &backend.work)?.identity().to_bytes();
            }
            Ok(Some(current))
        }).await?
    }

    async fn observe_allocation_identity(&self) -> Result<Option<[u8; 16]>, Self::Error> {
        let root = self.root.clone();
        let work = self.work.clone();
        acyclic_native_runtime::run_blocking_io(move || {
            crate::native_host::HostRoot::open_materialization_measured(root.as_path(), &work)
                .map(|held| Some(held.identity().to_bytes())).map_err(Self::Error::from)
        }).await?
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<OwnedMaterializationPreimage, Self::Error> {
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
                return Ok(OwnedMaterializationPreimage {
                    preimage: MaterializationPreimage { image }, custody: Some(custody),
                });
            }
            let before = backend.fingerprint_for_edit(&live, edit)?;
            let after = match edit {
                MaterializationEdit::Install { .. } => match backend.fingerprint(&target)? {
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
            Ok(OwnedMaterializationPreimage {
                preimage: MaterializationPreimage { image }, custody: Some(custody),
            })
        })
        .await?
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let (preimage, preimage_allocation) = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _allocations = (edit_allocation, preimage_allocation);
            let edit = &edit;
            let preimage = &preimage;
            let (before, after) = decode_native_witness(&preimage.image)?;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            let current = backend.fingerprint_for_edit(&live, edit)?;
            let target_current = if matches!(edit, MaterializationEdit::Install { .. }) {
                backend.fingerprint(&target)?
            } else {
                None
            };
            let backup_current = backend.fingerprint(&backup)?;
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

            // A crash may land between the two renames used for an install. The
            // durable backup proves the old binding was moved by this operation;
            // the still-present staged target means forward application can resume.
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
        })
        .await?
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let (preimage, preimage_allocation) = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _allocations = (edit_allocation, preimage_allocation);
            let edit = &edit;
            let preimage = &preimage;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            let (before, after) = decode_native_witness(&preimage.image)?;
            if let MaterializationEdit::SetMetadata { image, .. } = edit {
                let current = backend.fingerprint_for_edit(&live, edit)?;
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
            let target_fingerprint = backend.fingerprint(&target)?;
            let target_exists = target_fingerprint.is_some();
            let live_fingerprint = backend.fingerprint(&live)?;
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
                crate::native_host::sync_directory_held_measured(&parent, &backend.work)?;
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
        })
        .await?
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        let (preimage, preimage_allocation) = duplicate_native_preimage(preimage, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _allocations = (edit_allocation, preimage_allocation);
            let edit = &edit;
            let preimage = &preimage;
            let path = edit_path(edit);
            let (live, _, backup) = backend.paths(path);
            let (before, after) = decode_native_witness(&preimage.image)?;
            let current = backend.fingerprint_for_edit(&live, edit)?;
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
            let backup_current = backend.fingerprint(&backup)?;
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
        })
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
    identity: crate::NativeRootIdentity,
}

#[cfg(not(target_arch = "wasm32"))]
fn native_remove_frame(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, metadata: &crate::native_host::HostStat, work: &NativeMaterializationWork) -> Result<NativeRemoveFrame, NativeTreeMaterializationError> {
    let directory = open_native_walk_directory(parent, name, work)?;
    let actual = crate::native_host::directory_identity_materialization_measured(&directory, work)?;
    if actual != native_stat_identity(metadata) {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected: native_stat_identity(metadata), actual });
    }
    let (reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    Ok(NativeRemoveFrame { directory, reader, cursor, name: None, identity: native_stat_identity(metadata) })
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
    let mut walker = NativeRemoveWalker::new(root, work)?;
    while let Some(step) = walker.next(work, false)? {
        walker.execute(parent, name, step, work)?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
enum NativeRemoveStep {
    Leaf { name: NativeWalkName, allocation: NativeAllocation, identity: crate::NativeRootIdentity },
    Directory(NativeRemoveFrame),
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeRemoveWalker {
    frames: Vec<NativeRemoveFrame>,
    storage: NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeRemoveWalker {
    fn new(root: NativeRemoveFrame, work: &NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        let storage = work.claim(std::mem::size_of::<NativeRemoveFrame>() as u64)?;
        let mut frames = Vec::new();
        frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
        frames.push(root);
        Ok(Self { frames, storage })
    }

    fn next(&mut self, work: &NativeMaterializationWork, strict_owned: bool) -> Result<Option<NativeRemoveStep>, NativeTreeMaterializationError> {
        while let Some(frame) = self.frames.last_mut() {
            if let Some((child, allocation)) = next_native_walk_name(&mut frame.reader, work)? {
                let metadata = stat_native_walk_name(&frame.directory, &child, work)?;
                if strict_owned { native_require_owned_entry(&metadata)?; }
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    let mut next = if strict_owned {
                        native_owned_remove_frame(&frame.directory, &child, &metadata, work)?
                    } else {
                        native_remove_frame(&frame.directory, &child, &metadata, work)?
                    };
                    next.name = Some((child, allocation));
                    if self.frames.len() == self.frames.capacity() {
                        self.storage.grow(std::mem::size_of::<NativeRemoveFrame>() as u64)?;
                        self.frames.try_reserve_exact(1).map_err(NativeTreeMaterializationError::AllocationFailed)?;
                    }
                    self.frames.push(next);
                } else {
                    return Ok(Some(NativeRemoveStep::Leaf { name: child, allocation, identity: native_stat_identity(&metadata) }));
                }
            } else {
                return Ok(self.frames.pop().map(NativeRemoveStep::Directory));
            }
        }
        Ok(None)
    }

    fn verify(&self, parent: &cap_std::fs::Dir, root_name: &NativeWalkNameRef, step: &NativeRemoveStep, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        let mut ancestor = parent;
        for frame in &self.frames {
            let name = frame.name.as_ref().map(|(name, _)| &**name).unwrap_or(root_name);
            let metadata = stat_native_walk_name(ancestor, name, work)?;
            native_require_owned_entry(&metadata)?;
            crate::native_host::verify_owned_child_no_mount_with_work(ancestor, name, &mut &*work)?;
            let actual = native_stat_identity(&metadata);
            if actual != frame.identity { return Err(NativeTreeMaterializationError::AllocationChanged { expected: frame.identity, actual }); }
            if crate::native_host::directory_identity_materialization_measured(&frame.directory, work)? != frame.identity {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
            ancestor = &frame.directory;
        }
        let (name, expected) = match step {
            NativeRemoveStep::Leaf { name, identity, .. } => (&**name, *identity),
            NativeRemoveStep::Directory(frame) => (frame.name.as_ref().map(|(name, _)| &**name).unwrap_or(root_name), frame.identity),
        };
        let metadata = stat_native_walk_name(ancestor, name, work)?;
        native_require_owned_entry(&metadata)?;
        crate::native_host::verify_owned_child_no_mount_with_work(ancestor, name, &mut &*work)?;
        let actual = native_stat_identity(&metadata);
        if actual != expected { return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual }); }
        Ok(())
    }

    fn execute(&self, parent: &cap_std::fs::Dir, root_name: &NativeWalkNameRef, step: NativeRemoveStep, work: &NativeMaterializationWork) -> Result<(), NativeTreeMaterializationError> {
        let parent = self.frames.last().map(|frame| &frame.directory).unwrap_or(parent);
        match step {
            NativeRemoveStep::Leaf { name, allocation, .. } => {
                remove_native_walk_leaf(parent, &name, false, work)?;
                drop(allocation);
            }
            NativeRemoveStep::Directory(frame) => {
                crate::native_host::sync_directory_held_measured(&frame.directory, work)?;
                let NativeRemoveFrame { directory, reader, cursor, name, .. } = frame;
                drop(reader);
                drop(cursor);
                drop(directory);
                if let Some((name, allocation)) = name {
                    remove_native_walk_leaf(parent, &name, true, work)?;
                    drop(allocation);
                } else {
                    remove_native_walk_leaf(parent, root_name, true, work)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_require_owned_entry(metadata: &crate::native_host::HostStat) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(unix)]
    let forbidden = metadata.file_type().is_symlink();
    #[cfg(windows)]
    let forbidden = metadata.file_attributes() & 0x400 != 0;
    if forbidden { return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()); }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_owned_relative_parent(root: &crate::native_host::HostRoot, relative: &Path, work: &NativeMaterializationWork) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
    let mut parent = root.open_dir_materialization_measured(Path::new(""), work)?;
    for component in relative.components() {
        work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let std::path::Component::Normal(name) = component else { return Err(NativeTreeMaterializationError::InvalidLayout); };
        parent = with_native_walk_leaf(Path::new(name), work, |name| crate::native_host::open_owned_child_directory_with_work(&parent, name, &mut &*work))?;
    }
    Ok(parent)
}

#[cfg(not(target_arch = "wasm32"))]
fn native_owned_remove_frame(parent: &cap_std::fs::Dir, name: &NativeWalkNameRef, metadata: &crate::native_host::HostStat, work: &NativeMaterializationWork) -> Result<NativeRemoveFrame, NativeTreeMaterializationError> {
    native_require_owned_entry(metadata)?;
    let directory = crate::native_host::open_owned_child_directory_with_work(parent, name, &mut &*work)?;
    let identity = crate::native_host::directory_identity_materialization_measured(&directory, work)?;
    if identity != native_stat_identity(metadata) { return Err(NativeTreeMaterializationError::AllocationChanged { expected: native_stat_identity(metadata), actual: identity }); }
    let (reader, cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &*work)?;
    Ok(NativeRemoveFrame { directory, reader, cursor, name: None, identity })
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
    let mut writer = NativeImageWriter::new(work)?;
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
    fn new(work: &'a NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self { image: Vec::new(), work, encoded: true, failure: None, allocation: work.claim_allocation(0, 0)? })
    }

    fn new_bytes(work: &'a NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self { encoded: false, ..Self::new(work)? })
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
        let parent = path.root.open_dir_materialization_measured(path.path.parent().unwrap_or_else(|| Path::new("")), &self.work)?;
        let leaf = path.path.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?;
        Ok((parent, Path::new(leaf)))
    }

    fn fingerprint(&self, path: &NativeEntry<'_>) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
        self.fingerprint_scoped(path, true, None)
    }

    fn fingerprint_for_edit(&self, path: &NativeEntry<'_>, edit: &MaterializationEdit) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
        self.fingerprint_scoped(path, !matches!(edit, MaterializationEdit::SetMetadata { .. }), None)
    }

    fn fingerprint_scoped(&self, path: &NativeEntry<'_>, contents: bool, desired: Option<&NativeMetadataImage>) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
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
        hash_native_entry(self, &parent, &leaf, &metadata, &mut hasher, desired, contents)?;
        Ok(Some(*hasher.finalize().as_bytes()))
    }
}

#[cfg(windows)]
fn native_directory_contains(directory: &cap_std::fs::Dir, wanted: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
    if crate::native_host::directory_identity_materialization_measured(directory, work)? == wanted { return Ok(true); }
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
            let actual = crate::native_host::directory_identity_materialization_measured(&child, work)?;
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
fn native_host_name_matches(host: &NativeWalkNameRef, logical: &crate::LogicalName, work: &NativeMaterializationWork) -> Result<bool, NativeTreeMaterializationError> {
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
        crate::NameEncoding::WindowsUtf16Le => {
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
        crate::NameEncoding::Utf8 | crate::NameEncoding::PosixBytes => {
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
    storage: NativeAllocation,
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
    Ok(NativeHashFrame { directory, names, storage })
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
fn hash_native_entry(backend: &NativeTreeMaterializationBackend, parent: &cap_std::fs::Dir, leaf: &Path, metadata: &crate::native_host::HostStat, hasher: &mut blake3::Hasher, desired: Option<&NativeMetadataImage>, contents: bool) -> Result<(), NativeTreeMaterializationError> {
    let mut frames = Vec::new();
    let mut storage = backend.work.claim(0)?;
    let first = with_native_walk_leaf(leaf, &backend.work, |name| hash_native_entry_shallow(backend, parent, name, metadata, hasher, desired, contents))?;
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
        let next = hash_native_entry_shallow(backend, &frame.directory, &name, &child, hasher, None, true)?;
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
    desired: Option<&NativeMetadataImage>,
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
        hash_native_metadata_edit(metadata, desired, hasher);
        #[cfg(windows)]
        if metadata.is_dir() || metadata.file_type().is_file() {
            hasher.update(&native_stat_identity(metadata).to_bytes());
        }
        return Ok(None);
    }
    hasher.update(kind);
    hasher.update(&metadata.len().to_le_bytes());
    hasher.update(&[u8::from(desired.map_or_else(|| native_stat_readonly(metadata), |value| value.readonly))]);
    hash_native_metadata(metadata, desired, true, hasher);
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
        let opened = crate::native_host::directory_identity_materialization_measured(&directory, &backend.work)?;
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

#[cfg(not(target_arch = "wasm32"))]
fn hash_native_name(work: &NativeMaterializationWork, name: &std::ffi::OsStr, hasher: &mut blake3::Hasher) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(windows)]
    let length = { use std::os::windows::ffi::OsStrExt as _; name.encode_wide().count() as u64 * 2 };
    #[cfg(not(windows))]
    let length = name.as_encoded_bytes().len() as u64;
    work.charge(crate::WorkCounters { bytes_hashed: length, ..crate::WorkCounters::default() })?;
    hash_native_os_str(name, hasher);
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn hash_native_metadata(
    metadata: &crate::native_host::HostStat,
    desired: Option<&NativeMetadataImage>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(
            desired
                .and_then(|value| value.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        ),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    if include_contents {
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
    desired: Option<&NativeMetadataImage>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    let attributes = desired.map_or_else(
        || metadata.file_attributes(),
        |value| {
            if let Some(attributes) = value.windows_attributes {
                attributes
            } else if value.readonly {
                metadata.file_attributes() | 1
            } else {
                metadata.file_attributes() & !1
            }
        },
    );
    hasher.update(&u64::from(attributes).to_le_bytes());
    if include_contents {
        for value in [metadata.last_write_time(), metadata.len()] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
fn hash_native_metadata(
    _metadata: &cap_std::fs::Metadata,
    _desired: Option<&NativeMetadataImage>,
    _include_contents: bool,
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
    let mut writer = NativeImageWriter::new(work)?;
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

#[cfg(all(windows, not(target_arch = "wasm32")))]
fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::windows::ffi::OsStrExt as _;
    for code_unit in value.encode_wide() {
        hasher.update(&code_unit.to_le_bytes());
    }
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
    /// Changed paths overlap and cannot be exchanged independently.
    #[error("native materialization paths overlap")]
    OverlappingPaths,
    /// Cooperative cancellation at a host or journal boundary.
    #[error("native materialization cancelled")]
    Cancelled,
    /// The original accounting state panicked; only physical retirement remains.
    #[error("original native work state is poisoned")]
    OriginalWorkPoisoned,
    /// A finite work allowance was exhausted before the next operation.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A measured host buffer could not be admitted or allocated.
    #[error("native materialization allocation failed: {0}")]
    Allocation(String, Option<NativeAllocation>),
    /// The original fallible owned buffer reservation failed.
    #[error("native materialization allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// Source metadata requires observations this native publisher cannot prove.
    #[error("native materialization cannot verify complete source metadata at '{0}'")]
    UnverifiedSourceMetadata(String, Option<NativeAllocation>),
    /// The actual held checkout allocation differs from the original owner.
    #[error("native materialization root allocation changed: expected {expected:?}, observed {actual:?}")]
    AllocationChanged { expected: crate::NativeRootIdentity, actual: crate::NativeRootIdentity },
    /// An entire native builtin payload exceeds its admitted byte cap.
    #[error("native builtin payload is {actual} bytes; maximum is {maximum}")]
    PayloadTooLarge { maximum: u64, actual: u64 },
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
            | Self::UnverifiedSourceMetadata(_, allocation)
            | Self::Allocation(_, allocation) => Some(allocation),
            _ => None,
        }
    }

    /// Recovers original runtime-meter quota/cancellation causes without
    /// formatting or allocating another diagnostic payload. Other I/O causes,
    /// including caller-owned callback errors, keep their original box intact.
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
    match validate_plan_steps(
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
    ) {
        Ok(()) => Ok(true),
        Err(PlanValidationError::Invalid) => Ok(false),
        Err(PlanValidationError::Work(error)) => Err(error),
    }
}

/// Materializes and publishes one authenticated workspace generation to a
/// physical root through the core journal. Recovery resumes an existing
/// operation instead of rebuilding or accepting a partial checkout.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
pub struct NativeWorkspacePublication<'a> {
    /// Physical checkout root.
    pub root: &'a Path,
    /// Actual ORIGINAL checkout allocation captured by the owner, not a path
    /// reobservation or staging allocation.
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

#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
/// Executes or resumes one bounded, journaled physical-root publication.
pub async fn publish_native_workspace_generation<A, O>(
    workspace: &crate::Workspace<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|(error, work)| (NativeWorkspacePublicationError::Native(error), work))?;
    let result = async {
        let from_generation = workspace.generation_with_native_work(publication.from, &work, publication.cancellation).await?;
        let to_generation = workspace.generation_with_native_work(publication.to, &work, publication.cancellation).await?;
        let mut before_effect = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
        publish_native_transition_inner(&from_generation, &to_generation, state, publication, work.clone(), false, &mut before_effect).await
    }.await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

/// Publishes a transition between two authenticated generations, including
/// generations owned by sibling workspaces in the same distributed filesystem.
///
/// This is the branch-switch primitive: workspace identity may change while
/// physical checkout work remains proportional only to changed paths.
#[cfg(all(
    feature = "local",
    feature = "native-mount",
    not(target_arch = "wasm32")
))]
#[allow(
    clippy::too_many_lines,
    reason = "one transition plans, journals, and applies every host change between two generations"
)]
pub async fn publish_native_generation_transition<A, O>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    native_generation_transition(from_generation, to_generation, state, publication, false).await
}

/// Stages and captures the original journal before approval, without applying
/// any edit to the physical checkout root.
///
/// Existing physical paths absent from the original generation are conflicts,
/// not implicitly authorized lazy-source promotions.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn prepare_native_generation_transition<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)> {
    native_generation_transition(from_generation, to_generation, state, publication, true).await
}

/// Loads the original operation through bounded local journal IO without
/// opening, repairing, adopting, or mutating physical checkout/staging roots.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn load_native_materialization_journal(
    state: &crate::LocalCoreStateStore,
    operation_id: OperationId,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
)-> Result<crate::OperationReceipt<Option<OwnedMaterializationJournal>>, (NativeWorkspacePublicationError, crate::WorkCounters)> {
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

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn native_generation_transition<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    prepare_only: bool,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)> {
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|(error, work)| (NativeWorkspacePublicationError::Native(error), work))?;
    let mut before_effect = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), prepare_only, &mut before_effect).await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err((error, work.receipt())),
    }
}

/// Uses the original publication engine and meter, reauthorizing each physical
/// journal action. The callback supplies authority; neither a root observation
/// nor this filesystem engine creates a task grant.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn publish_native_generation_transition_with_work_guarded<A, O, F, Fut>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    mut before_effect: F,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)>
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

/// Prepares the same witnessed native journal using the caller's original work
/// scope and authorization guard. No generation is published by this operation.
#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
pub async fn prepare_native_generation_transition_with_work_guarded<A, O, F, Fut>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    mut before_effect: F,
)-> Result<crate::OperationReceipt<OwnedMaterializationJournal>, (NativeWorkspacePublicationError, crate::WorkCounters)>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), true, &mut before_effect).await;
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
) -> Result<OwnedMaterializationJournal, NativeWorkspacePublicationError>
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
    let mut root_owned = work.copy_path(root)?;
    let mut operation_owned = work.copy_path(operation_directory)?;
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
    }).await??;
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
            move || NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, path_custody, work)
        })
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
            let (reset, stage_identity) = materializer.reset_stage_owned_guarded(
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
                existing = materializer.stage_generation_owned_guarded(
                    to_generation, &install_paths, &path_options, existing,
                    &work.native_mount_work(), before_effect,
                ).await?;
                before_effect().await?;
                materializer.verify_allocation(&existing).await?;
            }
        }
        let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
        if prepare_only {
            if existing.phase == MaterializationPhase::Prepared { return Ok(existing); }
            return materializer.capture_preimages_with_guard(existing, &mut guarded).await;
        }
        return materializer
            .recover_journal_with_guard(existing, MaterializationRecovery::Complete, &mut guarded)
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
    let held_root = crate::native_host::HostRoot::open_materialization_measured(&root, &work)?;
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
    let parent = crate::native_host::HostRoot::open_materialization_measured(operation_directory.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &work)?;
    let journal = MaterializationJournal {
        version: JOURNAL_VERSION, revision: 0, plan,
        root_identity: Some(source_root_identity.to_bytes()), preparation_identity: None,
        preparation_progress: Some(MaterializationPreparationProgress { parent: parent.identity().to_bytes(), operation: None, target: None, backup: None, pending: None }),
        work: work.journal_work()?, preimages: Vec::new(),
        phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
    };
    let journal = OwnedMaterializationJournal { journal, custody: Some(plan_custody) };
    drop(parent);
    let (stored, original) = compare_next_journal::<_, NativeTreeMaterializationError>(&store, journal).await?;
    if !stored { return Err(NativeWorkspacePublicationError::MismatchedJournal); }
    let mut prepared = NativeTreeMaterializationBackend::prepare_allocations(&store, original, &root, &operation_directory, &work, before_effect).await?;
    let backend = acyclic_native_runtime::run_blocking_io({
        let work = work.clone();
        move || NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, path_custody, work)
    }).await??;
    let materializer = JournaledMaterializer::new(store, backend);
    materializer.verify_allocation(&prepared).await?;
    let allocated_target = crate::NativeRootIdentity::from_bytes(prepared.preparation_identity.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?.target);
    let (path_options, _options_custody) = native_stage_options(options, materializer.backend().target.as_path(), allocated_target, &work)?;
    if !install_paths.is_empty() {
        before_effect().await?;
        materializer.verify_allocation(&prepared).await?;
        prepared = materializer.stage_generation_owned_guarded(
            to_generation, &install_paths, &path_options, prepared,
            &work.native_mount_work(), before_effect,
        ).await?;
        before_effect().await?;
        materializer.verify_allocation(&prepared).await?;
    }
    let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
    if prepare_only {
        return materializer.capture_preimages_with_guard(prepared, &mut guarded).await;
    }
    materializer
        .apply_journal_with_guard(prepared, &mut guarded)
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
        // Only the first unapplied endpoint can be compared to the whole
        // original selected source. Partial/postimage recovery uses its durable
        // per-edit preimages instead of rejecting our own completed effects.
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
        let parent = match root.open_dir_materialization_measured(host_path.parent().unwrap_or_else(|| Path::new("")), work) {
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
                            let published = generation.read_symbolic_link_with_native_work(&absolute, work, cancellation).await?;
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
fn native_join_generation_child(path: &str, name: &crate::LogicalName, work: &NativeMaterializationWork) -> Result<(String, NativeAllocation), NativeTreeMaterializationError> {
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
    let directory = match root.open_dir_materialization_measured(Path::new(path), work) {
        Ok(directory) => directory,
        Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if crate::native_host::directory_identity_materialization_measured(&directory, work)? != native_stat_identity(observed) {
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
    let published = generation.read_with_native_work(&absolute, metadata.len(), work, cancellation).await?;
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
        // Reading content may itself update access time. Its original value was
        // checked before opening, while immutable identity/content and all
        // remaining represented scalar metadata are checked again here.
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
    /// The real original typed cause in the existing pre-funded stage carrier,
    /// retaining both the last durable lower bound and this invocation's prefix.
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
    journals: MemoryRecords<OperationId, OwnedMaterializationJournal>,
}

/// A pre-funded completion in the original process-local journal adapter.
#[cfg(not(target_arch = "wasm32"))]
pub struct MemoryMaterializationActionCompletion<'a> {
    store: &'a MemoryMaterializationJournalStore,
    journal: OwnedMaterializationJournal,
    replacement: OwnedMaterializationJournal,
    work: Option<NativeMaterializationWork>,
    expected_revision: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl MaterializationActionCompletion for MemoryMaterializationActionCompletion<'_> {
    type Error = MemoryMaterializationJournalStoreError;

    async fn complete(self) -> Result<OwnedMaterializationJournal, Self::Error> {
        let Self { store, mut journal, mut replacement, work, expected_revision } = self;
        if let Some(work) = &work {
            let completed = match work.journal_work() {
                Ok(completed) => completed,
                Err(error) => return Err(Self::Error::ActionNativeWork(error, journal)),
            };
            journal.work = completed;
            replacement.work = completed;
        }
        let retired = {
            let mut journals = match store.journals.0.lock() {
                Ok(journals) => journals,
                Err(_) => return Err(Self::Error::ActionUnavailable(journal)),
            };
            let Some(stored) = journals.get_mut(&journal.plan.operation_id) else {
                drop(journals);
                return Err(Self::Error::ActionConflict(journal));
            };
            if stored.revision != expected_revision {
                drop(journals);
                return Err(Self::Error::ActionConflict(journal));
            }
            // This existing BTree slot needs no new node or payload backing.
            std::mem::replace(stored, replacement)
        };
        // Retire the former payload and its accounting outside the map lock.
        drop(retired);
        Ok(journal)
    }
}

impl crate::record_store::Revisioned for OwnedMaterializationJournal {
    fn revision(&self) -> u64 { self.journal.revision }
}

impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> = MemoryMaterializationActionCompletion<'a> where Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(
        &'a self,
        mut journal: OwnedMaterializationJournal,
    ) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        if let Some(custody) = &journal.custody {
            let bytes = match journal_owned_bytes(&journal.journal) {
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
            let journals = match self.journals.0.lock() {
                Ok(journals) => journals,
                Err(_) => return Err(Self::Error::ActionUnavailable(journal)),
            };
            let Some(stored) = journals.get(&journal.plan.operation_id) else {
                drop(journals);
                return Err(Self::Error::ActionConflict(journal));
            };
            if stored.journal != journal.journal {
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
                    let bytes = match journal_owned_bytes(&stored.journal) {
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
                // The input was proven equal to the actual stored journal.
                // Copy its payload outside the map lock, using the captured
                // stored original ledger and its exact resource attribution.
                match duplicate_native_journal(&journal.journal, work) {
                    Ok(replacement) => replacement,
                    Err(error) => return Err(Self::Error::ActionNativeWork(error, journal)),
                }
            }
            None => OwnedMaterializationJournal::ordinary(journal.journal.clone()),
        };
        if let Some(work) = &work
            && let Err(error) = work.admit(crate::WorkCounters::UNCHARGED) {
            return Err(Self::Error::ActionNativeWork(error, journal));
        }
        journal.revision = next_revision;
        replacement.revision = next_revision;
        Ok(MemoryMaterializationActionCompletion {
            store: self, journal, replacement, work, expected_revision,
        })
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OwnedMaterializationJournal>, Self::Error> {
        let journals = self.journals.0.lock().map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
        journals.get(&operation_id).map(duplicate_owned_journal).transpose()
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: OwnedMaterializationJournal,
    ) -> Result<(bool, OwnedMaterializationJournal), Self::Error> {
        let mut journals = self.journals.0.lock().map_err(|_| MemoryMaterializationJournalStoreError::Unavailable)?;
        let stored = crate::record_store::stored_revision(&journals, &operation_id) == expected_revision;
        if stored {
            let copied = duplicate_owned_journal(&replacement)?;
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
    ActionConflict(OwnedMaterializationJournal),
    /// An admitted action could not access its store; its packet stays owned.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory action completion store is unavailable")]
    ActionUnavailable(OwnedMaterializationJournal),
    /// Original preparation or completion work failed with its journal intact.
    #[cfg(not(target_arch = "wasm32"))]
    #[error("materialization memory action completion native work failed: {0}")]
    ActionNativeWork(#[source] NativeTreeMaterializationError, OwnedMaterializationJournal),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Digest;
    use proptest::collection::btree_map;
    use proptest::option;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};

    #[derive(Default)]
    struct Backend(Mutex<Vec<Vec<u8>>>);

    #[derive(Debug, Error)]
    enum BackendError {
        #[error("test backend unavailable")]
        Unavailable,
        #[error(transparent)]
        Memory(#[from] MemoryMaterializationJournalStoreError),
    }

    impl MaterializationBackend for Backend {
        type Error = BackendError;

        fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
            journal.preimages.push(preimage.preimage);
            Ok(())
        }

        async fn capture(
            &self,
            _edit: &MaterializationEdit,
        ) -> Result<OwnedMaterializationPreimage, Self::Error> {
            Ok(OwnedMaterializationPreimage::ordinary(MaterializationPreimage { image: vec![0] }))
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

    /// Flat path model whose writes, like every journal CAS, are durable
    /// steps; the step numbered `crash_at` takes effect and then "crashes".
    #[derive(Default)]
    struct Crashing {
        tree: Mutex<BTreeMap<String, u8>>,
        steps: AtomicU64,
        crash_at: AtomicU64,
    }

    impl Crashing {
        fn step(&self) -> Result<(), BackendError> {
            let step = self.steps.fetch_add(1, SeqCst) + 1;
            if step == self.crash_at.load(SeqCst) {
                return Err(BackendError::Unavailable);
            }
            Ok(())
        }

        fn tree(&self) -> BTreeMap<String, u8> {
            self.tree.lock().expect("tree lock").clone()
        }

        fn current(&self, path: &str) -> Result<Option<u8>, BackendError> {
            Ok(self
                .tree
                .lock()
                .map_err(|_| BackendError::Unavailable)?
                .get(path)
                .copied())
        }

        fn set(&self, path: &str, value: Option<u8>) -> Result<(), BackendError> {
            let mut tree = self.tree.lock().map_err(|_| BackendError::Unavailable)?;
            match value {
                Some(value) => tree.insert(path.to_owned(), value),
                None => tree.remove(path),
            };
            drop(tree);
            self.step()
        }
    }

    fn endpoints(edit: &MaterializationEdit) -> (&str, Option<u8>) {
        match edit {
            MaterializationEdit::Install { path, image } => (path, image.first().copied()),
            MaterializationEdit::Remove { path } => (path, None),
            _ => ("", None),
        }
    }

    impl MaterializationBackend for &Crashing {
        type Error = BackendError;
        fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
            journal.preimages.push(preimage.preimage);
            Ok(())
        }


        async fn capture(
            &self,
            edit: &MaterializationEdit,
        ) -> Result<OwnedMaterializationPreimage, Self::Error> {
            let value = self.current(endpoints(edit).0)?;
            Ok(OwnedMaterializationPreimage::ordinary(MaterializationPreimage {
                image: value.into_iter().collect(),
            }))
        }

        async fn observe(
            &self,
            edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<MaterializationObservation, Self::Error> {
            let (path, postimage) = endpoints(edit);
            let current = self.current(path)?;
            Ok(if current == postimage {
                MaterializationObservation::Postimage
            } else if current == preimage.image.first().copied() {
                MaterializationObservation::Preimage
            } else {
                MaterializationObservation::Diverged
            })
        }

        async fn apply(
            &self,
            edit: &MaterializationEdit,
            _preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            let (path, postimage) = endpoints(edit);
            self.set(path, postimage)
        }

        async fn restore(
            &self,
            edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            self.set(endpoints(edit).0, preimage.image.first().copied())
        }
    }

    /// Journal store whose compare-and-swaps are durable steps of `.1`.
    struct CrashingStore<'a>(&'a MemoryMaterializationJournalStore, &'a Crashing);

    #[cfg(not(target_arch = "wasm32"))]
    struct CrashingActionCompletion<'a, C> {
        actual: C,
        crashing: &'a Crashing,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl<C: MaterializationActionCompletion<Error = MemoryMaterializationJournalStoreError>>
        MaterializationActionCompletion for CrashingActionCompletion<'_, C>
    {
        type Error = BackendError;

        async fn complete(self) -> Result<OwnedMaterializationJournal, Self::Error> {
            let journal = self.actual.complete().await?;
            self.crashing.step()?;
            Ok(journal)
        }
    }

    impl MaterializationJournalStore for CrashingStore<'_> {
        type Error = BackendError;

        #[cfg(not(target_arch = "wasm32"))]
        type ActionCompletion<'a> = CrashingActionCompletion<'a,
            <MemoryMaterializationJournalStore as MaterializationJournalStore>::ActionCompletion<'a>>
        where Self: 'a;

        #[cfg(not(target_arch = "wasm32"))]
        async fn prepare_action<'a>(&'a self, journal: OwnedMaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
            Ok(CrashingActionCompletion {
                actual: self.0.prepare_action(journal).await?,
                crashing: self.1,
            })
        }

        async fn load(
            &self,
            operation_id: OperationId,
        ) -> Result<Option<OwnedMaterializationJournal>, Self::Error> {
            self.0.load(operation_id).await.map_err(BackendError::from)
        }

        async fn compare_and_swap(
            &self,
            operation_id: OperationId,
            expected_revision: u64,
            replacement: OwnedMaterializationJournal,
        ) -> Result<(bool, OwnedMaterializationJournal), Self::Error> {
            let swapped = self
                .0
                .compare_and_swap(operation_id, expected_revision, replacement)
                .await
                .map_err(BackendError::from)?;
            self.1.step()?;
            Ok(swapped)
        }
    }

    /// Applies `plan` to `original`, crashing after durable step `crash_at`
    /// (zero never crashes).
    fn crashing_apply(
        plan: &MaterializationPlan,
        original: &BTreeMap<String, u8>,
        crash_at: u64,
    ) -> (Crashing, MemoryMaterializationJournalStore, bool) {
        let backend = Crashing::default();
        original.clone_into(&mut backend.tree.lock().expect("tree lock"));
        backend.crash_at.store(crash_at, SeqCst);
        let journals = MemoryMaterializationJournalStore::default();
        let crashed = futures::executor::block_on(
            JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend)
                .apply(plan.clone()),
        )
        .is_err();
        (backend, journals, crashed)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// A crash after any durable step, followed by either recovery,
        /// leaves exactly the planned tree or exactly the original one.
        #[test]
        fn recovery_after_a_crash_at_any_durable_step_is_all_or_nothing(
            original in btree_map("[a-e]", any::<u8>(), 0..5),
            targets in btree_map("[a-e]", option::of(any::<u8>()), 1..5),
            crash in any::<u64>(),
            forward in any::<bool>(),
        ) {
            let plan = MaterializationPlan {
                operation_id: OperationId::from_bytes([7; 16]),
                intent: MaterializationIntent::GenerationTransition {
                    from: GenerationId::new(Digest::from_bytes([1; 32])),
                    to: GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: targets
                    .iter()
                    .map(|(path, value)| match value {
                        Some(value) => MaterializationEdit::Install {
                            path: path.clone(),
                            image: vec![*value],
                        },
                        None => MaterializationEdit::Remove { path: path.clone() },
                    })
                    .collect(),
            };
            let mut planned = original.clone();
            for (path, value) in &targets {
                match value {
                    Some(value) => planned.insert(path.clone(), *value),
                    None => planned.remove(path),
                };
            }
            let (clean, _, crashed) = crashing_apply(&plan, &original, 0);
            prop_assert!(!crashed);
            prop_assert_eq!(clean.tree(), planned.clone());
            let steps = clean.steps.load(SeqCst);

            let (backend, journals, crashed) = crashing_apply(&plan, &original, 1 + crash % steps);
            prop_assert!(crashed);
            backend.crash_at.store(0, SeqCst);
            let restarted = JournaledMaterializer::new(CrashingStore(&journals, &backend), &backend);
            let recovery = if forward {
                MaterializationRecovery::Complete
            } else {
                MaterializationRecovery::RollBack
            };
            let journal = futures::executor::block_on(restarted.recover(plan.operation_id, recovery))
                .expect("recover")
                .expect("journal");
            if forward {
                prop_assert_eq!(journal.phase, MaterializationPhase::Applied);
                prop_assert_eq!(backend.tree(), planned);
            } else {
                prop_assert_eq!(journal.phase, MaterializationPhase::RolledBack);
                prop_assert_eq!(backend.tree(), original);
            }
        }
    }

    #[tokio::test]
    async fn obsolete_journal_versions_reject_recovery_without_effects() {
        for version in [0, 1, 3, u32::MAX] {
            let operation_id = OperationId::new();
            let journal = MaterializationJournal {
                version,
                revision: 1,
                root_identity: None,
                preparation_identity: None,
                preparation_progress: None,
                work: crate::WorkCounters::default(),
                plan: MaterializationPlan {
                    operation_id,
                    intent: MaterializationIntent::GenerationTransition {
                        from: GenerationId::new(Digest::from_bytes([1; 32])),
                        to: GenerationId::new(Digest::from_bytes([2; 32])),
                    },
                    edits: vec![MaterializationEdit::Install {
                        path: "file".into(),
                        image: vec![9],
                    }],
                },
                preimages: vec![MaterializationPreimage { image: vec![0] }],
                phase: MaterializationPhase::Prepared,
                applied: 0,
                restored: 0,
            };
            let store = MemoryMaterializationJournalStore::default();
            store
                .compare_and_swap(operation_id, 0, OwnedMaterializationJournal::ordinary(journal.clone()))
                .await
                .expect("seed obsolete journal");
            let materializer = JournaledMaterializer::new(store, Backend::default());
            assert!(matches!(
                materializer
                    .recover(operation_id, MaterializationRecovery::Complete)
                    .await,
                Err(MaterializationError::IncompatibleJournal)
            ));
            assert!(materializer.backend.0.lock().expect("backend").is_empty());
            assert_eq!(
                materializer
                    .store
                    .load(operation_id)
                    .await
                    .expect("retained journal").map(|owned| owned.journal),
                Some(journal)
            );
        }
    }
    #[tokio::test]
    async fn apply_and_rollback_are_journaled_and_idempotent() {
        let operation_id = OperationId::new();
        let materializer = JournaledMaterializer::new(
            MemoryMaterializationJournalStore::default(),
            Backend::default(),
        );
        let plan = MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: vec![MaterializationEdit::Install {
                path: "file".to_owned(),
                image: vec![9],
            }],
        };
        let applied = materializer.apply(plan.clone()).await.expect("apply");
        assert_eq!(applied.phase, MaterializationPhase::Applied);
        assert_eq!(
            materializer.apply(plan).await.expect("retry").phase,
            MaterializationPhase::Applied
        );
        assert_eq!(
            materializer
                .backend()
                .0
                .lock()
                .expect("backend lock")
                .as_slice(),
            &[vec![9]]
        );
        let rolled_back = materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("recover")
            .expect("journal");
        assert_eq!(rolled_back.phase, MaterializationPhase::RolledBack);
    }

    #[tokio::test]
    async fn interrupted_rollback_cannot_be_completed_forward() {
        let operation_id = OperationId::new();
        let store = MemoryMaterializationJournalStore::default();
        let plan = MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: vec![MaterializationEdit::Install {
                path: "file".to_owned(),
                image: vec![9],
            }],
        };
        store
            .compare_and_swap(
                operation_id,
                0,
                OwnedMaterializationJournal::ordinary(MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: None,
                    preparation_identity: None,
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan,
                    preimages: vec![MaterializationPreimage { image: vec![0] }],
                    phase: MaterializationPhase::RollingBack,
                    applied: 1,
                    restored: 1,
                }),
            )
            .await
            .expect("store journal");
        let materializer = JournaledMaterializer::new(store, Backend::default());
        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::Complete)
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
            materializer.apply(plan).await,
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["old.txt".to_owned(), "new.txt".to_owned()],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply tree");
        assert!(!root.join("old.txt").exists());
        assert_eq!(std::fs::read(root.join("new.txt")).expect("new"), b"new");
        assert_eq!(
            std::fs::read(root.join(".git/config")).expect("git"),
            b"git"
        );
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("rollback");
        assert_eq!(std::fs::read(root.join("old.txt")).expect("old"), b"old");
        assert!(!root.join("new.txt").exists());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_pauses_before_overwriting_an_external_mutation() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
        let store = MemoryMaterializationJournalStore::default();
        store
            .compare_and_swap(
                plan.operation_id,
                0,
                captured_fixture_journal(MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: backend.allocation_identity(),
                    preparation_identity: backend.preparation_identity(),
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan: plan.clone(),
                    preimages: Vec::new(),
                    phase: MaterializationPhase::Prepared,
                    applied: 0,
                    restored: 0,
                }, preimage, &backend),
            )
            .await
            .expect("journal");
        std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

        assert!(matches!(
            JournaledMaterializer::new(store, backend).apply(plan).await,
            Err(MaterializationError::ExternalMutation)
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external remains"),
            b"external"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_rejects_tampered_or_missing_staged_targets() {
        for replacement in [Some(b"tampered".as_slice()), None] {
            let temporary = tempfile::tempdir().expect("temporary root");
            let root = temporary.path().join("checkout");
            let operation = temporary.path().join("operation");
            let target = operation.join("target");
            std::fs::create_dir_all(&root).expect("root");
            std::fs::create_dir_all(&target).expect("target");
            std::fs::write(root.join("file.txt"), b"before").expect("before");
            std::fs::write(target.join("file.txt"), b"after").expect("after");
            let backend =
                NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
            let plan = backend
                .plan_paths(
                    OperationId::new(),
                    GenerationId::new(Digest::from_bytes([1; 32])),
                    GenerationId::new(Digest::from_bytes([2; 32])),
                    ["file.txt".to_owned()],
                )
                .expect("plan");
            let edit = plan.edits.first().expect("install edit");
            let preimage = backend.capture(edit).await.expect("capture");
            match replacement {
                Some(bytes) => std::fs::write(target.join("file.txt"), bytes).expect("tamper"),
                None => std::fs::remove_file(target.join("file.txt")).expect("delete target"),
            }
            assert_eq!(
                backend.observe(edit, &preimage.preimage).await.expect("observe"),
                MaterializationObservation::Diverged
            );
            assert_eq!(
                std::fs::read(root.join("file.txt")).expect("live remains"),
                b"before"
            );
        }
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
        let backend = NativeTreeMaterializationBackend::new_with_work(
            root.clone(), operation.clone(), work.clone(),
        ).expect("native backend");
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
    async fn refused_observation_diagnostic_preserves_original_native_cause_and_guard() {
        let temporary = tempfile::tempdir().expect("actual native operation");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("original user file");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("held native backend");
        let edit = MaterializationEdit::Install { path: "file.txt".to_owned(), image: Vec::new() };
        let error = backend.capture(&edit).await.expect_err("real missing staged target");
        let live = work.live_allocation_bytes();
        cancellation.cancel();
        let error = backend.describe_observation_error(error).expect_err("diagnostic formatting refused after cancellation");
        assert!(matches!(&error, NativeTreeMaterializationError::MissingTarget(path, Some(allocation))
            if path == "file.txt" && allocation.work().shares_ledger(&work)));
        assert_eq!(work.live_allocation_bytes(), live, "failed formatter retires only its own scratch, not the original cause");
        work.receipt().verify(budget).expect("original failure and formatter prefix remain available");
        assert_eq!(std::fs::read(root.join("file.txt")).expect("user file remains"), b"before");
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = &plan.edits[0];
        let preimage = backend.capture(edit).await.expect("capture");

        std::fs::rename(root.join("file.txt"), backup.join("file.txt"))
            .expect("simulate crash after backup rename");
        assert_eq!(
            backend.observe(edit, &preimage.preimage).await.expect("observe"),
            MaterializationObservation::Interrupted
        );
        backend
            .restore(edit, &preimage.preimage)
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
                NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
            let plan = backend
                .plan_paths(
                    OperationId::new(),
                    GenerationId::new(Digest::from_bytes([1; 32])),
                    GenerationId::new(Digest::from_bytes([2; 32])),
                    ["file.txt".to_owned()],
                )
                .expect("plan");
            let edit = plan.edits.first().expect("install edit");
            let preimage = backend.capture(edit).await.expect("capture");
            assert_eq!(
                backend.observe(edit, &preimage.preimage).await.expect("observe"),
                MaterializationObservation::Preimage
            );
            let changed = if mutate_target {
                target.join("file.txt")
            } else {
                root.join("file.txt")
            };
            std::fs::write(&changed, b"external").expect("mutate after observe");
            assert!(matches!(
                backend.apply(edit, &preimage.preimage).await,
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let edit = plan.edits.first().expect("install edit");
        let preimage = backend.capture(edit).await.expect("capture");
        backend.apply(edit, &preimage.preimage).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external write");

        assert!(matches!(
            backend.restore(edit, &preimage.preimage).await,
            Err(NativeTreeMaterializationError::ExternalMutation(path, Some(_))) if path == "file.txt"
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external content survives"),
            b"external"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_journals_directory_metadata_without_replacing_children() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir_all(root.join("directory")).expect("directory");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("directory/child.txt"), b"child").expect("child");
        let before = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("before metadata"),
        );
        let mut desired = before.clone();
        desired.readonly = !before.readonly;
        #[cfg(windows)]
        {
            const FILE_ATTRIBUTE_HIDDEN: u32 = 0x2;
            desired.windows_attributes = before
                .windows_attributes
                .map(|attributes| attributes ^ FILE_ATTRIBUTE_HIDDEN);
            desired.readonly = desired
                .windows_attributes
                .is_some_and(|attributes| attributes & 1 != 0);
        }
        #[cfg(unix)]
        {
            desired.posix_mode = before.posix_mode.map(|mode| mode ^ 0o200);
        }
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let operation_id = OperationId::new();
        let plan = MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: vec![MaterializationEdit::SetMetadata {
                path: "directory".to_owned(),
                image: native_metadata_bytes(&desired).to_vec(),
            }],
        };
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply metadata");
        assert_eq!(
            std::fs::read(root.join("directory/child.txt")).expect("child remains"),
            b"child"
        );
        let applied = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("applied metadata"),
        );
        assert_eq!(applied.readonly, desired.readonly);
        #[cfg(windows)]
        assert_eq!(applied.windows_attributes, desired.windows_attributes);
        #[cfg(unix)]
        assert_eq!(applied.posix_mode, desired.posix_mode);

        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
            .await
            .expect("rollback metadata");
        let restored = native_metadata(
            &std::fs::symlink_metadata(root.join("directory")).expect("restored metadata"),
        );
        assert_eq!(restored.readonly, before.readonly);
        #[cfg(windows)]
        assert_eq!(restored.windows_attributes, before.windows_attributes);
        #[cfg(unix)]
        assert_eq!(restored.posix_mode, before.posix_mode);
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
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
            backend.observe(&edit, &preimage.preimage).await.expect("observe"),
            MaterializationObservation::Preimage
        );
        backend
            .apply(&edit, &preimage.preimage)
            .await
            .expect("apply metadata");
        assert_eq!(
            std::fs::read(&child).expect("read child"),
            b"concurrent child write"
        );
        backend
            .restore(&edit, &preimage.preimage)
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
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
            backend.observe(&edit, &preimage.preimage).await.expect("observe"),
            MaterializationObservation::Diverged
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_recognizes_apply_before_progress_was_persisted() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let preimage = backend.capture(&plan.edits[0]).await.expect("capture");
        let store = MemoryMaterializationJournalStore::default();
        store
            .compare_and_swap(
                plan.operation_id,
                0,
                captured_fixture_journal(MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: backend.allocation_identity(),
                    preparation_identity: backend.preparation_identity(),
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan: plan.clone(),
                    preimages: Vec::new(),
                    phase: MaterializationPhase::Applying,
                    applied: 0,
                    restored: 0,
                }, preimage, &backend),
            )
            .await
            .expect("journal");
        let retained = store.load(plan.operation_id).await.expect("paid source clone").expect("journal");
        backend
            .apply(&plan.edits[0], &retained.preimages[0])
            .await
            .expect("physical apply");

        let recovered = JournaledMaterializer::new(store, backend)
            .recover(plan.operation_id, MaterializationRecovery::Complete)
            .await
            .expect("recover")
            .expect("journal");
        assert_eq!(recovered.phase, MaterializationPhase::Applied);
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("after remains"),
            b"after"
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_tree_backend_pauses_rollback_after_external_mutation() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&target).expect("target");
        std::fs::write(root.join("file.txt"), b"before").expect("before");
        std::fs::write(target.join("file.txt"), b"after").expect("after");
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["file.txt".to_owned()],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external mutation");

        assert!(matches!(
            materializer
                .recover(operation_id, MaterializationRecovery::RollBack)
                .await,
            Err(MaterializationError::ExternalMutation)
        ));
        assert_eq!(
            std::fs::read(root.join("file.txt")).expect("external remains"),
            b"external"
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
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let plan = backend
            .plan_paths(
                OperationId::new(),
                GenerationId::new(Digest::from_bytes([1; 32])),
                GenerationId::new(Digest::from_bytes([2; 32])),
                ["link".to_owned()],
            )
            .expect("plan");
        let operation_id = plan.operation_id;
        let materializer =
            JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), backend);
        materializer.apply(plan).await.expect("remove link");
        assert!(std::fs::symlink_metadata(root.join("link")).is_err());
        materializer
            .recover(operation_id, MaterializationRecovery::RollBack)
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
            NativeTreeMaterializationBackend::new(&root, &operation),
            Err(NativeTreeMaterializationError::InvalidLayout)
        ));
        assert_eq!(std::fs::read(sentinel).expect("sentinel remains"), b"keep");
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
        let (bytes, custody) = work.read_working_file_guarded(temporary.path(), identity, Path::new("file"),
            binary.len(), || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect("whole binary read");
        assert_eq!(bytes, binary);
        assert_eq!(custody.bytes(), bytes.capacity() as u64);
        assert!(custody.shares_ledger(&work.retain_owned(0).expect("same original ledger")));
        assert_eq!(work.receipt().source_bytes_read, binary.len() as u64);
        assert!(work.live_allocation_bytes() >= NativeMaterializationWork::accounting_state_bytes() + bytes.capacity() as u64);
        drop(bytes);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        let failure = work.read_working_file_guarded(temporary.path(), identity, Path::new("file"),
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
            let denied = work.write_working_file_guarded(temporary.path(), identity, Path::new(path), "payload", true, None,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("noncanonical relative path");
            assert!(matches!(denied, NativeTreeMaterializationError::InvalidLayout));
        }
        let oversized = "x".repeat(NativeMaterializationWork::MAXIMUM_BUILTIN_BYTES + 1);
        let denied = work.write_working_file_guarded(temporary.path(), identity, Path::new("new/file"), &oversized, true, None,
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
            assert!(work.read_working_file_guarded(&root, identity, Path::new(path), 32,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.is_err());
            assert!(work.write_working_file_guarded(&root, identity, Path::new(path), "overwrite", true, None,
                || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.is_err());
        }
        assert_eq!(std::fs::read(external.join("file")).expect("external remains"), b"keep");
        assert_eq!(work.receipt().output_bytes, 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_rechecks_replaced_root_and_opened_leaf() {
        for replace_root in [true, false] {
            let temporary = tempfile::tempdir().expect("real protected root");
            let root = temporary.path().join("root");
            let moved = temporary.path().join("moved");
            std::fs::create_dir(&root).expect("root");
            std::fs::write(root.join("file"), b"original").expect("original file");
            let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original work");
            let identity = work.root_identity(&root).expect("original witness");
            let calls = std::cell::Cell::new(0);
            let failed = work.read_working_file_guarded(&root, identity, Path::new("file"), 32, || {
                calls.set(calls.get() + 1);
                if calls.get() == if replace_root { 2 } else { 4 } {
                    if replace_root {
                        std::fs::rename(&root, &moved).expect("replace original root after held open");
                        std::fs::create_dir(&root).expect("replacement root");
                    } else {
                        std::fs::rename(root.join("file"), &moved).expect("replace original opened leaf");
                    }
                    std::fs::write(root.join("file"), b"replacement").expect("replacement user file");
                }
                std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))
            }).await.expect_err("held binding replacement must fail");
            assert!(matches!(failed, NativeTreeMaterializationError::AllocationChanged { .. }));
            assert_eq!(std::fs::read(root.join("file")).expect("replacement untouched"), b"replacement");
            let original = if replace_root { moved.join("file") } else { moved };
            assert_eq!(std::fs::read(original).expect("original untouched"), b"original");
            assert_eq!(work.receipt().output_bytes, 0);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_builtin_edit_compares_exact_preimage_and_creates_held_parent_directories() {
        use sha2::{Digest as _, Sha256};
        let temporary = tempfile::tempdir().expect("real root");
        std::fs::write(temporary.path().join("file"), b"before").expect("preimage");
        let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original work");
        let identity = work.root_identity(temporary.path()).expect("original witness");
        let denied = work.write_working_file_guarded(temporary.path(), identity, Path::new("file"), "after", false, Some([0; 32]),
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect_err("wrong exact preimage digest");
        assert!(matches!(denied, NativeTreeMaterializationError::PreimageConflict));
        assert_eq!(std::fs::read(temporary.path().join("file")).expect("no mismatch mutation"), b"before");
        assert_eq!(work.receipt().output_bytes, 0);
        let digest: [u8; 32] = Sha256::digest(b"before").into();
        work.write_working_file_guarded(temporary.path(), identity, Path::new("file"), "", false, Some(digest),
            || std::future::ready(Ok::<(), NativeTreeMaterializationError>(()))).await.expect("same held preimage edit can truncate to empty");
        assert_eq!(std::fs::read(temporary.path().join("file")).expect("empty postimage"), b"");
        work.write_working_file_guarded(temporary.path(), identity, Path::new("new/nested/file"), "héllo", true, None,
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
                work.write_working_file_guarded(temporary.path(), identity, Path::new("file"), "changed", false, None, guard).await
            } else {
                work.read_working_file_guarded(temporary.path(), identity, Path::new("file"), 32, guard).await.map(|(bytes, custody)| {
                    drop(bytes);
                    drop(custody);
                })
            }.expect_err("late cancellation cannot acknowledge success");
            assert!(matches!(failed, NativeTreeMaterializationError::Cancelled));
            let actual = work.receipt();
            assert_eq!(if writing { actual.output_bytes } else { actual.source_bytes_read }, if writing { 7 } else { 6 });
            assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
            assert_eq!(std::fs::read(temporary.path().join("file")).expect("actual physical prefix"), if writing { b"changed".as_slice() } else { b"before".as_slice() });
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_plan_validation_stops_at_exact_byte_prefix_before_journal_io() {
        let temporary = tempfile::tempdir().expect("temporary operation");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        let state_path = temporary.path().join("state");
        std::fs::create_dir(&root).expect("root fixture");
        let backend = NativeTreeMaterializationBackend::new(&root, &operation).expect("held backend fixture");
        let engine = JournaledMaterializer::new(crate::LocalCoreStateStore::new(&state_path), backend);
        let plan = MaterializationPlan {
            operation_id: OperationId::new(),
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(crate::Digest::from_bytes([1; 32])),
                to: GenerationId::new(crate::Digest::from_bytes([2; 32])),
            },
            edits: vec![MaterializationEdit::Remove { path: "a".repeat(1024 * 1024) }],
        };
        let mut budget = finite_native_budget();
        budget.items_examined = 4;
        let failure = engine.prepare_bounded(plan, budget, &crate::CancellationToken::new()).await.expect_err("one edit plus three actual path bytes");
        assert!(matches!(failure.0, MaterializationError::Backend(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(failure.1.items_examined, 4);
        assert_eq!(failure.1.backend_read_operations, 0);
        assert_eq!(failure.1.backend_write_operations, 0);
        assert!(!state_path.exists());
        assert_eq!(std::fs::read_dir(root).expect("untouched root").count(), 0);
        failure.1.verify(budget).expect("actual validation prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn current_directory_retains_actual_original_capacity_without_body_reads() {
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
            .expect("original operation");
        let alias = work.clone();
        let (path, allocation) = work.current_directory().expect("actual native CWD");
        assert!(path.is_absolute());
        assert_eq!(allocation.bytes(), path.capacity() as u64);
        assert_eq!(alias.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes() + allocation.bytes());
        assert!(alias.receipt().backend_read_operations > 0);
        assert_eq!(alias.receipt().source_bytes_read, 0);
        drop(work);
        drop(path);
        assert!(alias.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(allocation);
        assert_eq!(alias.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        alias.receipt().verify(budget).expect("one original finite receipt");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn current_directory_read_refusal_precedes_native_scratch_allocation() {
        let mut budget = finite_native_budget();
        budget.backend_read_operations = 0;
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
            .expect("original operation");
        let before = work.receipt();
        assert!(matches!(work.current_directory(),
            Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "backend_read_operations", ..
            }))));
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn current_directory_cancellation_precedes_native_io_and_allocation() {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation)
            .expect("original operation");
        let before = work.receipt();
        cancellation.cancel();
        assert!(work.current_directory().is_err());
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn current_directory_capacity_refusal_preserves_only_started_os_prefix() {
        let mut budget = finite_native_budget();
        budget.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
            .expect("original operation");
        assert!(matches!(work.current_directory(),
            Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }))));
        assert_eq!(work.receipt().allocation_operations, 1);
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        #[cfg(target_os = "linux")]
        assert_eq!(work.receipt().backend_read_operations, 0);
        #[cfg(any(windows, target_os = "macos"))]
        assert_eq!(work.receipt().backend_read_operations, 1);
        work.receipt().verify(budget).expect("actual failed prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    const NATIVE_ENVIRONMENT_FIXTURE: &str = "ACYCLIC_NATIVE_ENVIRONMENT_FIXTURE";

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    #[ignore = "runs only as a child of native_environment_real_process_controls_are_bounded"]
    fn native_environment_child() {
        use std::ffi::OsStr;
        let Some(expected) = std::env::var_os(NATIVE_ENVIRONMENT_FIXTURE) else { return; };
        let name = OsStr::new(NATIVE_ENVIRONMENT_FIXTURE);
        let bounded = || {
            let mut budget = finite_native_budget();
            // Environment controls are metadata, never application BODY bytes.
            budget.source_bytes_read = 0;
            budget
        };
        let state_bytes = NativeMaterializationWork::accounting_state_bytes();
        let cancellation = crate::CancellationToken::new();
        let budget = bounded();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original meter");
        let original = std::sync::Arc::downgrade(&work.state);
        let result = work.environment_variable(name, expected.len()).expect("bounded native read").expect("present environment");
        assert_eq!(result.0, expected);
        assert!(work.shares_ledger(result.1.work()));
        assert_eq!(result.1.bytes(), result.0.capacity() as u64);
        assert_eq!(work.live_allocation_bytes(), state_bytes + result.0.capacity() as u64);
        assert_eq!(work.receipt().source_bytes_read, 0);
        // State, terminated name, and the concrete value/native UTF-16 buffer
        // are the only visible allocations. Opaque std decode is not another.
        assert_eq!(work.receipt().allocation_operations, 3);
        #[cfg(unix)]
        assert_eq!(work.receipt().backend_read_operations, 1);
        #[cfg(windows)]
        assert_eq!(work.receipt().backend_read_operations, 2);
        work.receipt().verify(budget).expect("actual original receipt");
        drop(work);
        assert!(original.upgrade().is_some(), "returned guard pins the actual original meter");
        drop(result.0);
        assert_eq!(result.1.work().live_allocation_bytes(), state_bytes + result.1.bytes());
        drop(result.1);
        assert!(original.upgrade().is_none(), "no extra root meter survives guard retirement");

        for counter in ["backend_read_operations", "allocation_operations", "peak_allocation_bytes", "name_encoding"] {
            let mut budget = bounded();
            match counter {
                "backend_read_operations" => budget.backend_read_operations = 0,
                "allocation_operations" => budget.allocation_operations = 1,
                "peak_allocation_bytes" => budget.peak_allocation_bytes = state_bytes,
                _ => {
                    #[cfg(unix)]
                    { budget.bytes_copied = 0; }
                    #[cfg(windows)]
                    { budget.bytes_encoded = 0; }
                }
            }
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("original meter");
            assert!(matches!(work.environment_variable(name, expected.len()),
                Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { .. }))));
            assert_eq!(work.receipt().backend_read_operations, 0);
            assert_eq!(work.receipt().allocation_operations, 1);
            assert_eq!(work.live_allocation_bytes(), state_bytes);
            work.receipt().verify(budget).expect("only actual denied prefix");
        }

        let work = NativeMaterializationWork::new(bounded(), &cancellation).expect("original meter");
        let invalid = work.environment_variable(OsStr::new("ACYCLIC\0INVALID"), 32).err().expect("invalid native name");
        assert!(matches!(&invalid, NativeTreeMaterializationError::Io(error)
            if error.kind() == std::io::ErrorKind::InvalidInput));
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().allocation_operations, 1);
        assert_eq!(work.live_allocation_bytes(), state_bytes);

        let work = NativeMaterializationWork::new(bounded(), &cancellation).expect("original meter");
        let oversized = work.environment_variable(name, expected.len() - 1).err().expect("no truncated value");
        assert!(matches!(&oversized, NativeTreeMaterializationError::Io(error)
            if error.kind() == std::io::ErrorKind::FileTooLarge));
        assert!(work.receipt().backend_read_operations > 0);
        assert_eq!(work.receipt().source_bytes_read, 0);
        assert_eq!(work.live_allocation_bytes(), state_bytes);

        let work = NativeMaterializationWork::new(bounded(), &cancellation).expect("original meter");
        assert!(work.environment_variable(OsStr::new("ACYCLIC_NATIVE_ENVIRONMENT_ABSENT"), 0).expect("native missing value").is_none());
        assert_eq!(work.live_allocation_bytes(), state_bytes);
        let empty = work.environment_variable(OsStr::new("ACYCLIC_NATIVE_ENVIRONMENT_EMPTY"), 0).expect("native empty value").expect("present empty value");
        assert!(empty.0.is_empty());
        assert_eq!(empty.1.bytes(), empty.0.capacity() as u64);
        drop(empty);
        assert_eq!(work.live_allocation_bytes(), state_bytes);

        #[cfg(unix)]
        {
            let mut budget = bounded();
            budget.items_examined = name.len() as u64 + 2;
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("original meter");
            assert!(matches!(work.environment_variable(name, expected.len()),
                Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
            assert_eq!(work.receipt().items_examined, name.len() as u64 + 2);
            assert_eq!(work.receipt().backend_read_operations, 1);
            assert_eq!(work.receipt().bytes_copied, name.len() as u64);
            assert_eq!(work.receipt().allocation_operations, 2);
            assert_eq!(work.live_allocation_bytes(), state_bytes);
        }
        #[cfg(windows)]
        {
            let mut budget = bounded();
            budget.backend_read_operations = 1;
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("original meter");
            assert!(matches!(work.environment_variable(name, expected.len()),
                Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "backend_read_operations", .. }))));
            assert_eq!(work.receipt().backend_read_operations, 1);
            assert_eq!(work.receipt().allocation_operations, 2, "refused retry never allocates its buffer");
            assert_eq!(work.live_allocation_bytes(), state_bytes);

            // Windows explicitly permits process-environment mutation with
            // other threads present. Verify the producer observes current
            // state, not the child's initial environment block.
            unsafe { std::env::set_var("ACYCLIC_NATIVE_ENVIRONMENT_CHANGED", "after-startup"); }
            let work = NativeMaterializationWork::new(bounded(), &cancellation).expect("original meter");
            let changed = work.environment_variable(OsStr::new("ACYCLIC_NATIVE_ENVIRONMENT_CHANGED"), 13).expect("live changed value").expect("present changed value");
            assert_eq!(changed.0, "after-startup");
            drop(changed);
            assert_eq!(work.live_allocation_bytes(), state_bytes);
        }

        let cancelled = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(bounded(), &cancelled).expect("original meter");
        let before = work.receipt();
        cancelled.cancel();
        assert!(matches!(work.environment_variable(name, expected.len()), Err(NativeTreeMaterializationError::Cancelled)));
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), state_bytes);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_environment_real_process_controls_are_bounded() -> std::io::Result<()> {
        let mut values = vec![
            std::ffi::OsString::from("actual-live-environment"),
            std::ffi::OsString::from("a\u{1F680}\u{E9}"),
        ];
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt as _;
            values.push(std::ffi::OsString::from_vec(vec![b'a', 0xFF, b'z']));
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStringExt as _;
            values.push(std::ffi::OsString::from_wide(&[97, 0xD800, 122, 0xD83D, 0xDE80]));
        }
        for value in values {
            // Command-owned child controls avoid any unsafe mutation of the
            // multithreaded Unix test runner's current-process environment.
            let status = acyclic_native_runtime::process_status(
                std::process::Command::new(std::env::current_exe()?)
                    .args(["--ignored", "--exact", "materializer::tests::native_environment_child", "--test-threads=1"])
                    .env(NATIVE_ENVIRONMENT_FIXTURE, value)
                    .env("ACYCLIC_NATIVE_ENVIRONMENT_EMPTY", "")
                    .env_remove("ACYCLIC_NATIVE_ENVIRONMENT_ABSENT"),
                std::time::Duration::from_secs(120),
            )?;
            assert!(status.success(), "real native environment fixture failed: {status}");
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_meter_admits_its_actual_state_before_allocation() {
        let cancellation = crate::CancellationToken::new();
        let mut denied = finite_native_budget();
        denied.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes() - 1;
        let (error, work) = NativeMaterializationWork::new(denied, &cancellation).err().expect("state admission fails");
        assert!(matches!(error, NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. })));
        assert_eq!(work, crate::WorkCounters::UNCHARGED);
        let mut exact = finite_native_budget();
        exact.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes();
        exact.allocation_operations = 1;
        let work = NativeMaterializationWork::new(exact, &cancellation).expect("exact state body fits");
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert_eq!(work.receipt().allocation_operations, 1);
        assert!(work.claim(1).is_err());
        assert_eq!(work.receipt().allocation_operations, 1);
        work.receipt().verify(exact).expect("exact state receipt");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_runtime_meter_preserves_original_reservations_without_watch_producers() {
        use std::io::Read as _;
        use acyclic_native_runtime::{NativeByteReservation, NativeOperationKind, NativeOperationReservation};
        let directory = tempfile::tempdir().expect("native read fixture");
        let path = directory.path().join("payload");
        std::fs::write(&path, b"original").expect("fixture payload");
        let mut file = std::fs::File::open(path).expect("fixture reader");
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.backend_read_operations = 1;
        budget.source_bytes_read = 8;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original native meter");
        let custody = work.claim(8).expect("original destination capacity");
        let mut bytes = Vec::<u8>::new();
        bytes.try_reserve_exact(8).expect("destination buffer");
        bytes.resize(8, 0);
        let mut operation = NativeOperationReservation::new(work.runtime_meter(), NativeOperationKind::Read, 1).expect("original operation credit");
        let mut transfer = NativeByteReservation::new(work.runtime_meter(), NativeOperationKind::Read, 8).expect("original byte credit");
        assert_eq!(work.remaining().expect("shared remaining quota").backend_read_operations, 0);
        assert_eq!(work.remaining().expect("shared remaining quota").source_bytes_read, 0);
        let (read, recorded) = operation.attempt(|| file.read(&mut bytes));
        recorded.expect("actual original native read");
        let read = read.expect("physical read");
        assert_eq!(&bytes[..read], b"original");
        cancellation.cancel();
        transfer.record(read).expect("completed original bytes survive cancellation");
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.receipt().source_bytes_read, 8);
        drop(transfer);
        drop(operation);
        drop(bytes);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        work.receipt().verify(budget).expect("one original finite receipt");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_runtime_allocation_failure_keeps_the_original_typed_cause() {
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .expect("ordinary unbounded measured invocation");
        let requested = (isize::MAX as usize).checked_add(1).expect("invalid Rust buffer capacity");
        let custody = work.claim(requested as u64).expect("original allocation admission");
        let mut buffer = Vec::<u8>::new();
        let actual_failure = buffer.try_reserve_exact(requested).expect_err("Rust rejects actual capacity");
        let marker = work.runtime_meter().allocation_failed(actual_failure);
        assert_eq!(marker.kind(), std::io::ErrorKind::OutOfMemory);
        assert!(marker.get_ref().is_none(), "the callback marker has no allocated diagnostic payload");
        assert!(matches!(work.runtime_error(marker), NativeTreeMaterializationError::AllocationFailed(_)));
        assert_eq!(work.receipt().allocation_operations, 2, "state and actual attempted buffer");
        drop(buffer);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_original_caller_resumes_with_live_intrinsic_custody() {
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new())
            .expect("original state");
        let original = work.original_demand_work();
        let lifetime = std::sync::Arc::downgrade(&work.state);
        let custody = work.claim(8).expect("actual retained payload");
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(8).expect("physical payload");
        payload.extend_from_slice(b"original");
        let counters = work.receipt();
        drop(work);
        assert_eq!(original.counters().expect("caller remains original"), counters);
        assert_eq!(original.full_budget().expect("complete grant retained"), budget);
        assert_eq!(original.with_original_leaf(|state| state.live_allocation_bytes())
            .expect("original live ledger"), NativeMaterializationWork::accounting_state_bytes() + 8);
        let receipt = original.finish_operation(Ok(payload.len())).expect("finish without unique Arc");
        assert_eq!(receipt.value, 8);
        assert_eq!(receipt.work, counters);
        drop(original);
        assert!(lifetime.upgrade().is_some(), "actual payload still owns original state");
        drop(payload);
        drop(custody);
        assert!(lifetime.upgrade().is_none(), "the original state has no self-cycle");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn owned_reader_and_copy_use_original_byte_custody_and_exact_prefix() {
        let directory = tempfile::tempdir().expect("real byte reader fixture");
        let path = directory.path().join("payload");
        std::fs::write(&path, b"original").expect("real byte fixture");
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(crate::WorkBudget {
            backend_read_operations: 2, source_bytes_read: 9, bytes_copied: 11,
            bytes_encoded: 0, ..finite_native_budget()
        }, &cancellation).map_err(|(error, _)| error).expect("original work");
        let before = work.receipt();
        let baseline = work.live_allocation_bytes();
        let mut file = std::fs::File::open(&path).expect("authorized fixture file");
        let (bytes, allocation) = work.read_owned_bytes(&mut file, 8).expect("exact bounded read");
        assert_eq!(bytes, b"original");
        assert!(allocation.matches_work(&work));
        assert_eq!(allocation.bytes(), bytes.capacity() as u64);
        assert_eq!(work.receipt().backend_read_operations - before.backend_read_operations, 2);
        assert_eq!(work.receipt().source_bytes_read - before.source_bytes_read, 8);
        assert_eq!(work.receipt().bytes_copied - before.bytes_copied, 8);
        assert_eq!(work.receipt().bytes_encoded - before.bytes_encoded, 0);
        let (copy, copy_allocation) = work.copy_bytes(b"new").expect("same byte engine copy");
        assert_eq!(copy, b"new");
        assert_eq!(copy_allocation.bytes(), copy.capacity() as u64);
        assert_eq!(work.receipt().bytes_copied - before.bytes_copied, 11);
        drop(copy);
        drop(copy_allocation);
        assert_eq!(work.live_allocation_bytes(), baseline + allocation.bytes());
        drop(bytes);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn owned_reader_denial_keeps_read_prefix_without_an_unfunded_copy() {
        let directory = tempfile::tempdir().expect("real denied reader fixture");
        let path = directory.path().join("payload");
        std::fs::write(&path, b"original").expect("real byte fixture");
        for maximum in [7, 8] {
            let work = NativeMaterializationWork::new(crate::WorkBudget {
                bytes_copied: 0, ..finite_native_budget()
            }, &crate::CancellationToken::new()).map_err(|(error, _)| error).expect("original work");
            let before = work.receipt();
            let baseline = work.live_allocation_bytes();
            let mut file = std::fs::File::open(&path).expect("authorized fixture file");
            let error = work.read_owned_bytes(&mut file, maximum).expect_err("bounded refusal");
            if maximum == 7 {
                assert!(matches!(error, NativeTreeMaterializationError::PayloadTooLarge { maximum: 7, actual: 8 }));
            } else {
                assert!(matches!(error, NativeTreeMaterializationError::Work(_)));
            }
            assert_eq!(work.receipt().backend_read_operations - before.backend_read_operations, 1);
            assert_eq!(work.receipt().source_bytes_read - before.source_bytes_read, 8);
            assert_eq!(work.receipt().bytes_copied - before.bytes_copied, 0);
            assert_eq!(work.receipt().allocation_operations - before.allocation_operations, 0);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn runtime_payload_custody_transfer_after_cancel_preserves_original_claim() {
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(finite_native_budget(), &cancellation)
            .expect("original admitted accounting");
        let baseline = original.live_allocation_bytes();
        let meter = original.runtime_meter_owner();
        let mut incoming = acyclic_native_runtime::ReadAllocation::new(&meter, 4)
            .expect("admitted actual vector allocation");
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(4).expect("actual vector backing");
        incoming.reconcile_capacity(payload.capacity() as u64).expect("actual capacity");
        meter.admit_examination(0, 4).expect("admitted actual payload copy");
        payload.extend_from_slice(b"data");
        meter.record_examination(0, 4).expect("completed actual payload copy");
        cancellation.cancel();
        let before = (original.receipt(), original.live_allocation_bytes());
        let custody = NativeAllocation::from_runtime_allocation(&original, incoming)
            .unwrap_or_else(|_| panic!("the exact original runtime owner must transfer"));
        assert_eq!((original.receipt(), original.live_allocation_bytes()), before);
        assert_eq!(custody.bytes(), payload.capacity() as u64);
        assert_eq!(payload, b"data");
        assert!(matches!(original.claim(1), Err(NativeTreeMaterializationError::Cancelled)));
        // Physical data drop precedes retirement of the transferred guard.
        drop(payload);
        assert_eq!(original.live_allocation_bytes(), before.1);
        drop(custody);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn runtime_payload_custody_mismatch_returns_the_unchanged_original_guard() {
        let original = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new())
            .expect("original admitted accounting");
        let other = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new())
            .expect("a genuinely different original ledger");
        let baseline = original.live_allocation_bytes();
        let meter = original.runtime_meter_owner();
        let mut incoming = acyclic_native_runtime::ReadAllocation::new(&meter, 4)
            .expect("admitted actual vector allocation");
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(4).expect("actual vector backing");
        incoming.reconcile_capacity(payload.capacity() as u64).expect("actual capacity");
        let before = (original.receipt(), original.live_allocation_bytes(), other.receipt(), other.live_allocation_bytes());
        let (error, incoming) = match NativeAllocation::from_runtime_allocation(&other, incoming) {
            Err(failure) => failure,
            Ok(_) => panic!("a foreign ledger cannot adopt the runtime claim"),
        };
        assert!(matches!(error, NativeTreeMaterializationError::InvalidLayout));
        assert_eq!((original.receipt(), original.live_allocation_bytes(), other.receipt(), other.live_allocation_bytes()), before);
        drop(payload);
        drop(incoming);
        assert_eq!(original.live_allocation_bytes(), baseline);
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct PartialFileWriter<'a> {
        file: &'a mut std::fs::File,
        cancellation: Option<&'a crate::CancellationToken>,
        fail_after_first: bool,
        completed: usize,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl std::io::Write for PartialFileWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.fail_after_first && self.completed != 0 {
                return Err(std::io::ErrorKind::PermissionDenied.into());
            }
            let count = std::io::Write::write(self.file, &bytes[..bytes.len().min(3)])?;
            self.completed += count;
            if let Some(cancellation) = self.cancellation {
                cancellation.cancel();
            }
            Ok(count)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            std::io::Write::flush(self.file)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn owned_writer_records_real_partial_prefix_on_quota_cancel_and_io_failure() {
        for (write_limit, cancel, fail, attempts, completed) in [
            (3, false, false, 3, 8),
            (1, false, false, 1, 3),
            (3, true, false, 1, 3),
            (3, false, true, 2, 3),
        ] {
            let directory = tempfile::tempdir().expect("real partial writer fixture");
            let path = directory.path().join("payload");
            let mut file = std::fs::File::create(&path).expect("authorized writer");
            let cancellation = crate::CancellationToken::new();
            let budget = crate::WorkBudget {
                backend_write_operations: write_limit, output_bytes: 8,
                bytes_copied: 0, bytes_encoded: 0, ..finite_native_budget()
            };
            let work = NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(error, _)| error).expect("original work");
            let before = work.receipt();
            let baseline = work.live_allocation_bytes();
            let mut writer = PartialFileWriter {
                file: &mut file, cancellation: cancel.then_some(&cancellation),
                fail_after_first: fail, completed: 0,
            };
            let result = work.write_owned_bytes(&mut writer, b"original");
            assert_eq!(writer.completed, completed);
            drop(writer);
            drop(file);
            assert_eq!(std::fs::read(&path).expect("actual physical prefix"), &b"original"[..completed]);
            assert_eq!(work.receipt().backend_write_operations - before.backend_write_operations, attempts);
            assert_eq!(work.receipt().output_bytes - before.output_bytes, completed as u64);
            assert_eq!(work.receipt().bytes_copied - before.bytes_copied, 0);
            assert_eq!(work.receipt().allocation_operations - before.allocation_operations, 0);
            assert_eq!(work.live_allocation_bytes(), baseline);
            if attempts == 3 {
                result.expect("all real partial writes");
            } else if cancel {
                assert!(matches!(result, Err(NativeTreeMaterializationError::Cancelled)));
            } else if fail {
                assert!(matches!(result, Err(NativeTreeMaterializationError::Io(_))));
            } else {
                assert!(matches!(result, Err(NativeTreeMaterializationError::Work(_))));
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn owned_writer_preflight_denial_never_calls_or_edits_its_writer() {
        let directory = tempfile::tempdir().expect("real denied writer fixture");
        let path = directory.path().join("payload");
        let mut file = std::fs::File::create(&path).expect("authorized writer");
        let work = NativeMaterializationWork::new(crate::WorkBudget {
            output_bytes: 7, ..finite_native_budget()
        }, &crate::CancellationToken::new()).map_err(|(error, _)| error).expect("original work");
        let before = work.receipt();
        assert!(matches!(work.write_owned_bytes(&mut file, b"original"),
            Err(NativeTreeMaterializationError::Work(_))));
        assert_eq!(work.receipt(), before);
        drop(file);
        assert!(std::fs::read(path).expect("untouched real file").is_empty());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_resource_guards_attribute_live_peak_to_the_same_engine() {
        let root = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original");
        let baseline = root.live_allocation_bytes();
        let mut budget = finite_native_budget();
        budget.peak_allocation_bytes = 8;
        budget.allocation_operations = 1;
        let scope = root.native_volume_work(budget).expect("funded resource view");
        let lifetime = std::sync::Arc::downgrade(scope.resource.as_ref().expect("scope"));
        let mut guard = scope.claim(8).expect("scoped real buffer");
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(8).expect("physical scoped backing");
        guard.reconcile_capacity(buffer.capacity() as u64).expect("actual capacity");
        buffer.extend_from_slice(b"original");
        assert!(scope.shares_ledger(&root));
        let scoped = scope.resource.as_ref().expect("scope").state.lock().expect("scope state").work;
        assert_eq!(scoped.peak_allocation_bytes, 8, "root metadata is not scope data");
        assert_eq!(scoped.allocation_operations, 1);
        assert!(root.receipt().peak_allocation_bytes > scoped.peak_allocation_bytes);
        let before = root.receipt();
        assert!(matches!(scope.claim(1), Err(NativeTreeMaterializationError::Work(_))));
        assert_eq!(root.receipt(), before, "scope refuses before the original attempt");
        drop(scope);
        assert!(lifetime.upgrade().is_some(), "the accepted buffer retains its scope ceiling");
        drop(buffer);
        drop(guard);
        assert!(lifetime.upgrade().is_none());
        assert_eq!(root.live_allocation_bytes(), baseline);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_resource_copy_credit_records_only_the_actual_cancelled_prefix() {
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.bytes_copied = 4;
        let root = NativeMaterializationWork::new(budget, &cancellation).expect("original");
        let parent = root.native_volume_work(budget).expect("parent resource");
        let child = parent.native_volume_work(budget).expect("child resource");
        let copy = child.reserve_copy(4).expect("accepted copy");
        assert_eq!(root.receipt().bytes_copied, 0);
        assert_eq!(parent.remaining().expect("withheld ancestor").bytes_copied, 0);
        assert!(matches!(parent.reserve_copy(1), Err(NativeTreeMaterializationError::Work(_))));
        let mut destination = [0_u8; 2];
        destination.copy_from_slice(b"ok");
        cancellation.cancel();
        assert!(matches!(copy.record(destination.len() as u64), Err(NativeTreeMaterializationError::Cancelled)));
        assert_eq!(&destination, b"ok");
        assert_eq!(root.receipt().bytes_copied, 2);
        for scope in [&parent, &child] {
            let state = scope.resource.as_ref().expect("scope").state.lock().expect("scope state");
            assert_eq!(state.work.bytes_copied, 2);
            assert_eq!(state.pending.bytes_copied, 0);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_general_quota_keeps_real_prefix_and_refunds_only_unused_credit() {
        use std::io::Write as _;
        let directory = tempfile::tempdir().expect("owned sink fixture");
        let path = directory.path().join("accepted");
        let mut file = std::fs::File::create(&path).expect("owned physical sink");
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.authority_records_appended = 2;
        budget.authority_bytes_written = 4;
        budget.backend_write_operations = 2;
        budget.output_bytes = 4;
        budget.bytes_copied = 4;
        budget.items_examined = 4;
        budget.items_returned = 2;
        let root = NativeMaterializationWork::new(budget, &cancellation).expect("original");
        let parent = root.native_volume_work(budget).expect("ancestor allowance");
        let scope = parent.native_volume_work(budget).expect("resource allowance");
        let maximum = crate::WorkCounters {
            authority_records_appended: 2, authority_bytes_written: 4,
            backend_write_operations: 2, output_bytes: 4, bytes_copied: 4,
            items_examined: 4, items_returned: 2, ..crate::WorkCounters::UNCHARGED
        };
        let before = root.receipt();
        let mut reservation = scope.reserve_work(maximum).expect("accepted additive quota");
        assert_eq!(root.receipt(), before, "a maximum is not completed work");
        assert!(matches!(root.admit(crate::WorkCounters {
            authority_records_appended: 1, ..crate::WorkCounters::UNCHARGED
        }), Err(NativeTreeMaterializationError::Work(_))));
        assert!(matches!(parent.admit(crate::WorkCounters {
            output_bytes: 1, ..crate::WorkCounters::UNCHARGED
        }), Err(NativeTreeMaterializationError::Work(_))));
        let mut bytes = [0_u8; 2];
        bytes.copy_from_slice(b"ok");
        assert!(bytes.iter().all(|byte| *byte != 0));
        let written = file.write(&bytes).expect("actual accepted sink request");
        cancellation.cancel();
        let actual = crate::WorkCounters {
            backend_write_operations: 1, output_bytes: written as u64,
            bytes_copied: 2, items_examined: 2, ..crate::WorkCounters::UNCHARGED
        };
        reservation.record_completed(actual).expect("accepted physical prefix completes after cancellation");
        assert!(matches!(scope.admit(crate::WorkCounters { backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED }), Err(NativeTreeMaterializationError::Cancelled)),
            "recording accepted completion never permits the next action");
        assert_eq!(root.receipt().backend_write_operations, before.backend_write_operations + 1);
        assert_eq!(root.receipt().output_bytes, before.output_bytes + written as u64);
        assert_eq!(root.receipt().authority_records_appended, before.authority_records_appended);
        drop(reservation);
        assert_eq!(root.state().budget, budget, "only unused owned limits are restored");
        for work in [&parent, &scope] {
            let state = work.resource.as_ref().expect("resource").state.lock().expect("state");
            assert_eq!(state.pending, crate::WorkCounters::UNCHARGED);
            assert_eq!(state.work.output_bytes, written as u64);
            assert_eq!(state.work.backend_write_operations, 1);
        }
        drop(file);
        assert_eq!(std::fs::read(path).expect("actual physical prefix").len(), written);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_general_quota_rejects_allocation_dimensions_without_mutation() {
        let root = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original");
        let before = (root.receipt(), root.remaining().expect("allowance"), root.live_allocation_bytes());
        for maximum in [
            crate::WorkCounters { allocation_operations: 1, ..crate::WorkCounters::UNCHARGED },
            crate::WorkCounters { peak_allocation_bytes: 1, ..crate::WorkCounters::UNCHARGED },
        ] {
            assert!(matches!(root.reserve_work(maximum), Err(NativeTreeMaterializationError::InvalidLayout)));
        }
        assert_eq!((root.receipt(), root.remaining().expect("allowance"), root.live_allocation_bytes()), before);
        let maximum = crate::WorkCounters { bytes_encoded: 8, ..crate::WorkCounters::UNCHARGED };
        let mut reservation = root.reserve_work(maximum).expect("owned codec completion credit");
        assert!(matches!(reservation.record_completed(crate::WorkCounters {
            bytes_encoded: 9, ..crate::WorkCounters::UNCHARGED
        }), Err(NativeTreeMaterializationError::Work(_))));
        assert_eq!(root.receipt(), before.0);
        drop(reservation);
        assert_eq!(root.remaining().expect("exact unused refund"), before.1);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_cleanup_binding_keeps_birth_and_current_credits_without_double_root() {
        let directory = tempfile::tempdir().expect("real cleanup fixture");
        let file = directory.path().join("owned");
        std::fs::write(&file, b"owned").expect("real owned payload");
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.backend_write_operations = 1;
        let root = NativeMaterializationWork::new(budget, &cancellation).expect("original");
        let parent = root.native_volume_work(budget).expect("shared ancestor");
        let birth = parent.native_volume_work(budget).expect("birth volume");
        let current = parent.native_volume_work(budget).expect("current volume");
        let mut cleanup = birth.reserve_cleanup_write().expect("accepted birth cleanup");
        cleanup.bind_current_work(&current).expect("shared ancestor is reserved once");
        assert_eq!(root.receipt().backend_write_operations, 0);
        assert_eq!(root.remaining().expect("original allowance").backend_write_operations, 0);
        assert_eq!(current.remaining().expect("current allowance").backend_write_operations, 0);
        cancellation.cancel();
        let (removed, recorded) = cleanup.attempt_recorded(|| std::fs::remove_file(&file));
        removed.expect("actual physical cleanup after cancellation");
        recorded.expect("accepted cleanup records its real prefix");
        assert!(!file.exists());
        assert_eq!(root.receipt().backend_write_operations, 1);
        for scope in [&parent, &birth, &current] {
            let state = scope.resource.as_ref().expect("scope").state.lock().expect("scope state");
            assert_eq!(state.work.backend_write_operations, 1);
            assert_eq!(state.pending.backend_write_operations, 0);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_invocation_stop_is_local_and_keeps_original_custody() {
        let root_token = crate::CancellationToken::new();
        let root = NativeMaterializationWork::new(finite_native_budget(), &root_token).expect("original");
        let (invocation, local_stop) = root.invocation_view().expect("funded linked local view");
        let alias = invocation.original_demand_work();
        let guard = invocation.claim(8).expect("accepted original buffer");
        let mut buffer = Vec::<u8>::new();
        buffer.try_reserve_exact(8).expect("physical original backing");
        buffer.extend_from_slice(b"original");
        let before = root.receipt();
        local_stop.cancel();
        assert!(!root_token.is_cancelled(), "local drop never cancels the task root");
        assert!(matches!(invocation.claim(1), Err(NativeTreeMaterializationError::Cancelled)));
        assert_eq!(root.receipt(), before);
        assert!(alias.with_original_leaf(|work| work.check()).expect("same original leaf").is_err());
        let independent = root.claim(1).expect("the same task root remains admitted");
        let mut later = Vec::<u8>::new();
        later.try_reserve_exact(1).expect("independent physical backing");
        assert!(guard.shares_ledger(&independent));
        drop(later);
        drop(independent);
        drop(invocation);
        drop(alias);
        assert_eq!(buffer.as_slice(), b"original");
        drop(buffer);
        drop(guard);
        drop(local_stop);
        assert_eq!(root.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_checkout_registry_retains_paid_headers_on_the_original() {
        let root = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original");
        let lifetime = std::sync::Arc::downgrade(&root.state);
        for _ in 0..9 {
            let mut header = root.claim_checkout_token_metadata().expect("actual header admission");
            let token = std::sync::Arc::new(crate::kernel::ProofDataCustody::default());
            let membership = root.register_native_checkout_control(&token, &mut header).expect("actual funded registration");
            assert_eq!(header.bytes(), 0, "the original registry now owns the paid scalar");
            assert!(membership.owns(&root.state, &token));
        }
        assert!(root.receipt().bytes_copied > 0, "actual overflow metadata copying is measured");
        assert!(root.live_allocation_bytes() > NativeMaterializationWork::accounting_state_bytes());
        drop(root);
        assert!(lifetime.upgrade().is_none(), "root-owned metadata never owns a strong-root guard");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_allocation_transfers_preserve_original_custody_on_rejection() {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("original");
        let foreign = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("independent original");
        let mut custody = work.claim(8).expect("original payload");
        let mut other = foreign.claim(4).expect("foreign payload");
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(8).expect("physical original buffer");
        let mut other_payload = Vec::<u8>::new();
        other_payload.try_reserve_exact(4).expect("physical foreign buffer");
        let before = (work.receipt(), foreign.receipt());
        assert!(matches!(custody.absorb(&mut other), Err(NativeTreeMaterializationError::InvalidLayout)));
        assert_eq!((custody.bytes(), other.bytes()), (8, 4));
        assert_eq!((work.receipt(), foreign.receipt()), before);
        assert!(matches!(custody.split_off(9), Err(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))));
        assert_eq!(custody.bytes(), 8);
        cancellation.cancel();
        let mut split = custody.split_off(3).expect("pure ownership transfer after cancellation");
        assert!(custody.shares_ledger(&split));
        assert_eq!((custody.bytes(), split.bytes()), (5, 3));
        custody.absorb(&mut split).expect("same original custody recombines");
        assert_eq!((custody.bytes(), split.bytes()), (8, 0));
        assert_eq!(work.receipt(), before.0, "pure transfers do not re-admit or charge");
        assert!(matches!(custody.reconcile_capacity(payload.capacity() as u64),
            Err(NativeTreeMaterializationError::Cancelled)));
        assert_eq!(custody.bytes(), payload.capacity() as u64, "late refusal keeps actual backing owned");
        drop(payload);
        drop(custody);
        drop(split);
        drop(other_payload);
        drop(other);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        assert_eq!(foreign.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn native_sdk_alias_retains_original_guard_and_exact_live_budget() {
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes() + 11;
        budget.allocation_operations = 3;
        budget.bytes_copied = 3;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original invocation");
        let root_custody = work.claim(8).expect("original root capacity");
        let mut root = Vec::<u8>::new();
        root.try_reserve_exact(8).expect("original root buffer");
        root.extend_from_slice(b"root");
        let sdk_work = work.native_mount_work();
        let sdk_alias = sdk_work.native_materialization_work();
        assert!(std::sync::Arc::ptr_eq(&work.state, &sdk_alias.state));
        assert_eq!(sdk_alias.receipt(), work.receipt(), "aliasing performs no allocation or work");
        let (sdk_path, sdk_custody) = tokio::spawn(async move {
            tokio::task::yield_now().await;
            sdk_alias.copy_path(Path::new("sdk"))
        }).await.expect("SDK callback completes").expect("same original live allowance");
        assert_eq!(sdk_path, Path::new("sdk"));
        assert!(root_custody.shares_ledger(&sdk_custody));
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes() + 11);
        assert!(matches!(sdk_work.native_materialization_work().claim(1),
            Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. }))));
        assert_eq!(work.receipt().allocation_operations, 3);
        drop(root);
        drop(root_custody);
        assert_eq!(sdk_work.native_materialization_work().live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes() + 3);
        drop(sdk_path);
        drop(sdk_custody);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        work.receipt().verify(budget).expect("one canonical receipt");
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn native_sdk_copy_reservation_refunds_and_records_only_physical_prefix() {
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.bytes_copied = 4;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original invocation");
        let custody = work.claim(4).expect("actual destination capacity");
        let mut destination = Vec::<u8>::new();
        destination.try_reserve_exact(4).expect("destination buffer");
        let sdk = work.native_mount_work();
        let alias = sdk.native_materialization_work();
        let unused = work.reserve_copy(4).expect("original copy quota");
        assert!(matches!(alias.reserve_copy(1),
            Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "bytes_copied", .. }))));
        assert_eq!(work.receipt().bytes_copied, 0);
        drop(unused);
        let pending = alias.reserve_copy(4).expect("unused quota refunded to the same invocation");
        destination.extend_from_slice(b"ab");
        cancellation.cancel();
        pending.record(destination.len() as u64).expect("actual completed prefix survives cancellation");
        assert_eq!(work.receipt().bytes_copied, 2);
        assert_eq!(sdk.meter().lock().budget.bytes_copied, 4, "original full quota is restored");
        drop(destination);
        drop(custody);
        assert_eq!(alias.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        work.receipt().verify(budget).expect("no maximum-sized synthetic receipt");
    }

    #[cfg(feature = "native-mount")]
    #[tokio::test]
    async fn native_sdk_alias_preserves_cleanup_prefix_after_panic_and_cancellation() {
        let directory = tempfile::tempdir().expect("original root");
        let leaf = directory.path().join("owned");
        std::fs::write(&leaf, b"retained").expect("owned payload");
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.backend_write_operations = 1;
        budget.backend_read_operations = 1;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original invocation");
        let prefix = crate::WorkCounters { bytes_hashed: 7, ..crate::WorkCounters::UNCHARGED };
        work.set_original_work(prefix);
        let root_custody = work.claim(8).expect("original payload custody");
        let mut root = Vec::<u8>::new();
        root.try_reserve_exact(8).expect("payload allocation");
        root.extend_from_slice(b"retained");
        let cleanup = work.reserve_cleanup_write().expect("original cleanup reservation");
        let sdk_work = work.native_mount_work();
        assert_eq!(sdk_work.native_materialization_work().remaining().expect("same budget").backend_write_operations, 0);
        let sdk_alias = sdk_work.native_materialization_work();
        tokio::task::yield_now().await;
        let metadata = crate::native_host::NativeHostWork::attempt_io(
            &mut &sdk_alias,
            crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED },
            || std::fs::metadata(&leaf),
        ).expect("actual native read through the same engine");
        assert_eq!(metadata.len(), 8);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = sdk_work.native_materialization_work().receipt();
            panic!("native callback unwinds outside the original state lock");
        }));
        assert!(panic.is_err());
        cancellation.cancel();
        assert!(matches!(sdk_alias.charge(crate::WorkCounters::UNCHARGED), Err(NativeTreeMaterializationError::Cancelled)));
        assert_eq!(work.receipt().backend_read_operations, 1);
        assert_eq!(work.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes() + 8);
        cleanup.attempt(|| std::fs::remove_file(&leaf)).expect("original reservation restores").expect("physical cleanup");
        assert!(!leaf.exists());
        assert_eq!(sdk_alias.receipt().backend_write_operations, 1);
        assert_eq!(sdk_alias.journal_work().expect("original durable prefix"), prefix.checked_add(work.receipt()).expect("receipt composition"));
        drop(root);
        drop(root_custody);
        assert_eq!(sdk_alias.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        work.receipt().verify(budget).expect("actual terminal prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_display_preserves_exact_partial_copy_and_live_buffer_custody() {
        let mut budget = finite_native_budget();
        budget.bytes_encoded = 3;
        budget.bytes_copied = 3;
        budget.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes() + 8;
        budget.allocation_operations = 2;
        let failed = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        assert!(matches!(failed.display("abcd"), Err(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { .. }))));
        assert_eq!(failed.receipt().bytes_encoded, 3);
        assert_eq!(failed.receipt().bytes_copied, 3);
        assert_eq!(failed.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        failed.receipt().verify(budget).expect("partial diagnostic prefix");
        let successful = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        let (text, custody) = successful.display("abc").expect("exact diagnostic allowance");
        assert_eq!(text, "abc");
        assert_eq!(custody.bytes(), text.capacity() as u64);
        assert_eq!(successful.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes() + custody.bytes());
        drop(text);
        drop(custody);
        assert_eq!(successful.live_allocation_bytes(), NativeMaterializationWork::accounting_state_bytes());
        successful.receipt().verify(budget).expect("retained diagnostic receipt");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_cleanup_permit_withholds_exact_work_and_survives_cancellation() {
        let directory = tempfile::tempdir().expect("temporary cleanup");
        let leaf = directory.path().join("owned");
        std::fs::write(&leaf, b"owned cleanup").expect("cleanup fixture");
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.backend_write_operations = 1;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("native meter");
        let permit = work.reserve_cleanup_write().expect("one actual cleanup write reserved");
        assert_eq!(work.remaining().expect("remaining").backend_write_operations, 0);
        assert!(work.charge(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::UNCHARGED }).is_err());
        assert_eq!(work.receipt().backend_write_operations, 0);
        cancellation.cancel();
        permit.attempt(|| std::fs::remove_file(&leaf)).expect("reserved accounting").expect("actual cleanup");
        assert!(!leaf.exists());
        assert_eq!(work.receipt().backend_write_operations, 1);
        work.receipt().verify(budget).expect("cleanup fits exact allowance");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_metadata_image_is_fixed_and_rejects_noncanonical_payloads() {
        let desired = NativeMetadataImage { readonly: true, posix_mode: Some(0o40555), windows_attributes: Some(0x21) };
        let image = native_metadata_bytes(&desired);
        assert_eq!(image.len(), 11);
        let decoded = decode_native_metadata(&image).expect("scalar metadata");
        assert_eq!(decoded.readonly, desired.readonly);
        assert_eq!(decoded.posix_mode, desired.posix_mode);
        assert_eq!(decoded.windows_attributes, desired.windows_attributes);
        let mut invalid = image;
        invalid[1] = 0;
        assert!(matches!(decode_native_metadata(&invalid), Err(NativeTreeMaterializationError::InvalidPreimage)));
        assert!(matches!(decode_native_metadata(&vec![b' '; 2 * 1024 * 1024]), Err(NativeTreeMaterializationError::InvalidPreimage)));
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[test]
    fn native_metadata_encoder_transfers_original_buffer_custody() {
        let metadata = crate::WorkspaceMetadata {
            posix_mode: Some(0o40555), posix_uid: None, posix_gid: None,
            posix_flags: None, windows_attributes: Some(0x21),
            created_ns: None, modified_ns: None, accessed_ns: None, changed_ns: None,
            has_named_attributes: false, has_acl: false, has_security_descriptor: false,
        };
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_native_budget();
        budget.bytes_copied = 11;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
        let baseline = work.live_allocation_bytes();
        let (image, allocation) = encode_native_metadata(&metadata, &work).expect("actual metadata producer");
        assert_eq!(image.len(), 11);
        assert_eq!(allocation.bytes(), image.capacity() as u64);
        assert!(allocation.work().shares_ledger(&work));
        assert_eq!(work.live_allocation_bytes(), baseline + allocation.bytes());
        assert_eq!(work.receipt().bytes_copied, budget.bytes_copied);
        work.receipt().verify(budget).expect("exact encoding copy allowance");
        cancellation.cancel();
        let prefix = work.receipt();
        drop(image);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt(), prefix, "original writer custody retires without post-encoding adoption");
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn publication_uncertainty_reuses_prepaid_generic_slot_and_original_cause() {
        let temporary = tempfile::tempdir().expect("real native source");
        let root = temporary.path().join("checkout");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("original root");
        std::fs::write(root.join("file.txt"), b"before").expect("original user file");
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("original meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("actual held backend");
        let baseline = work.live_allocation_bytes();
        let slot = NativeStageFailureSlot::<NativeWorkspacePublicationError>::new(&work).expect("prepaid before the failing producer");
        let edit = MaterializationEdit::Install { path: "file.txt".to_owned(), image: Vec::new() };
        let primary = backend.capture(&edit).await.expect_err("real missing staged target");
        let observed_work = work.receipt();
        cancellation.cancel();
        let error = NativeWorkspacePublicationError::from(slot.finish(
            NativeWorkspacePublicationError::Native(primary), crate::WorkCounters::UNCHARGED, observed_work,
        ));
        let NativeWorkspacePublicationError::NativeStageUncertain(failure) = &error else { panic!("actual existing generic carrier"); };
        assert!(matches!(failure.cause(), NativeWorkspacePublicationError::Native(
            NativeTreeMaterializationError::MissingTarget(path, Some(allocation))
        ) if path == "file.txt" && allocation.work().shares_ledger(&work)));
        assert_eq!(failure.last_durable_work, crate::WorkCounters::UNCHARGED);
        assert_eq!(failure.observed_work, observed_work);
        assert_eq!(work.receipt(), observed_work, "wrapping after cancellation only moves the already funded box and original cause");
        assert!(work.live_allocation_bytes() > baseline);
        drop(error);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(std::fs::read(root.join("file.txt")).expect("user file untouched"), b"before");
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn native_readonly_metadata_without_explicit_attributes_matches_its_durable_witness() {
        let temporary = tempfile::tempdir().expect("temporary operation");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("root fixture");
        let file = root.join("file");
        std::fs::write(&file, b"owned").expect("file fixture");
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root, operation, work.clone()).expect("held roots");
        let edit = MaterializationEdit::SetMetadata {
            path: "file".to_owned(),
            image: native_metadata_bytes(&NativeMetadataImage { readonly: true, posix_mode: None, windows_attributes: None }).to_vec(),
        };
        let preimage = backend.capture(&edit).await.expect("durable metadata witness");
        backend.apply(&edit, &preimage.preimage).await.expect("real readonly mutation");
        assert!(std::fs::metadata(&file).expect("physical metadata").permissions().readonly());
        assert_eq!(backend.observe(&edit, &preimage.preimage).await.expect("actual postimage"), MaterializationObservation::Postimage);
        backend.restore(&edit, &preimage.preimage).await.expect("restore witnessed attributes");
        assert!(!std::fs::metadata(file).expect("restored metadata").permissions().readonly());
        work.receipt().verify(budget).expect("actual metadata work");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_fingerprint_stops_at_exact_byte_allowance_without_mutation() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().join("root");
        let operation = directory.path().join("operation");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(root.join("file"), vec![7; 2 * 1024 * 1024]).expect("large source");
        std::fs::write(operation.join("target/file"), b"after").expect("staged file");
        let mut budget = finite_native_budget();
        budget.source_bytes_read = 7;
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("native meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation.clone(), work.clone()).expect("bounded backend");
        let error = backend.capture(&MaterializationEdit::Install { path: "file".to_owned(), image: vec![1] }).await.expect_err("bounded capture rejects");
        assert!(matches!(error, NativeTreeMaterializationError::Work(_)));
        assert_eq!(work.receipt().source_bytes_read, 7);
        work.receipt().verify(budget).expect("exact finite work");
        assert_eq!(std::fs::metadata(root.join("file")).expect("source retained").len(), 2 * 1024 * 1024);
        assert!(!operation.join("backup/file").exists());
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
        let backend = NativeTreeMaterializationBackend::new_with_work(root, operation.clone(), work.clone()).expect("backend");
        cancellation.cancel();
        assert!(matches!(backend.reset_target_directory("").await, Err(NativeTreeMaterializationError::Cancelled)));
        assert!(operation.join("target/partial").exists());
        let original = operation.join("target-original");
        std::fs::rename(operation.join("target"), &original).expect("displace original stage");
        std::fs::create_dir(operation.join("target")).expect("replacement stage");
        std::fs::write(operation.join("target/user"), b"keep").expect("user data");
        let replacement = NativeTreeMaterializationBackend::new(backend.root.as_ref(), &operation).expect("reopened backend");
        assert_ne!(replacement.preparation_identity(), backend.preparation_identity());
        assert_eq!(std::fs::read(operation.join("target/user")).expect("user data retained"), b"keep");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_cleanup_prefix_restarts_with_original_local_journal_identity() {
        let temporary = tempfile::tempdir().expect("temporary operation");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        let state_path = temporary.path().join("state");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(operation.join("target")).expect("target");
        std::fs::write(operation.join("target/one"), b"one").expect("stage one");
        std::fs::write(operation.join("target/two"), b"two").expect("stage two");
        let cancellation = crate::CancellationToken::new();
        let full = finite_native_budget();
        let work = NativeMaterializationWork::new(full, &cancellation).expect("native meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation.clone(), work.clone()).expect("backend");
        let state = crate::LocalCoreStateStore::new(&state_path);
        let plan = backend.plan_paths(OperationId::new(), GenerationId::new(crate::Digest::from_bytes([3; 32])), GenerationId::new(crate::Digest::from_bytes([4; 32])), ["one".to_owned(), "two".to_owned()]).expect("original plan");
        let id = plan.operation_id;
        let engine = JournaledMaterializer::new(state.materialization_with_work(work), backend);
        let original = engine.prepare(plan).await.expect("durable intent");
        drop(engine);
        drop(state);

        let mut bounded = full;
        bounded.backend_write_operations = 1;
        let cleanup_work = NativeMaterializationWork::new(bounded, &cancellation).expect("cleanup meter");
        let cleanup = NativeTreeMaterializationBackend::open_with_work(root.clone(), operation.clone(), cleanup_work.clone()).expect("existing allocations");
        assert!(matches!(cleanup.reset_target_directory("").await, Err(NativeTreeMaterializationError::Work(_))));
        assert_eq!(cleanup_work.receipt().backend_write_operations, 1);
        cleanup_work.receipt().verify(bounded).expect("exact cleanup prefix");
        assert_eq!(std::fs::read_dir(operation.join("target")).expect("remaining stage").count(), 1);
        drop(cleanup);
        drop(cleanup_work);

        let resumed_work = NativeMaterializationWork::new(full, &cancellation).expect("restart meter");
        let resumed_backend = NativeTreeMaterializationBackend::open_with_work(root.clone(), operation.clone(), resumed_work.clone()).expect("original held allocations");
        let restarted = crate::LocalCoreStateStore::new(&state_path);
        let resumed = JournaledMaterializer::new(restarted.materialization_with_work(resumed_work.clone()), resumed_backend);
        let (inspection, custody) = resumed.inspect_owned(id).await.expect("real restart inspection").expect("original journal retained");
        let custody = custody.expect("actual observation vector custody");
        assert!(custody.work().shares_ledger(&resumed_work));
        let output_bytes = journal_owned_bytes(&inspection.journal).expect("original journal payload")
            .checked_add(owned_size(inspection.observations.capacity(), std::mem::size_of::<MaterializationObservation>()).expect("actual observation capacity"))
            .expect("finite owned output");
        assert_eq!(custody.bytes() + inspection.journal.custody.as_ref().expect("intrinsic journal custody").bytes(), output_bytes, "inspection keeps journal custody nested and returns only actual tail custody");
        assert_eq!(inspection.journal.plan, original.plan);
        assert_eq!(inspection.journal.phase, MaterializationPhase::Preparing);
        assert_eq!(inspection.root, MaterializationRootObservation::Identical(original.root_identity.expect("root identity")));
        assert_eq!(inspection.preparation, MaterializationPreparationObservation::Identical(original.preparation_identity.expect("cleanup identity")));
        assert_eq!(resumed.backend().reset_target_directory("").await.expect("finish original cleanup").to_bytes(), original.preparation_identity.expect("target identity").target);
        assert_eq!(std::fs::read_dir(operation.join("target")).expect("empty original stage").count(), 0);
        assert!(root.is_dir());
        resumed_work.receipt().verify(full).expect("restart prefix retained");
        cancellation.cancel();
        let prefix = resumed_work.receipt();
        let live = resumed_work.live_allocation_bytes();
        drop(inspection);
        drop(custody);
        assert_eq!(resumed_work.receipt(), prefix, "output retirement never allocates or checks cancellation");
        assert_eq!(resumed_work.live_allocation_bytes(), live - output_bytes);
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct FaultingLocalJournal<'a> {
        store: crate::core_state::MeasuredMaterializationStore<'a>,
        fail_revision: u64,
        lose_ack: bool,
        stage_fault: Option<&'a FaultingStageWrite>,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct FaultingStageWrite {
        file: PathBuf,
        expected: &'static [u8],
        work: NativeMaterializationWork,
        boundary: Mutex<Option<crate::WorkCounters>>,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct FaultingLocalActionCompletion<'a, C> {
        actual: C,
        fail: bool,
        lose_ack: bool,
        stage_fault: Option<&'a FaultingStageWrite>,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl<C: MaterializationActionCompletion<Error = crate::LocalCoreStateStoreError>>
        MaterializationActionCompletion for FaultingLocalActionCompletion<'_, C>
    {
        type Error = crate::LocalCoreStateStoreError;

        async fn complete(self) -> Result<OwnedMaterializationJournal, Self::Error> {
            let stage_failed = self.stage_fault.is_some_and(|fault| {
                let mut bytes = [0; 64];
                let Some(bytes) = bytes.get_mut(..fault.expected.len()) else { return false; };
                std::fs::File::open(&fault.file)
                    .and_then(|mut file| std::io::Read::read_exact(&mut file, bytes)).is_ok()
                    && bytes == fault.expected
            });
            let fail = self.fail || stage_failed;
            if fail && !self.lose_ack {
                if let Some(fault) = self.stage_fault {
                    *fault.boundary.lock().expect("actual failure boundary") = Some(fault.work.receipt());
                }
                return Err(if stage_failed { std::io::Error::from(std::io::ErrorKind::PermissionDenied) }
                    else { std::io::Error::other("injected pre-journal failure") }.into());
            }
            let journal = self.actual.complete().await?;
            if fail {
                if let Some(fault) = self.stage_fault {
                    *fault.boundary.lock().expect("actual lost-ack boundary") = Some(fault.work.receipt());
                }
                return Err(if stage_failed { std::io::Error::from(std::io::ErrorKind::PermissionDenied) }
                    else { std::io::Error::other("injected lost journal acknowledgement") }.into());
            }
            Ok(journal)
        }
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl<'store> MaterializationJournalStore for FaultingLocalJournal<'store> {
        type Error = crate::LocalCoreStateStoreError;

        type ActionCompletion<'a> = FaultingLocalActionCompletion<'a,
            <crate::core_state::MeasuredMaterializationStore<'store> as MaterializationJournalStore>::ActionCompletion<'a>>
        where Self: 'a;

        async fn prepare_action<'a>(&'a self, journal: OwnedMaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
            let fail = journal.revision.checked_add(1) == Some(self.fail_revision);
            Ok(FaultingLocalActionCompletion {
                actual: self.store.prepare_action(journal).await?,
                fail,
                lose_ack: self.lose_ack,
                stage_fault: self.stage_fault,
            })
        }

        async fn load(&self, operation_id: OperationId) -> Result<Option<OwnedMaterializationJournal>, Self::Error> {
            self.store.load(operation_id).await
        }

        async fn compare_and_swap(&self, operation_id: OperationId, expected_revision: u64, replacement: OwnedMaterializationJournal) -> Result<(bool, OwnedMaterializationJournal), Self::Error> {
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

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn bounded_native_restart_retains_work_after_physical_and_journal_failures() {
        for lose_ack in [false, true] {
            let directory = tempfile::tempdir().expect("temporary directory");
            let root = directory.path().join("root");
            let operation = directory.path().join("operation");
            let state_root = directory.path().join("state");
            std::fs::create_dir_all(&root).expect("root");
            std::fs::create_dir_all(operation.join("target")).expect("target");
            std::fs::write(root.join("file"), b"before").expect("source");
            std::fs::write(operation.join("target/file"), b"after").expect("target file");
            let cancellation = crate::CancellationToken::new();
            let budget = finite_native_budget();
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("native meter");
            let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation.clone(), work.clone()).expect("backend");
            let plan = backend.plan_paths(OperationId::new(), GenerationId::new(crate::Digest::from_bytes([1; 32])), GenerationId::new(crate::Digest::from_bytes([2; 32])), ["file".to_owned()]).expect("plan");
            let operation_id = plan.operation_id;
            let original_root = backend.allocation_identity();
            let original_preparation = backend.preparation_identity();
            let state = crate::LocalCoreStateStore::new(&state_root);
            let fault = FaultingLocalJournal { store: state.materialization_with_work(work.clone()), fail_revision: 5, lose_ack, stage_fault: None };
            let materializer = JournaledMaterializer::new(fault, backend);
            let error = materializer.apply(plan).await.expect_err("injected journal failure");
            assert!(matches!(error, MaterializationError::Store(_)));
            assert_eq!(std::fs::read(root.join("file")).expect("physical mutation"), b"after");
            assert!(work.receipt().bytes_hashed > 0);
            assert!(work.receipt().object_bytes_written > 0);
            assert!(work.receipt().durability_operations > 0);
            work.receipt().verify(budget).expect("failure work fits finite budget");
            drop(materializer);
            drop(state);
            let reopened = JournaledMaterializer::new(crate::LocalCoreStateStore::new(&state_root), NativeTreeMaterializationBackend::new(&root, &operation).expect("real backend restart"));
            let inspection = reopened.inspect_bounded(operation_id, budget, &cancellation).await.expect("inspect retained operation").value.expect("retained journal");
            assert_eq!(inspection.journal.root_identity, original_root);
            assert_eq!(inspection.journal.preparation_identity, original_preparation);
            assert!(inspection.journal.work.object_bytes_written > 0);
            assert_eq!(inspection.journal.applied, u32::from(lose_ack));
            assert_eq!(inspection.observations, [MaterializationObservation::Postimage]);
            let completed = reopened.recover_bounded(operation_id, MaterializationRecovery::Complete, budget, &cancellation).await.expect("bounded recovery");
            assert_eq!(completed.value.expect("original operation").phase, MaterializationPhase::Applied);
            completed.work.verify(budget).expect("recovery work fits finite budget");
            let loaded = reopened.load_bounded(operation_id, budget, &cancellation).await.expect("baseline bounded read");
            let mut exact = loaded.work;
            exact.peak_allocation_bytes = exact.peak_allocation_bytes.max(64 * 1024);
            let replay = reopened.load_bounded(operation_id, exact, &cancellation).await.expect("exact journal read boundary");
            assert_eq!(replay.work, loaded.work);
            exact.source_bytes_read -= 1;
            let failure = reopened.load_bounded(operation_id, exact, &cancellation).await.expect_err("journal byte exhaustion");
            assert_eq!(failure.1.source_bytes_read, exact.source_bytes_read);
            failure.1.verify(exact).expect("failure retains exact spent read prefix");
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
        let workspace = fs.create_workspace("native-publication").await.expect("workspace");
        for name in ["/one", "/two", "/untouched"] { workspace.write_text(name, "before").await.expect("logical baseline"); }
        let from = workspace.head().await.expect("original generation");
        for name in ["/one", "/two"] { workspace.write_text(name, "after").await.expect("logical target"); }
        let to = workspace.head().await.expect("target generation");
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
        assert_eq!(loaded.value.as_ref().map(|owned| &owned.journal), Some(&prepared.value.journal));
        std::fs::rename(moved_root, &root).expect("original allocation returns");
        std::fs::rename(moved_operation, &operation).expect("original staging returns");
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("apply meter");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let failure = publish_native_generation_transition_with_work_guarded(
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
        let resumed = publish_native_generation_transition_with_work_guarded(
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
        let workspace = fs.create_workspace("before-absent").await.expect("workspace");
        let from = workspace.head().await.expect("empty baseline");
        workspace.write_text("/new", "after").await.expect("target");
        let to = workspace.head().await.expect("target generation");
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
        let failure = publish_native_generation_transition_with_work_guarded(&from, &to, &state, publication(),
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

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_substitute_preserves_quota_on_overflow_and_releases_after_cancellation() {
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("meter");
        let mut allocation = work.claim(17).expect("original opaque quota");
        let live = work.live_allocation_bytes();
        let peak = work.receipt().peak_allocation_bytes;
        assert!(matches!(allocation.substitute(18), Err(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))));
        assert_eq!(allocation.bytes(), 17);
        assert_eq!(work.live_allocation_bytes(), live);
        cancellation.cancel();
        allocation.substitute(5).expect("actual retained payload substitution is not a new allocation");
        assert_eq!(work.live_allocation_bytes(), live - 12);
        assert_eq!(work.receipt().peak_allocation_bytes, peak);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_owned_disposal_retains_partial_work_and_refuses_replacement_allocation() {
        let temporary = tempfile::tempdir().expect("protected service");
        let service = temporary.path().join("service");
        std::fs::create_dir(&service).expect("protected fixture");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("owned meter");
        let service_id = work.root_identity(&service).expect("service original");
        let scratch_id = work.create_owned_directory(&service, service_id, Path::new("task/scratch"), true).expect("actual newly allocated scratch");
        assert!(matches!(work.create_owned_directory(&service, service_id, Path::new("task/scratch"), true), Err(NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists));
        for name in ["one", "two"] { std::fs::write(service.join("task/scratch").join(name), b"changed private data").expect("private contents"); }
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let error = work.release_owned_directory_guarded(&service, service_id, Path::new("task/scratch"), scratch_id, || std::future::ready(
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { Ok(()) }
            else { Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()) }
        )).await.expect_err("original owner loses permission after one deletion");
        assert!(matches!(error, NativeTreeMaterializationError::Io(_)));
        assert_eq!(std::fs::read_dir(service.join("task/scratch")).expect("partial private root").count(), 1);
        assert_eq!(work.root_identity(&service.join("task/scratch")).expect("original root retained"), scratch_id);
        work.receipt().verify(budget).expect("all partial deletion work");
        let moved = service.join("task/original");
        std::fs::rename(service.join("task/scratch"), &moved).expect("replace scratch binding");
        std::fs::create_dir(service.join("task/scratch")).expect("replacement user root");
        std::fs::write(service.join("task/scratch/user"), b"never adopt").expect("replacement data");
        let error = work.release_owned_directory_guarded(&service, service_id, Path::new("task/scratch"), scratch_id, || std::future::ready(Ok(()))).await.expect_err("cannot adopt replacement");
        assert!(matches!(error, NativeTreeMaterializationError::AllocationChanged { expected, .. } if expected == scratch_id));
        assert_eq!(std::fs::read(service.join("task/scratch/user")).expect("replacement untouched"), b"never adopt");
        std::fs::remove_dir_all(service.join("task/scratch")).expect("remove fixture replacement");
        std::fs::rename(moved, service.join("task/scratch")).expect("original allocation returns");
        work.release_owned_directory_guarded(&service, service_id, Path::new("task/scratch"), scratch_id, || std::future::ready(Ok(()))).await.expect("changed original private contents can be disposed");
        assert!(!service.join("task/scratch").exists());
        work.receipt().verify(budget).expect("complete disposal work");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn private_owned_disposal_rejects_symlink_without_touching_external_tree() {
        let temporary = tempfile::tempdir().expect("protected service");
        let service = temporary.path().join("service");
        let external = temporary.path().join("external");
        std::fs::create_dir(&service).expect("service fixture");
        std::fs::create_dir(&external).expect("external fixture");
        std::fs::write(external.join("user"), b"outside").expect("external data");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("meter");
        let service_id = work.root_identity(&service).expect("original protected service");
        let scratch_id = work.create_owned_directory(&service, service_id, Path::new("scratch"), true).expect("original scratch");
        std::os::unix::fs::symlink(&external, service.join("scratch/link")).expect("untrusted link");
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let error = work.release_owned_directory_guarded(&service, service_id, Path::new("scratch"), scratch_id, || {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::future::ready(Ok(()))
        }).await.expect_err("reparse-like child is not followed or silently deleted");
        assert!(matches!(error, NativeTreeMaterializationError::Io(error) if error.kind() == std::io::ErrorKind::PermissionDenied));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(std::fs::symlink_metadata(service.join("scratch/link")).expect("link retained").file_type().is_symlink());
        assert_eq!(std::fs::read(external.join("user")).expect("external untouched"), b"outside");
        work.receipt().verify(budget).expect("finite denied traversal");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn private_owned_disposal_cancellation_retains_original_partial_allocation() {
        let temporary = tempfile::tempdir().expect("protected service");
        let service = temporary.path().join("service");
        std::fs::create_dir(&service).expect("service fixture");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("meter");
        let service_id = work.root_identity(&service).expect("service allocation");
        let scratch_id = work.create_owned_directory(&service, service_id, Path::new("scratch"), true).expect("private leaf");
        for name in ["one", "two"] { std::fs::write(service.join("scratch").join(name), b"private").expect("private data"); }
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let error = work.release_owned_directory_guarded(&service, service_id, Path::new("scratch"), scratch_id, || {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 { cancellation.cancel(); }
            std::future::ready(Ok(()))
        }).await.expect_err("same token stops second actual action");
        assert!(matches!(error, NativeTreeMaterializationError::Cancelled));
        assert_eq!(std::fs::read_dir(service.join("scratch")).expect("incomplete allocation").count(), 1);
        assert!(work.receipt().backend_write_operations > 0);
        work.receipt().verify(budget).expect("actual cancellation prefix retained");
        let resumed = NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("restart meter");
        assert_eq!(resumed.root_identity(&service.join("scratch")).expect("original incomplete physical allocation"), scratch_id);
        resumed.release_owned_directory_guarded(&service, service_id, Path::new("scratch"), scratch_id, || std::future::ready(Ok(()))).await.expect("resume same original allocation disposal");
        assert!(!service.join("scratch").exists());
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
        let workspace = fs.create_workspace("structural-source").await.expect("workspace");
        let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new()).await.expect("logical transaction");
        transaction.create_directory("/tree").await.expect("logical directory");
        transaction.write_text("/tree/file", "before").await.expect("logical descendant");
        transaction.commit().await.expect("logical publication");
        let generation = workspace.head().await.expect("baseline");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("meter");
        let held = crate::native_host::HostRoot::open_materialization_measured(&root, &work).expect("held checkout");
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
                let plan = MaterializationPlan { operation_id,
                    intent: MaterializationIntent::GenerationTransition {
                        from: GenerationId::new(Digest::from_bytes([1; 32])),
                        to: GenerationId::new(Digest::from_bytes([2; 32])),
                    },
                    edits: vec![MaterializationEdit::Remove { path: "file".to_owned() }],
                };
                let state = crate::LocalCoreStateStore::new(&state_path);
                let journal = MaterializationJournal {
                    version: JOURNAL_VERSION, revision: 0, plan,
                    root_identity: Some(source.to_bytes()), preparation_identity: None,
                    preparation_progress: Some(MaterializationPreparationProgress {
                        parent: parent.to_bytes(), operation: None, target: None, backup: None, pending: None,
                    }),
                    work: work.journal_work().expect("original prefix"), preimages: Vec::new(),
                    phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
                };
                let custody = work.retain_owned(journal_owned_bytes(&journal).expect("actual fixture capacity")).expect("original fixture admission");
                let journal = OwnedMaterializationJournal { journal, custody: Some(custody) };
                let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), journal).await.expect("first original intent").1;
                assert!(!operation.exists());
                let fault = FaultingLocalJournal { store: state.materialization_with_work(work.clone()), fail_revision, lose_ack, stage_fault: None };
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

    struct SourceCaptureBarrier {
        valid: std::sync::atomic::AtomicBool,
        effects: std::sync::atomic::AtomicUsize,
    }
    impl MaterializationBackend for SourceCaptureBarrier {
        type Error = std::io::Error;
        fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> {
            journal.preimages.push(preimage.preimage);
            Ok(())
        }
        async fn verify_source(&self, _: GenerationId) -> Result<bool, Self::Error> { Ok(self.valid.load(std::sync::atomic::Ordering::SeqCst)) }
        async fn capture(&self, _: &MaterializationEdit) -> Result<OwnedMaterializationPreimage, Self::Error> {
            self.valid.store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(OwnedMaterializationPreimage::ordinary(MaterializationPreimage { image: vec![1] }))
        }
        async fn apply(&self, _: &MaterializationEdit, _: &MaterializationPreimage) -> Result<(), Self::Error> { self.effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst); Ok(()) }
        async fn restore(&self, _: &MaterializationEdit, _: &MaterializationPreimage) -> Result<(), Self::Error> { Ok(()) }
    }

    #[tokio::test]
    async fn original_source_is_reverified_after_capture_before_first_generic_effect() {
        let engine = JournaledMaterializer::new(MemoryMaterializationJournalStore::default(), SourceCaptureBarrier {
            valid: std::sync::atomic::AtomicBool::new(true), effects: std::sync::atomic::AtomicUsize::new(0),
        });
        let operation_id = OperationId::new();
        let error = engine.apply(MaterializationPlan { operation_id,
            intent: MaterializationIntent::GenerationTransition {
                from: GenerationId::new(Digest::from_bytes([1; 32])),
                to: GenerationId::new(Digest::from_bytes([2; 32])),
            },
            edits: vec![MaterializationEdit::Remove { path: "file".to_owned() }],
        }).await.expect_err("capture barrier invalidates the original source");
        assert!(matches!(error, MaterializationError::PlanConflict));
        assert_eq!(engine.backend().effects.load(std::sync::atomic::Ordering::SeqCst), 0);
        let journal = engine.load(operation_id).await.expect("original query").expect("original operation");
        assert_eq!(journal.phase, MaterializationPhase::Applying);
        assert_eq!(journal.applied, 0);
        assert_eq!(journal.preimages.len(), 1);
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
        fn append_preimage(&self, journal: &mut OwnedMaterializationJournal, preimage: OwnedMaterializationPreimage) -> Result<(), Self::Error> { self.inner.append_preimage(journal, preimage) }
        async fn observe_allocation_identity(&self) -> Result<Option<[u8; 16]>, Self::Error> { self.inner.observe_allocation_identity().await }
        async fn observe_preparation_identity(&self) -> Result<Option<MaterializationPreparationIdentity>, Self::Error> { self.inner.observe_preparation_identity().await }
        async fn observe(&self, edit: &MaterializationEdit, preimage: &MaterializationPreimage) -> Result<MaterializationObservation, Self::Error> { self.inner.observe(edit, preimage).await }
        async fn capture(&self, edit: &MaterializationEdit) -> Result<OwnedMaterializationPreimage, Self::Error> {
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
        let workspace = fs.create_workspace("native-capture-barrier").await.expect("workspace");
        workspace.write_text("/file", "before").await.expect("original authored source");
        let from = workspace.head().await.expect("immutable original generation");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("same actual native meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation.clone(), work.clone()).expect("actual native capabilities");
        let plan = backend.plan_paths(OperationId::new(), from.id(), GenerationId::new(Digest::from_bytes([2; 32])), ["file".to_owned()]).expect("original immutable plan");
        let operation_id = plan.operation_id;
        native_preflight_plan(&backend.held_root, &plan, &from, &work, &cancellation).await.expect("source really matches before capture");
        let source = backend.allocation_identity();
        let preparation = backend.preparation_identity();
        let state = crate::LocalCoreStateStore::new(&state_path);
        let engine = JournaledMaterializer::new(state.materialization_with_work(work.clone()), NativeCaptureBarrier {
            inner: backend, file: root.join("file"), armed: std::sync::atomic::AtomicBool::new(true),
            effects: std::sync::atomic::AtomicUsize::new(0),
        });
        let owners = std::sync::atomic::AtomicUsize::new(0);
        let mut guarded = |verify| {
            owners.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            native_before_effect(std::future::ready(Ok(())), verify, &engine.store, &engine.backend().inner.held_root, operation_id, &from, &work, &cancellation)
        };
        let error = engine.apply_with_guard(plan, &mut guarded).await.expect_err("fresh original-source verification rejects the captured user change");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_, Some(_)))));
        assert_eq!(owners.load(std::sync::atomic::Ordering::SeqCst), 3);
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

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct AllocationBoundaryJournal<'a> {
        store: crate::core_state::MeasuredMaterializationStore<'a>,
        work: NativeMaterializationWork,
        boundary: std::sync::Arc<std::sync::Mutex<Option<crate::WorkCounters>>>,
        cancel_after: Option<u64>,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct AllocationBoundaryActionCompletion<C> {
        actual: C,
        work: NativeMaterializationWork,
        boundary: std::sync::Arc<std::sync::Mutex<Option<crate::WorkCounters>>>,
        revision: Option<u64>,
        cancel_after: Option<u64>,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl<C: MaterializationActionCompletion<Error = crate::LocalCoreStateStoreError>>
        MaterializationActionCompletion for AllocationBoundaryActionCompletion<C>
    {
        type Error = crate::LocalCoreStateStoreError;

        async fn complete(self) -> Result<OwnedMaterializationJournal, Self::Error> {
            if self.revision == Some(3) {
                *self.boundary.lock().expect("boundary fixture") = Some(self.work.receipt());
            }
            let journal = self.actual.complete().await?;
            if self.cancel_after == self.revision && self.cancel_after.is_some() {
                self.work.cancellation.cancel();
            }
            Ok(journal)
        }
    }
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl<'store> MaterializationJournalStore for AllocationBoundaryJournal<'store> {
        type Error = crate::LocalCoreStateStoreError;
        type ActionCompletion<'a> = AllocationBoundaryActionCompletion<
            <crate::core_state::MeasuredMaterializationStore<'store> as MaterializationJournalStore>::ActionCompletion<'a>>
        where Self: 'a;

        async fn prepare_action<'a>(&'a self, journal: OwnedMaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
            let revision = journal.revision.checked_add(1);
            Ok(AllocationBoundaryActionCompletion {
                actual: self.store.prepare_action(journal).await?,
                work: self.work.clone(),
                boundary: self.boundary.clone(),
                revision,
                cancel_after: self.cancel_after,
            })
        }

        async fn load(&self, operation: OperationId) -> Result<Option<OwnedMaterializationJournal>, Self::Error> { self.store.load(operation).await }
        async fn compare_and_swap(&self, operation: OperationId, expected: u64, journal: OwnedMaterializationJournal) -> Result<(bool, OwnedMaterializationJournal), Self::Error> {
            let revision = journal.revision;
            if revision == 3 { *self.boundary.lock().expect("boundary fixture") = Some(self.work.receipt()); }
            let result = self.store.compare_and_swap(operation, expected, journal).await?;
            if result.0 && self.cancel_after == Some(revision) { self.work.cancellation.cancel(); }
            Ok(result)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_diverged_root_inspection_has_no_synthetic_tail_custody() {
        let temporary = tempfile::tempdir().expect("real inspection fixture");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("original root");
        let work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("original meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("held roots");
        let source = work.root_identity(&root).expect("original identity");
        let parent = work.root_identity(temporary.path()).expect("parent identity");
        let id = OperationId::new();
        let mut journal = allocation_fixture_journal(id, source, parent, &work);
        journal.revision = 1;
        let store = MemoryMaterializationJournalStore::default();
        let (stored, journal) = store.compare_and_swap(id, 0, journal).await.expect("original packet CAS");
        assert!(stored);
        drop(journal);
        std::fs::rename(&root, temporary.path().join("displaced")).expect("root swap");
        std::fs::create_dir(&root).expect("replacement root");
        let engine = JournaledMaterializer::new(store, backend);
        let (inspection, tail) = engine.inspect_owned(id).await.expect("inspect original packet").expect("retained journal");
        assert!(matches!(inspection.root, MaterializationRootObservation::Diverged { original, .. } if original == source.to_bytes()));
        assert!(inspection.observations.is_empty());
        assert!(tail.is_none(), "no observation or diagnostic backing means no native marker");
        assert!(inspection.journal.custody.as_ref().expect("intrinsic journal custody").work().shares_ledger(&work));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn native_captured_witness_append_keeps_original_custody_until_packet_drop() {
        let temporary = tempfile::tempdir().expect("real capture fixture");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("source root");
        std::fs::create_dir_all(operation.join("target")).expect("staging root");
        std::fs::write(root.join("file"), b"before").expect("source");
        std::fs::write(operation.join("target/file"), b"after").expect("target");
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("original meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("held native backend");
        let baseline = work.live_allocation_bytes();
        let source = work.root_identity(&root).expect("source witness");
        let parent = work.root_identity(temporary.path()).expect("parent witness");
        let mut journal = allocation_fixture_journal(OperationId::new(), source, parent, &work);
        let preimage = backend.capture(&MaterializationEdit::Install { path: "file".to_owned(), image: Vec::new() }).await.expect("actual captured witness");
        let witness_bytes = preimage.preimage.image.capacity() as u64;
        let witness_guard = preimage.custody.as_ref().expect("original witness custody");
        assert_eq!(witness_guard.bytes(), witness_bytes);
        assert!(witness_guard.work().shares_ledger(&work));
        backend.append_preimage(&mut journal, preimage).expect("transfer original witness and actual vector growth");
        let custody = journal.custody.as_ref().expect("intrinsic packet custody");
        assert!(custody.work().shares_ledger(&work));
        assert_eq!(custody.bytes(), journal_owned_bytes(&journal).expect("actual nested capacities"));
        assert_eq!(work.live_allocation_bytes(), baseline + custody.bytes());
        cancellation.cancel();
        let prefix = work.receipt();
        drop(journal);
        assert_eq!(work.live_allocation_bytes(), baseline);
        assert_eq!(work.receipt(), prefix, "packet retirement performs no late admission");
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn captured_fixture_journal(journal: MaterializationJournal, preimage: OwnedMaterializationPreimage, backend: &NativeTreeMaterializationBackend) -> OwnedMaterializationJournal {
        let custody = Some(backend.work.retain_owned(journal_owned_bytes(&journal).expect("actual fixture capacity")).expect("actual fixture journal admission"));
        let mut owned = OwnedMaterializationJournal { journal, custody };
        backend.append_preimage(&mut owned, preimage).expect("original captured witness transfer");
        owned
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn allocation_fixture_journal(operation: OperationId, root: crate::NativeRootIdentity, parent: crate::NativeRootIdentity, work: &NativeMaterializationWork) -> OwnedMaterializationJournal {
        let journal = MaterializationJournal {
            version: JOURNAL_VERSION, revision: 0,
            plan: MaterializationPlan { operation_id: operation,
                intent: MaterializationIntent::GenerationTransition {
                    from: GenerationId::new(Digest::from_bytes([1; 32])), to: GenerationId::new(Digest::from_bytes([2; 32])),
                }, edits: vec![MaterializationEdit::Remove { path: "file".to_owned() }],
            },
            root_identity: Some(root.to_bytes()), preparation_identity: None,
            preparation_progress: Some(MaterializationPreparationProgress { parent: parent.to_bytes(), operation: None, target: None, backup: None, pending: None }),
            work: crate::WorkCounters::UNCHARGED, preimages: Vec::new(), phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
        };
        let custody = work.retain_owned(journal_owned_bytes(&journal).expect("actual fixture capacity")).expect("original fixture admission");
        OwnedMaterializationJournal { journal, custody: Some(custody) }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn memory_action_completion_funds_original_native_payload_before_effect() {
        let directory = tempfile::tempdir().expect("physical action fixture");
        let file = directory.path().join("file");
        std::fs::write(&file, b"old").expect("original file");
        let cancellation = crate::CancellationToken::new();
        let budget = finite_native_budget();
        let original = NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
        let work = original.native_volume_work(budget).expect("funded original resource");
        let identity = work.root_identity(directory.path()).expect("actual root");
        let id = OperationId::new();
        let mut journal = allocation_fixture_journal(id, identity, identity, &work);
        journal.revision = 1;
        let store = MemoryMaterializationJournalStore::default();
        let (stored, journal) = store.compare_and_swap(id, 0, journal).await.expect("original durable packet");
        assert!(stored);
        let payload = journal.plan.edits.as_ptr() as usize;
        let original_bytes = journal.custody.as_ref().expect("actual Some custody").bytes();
        let mut output = std::fs::OpenOptions::new().write(true).open(&file).expect("actual output");
        let before = original.receipt();
        let live = original.live_allocation_bytes();
        let completion = store.prepare_action(journal).await.expect("actual provider reservation before effect");
        let admitted = original.receipt();
        assert!(admitted.bytes_copied > before.bytes_copied, "actual replacement payload copied before the action");
        assert!(admitted.allocation_operations > before.allocation_operations);
        assert!(original.live_allocation_bytes() >= live + original_bytes);
        assert_eq!(std::fs::read(&file).expect("before effect"), b"old");
        work.write_owned_bytes(&mut output, b"new").expect("actual original-resource effect");
        cancellation.cancel();
        let prefix = original.receipt();
        let durable_prefix = work.journal_work().expect("pure original prefix");
        let admitted_live = original.live_allocation_bytes();
        let completed = completion.complete().await.expect("accepted completion survives cancellation");
        assert_eq!(original.receipt(), prefix, "completion performs no new payload copying or allocation; opaque BTreeMap overhead is excluded");
        assert!(original.live_allocation_bytes() <= admitted_live);
        assert_eq!(completed.plan.edits.as_ptr() as usize, payload, "the original journal packet returns");
        assert_eq!(completed.revision, 2);
        assert_eq!(completed.work, durable_prefix);
        let custody = completed.custody.as_ref().expect("original Some custody remains intrinsic");
        assert!(custody.work().shares_allocation_context(&work));
        assert_eq!(std::fs::read(&file).expect("physical effect"), b"new");
        let records = store.journals.0.lock().expect("actual memory records");
        let persisted = records.get(&id).expect("completed original record");
        assert_eq!(persisted.journal, completed.journal);
        assert_ne!(persisted.plan.edits.as_ptr() as usize, payload, "the genuinely pre-admitted clone is persisted");
        assert!(persisted.custody.as_ref().expect("funded stored clone").work().shares_allocation_context(&work));
        prefix.verify(budget).expect("actual whole-operation prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn memory_action_completion_copy_refusal_precedes_physical_effect() {
        let directory = tempfile::tempdir().expect("physical refusal fixture");
        let file = directory.path().join("file");
        std::fs::write(&file, b"old").expect("unchanged original file");
        let identity_work = NativeMaterializationWork::new(finite_native_budget(), &crate::CancellationToken::new()).expect("identity-only meter");
        let identity = identity_work.root_identity(directory.path()).expect("actual root");
        let mut budget = finite_native_budget();
        budget.bytes_copied = 4;
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(budget, &cancellation).expect("exact original copy meter");
        let work = original.native_volume_work(budget).expect("funded original resource");
        let id = OperationId::new();
        let mut journal = allocation_fixture_journal(id, identity, identity, &work);
        journal.revision = 1;
        let store = MemoryMaterializationJournalStore::default();
        let (stored, journal) = store.compare_and_swap(id, 0, journal).await.expect("four path bytes in original stored clone");
        assert!(stored);
        assert_eq!(original.receipt().bytes_copied, 4);
        let payload = journal.plan.edits.as_ptr() as usize;
        let denied = store.prepare_action(journal).await.err().expect("real completion copy cannot be admitted");
        let MemoryMaterializationJournalStoreError::ActionNativeWork(
            NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "bytes_copied", .. }), journal,
        ) = denied else {
            panic!("expected the original provider copy-budget failure");
        };
        assert_eq!(journal.plan.edits.as_ptr() as usize, payload);
        assert!(journal.custody.as_ref().expect("original denied packet custody").work().shares_allocation_context(&work));
        assert_eq!(std::fs::read(&file).expect("before any effect"), b"old");
        assert_eq!(store.journals.0.lock().expect("actual records").get(&id).expect("original durable journal").revision, 1);
        original.receipt().verify(budget).expect("only genuinely spent preparation prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn memory_action_completion_conflict_retains_original_native_packet() {
        let directory = tempfile::tempdir().expect("original root fixture");
        let cancellation = crate::CancellationToken::new();
        let original = NativeMaterializationWork::new(finite_native_budget(), &cancellation).expect("original meter");
        let work = original.native_volume_work(finite_native_budget()).expect("funded original resource");
        let identity = work.root_identity(directory.path()).expect("actual root");
        let id = OperationId::new();
        let mut journal = allocation_fixture_journal(id, identity, identity, &work);
        journal.revision = 1;
        let store = MemoryMaterializationJournalStore::default();
        let (stored, journal) = store.compare_and_swap(id, 0, journal).await.expect("original durable packet");
        assert!(stored);
        let payload = journal.plan.edits.as_ptr() as usize;
        let completion = store.prepare_action(journal).await.expect("actual provider completion reservation");
        let mut competitor = store.load(id).await.expect("actual competing load").expect("original record");
        competitor.revision = 2;
        let (stored, competitor) = store.compare_and_swap(id, 1, competitor).await.expect("real competing CAS");
        assert!(stored);
        let before = original.receipt();
        let error = completion.complete().await.expect_err("the genuine CAS must observe the competing revision");
        let MemoryMaterializationJournalStoreError::ActionConflict(journal) = error else {
            panic!("expected actual completion CAS conflict");
        };
        assert_eq!(original.receipt(), before, "conflicted completion makes no late payload copy");
        assert_eq!(journal.plan.edits.as_ptr() as usize, payload);
        assert!(journal.custody.as_ref().expect("original error packet custody").work().shares_allocation_context(&work));
        assert_eq!(store.journals.0.lock().expect("actual records").get(&id).expect("winning record").journal, competitor.journal);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn memory_action_completion_preserves_genuine_ordinary_none_custody() {
        let id = OperationId::new();
        let journal = OwnedMaterializationJournal::ordinary(MaterializationJournal {
            version: JOURNAL_VERSION, revision: 1,
            plan: MaterializationPlan {
                operation_id: id,
                intent: MaterializationIntent::GenerationTransition {
                    from: GenerationId::new(Digest::from_bytes([1; 32])),
                    to: GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: vec![MaterializationEdit::Remove { path: "file".to_owned() }],
            },
            root_identity: None, preparation_identity: None, preparation_progress: None,
            work: crate::WorkCounters::UNCHARGED, preimages: Vec::new(),
            phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
        });
        let store = MemoryMaterializationJournalStore::default();
        let (stored, journal) = store.compare_and_swap(id, 0, journal).await.expect("ordinary durable packet");
        assert!(stored);
        assert!(journal.custody.is_none());
        let completion = store.prepare_action(journal).await.expect("genuine ordinary provider completion");
        let journal = completion.complete().await.expect("ordinary completion performs its actual CAS");
        assert!(journal.custody.is_none());
        assert_eq!(journal.revision, 2);
        assert_eq!(journal.work, crate::WorkCounters::UNCHARGED);
        let loaded = store.load(id).await.expect("actual ordinary load").expect("completed record");
        assert!(loaded.custody.is_none());
        assert_eq!(loaded.journal, journal.journal);
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    fn staging_fixture_journal(
        operation: OperationId, from: GenerationId, to: GenerationId,
        backend: &NativeTreeMaterializationBackend, work: &NativeMaterializationWork,
    ) -> OwnedMaterializationJournal {
        let journal = MaterializationJournal {
            version: JOURNAL_VERSION, revision: 1,
            plan: MaterializationPlan {
                operation_id: operation, intent: MaterializationIntent::GenerationTransition { from, to },
                edits: ["one", "two"].into_iter().map(|path| MaterializationEdit::Install {
                    path: path.to_owned(), image: vec![1],
                }).collect(),
            },
            root_identity: backend.allocation_identity(), preparation_identity: backend.preparation_identity(),
            preparation_progress: None, work: work.journal_work().expect("original known work"),
            preimages: Vec::new(), phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
        };
        let custody = work.retain_journal(&journal).expect("original staging packet capacities");
        OwnedMaterializationJournal { journal, custody: Some(custody) }
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn real_local_sdk_write_completion_is_durable_before_next_owner_denial() {
        let temporary = tempfile::tempdir().expect("real stage fixture");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        let target = operation.join("target");
        std::fs::create_dir(&root).expect("original checkout");
        std::fs::create_dir_all(&target).expect("owned stage allocation");
        for path in ["one", "two"] { std::fs::write(root.join(path), b"before").expect("original checkout content"); }
        let fs = crate::Fs::memory();
        let workspace = fs.create_workspace("real-stage-completion-before-owner").await.expect("workspace");
        let from = workspace.head().await.expect("initial generation");
        workspace.write_text("/one", "after-one").await.expect("first target");
        workspace.write_text("/two", "after-two").await.expect("second target");
        let to = workspace.head().await.expect("immutable target generation");
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
        let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("original held roots");
        let original_root = backend.allocation_identity();
        let original_preparation = backend.preparation_identity();
        let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
        let id = OperationId::new();
        let (stored, journal) = state.materialization_with_work(work.clone())
            .compare_and_swap(id, 0, staging_fixture_journal(id, from.id(), to.id(), &backend, &work))
            .await.expect("real Local initial journal");
        assert!(stored);
        let initial_work = journal.work;
        let engine = JournaledMaterializer::new(state.materialization_with_work(work.clone()), backend);
        let expected_target = crate::NativeRootIdentity::from_bytes(original_preparation.expect("actual held preparation").target);
        let (options, _options_custody) = native_stage_options(&crate::MaterializeOptions::native(&root), &target, expected_target, &work).expect("actual stage options");
        let denied = std::sync::atomic::AtomicBool::new(false);
        let durable_at_denial = Mutex::new(None);
        let mut owner = || async {
            if std::fs::read(target.join("one")).is_ok_and(|bytes| bytes == b"after-one") {
                let current = state.materialization_with_work(work.clone()).load(id).await
                    .map_err(NativeWorkspacePublicationError::State)?.expect("original durable journal at next owner");
                assert!(current.work.output_bytes >= initial_work.output_bytes + 9, "the first SDK write is in the previously completed original journal");
                assert_eq!(current.phase, MaterializationPhase::Preparing);
                assert_eq!(current.root_identity, original_root);
                assert_eq!(current.preparation_identity, original_preparation);
                *durable_at_denial.lock().expect("owner boundary") = Some(current.work);
                denied.store(true, SeqCst);
                Err(NativeWorkspacePublicationError::Native(std::io::Error::from(std::io::ErrorKind::PermissionDenied).into()))
            } else {
                Ok(())
            }
        };
        let error = engine.stage_generation_owned_guarded(&to, &["one".to_owned(), "two".to_owned()], &options,
            journal, &work.native_mount_work(), &mut owner).await.expect_err("next real SDK action loses original owner authority");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(_)));
        assert!(denied.load(SeqCst));
        assert_eq!(std::fs::read(target.join("one")).expect("acknowledged physical SDK prefix"), b"after-one");
        assert!(!target.join("two").exists(), "no next selected-file mutation");
        for path in ["one", "two"] { assert_eq!(std::fs::read(root.join(path)).expect("checkout untouched"), b"before"); }
        let retained = load_native_materialization_journal(&crate::LocalCoreStateStore::new(state.root().to_path_buf()), id, budget,
            &crate::CancellationToken::new()).await.expect("cold original journal query").value.expect("original journal remains");
        assert_eq!(retained.work, durable_at_denial.lock().expect("owner boundary").expect("durable first write"));
        assert_eq!(retained.root_identity, original_root);
        assert_eq!(retained.preparation_identity, original_preparation);
        assert_eq!(retained.phase, MaterializationPhase::Preparing);
        work.receipt().verify(budget).expect("actual finite prefix");
    }

    #[cfg(all(feature = "local", feature = "native-mount", feature = "memory", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn real_local_sdk_write_completion_failure_keeps_honest_uncertain_prefix() {
        for lose_ack in [false, true] {
            let temporary = tempfile::tempdir().expect("real failed completion fixture");
            let root = temporary.path().join("root");
            let operation = temporary.path().join("operation");
            let target = operation.join("target");
            std::fs::create_dir(&root).expect("original checkout");
            std::fs::create_dir_all(&target).expect("owned stage allocation");
            for path in ["one", "two"] { std::fs::write(root.join(path), b"before").expect("original checkout content"); }
            let fs = crate::Fs::memory();
            let workspace = fs.create_workspace("real-stage-completion-failure").await.expect("workspace");
            let from = workspace.head().await.expect("initial generation");
            workspace.write_text("/one", "after-one").await.expect("first target");
            workspace.write_text("/two", "after-two").await.expect("second target");
            let to = workspace.head().await.expect("immutable target generation");
            let budget = finite_native_budget();
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
            let backend = NativeTreeMaterializationBackend::new_with_work(root.clone(), operation, work.clone()).expect("original held roots");
            let original_root = backend.allocation_identity();
            let original_preparation = backend.preparation_identity();
            let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
            let id = OperationId::new();
            let (stored, journal) = state.materialization_with_work(work.clone())
                .compare_and_swap(id, 0, staging_fixture_journal(id, from.id(), to.id(), &backend, &work))
                .await.expect("real Local original journal");
            assert!(stored);
            let fault = FaultingStageWrite { file: target.join("one"), expected: b"after-one", work: work.clone(), boundary: Mutex::new(None) };
            let engine = JournaledMaterializer::new(FaultingLocalJournal {
                store: state.materialization_with_work(work.clone()), fail_revision: u64::MAX,
                lose_ack, stage_fault: Some(&fault),
            }, backend);
            let expected_target = crate::NativeRootIdentity::from_bytes(original_preparation.expect("actual held preparation").target);
            let (options, _options_custody) = native_stage_options(&crate::MaterializeOptions::native(&root), &target, expected_target, &work).expect("actual stage options");
            let mut owner = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
            let error = engine.stage_generation_owned_guarded(&to, &["one".to_owned(), "two".to_owned()], &options,
                journal, &work.native_mount_work(), &mut owner).await.expect_err("actual completion failure after the first physical SDK write");
            let NativeWorkspacePublicationError::NativeStageUncertain(uncertainty) = &error else {
                panic!("post-effect completion failure must retain the actual uncertainty carrier");
            };
            assert!(matches!(uncertainty.cause(), NativeWorkspacePublicationError::Materialization(MaterializationError::Store(_))));
            assert_eq!(uncertainty.observed_work, fault.boundary.lock().expect("failure boundary").expect("actual failing completion prefix"));
            assert_eq!(work.receipt(), uncertainty.observed_work, "uncertainty reuses its prepaid slot without post-failure allocation or copying");
            assert!(uncertainty.custody.work().shares_allocation_context(&work));
            assert!(uncertainty.observed_work.output_bytes > uncertainty.last_durable_work.output_bytes);
            assert_eq!(std::fs::read(target.join("one")).expect("physical prefix survives journal failure"), b"after-one");
            assert!(!target.join("two").exists(), "failed completion forbids the next mutation");
            for path in ["one", "two"] { assert_eq!(std::fs::read(root.join(path)).expect("checkout untouched"), b"before"); }
            let retained = load_native_materialization_journal(&crate::LocalCoreStateStore::new(state.root().to_path_buf()), id, budget,
                &crate::CancellationToken::new()).await.expect("cold partial original journal query").value.expect("original intent survives");
            assert_eq!(retained.root_identity, original_root);
            assert_eq!(retained.preparation_identity, original_preparation);
            assert_eq!(retained.phase, MaterializationPhase::Preparing);
            assert_eq!(retained.applied, 0);
            assert!(retained.preimages.is_empty());
            if lose_ack {
                assert!(retained.work.output_bytes > uncertainty.last_durable_work.output_bytes, "the unacknowledged completion really reached Local storage");
            } else {
                assert_eq!(retained.work, uncertainty.last_durable_work, "restart only knows the acknowledged lower bound, not a fabricated complete prefix");
            }
            retained.work.verify(budget).expect("durable lower bound");
            uncertainty.observed_work.verify(budget).expect("actual full observed invocation prefix");
        }
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn original_allocation_guard_denial_retains_only_witnessed_created_prefix() {
        #[derive(Debug, Error)]
        enum GuardFailure {
            #[error("original owner refused preparation")]
            Owner,
            #[error(transparent)]
            Materialization(#[from] MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>),
        }
        for deny_at in [1, 2] {
            let temporary = tempfile::tempdir().expect("original guarded preparation");
            let root = temporary.path().join("root");
            let operation = temporary.path().join("operation");
            std::fs::create_dir(&root).expect("source root");
            std::fs::write(root.join("file"), b"source").expect("original source");
            let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
            let budget = finite_native_budget();
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("original work");
            let source = work.root_identity(&root).expect("original source identity");
            let parent = work.root_identity(temporary.path()).expect("original parent identity");
            let operation_id = OperationId::new();
            let store = state.materialization_with_work(work.clone());
            let initial = compare_next_journal::<_, NativeTreeMaterializationError>(
                &store, allocation_fixture_journal(operation_id, source, parent, &work),
            ).await.expect("same actual durable intent").1;
            let before = work.receipt();
            let mut calls = 0;
            let mut guard = || {
                calls += 1;
                std::future::ready(if calls == deny_at { Err(GuardFailure::Owner) } else { Ok(()) })
            };
            let error = NativeTreeMaterializationBackend::prepare_allocations(
                &store, initial, &root, &operation, &work, &mut guard,
            ).await.expect_err("original owner denial");
            assert!(matches!(error, GuardFailure::Owner));
            assert_eq!(calls, deny_at);
            work.receipt().verify(budget).expect("all original spent work remains bounded");
            if deny_at == 1 {
                assert_eq!(work.receipt(), before);
                assert!(!operation.exists());
            } else {
                assert!(work.receipt().metadata_operations > before.metadata_operations);
                assert!(operation.is_dir());
                assert!(!operation.join("target").exists());
                assert!(!operation.join("backup").exists());
            }
            let retained = store.load(operation_id).await.expect("same durable journal").expect("original journal retained");
            let progress = retained.preparation_progress.expect("not falsely Ready");
            assert_eq!(progress.pending, None);
            assert_eq!(progress.operation.is_some(), deny_at == 2);
            assert_eq!(progress.target, None);
            assert_eq!(progress.backup, None);
            assert_eq!(retained.preparation_identity, None);
            if let Some(identity) = progress.operation {
                assert_eq!(work.root_identity(&operation).expect("actual created allocation").to_bytes(), identity);
            }
            assert_eq!(std::fs::read(root.join("file")).expect("unchanged source"), b"source");
        }
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_allocation_exact_metadata_budget_retains_unknown_created_gap() {
        let temporary = tempfile::tempdir().expect("allocation boundary");
        let root = temporary.path().join("root");
        let operation = temporary.path().join("operation");
        std::fs::create_dir(&root).expect("source fixture");
        std::fs::write(root.join("file"), b"source").expect("original source");
        let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
        let budget = finite_native_budget();
        let cancellation = crate::CancellationToken::new();
        let identity = NativeMaterializationWork::new(budget, &cancellation).expect("identity meter");
        let source = identity.root_identity(&root).expect("original source");
        let parent = identity.root_identity(temporary.path()).expect("original parent");
        let probe_id = OperationId::new();
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("probe actual allocation meter");
        let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), allocation_fixture_journal(probe_id, source, parent, &work)).await.expect("original fixture intent").1;
        let boundary = std::sync::Arc::new(std::sync::Mutex::new(None));
        let store = AllocationBoundaryJournal { store: state.materialization_with_work(work.clone()), work: work.clone(), boundary: boundary.clone(), cancel_after: None };
        NativeTreeMaterializationBackend::prepare_allocations(&store, initial, &root, &operation, &work, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect("measure actual first-mkdir boundary");
        let before_actual_id_cas = boundary.lock().expect("actual boundary").expect("actual first mkdir and identity reads");
        std::fs::remove_dir_all(&operation).expect("reset owned fixture only");
        let replay_id = OperationId::new();
        let mut exact = budget;
        exact.metadata_operations = before_actual_id_cas.metadata_operations;
        let replay = NativeMaterializationWork::new(exact, &cancellation).expect("exact finite boundary");
        let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(replay.clone()), allocation_fixture_journal(replay_id, source, parent, &replay)).await.expect("independent original fixture intent").1;
        NativeTreeMaterializationBackend::prepare_allocations(&state.materialization_with_work(replay.clone()), initial, &root, &operation, &replay, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect_err("actual-ID journal work cannot pass the first-mkdir metadata boundary");
        assert_eq!(replay.receipt().metadata_operations, exact.metadata_operations);
        replay.receipt().verify(exact).expect("all exact-budget spent work");
        assert!(operation.is_dir());
        assert!(!operation.join("target").exists());
        assert!(!operation.join("backup").exists());
        assert_eq!(std::fs::read(root.join("file")).expect("source unchanged"), b"source");
        let retained = load_native_materialization_journal(&crate::LocalCoreStateStore::new(state.root().to_path_buf()), replay_id, budget, &cancellation).await.expect("real local restart after post-mkdir journal denial").value.expect("original pending intent");
        let progress = retained.preparation_progress.expect("actual known prefix");
        assert_eq!(progress.pending, Some(MaterializationPreparationAllocation::Operation));
        assert!(progress.operation.is_none());
        assert!(retained.preparation_identity.is_none());
    }

    #[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
    #[tokio::test]
    async fn native_allocation_cancellation_keeps_each_acknowledged_identity_prefix() {
        for cancel_after in [2, 3, 5, 7] {
            let temporary = tempfile::tempdir().expect("allocation cancellation");
            let root = temporary.path().join("root");
            let operation = temporary.path().join("operation");
            std::fs::create_dir(&root).expect("source fixture");
            std::fs::write(root.join("file"), b"source").expect("source data");
            let state = crate::LocalCoreStateStore::new(temporary.path().join("state"));
            let budget = finite_native_budget();
            let cancellation = crate::CancellationToken::new();
            let work = NativeMaterializationWork::new(budget, &cancellation).expect("actual native meter");
            let source = work.root_identity(&root).expect("original source");
            let parent = work.root_identity(temporary.path()).expect("original parent");
            let operation_id = OperationId::new();
            let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), allocation_fixture_journal(operation_id, source, parent, &work)).await.expect("same original intent").1;
            let store = AllocationBoundaryJournal { store: state.materialization_with_work(work.clone()), work: work.clone(), boundary: std::sync::Arc::default(), cancel_after: Some(cancel_after) };
            NativeTreeMaterializationBackend::prepare_allocations(&store, initial, &root, &operation, &work, &mut || std::future::ready(Ok::<(), MaterializationError<_, NativeTreeMaterializationError>>(()))).await.expect_err("shared cancellation after actual original journal acknowledgment");
            assert!(cancellation.is_cancelled());
            work.receipt().verify(budget).expect("cancellation preserves all spent IO");
            assert_eq!(std::fs::read(root.join("file")).expect("source retained"), b"source");
            let fresh = crate::CancellationToken::new();
            let retained = load_native_materialization_journal(&crate::LocalCoreStateStore::new(state.root().to_path_buf()), operation_id, budget, &fresh).await.expect("independent restart query").value.expect("original operation retained");
            let progress = retained.preparation_progress.expect("incomplete original allocation");
            assert_eq!(retained.root_identity, Some(source.to_bytes()));
            assert_eq!(progress.operation.is_some(), cancel_after >= 3);
            assert_eq!(progress.target.is_some(), cancel_after >= 5);
            assert_eq!(progress.backup.is_some(), cancel_after >= 7);
            assert_eq!(progress.pending, if cancel_after == 2 { Some(MaterializationPreparationAllocation::Operation) } else { None });
            assert!(retained.preparation_identity.is_none());
            assert_eq!(operation.exists(), cancel_after >= 3);
        }
    }
}
