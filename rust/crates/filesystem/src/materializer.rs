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

/// Shared accounting boundary for native host and durable journal work.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct NativeMaterializationWork {
    state: std::sync::Arc<std::sync::Mutex<NativeWorkState>>,
    cancellation: crate::CancellationToken,
    budget: crate::WorkBudget,
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeWorkState {
    work: crate::WorkCounters,
    original_work: Option<crate::WorkCounters>,
    reserved_writes: u64,
    allocations: crate::kernel::allocation::AllocationLedger,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeWorkState {
    fn executable_budget(&self, mut budget: crate::WorkBudget) -> crate::WorkBudget {
        budget.backend_write_operations -= self.reserved_writes;
        budget
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeMaterializationWork {
    /// Canonical logical allocation body needed before constructing a meter.
    pub const fn accounting_state_bytes() -> u64 { std::mem::size_of::<std::sync::Mutex<NativeWorkState>>() as u64 }

    /// Creates one operation-wide native and journal accounting boundary.
    ///
    /// # Errors
    /// Rejects cancellation or insufficient allowance before allocating its
    /// actual mutex/state body.
    pub fn new(budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<Self, NativeTreeMaterializationError> {
        let mut work = crate::WorkCounters::default();
        cancellation.check().map_err(|_| crate::OperationFailure::new(NativeTreeMaterializationError::Cancelled, work))?;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(Self::accounting_state_bytes(), 1, &mut work, budget)
            .map_err(|error| crate::OperationFailure::new(error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)), work))?;
        Ok(Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(NativeWorkState {
                work,
                original_work: None,
                reserved_writes: 0,
                allocations,
            })),
            cancellation: cancellation.clone(),
            budget,
        })
    }

    /// Borrows the operation's original cancellation token.
    pub const fn cancellation(&self) -> &crate::CancellationToken { &self.cancellation }

    fn state(&self) -> std::sync::MutexGuard<'_, NativeWorkState> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Admits and charges one actually attempted operation before its boundary.
    pub fn charge(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let mut state = self.state();
        let budget = state.executable_budget(self.budget);
        state.work.charge(&delta, &budget)?;
        Ok(())
    }

    /// Retains completed nested work with simultaneously live caller buffers.
    pub fn spent(&self, mut delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        delta.peak_allocation_bytes = delta.peak_allocation_bytes.checked_add(state.allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
        state.work.try_add_assign(&delta)?;
        Ok(())
    }

    pub(crate) fn record_admitted(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        let mut state = self.state();
        let budget = state.executable_budget(self.budget);
        state.work.charge(&delta, &budget)?;
        Ok(())
    }

    pub(crate) fn with_allocations<T, E>(&self, operation: impl FnOnce(&mut crate::kernel::allocation::AllocationLedger, &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken) -> Result<T, E>) -> Result<T, E> {
        let mut state = self.state();
        let budget = state.executable_budget(self.budget);
        let NativeWorkState { work, allocations, .. } = &mut *state;
        operation(allocations, work, budget, &self.cancellation)
    }

    pub(crate) fn with_owned_allocations<T, E: From<NativeTreeMaterializationError>>(&self, operation: impl FnOnce(&mut crate::kernel::allocation::AllocationLedger, &mut crate::WorkCounters, crate::WorkBudget, &crate::CancellationToken) -> Result<T, E>) -> Result<(T, NativeAllocation), E> {
        let mut state = self.state();
        let budget = state.executable_budget(self.budget);
        let NativeWorkState { work, allocations, .. } = &mut *state;
        let before = allocations.live_bytes();
        let result = operation(allocations, work, budget, &self.cancellation);
        let bytes = allocations.live_bytes().checked_sub(before)
            .ok_or_else(|| E::from(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        match result {
            Ok(value) => Ok((value, NativeAllocation { meter: self.clone(), bytes })),
            Err(error) => {
                allocations.release(bytes).map_err(|error| E::from(error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow))))?;
                Err(error)
            }
        }
    }

    /// Remaining cumulative allowance.
    pub fn remaining(&self) -> Result<crate::WorkBudget, NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let state = self.state();
        let mut remaining = state.work.remaining(state.executable_budget(self.budget))?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(state.allocations.live_bytes()).ok_or(crate::WorkError::Overflow)?;
        Ok(remaining)
    }

    /// Work spent by this invocation, not previous recovery invocations.
    pub fn receipt(&self) -> crate::WorkCounters { self.state().work }

    /// Retains the durable work prefix once, without charging it to this call.
    pub fn set_original_work(&self, original: crate::WorkCounters) {
        self.state().original_work.get_or_insert(original);
    }

    /// Durable prefix plus actual work known before the next journal write.
    pub fn journal_work(&self) -> Result<crate::WorkCounters, NativeTreeMaterializationError> {
        let state = self.state();
        Ok(state.original_work.unwrap_or_default().checked_add(state.work)?)
    }

    /// Checks a bounded operation without reporting work not yet performed.
    pub fn admit(&self, delta: crate::WorkCounters) -> Result<(), NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let state = self.state();
        state.work.admit(&delta, &state.executable_budget(self.budget))?;
        Ok(())
    }

    /// Preflights additional owned logical capacity on the original live ledger.
    /// This neither retains storage nor reports an allocation or completed work.
    pub fn admit_retained(&self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let state = self.state();
        let peak = state.allocations.live_bytes().checked_add(bytes)
            .ok_or(crate::WorkError::Overflow)?;
        state.work.admit(
            &crate::WorkCounters { peak_allocation_bytes: peak, ..crate::WorkCounters::default() },
            &state.executable_budget(self.budget),
        )?;
        Ok(())
    }

    pub(crate) fn reserve_cleanup_write(&self) -> Result<NativeCleanupWrite, NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let mut state = self.state();
        state.work.admit(&crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::default() }, &state.executable_budget(self.budget))?;
        state.reserved_writes = state.reserved_writes.checked_add(1).ok_or(crate::WorkError::Overflow)?;
        Ok(NativeCleanupWrite { meter: self.clone(), reserved: true })
    }

    /// Admits an actual allocation before its allocator is called; retain the
    /// returned guard for the complete buffer lifetime.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or the exact exceeded work dimension.
    pub fn claim(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let mut state = self.state();
        let NativeWorkState { work, allocations, .. } = &mut *state;
        allocations.claim_bytes(bytes, u64::from(bytes != 0), work, self.budget)
            .map_err(|error| error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        Ok(NativeAllocation { meter: self.clone(), bytes })
    }

    /// Takes custody of an already measured owned buffer without charging a
    /// second allocation operation.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient live-byte allowance.
    pub fn retain_owned(&self, bytes: u64) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let mut state = self.state();
        let NativeWorkState { work, allocations, .. } = &mut *state;
        allocations.claim_bytes(bytes, 0, work, self.budget)
            .map_err(|error| error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        Ok(NativeAllocation { meter: self.clone(), bytes })
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

    /// Retains an already decoded journal without opening its physical roots.
    /// Keep this guard alive while querying missing or replaced allocations.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient live-byte allowance.
    pub fn retain_journal(&self, journal: &MaterializationJournal) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        self.retain_owned(Self::journal_control_bytes(journal)?)
    }

    /// Retains direct String diagnostics transferred by a native failure.
    /// Opaque platform I/O payloads are not sized by this string-only seam.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient live-byte allowance.
    pub fn retain_error_text(&self, error: &NativeTreeMaterializationError) -> Result<NativeAllocation, NativeTreeMaterializationError> {
        let bytes = match error {
            NativeTreeMaterializationError::MissingTarget(text)
            | NativeTreeMaterializationError::ExternalMutation(text)
            | NativeTreeMaterializationError::MissingPreimage(text)
            | NativeTreeMaterializationError::UnexpectedBackup(text)
            | NativeTreeMaterializationError::InvalidPath(text)
            | NativeTreeMaterializationError::UnverifiedSourceMetadata(text)
            | NativeTreeMaterializationError::Allocation(text) => text.capacity() as u64,
            _ => 0,
        };
        self.retain_owned(bytes)
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
    pub fn live_allocation_bytes(&self) -> u64 { self.state().allocations.live_bytes() }

    /// Observes the actual opened root allocation without copying or
    /// canonicalizing its lexical path.
    ///
    /// # Errors
    /// Returns the native open failure or all spent finite-work failures.
    pub fn root_identity(&self, path: &std::path::Path) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        Ok(crate::native_host::HostRoot::open_materialization_measured(path, self)?.identity())
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
        if held.identity() != expected { return Err(NativeTreeMaterializationError::ExternalMutation(String::new())); }
        let directory = held.open_dir_materialization_measured(relative, self)?;
        if self.root_identity(root)? != expected { return Err(NativeTreeMaterializationError::ExternalMutation(String::new())); }
        Ok(directory.into_std_file())
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
    pub async fn release_owned_directory_guarded<F, Fut>(
        &self,
        root: &Path,
        expected_root: crate::NativeRootIdentity,
        relative: &Path,
        expected_directory: crate::NativeRootIdentity,
        mut before_effect: F,
    ) -> Result<(), NativeTreeMaterializationError>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), NativeTreeMaterializationError>> + Send,
    {
        if relative.as_os_str().is_empty() { return Err(NativeTreeMaterializationError::InvalidLayout); }
        for component in relative.components() {
            self.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if !matches!(component, std::path::Component::Normal(_)) { return Err(NativeTreeMaterializationError::InvalidLayout); }
        }
        let held = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
        if held.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: held.identity() }); }
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
        if actual != expected_directory { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_directory, actual }); }
        if !metadata.is_dir() { return Err(std::io::Error::from(std::io::ErrorKind::NotADirectory).into()); }
        let frame = native_owned_remove_frame(&parent, leaf_name, &metadata, self)?;
        let mut walker = NativeRemoveWalker::new(frame, self)?;
        while let Some(step) = walker.next(self, true)? {
            before_effect().await?;
            let current = crate::native_host::HostRoot::open_materialization_measured(root, self)?;
            if current.identity() != expected_root { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_root, actual: current.identity() }); }
            let current_parent = native_owned_relative_parent(&current, parent_path, self)?;
            let actual = crate::native_host::directory_identity_materialization_measured(&current_parent, self)?;
            if actual != parent_identity { return Err(NativeTreeMaterializationError::AllocationChanged { expected: parent_identity, actual }); }
            let actual = native_stat_identity(&stat_native_walk_name(&current_parent, leaf_name, self)?);
            if actual != expected_directory { return Err(NativeTreeMaterializationError::AllocationChanged { expected: expected_directory, actual }); }
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
pub(crate) struct NativeCleanupWrite {
    meter: NativeMaterializationWork,
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
    pub(crate) fn attempt<T>(self, operation: impl FnOnce() -> T) -> Result<T, NativeTreeMaterializationError> {
        let (value, recorded) = self.attempt_recorded(operation);
        recorded.map(|()| value)
    }

    pub(crate) fn attempt_recorded<T>(mut self, operation: impl FnOnce() -> T) -> (T, Result<(), NativeTreeMaterializationError>) {
        let recorded = {
            let mut state = self.meter.state();
            let released = state.reserved_writes.checked_sub(1);
            self.reserved = false;
            if let Some(remaining) = released { state.reserved_writes = remaining; }
            let delta = crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::default() };
            let budget = state.executable_budget(self.meter.budget);
            match state.work.charge(&delta, &budget) {
                Ok(()) if released.is_some() => Ok(()),
                Ok(()) => Err(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)),
                Err(error) => match state.work.try_add_assign(&delta) {
                    Ok(()) => Err(error.into()),
                    Err(error) => Err(error.into()),
                },
            }
        };
        (operation(), recorded)
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeCleanupWrite {
    fn drop(&mut self) {
        if self.reserved {
            let mut state = self.meter.state();
            state.reserved_writes -= 1;
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
/// A scoped live-buffer admission in the native operation's allocation ledger.
pub struct NativeAllocation {
    meter: NativeMaterializationWork,
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
    /// Admits one actual buffer growth before reallocating.
    ///
    /// # Errors
    /// Returns cancellation, overflow, or insufficient remaining allowance.
    pub fn grow(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        self.meter.cancellation.check().map_err(|_| NativeTreeMaterializationError::Cancelled)?;
        let mut state = self.meter.state();
        let NativeWorkState { work, allocations, .. } = &mut *state;
        allocations.claim_bytes(bytes, u64::from(bytes != 0), work, self.meter.budget)
            .map_err(|error| error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        self.bytes = self.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
        Ok(())
    }

    /// Returns this guard's currently owned byte capacity.
    pub fn bytes(&self) -> u64 { self.bytes }

    /// Borrows this guard's original operation ledger without creating a meter.
    pub const fn work(&self) -> &NativeMaterializationWork { &self.meter }

    /// Transfers existing custody without another allocation.
    ///
    /// # Errors
    /// Rejects distinct meters or byte-count overflow.
    pub fn absorb(&mut self, mut other: Self) -> Result<(), NativeTreeMaterializationError> {
        if !std::sync::Arc::ptr_eq(&self.meter.state, &other.meter.state) {
            return Err(NativeTreeMaterializationError::Allocation("cannot transfer between distinct allocation ledgers".to_owned()));
        }
        self.bytes = self.bytes.checked_add(other.bytes).ok_or(crate::WorkError::Overflow)?;
        other.bytes = 0;
        Ok(())
    }

    /// Releases capacity only after its buffer has been freed.
    ///
    /// # Errors
    /// Rejects releasing bytes not owned by this guard.
    pub fn release(&mut self, bytes: u64) -> Result<(), NativeTreeMaterializationError> {
        let remaining = self.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
        self.meter.state().allocations.release(bytes)
            .map_err(|error| error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        self.bytes = remaining;
        Ok(())
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
        let NativeWorkState { work, allocations, .. } = &mut *state;
        allocations.claim_bytes(bytes - self.bytes, 0, work, self.meter.budget)
            .map_err(|error| error.into_work_or(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        self.bytes = bytes;
        Ok(())
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeAllocation {
    fn drop(&mut self) {
        let _ = self.meter.state().allocations.release(self.bytes);
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
    /// Work known before the last durable journal write; final acknowledgements
    /// are reported by the invocation receipt, not invented in this prefix.
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

/// Durable optimistic-concurrency adapter for materialization journals.
pub trait MaterializationJournalStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Loads one operation journal.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<MaterializationJournal>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a journal.
    /// Returns the admitted replacement ownership alongside the CAS decision.
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

    /// Formats an actionable observation error at the backend's real boundary.
    fn describe_observation_error(&self, error: &Self::Error) -> Result<String, Self::Error> { Ok(error.to_string()) }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_observation_text(&self, _text: &String) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    /// Allocates the already admitted observation output at this backend.
    fn reserve_observations(&self, observations: &mut Vec<MaterializationObservation>, count: usize) -> Result<(), Self::Error> {
        observations.reserve_exact(count);
        Ok(())
    }

    /// Completes barriers for a witnessed endpoint after a lost mutation acknowledgement.
    /// Unwitnessed adapters must provide equivalent external durability.
    fn settle_observed(&self, _edit: &MaterializationEdit) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_journal(&self, _journal: &MaterializationJournal) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    #[cfg(not(target_arch = "wasm32"))]
    fn retain_plan(&self, _plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    #[cfg(not(target_arch = "wasm32"))]
    fn admit_observations(&self, _count: usize) -> Result<Option<NativeAllocation>, Self::Error> { Ok(None) }

    fn append_preimage(&self, journal: &mut MaterializationJournal, preimage: MaterializationPreimage) -> Result<(), Self::Error> {
        journal.preimages.push(preimage);
        Ok(())
    }

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
    ) -> impl Future<Output = Result<MaterializationPreimage, Self::Error>> + Send;

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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationInspection {
    /// Original operation, preparation identities and exact recovery progress.
    pub journal: MaterializationJournal,
    /// Root status even when preimage capture is incomplete.
    pub root: MaterializationRootObservation,
    /// Staging and cleanup status even when capture is incomplete.
    pub preparation: MaterializationPreparationObservation,
    /// Actual observations of captured endpoints, empty if the root is divergent.
    pub observations: Vec<MaterializationObservation>,
}

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

struct PreparedMaterialization {
    journal: MaterializationJournal,
    #[cfg(not(target_arch = "wasm32"))]
    custody: Option<NativeAllocation>,
}

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
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        let mut before_effect = |_verify_source| std::future::ready(Ok::<(), MaterializationError<S::Error, B::Error>>(()));
        self.apply_with_guard(plan, &mut before_effect).await
    }

    async fn apply_with_guard<E, F, Fut>(&self, plan: MaterializationPlan, before_effect: &mut F) -> Result<MaterializationJournal, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let input = self.backend.retain_plan(&plan).map_err(MaterializationError::Backend)?;
        if !self.backend.check_plan(&plan).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::IncompatibleJournal.into());
        }
        let loaded = self.load(plan.operation_id).await?;
        #[cfg(not(target_arch = "wasm32"))]
        drop(input);
        let mut journal = match loaded {
            Some(existing) => {
                if existing.plan != plan {
                    return Err(MaterializationError::PlanConflict.into());
                }
                drop(plan);
                existing
            }
            None => self.prepare(plan).await?,
        };
        if matches!(
            journal.phase,
            MaterializationPhase::RolledBack | MaterializationPhase::RollingBack
        ) {
            return Err(MaterializationError::PlanConflict.into());
        }
        self.verify_allocation(&journal).await?;
        if journal.phase == MaterializationPhase::Preparing {
            journal = self.capture_preimages(journal).await?;
        }
        if journal.phase == MaterializationPhase::Applied {
            self.repair_applied_with_guard(&journal, before_effect).await?;
            return Ok(journal);
        }
        journal.phase = MaterializationPhase::Applying;
        journal = self.persist(journal).await?;
        while usize::try_from(journal.applied).unwrap_or(usize::MAX) < journal.plan.edits.len() {
            #[cfg(not(target_arch = "wasm32"))]
            let live = self.backend.retain_journal(&journal).map_err(MaterializationError::Backend)?;
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
            #[cfg(not(target_arch = "wasm32"))]
            drop(live);
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
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let mut before_effect = |_verify_source| std::future::ready(Ok::<(), MaterializationError<S::Error, B::Error>>(()));
        self.recover_with_guard(operation_id, recovery, &mut before_effect).await
    }

    async fn recover_with_guard<E, F, Fut>(&self, operation_id: OperationId, recovery: MaterializationRecovery, before_effect: &mut F) -> Result<Option<MaterializationJournal>, E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let Some(journal) = self.load(operation_id).await? else {
            return Ok(None);
        };
        match recovery {
            MaterializationRecovery::Complete => {
                if journal.phase == MaterializationPhase::RollingBack || journal.restored != 0 {
                    return Err(MaterializationError::IncompatibleJournal.into());
                }
                let MaterializationJournal { plan, .. } = journal;
                self.apply_with_guard(plan, before_effect).await.map(Some)
            }
            MaterializationRecovery::RollBack => self.rollback_with_guard(journal, before_effect).await.map(Some),
        }
    }

    /// Retains original immutable intent and allocation witnesses before staging.
    pub async fn prepare(
        &self,
        plan: MaterializationPlan,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        self.prepare_with_custody(MaterializationPreparation {
            plan,
            #[cfg(not(target_arch = "wasm32"))]
            custody: None,
        }).await.map(|prepared| prepared.journal)
    }

    async fn prepare_with_custody(&self, preparation: MaterializationPreparation) -> Result<PreparedMaterialization, MaterializationError<S::Error, B::Error>> {
        let MaterializationPreparation { plan, #[cfg(not(target_arch = "wasm32"))] custody } = preparation;
        #[cfg(not(target_arch = "wasm32"))]
        let input = match custody {
            Some(custody) => Some(custody),
            None => self.backend.retain_plan(&plan).map_err(MaterializationError::Backend)?,
        };
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
        let (stored, journal) = self
            .store
            .compare_and_swap(journal.plan.operation_id, 0, journal)
            .await
            .map_err(MaterializationError::Store)?;
        if stored {
            self.verify_allocation(&journal).await?;
            Ok(PreparedMaterialization { journal, #[cfg(not(target_arch = "wasm32"))] custody: input })
        } else {
            let retained = self
                .load(journal.plan.operation_id)
                .await?
                .ok_or(MaterializationError::Contended)?;
            #[cfg(not(target_arch = "wasm32"))]
            let retained_custody = self.backend.retain_journal(&retained).map_err(MaterializationError::Backend)?;
            if retained.plan != journal.plan {
                return Err(MaterializationError::PlanConflict);
            }
            self.verify_allocation(&retained).await?;
            Ok(PreparedMaterialization { journal: retained, #[cfg(not(target_arch = "wasm32"))] custody: retained_custody })
        }
    }

    async fn capture_preimages(
        &self,
        mut journal: MaterializationJournal,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, B::Error>> {
        if journal.preparation_progress.is_some() { return Err(MaterializationError::IncompatibleJournal); }
        if let MaterializationIntent::GenerationTransition { from, .. } = &journal.plan.intent
            && !self.backend.verify_source(*from).await.map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::PlanConflict);
        }
        while journal.preimages.len() < journal.plan.edits.len() {
            #[cfg(not(target_arch = "wasm32"))]
            let live = self.backend.retain_journal(&journal).map_err(MaterializationError::Backend)?;
            self.verify_allocation(&journal).await?;
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
            #[cfg(not(target_arch = "wasm32"))]
            drop(live);
            journal = self.persist(journal).await?;
        }
        journal.phase = MaterializationPhase::Prepared;
        self.persist(journal).await
    }

    async fn rollback_with_guard<E, F, Fut>(&self, mut journal: MaterializationJournal, before_effect: &mut F) -> Result<MaterializationJournal, E>
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
            #[cfg(not(target_arch = "wasm32"))]
            let live = self.backend.retain_journal(&journal).map_err(MaterializationError::Backend)?;
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
            #[cfg(not(target_arch = "wasm32"))]
            drop(live);
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
    ) -> Result<Option<MaterializationJournal>, MaterializationError<S::Error, B::Error>> {
        let journal = self
            .store
            .load(operation_id)
            .await
            .map_err(MaterializationError::Store)?;
        if let Some(journal) = &journal {
            #[cfg(not(target_arch = "wasm32"))]
            let _live = self.backend.retain_journal(journal).map_err(MaterializationError::Backend)?;
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
        let Some(journal) = self.load(operation_id).await? else { return Ok(None); };
        #[cfg(not(target_arch = "wasm32"))]
        let _live = self.backend.retain_journal(&journal).map_err(MaterializationError::Backend)?;
        let root = match journal.root_identity {
            None => MaterializationRootObservation::Unverified,
            Some(original) => match self.backend.observe_allocation_identity().await {
                Ok(Some(current)) if current == original => MaterializationRootObservation::Identical(original),
                Ok(current) => MaterializationRootObservation::Diverged { original, current },
                Err(error) if self.backend.observation_interrupted(&error) => return Err(MaterializationError::Backend(error)),
                Err(error) => MaterializationRootObservation::Unavailable(self.backend.describe_observation_error(&error).map_err(MaterializationError::Backend)?),
            },
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _root_text = match &root {
            MaterializationRootObservation::Unavailable(text) => self.backend.retain_observation_text(text).map_err(MaterializationError::Backend)?,
            _ => None,
        };
        let preparation = match journal.preparation_identity {
            None => journal.preparation_progress.map_or(MaterializationPreparationObservation::Unverified, MaterializationPreparationObservation::Incomplete),
            Some(original) => match self.backend.observe_preparation_identity().await {
                Ok(Some(current)) if current == original => MaterializationPreparationObservation::Identical(original),
                Ok(current) => MaterializationPreparationObservation::Diverged { original, current },
                Err(error) if self.backend.observation_interrupted(&error) => return Err(MaterializationError::Backend(error)),
                Err(error) => MaterializationPreparationObservation::Unavailable(self.backend.describe_observation_error(&error).map_err(MaterializationError::Backend)?),
            },
        };
        #[cfg(not(target_arch = "wasm32"))]
        let _preparation_text = match &preparation {
            MaterializationPreparationObservation::Unavailable(text) => self.backend.retain_observation_text(text).map_err(MaterializationError::Backend)?,
            _ => None,
        };
        let mut observations = Vec::new();
        if matches!(root, MaterializationRootObservation::Unverified | MaterializationRootObservation::Identical(_))
            && matches!(preparation, MaterializationPreparationObservation::Unverified | MaterializationPreparationObservation::Identical(_)) {
            #[cfg(not(target_arch = "wasm32"))]
            let _capacity = self.backend.admit_observations(journal.preimages.len()).map_err(MaterializationError::Backend)?;
            self.backend.reserve_observations(&mut observations, journal.preimages.len()).map_err(MaterializationError::Backend)?;
            for (edit, preimage) in journal.plan.edits.iter().zip(&journal.preimages) {
                observations.push(self.backend.observe(edit, preimage).await.map_err(MaterializationError::Backend)?);
            }
        }
        Ok(Some(MaterializationInspection { journal, root, preparation, observations }))
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
        #[cfg(not(target_arch = "wasm32"))]
        let _current_custody = self.backend.retain_journal(&current).map_err(MaterializationError::Backend)?;
        if current != *journal { return Err(MaterializationError::PlanConflict); }
        self.verify_allocation(journal).await
    }

    async fn repair_applied_with_guard<E, F, Fut>(&self, journal: &MaterializationJournal, before_effect: &mut F) -> Result<(), E>
    where
        E: From<MaterializationError<S::Error, B::Error>>,
        F: FnMut(bool) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let _live = self.backend.retain_journal(journal).map_err(MaterializationError::Backend)?;
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
                    #[cfg(not(target_arch = "wasm32"))]
                    let _live = self.backend.retain_journal(&journal).map_err(MaterializationError::Backend)?;
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
            return Err(NativeTreeMaterializationError::InvalidPath(work.display("<non-Unicode>")?.0));
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
            return Err(NativeTreeMaterializationError::InvalidPath(work.display("<non-Unicode>")?.0));
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
    let mut output = Vec::new();
    let mut custody = work.claim(0)?;
    for path in paths {
        let (absolute, allocation) = native_absolute_string(path, work)?;
        custody.absorb(allocation)?;
        native_vec_push(&mut output, absolute, &mut custody)?;
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

fn validate_materialization_path(path: &str) -> Result<(), ()> {
    crate::path::is_canonical_relative(path)
        .then_some(())
        .ok_or(())
}

#[cfg(not(target_arch = "wasm32"))]
impl<S: MaterializationJournalStore> JournaledMaterializer<S, NativeTreeMaterializationBackend> {
    /// Transfers an already admitted immutable plan into the same durable
    /// Preparing operation, retaining its buffer custody throughout the CAS.
    ///
    /// # Errors
    /// Returns original store/native errors, custody mismatch or contention.
    pub async fn prepare_owned(&self, plan: MaterializationPlan, mut custody: NativeAllocation) -> Result<(MaterializationJournal, NativeAllocation), MaterializationError<S::Error, NativeTreeMaterializationError>> {
        custody.absorb(self.backend.work.claim(0).map_err(MaterializationError::Backend)?).map_err(MaterializationError::Backend)?;
        if custody.bytes() < plan_owned_bytes(&plan).map_err(MaterializationError::Backend)? {
            return Err(MaterializationError::Backend(NativeTreeMaterializationError::Allocation("plan custody does not cover its actual buffers".to_owned())));
        }
        let prepared = self.prepare_with_custody(MaterializationPreparation { plan, custody: Some(custody) }).await?;
        let custody = prepared.custody.ok_or_else(|| MaterializationError::Backend(NativeTreeMaterializationError::Allocation("preparation journal lost native custody".to_owned())))?;
        Ok((prepared.journal, custody))
    }

    /// Returns the original journal with retained actual output capacities.
    ///
    /// # Errors
    /// Returns original load and finite admission failures.
    pub async fn load_owned(&self, operation_id: OperationId) -> Result<Option<(MaterializationJournal, NativeAllocation)>, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let Some(journal) = self.load(operation_id).await? else { return Ok(None); };
        let custody = self.backend.work.retain_owned(journal_owned_bytes(&journal).map_err(MaterializationError::Backend)?).map_err(MaterializationError::Backend)?;
        Ok(Some((journal, custody)))
    }

    /// Completes or rolls back the original operation and retains its output.
    ///
    /// # Errors
    /// Returns original recovery and finite output-admission failures.
    pub async fn recover_owned(&self, operation_id: OperationId, recovery: MaterializationRecovery) -> Result<Option<(MaterializationJournal, NativeAllocation)>, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let Some(journal) = self.recover(operation_id, recovery).await? else { return Ok(None); };
        let custody = self.backend.work.retain_owned(journal_owned_bytes(&journal).map_err(MaterializationError::Backend)?).map_err(MaterializationError::Backend)?;
        Ok(Some((journal, custody)))
    }

    /// Inspects original operation/root/cleanup identity with live output
    /// custody, including the real observation and diagnostic buffers.
    ///
    /// # Errors
    /// Returns original inspection or finite output-admission failures.
    pub async fn inspect_owned(&self, operation_id: OperationId) -> Result<Option<(MaterializationInspection, NativeAllocation)>, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let Some(inspection) = self.inspect(operation_id).await? else { return Ok(None); };
        let bytes = journal_owned_bytes(&inspection.journal).map_err(MaterializationError::Backend)?
            .checked_add(owned_size(inspection.observations.capacity(), std::mem::size_of::<MaterializationObservation>()).map_err(MaterializationError::Backend)?)
            .and_then(|bytes| bytes.checked_add(match &inspection.root { MaterializationRootObservation::Unavailable(error) => error.capacity() as u64, _ => 0 }))
            .and_then(|bytes| bytes.checked_add(match &inspection.preparation { MaterializationPreparationObservation::Unavailable(error) => error.capacity() as u64, _ => 0 }))
            .ok_or(MaterializationError::Backend(NativeTreeMaterializationError::Work(crate::WorkError::Overflow)))?;
        let custody = self.backend.work.retain_owned(bytes).map_err(MaterializationError::Backend)?;
        Ok(Some((inspection, custody)))
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl JournaledMaterializer<crate::LocalCoreStateStore, NativeTreeMaterializationBackend> {
    fn measured_engine(&self, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<(JournaledMaterializer<crate::core_state::MeasuredMaterializationStore<'_>, NativeTreeMaterializationBackend>, NativeMaterializationWork), MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let work = NativeMaterializationWork::new(budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, MaterializationError::Backend))?;
        let mut custody = work.retain_owned(self.backend.allocation.bytes())
            .map_err(|error| crate::OperationFailure::new(MaterializationError::Backend(error), work.receipt()))?;
        custody.absorb(work.claim(std::mem::size_of::<NativeAllocation>() as u64)
            .map_err(|error| crate::OperationFailure::new(MaterializationError::Backend(error), work.receipt()))?)
            .map_err(|error| crate::OperationFailure::new(MaterializationError::Backend(error), work.receipt()))?;
        let mut backend = self.backend.clone();
        backend.work = work.clone();
        backend.allocation = std::sync::Arc::new(custody);
        Ok((JournaledMaterializer::new(self.store.materialization_with_work(work.clone()), backend), work))
    }

    /// Applies through the same engine with bounded native and durable IO.
    pub async fn apply_bounded(&self, plan: MaterializationPlan, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.apply(plan).await, &work)
    }

    /// Retains original Preparing intent before any target writes or capture.
    pub async fn prepare_bounded(&self, plan: MaterializationPlan, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.prepare(plan).await, &work)
    }

    /// Resumes the original durable operation without dropping failure work.
    pub async fn recover_bounded(&self, operation_id: OperationId, recovery: MaterializationRecovery, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Option<MaterializationJournal>>, MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.recover(operation_id, recovery).await, &work)
    }

    /// Reads retained intent and durable progress without physical mutation.
    pub async fn load_bounded(&self, operation_id: OperationId, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Option<MaterializationJournal>>, MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.load(operation_id).await, &work)
    }

    /// Queries actual root, preparation, and captured endpoints without repair.
    pub async fn inspect_bounded(&self, operation_id: OperationId, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Option<MaterializationInspection>>, MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>> {
        let (engine, work) = self.measured_engine(budget, cancellation)?;
        native_measured_result(engine.inspect(operation_id).await, &work)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_measured_result<T, E>(result: Result<T, E>, work: &NativeMaterializationWork) -> crate::MeasuredResult<crate::OperationReceipt<T>, E> {
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
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
        let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).map_err(|failure| failure.error)?;
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

    async fn prepare_allocations<S: MaterializationJournalStore>(
        store: &S, mut journal: MaterializationJournal, root: &Path,
        operation_directory: &Path, work: &NativeMaterializationWork,
    ) -> Result<MaterializationJournal, MaterializationError<S::Error, NativeTreeMaterializationError>> {
        let expected_root = crate::NativeRootIdentity::from_bytes(journal.root_identity.ok_or(MaterializationError::IncompatibleJournal)?);
        let mut progress = journal.preparation_progress.ok_or(MaterializationError::IncompatibleJournal)?;
        if !valid_journal(&journal, journal.plan.operation_id) || !native_check_plan(&journal.plan, work).map_err(MaterializationError::Backend)? { return Err(MaterializationError::IncompatibleJournal); }
        if progress.pending.is_some() { return Err(MaterializationError::Backend(NativeTreeMaterializationError::IndeterminatePreparation)); }
        for step in [MaterializationPreparationAllocation::Operation, MaterializationPreparationAllocation::Target, MaterializationPreparationAllocation::Backup] {
            let known = match step {
                MaterializationPreparationAllocation::Operation => progress.operation,
                MaterializationPreparationAllocation::Target => progress.target,
                MaterializationPreparationAllocation::Backup => progress.backup,
            };
            if known.is_some() { continue; }
            progress.pending = Some(step);
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(MaterializationError::Backend)?;
            let (stored, pending) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = pending;
            if !stored { return Err(MaterializationError::Contended); }
            let current = store.load(journal.plan.operation_id).await.map_err(MaterializationError::Store)?.ok_or(MaterializationError::IncompatibleJournal)?;
            let _current_custody = work.retain_journal(&current).map_err(MaterializationError::Backend)?;
            if current != journal { return Err(MaterializationError::PlanConflict); }
            let (root_copy, root_custody) = work.copy_path(root).map_err(MaterializationError::Backend)?;
            let (operation_copy, operation_custody) = work.copy_path(operation_directory).map_err(MaterializationError::Backend)?;
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
            }).await.map_err(|error| MaterializationError::Backend(NativeTreeMaterializationError::Io(error)))?.map_err(MaterializationError::Backend)?;
            match step {
                MaterializationPreparationAllocation::Operation => progress.operation = Some(actual),
                MaterializationPreparationAllocation::Target => progress.target = Some(actual),
                MaterializationPreparationAllocation::Backup => progress.backup = Some(actual),
            }
            progress.pending = None;
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(MaterializationError::Backend)?;
            let (stored, completed) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = completed;
            if !stored { return Err(MaterializationError::Contended); }
        }
        Self::verify_recorded_preparation(root, expected_root, operation_directory, progress, work).map_err(MaterializationError::Backend)?;
        journal.preparation_identity = Some(MaterializationPreparationIdentity {
            operation: progress.operation.ok_or(MaterializationError::IncompatibleJournal)?,
            target: progress.target.ok_or(MaterializationError::IncompatibleJournal)?,
            backup: progress.backup.ok_or(MaterializationError::IncompatibleJournal)?,
        });
        journal.preparation_progress = None;
        journal.work = work.journal_work().map_err(MaterializationError::Backend)?;
        let (stored, journal) = compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
        if !stored { return Err(MaterializationError::Contended); }
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
        let operation = Self::recorded_preparation_child(&parent, Path::new(operation_directory.file_name().ok_or(NativeTreeMaterializationError::InvalidLayout)?), progress.operation.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?, work)?;
        Self::recorded_preparation_child(&operation, Path::new("target"), progress.target.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?, work)?;
        Self::recorded_preparation_child(&operation, Path::new("backup"), progress.backup.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?, work)?;
        Ok(())
    }

    fn bind_with_work(root: PathBuf, operation_directory: PathBuf, work: NativeMaterializationWork, create_backup: bool, mut custody: NativeAllocation) -> Result<Self, NativeTreeMaterializationError> {
        custody.absorb(work.claim(0)?)?;
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
        let (target, target_custody) = work.join_path(&operation_directory, Path::new("target"))?;
        custody.absorb(target_custody)?;
        let (backup, backup_custody) = work.join_path(&operation_directory, Path::new("backup"))?;
        custody.absorb(backup_custody)?;
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
                custody.absorb(work.claim(std::mem::size_of::<NativeAllocation>() as u64)?)?;
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
    pub fn new_bounded(root: impl AsRef<Path>, operation_directory: impl AsRef<Path>, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> crate::MeasuredResult<crate::OperationReceipt<Self>, NativeTreeMaterializationError> {
        let work = NativeMaterializationWork::new(budget, cancellation)?;
        let value = (|| {
            let (root, mut custody) = work.copy_path(root.as_ref())?;
            let (operation_directory, operation_custody) = work.copy_path(operation_directory.as_ref())?;
            custody.absorb(operation_custody)?;
            Self::new_owned_with_work(root, operation_directory, custody, work.clone())
        })();
        match value {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
            Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
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
            return Err(NativeTreeMaterializationError::InvalidPath(self.work.display(path)?.0));
        }
        if self.work.root_identity(self.root_path())? != self.root_identity {
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
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
            return Err(NativeTreeMaterializationError::InvalidPath(self.work.display(path)?.0));
        }
        self.work.join_path(self.target.as_path(), Path::new(path))
    }

    /// Checks each actual held initial allocation before staging can write.
    ///
    /// # Errors
    /// Returns divergence or the actual finite scan/open failure.
    pub fn require_initial_empty(&self) -> Result<(), NativeTreeMaterializationError> {
        for root in [&self.held_root, &self.held_target, &self.held_backup] {
            let directory = root.open_dir_materialization_measured(Path::new(""), &self.work)?;
            let (mut reader, _cursor) = crate::native_host::read_dir_held_with_work(&directory, &mut &self.work)?;
            if next_native_walk_name(&mut reader, &self.work)?.is_some() {
                return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
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
            if !selected { return Err(NativeTreeMaterializationError::ExternalMutation(String::new())); }
        }
        Ok(())
    }

    /// Clears an operation-owned staged directory while retaining the witnessed
    /// target root allocation. Only a Preparing journal may authorize this.
    pub async fn reset_target_directory(&self, path: &str) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        if !path.is_empty() && !crate::path::is_canonical_relative_with_step(path, || self.work.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }))? {
            return Err(NativeTreeMaterializationError::InvalidPath(self.work.display(path)?.0));
        }
        if self.observe_allocation_identity().await? != self.allocation_identity()
            || self.observe_preparation_identity().await? != self.preparation_identity() {
            return Err(NativeTreeMaterializationError::ExternalMutation(self.work.display(path)?.0));
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
            return Err(NativeTreeMaterializationError::ExternalMutation(self.work.display(&path.path.display())?.0));
        }
        Ok((file, actual))
    }


    fn set_metadata(&self, path: &NativeEntry<'_>, desired: &NativeMetadataImage, expected: Option<[u8; 32]>) -> Result<(), NativeTreeMaterializationError> {
        let (_file, metadata) = self.metadata_file(path)?;
        if Some(native_metadata_fingerprint(&metadata, None, &self.work)?) != expected {
            return Err(NativeTreeMaterializationError::ExternalMutation(self.work.display(&path.path.display())?.0));
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
                validate_materialization_path(&path)
                    .map_err(|()| NativeTreeMaterializationError::InvalidPath(path.clone()))?;
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
    custody.absorb(work.claim(bytes as u64)?)?;
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
        matches!(error, NativeTreeMaterializationError::Work(_) | NativeTreeMaterializationError::Cancelled | NativeTreeMaterializationError::Allocation(_))
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

    fn retain_journal(&self, journal: &MaterializationJournal) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.retain_owned(journal_owned_bytes(journal)?).map(Some)
    }

    fn retain_plan(&self, plan: &MaterializationPlan) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.retain_owned(plan_owned_bytes(plan)?).map(Some)
    }

    fn describe_observation_error(&self, error: &Self::Error) -> Result<String, Self::Error> {
        let _original_text = self.work.retain_error_text(error)?;
        self.work.display(error).map(|(text, _custody)| text)
    }

    fn retain_observation_text(&self, text: &String) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.retain_owned(text.capacity() as u64).map(Some)
    }

    fn reserve_observations(&self, observations: &mut Vec<MaterializationObservation>, count: usize) -> Result<(), Self::Error> {
        observations.try_reserve_exact(count).map_err(NativeTreeMaterializationError::AllocationFailed)
    }

    fn admit_observations(&self, count: usize) -> Result<Option<NativeAllocation>, Self::Error> {
        self.work.claim(owned_size(count, std::mem::size_of::<MaterializationObservation>())?).map(Some)
    }

    fn append_preimage(&self, journal: &mut MaterializationJournal, preimage: MaterializationPreimage) -> Result<(), Self::Error> {
        let _image = self.work.retain_owned(owned_size(preimage.image.capacity(), 1)?)?;
        let capacity = journal.preimages.capacity();
        let _growth = if journal.preimages.len() == capacity {
            let next = capacity.checked_mul(2).ok_or(crate::WorkError::Overflow)?.max(4);
            let allocation = self.work.claim(owned_size(next - capacity, std::mem::size_of::<MaterializationPreimage>())?)?;
            journal.preimages.try_reserve_exact(next - journal.preimages.len())
                .map_err(|_| NativeTreeMaterializationError::Allocation("preimage prefix growth failed".to_owned()))?;
            Some(allocation)
        } else { None };
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
    ) -> Result<MaterializationPreimage, Self::Error> {
        let backend = self.clone();
        let (edit, edit_allocation) = duplicate_native_edit(edit, &self.work)?;
        acyclic_native_runtime::run_blocking_io(move || {
            let _edit_allocation = edit_allocation;
            let edit = &edit;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            if backend.exists(&backup)? {
                return Err(NativeTreeMaterializationError::UnexpectedBackup(
                    backend.work.display(path)?.0,
                ));
            }
            if let MaterializationEdit::Install { image, .. } = edit
                && image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)
                && backend.exists(&live)? {
                return Err(NativeTreeMaterializationError::ExternalMutation(backend.work.display(path)?.0));
            }
            if let MaterializationEdit::SetMetadata { image, .. } = edit {
                let desired = decode_native_metadata(image)?;
                let (_file, metadata) = backend.metadata_file(&live)?;
                let before = native_metadata_fingerprint(&metadata, None, &backend.work)?;
                let after = native_metadata_fingerprint(&metadata, Some(&desired), &backend.work)?;
                return Ok(MaterializationPreimage {
                    image: encode_native_witness(Some(before), Some(after), Some(native_metadata_from_stat(&metadata)), &backend.work)?,
                });
            }
            let before = backend.fingerprint_for_edit(&live, edit)?;
            let after = match edit {
                MaterializationEdit::Install { .. } => match backend.fingerprint(&target)? {
                    Some(image) => Some(image),
                    None => return Err(NativeTreeMaterializationError::MissingTarget(backend.work.display(path)?.0)),
                },
                MaterializationEdit::Remove { .. } => None,
                MaterializationEdit::SetMetadata { .. } => unreachable!("metadata captured from its held allocation"),
                MaterializationEdit::Rename { .. } => {
                    return Err(NativeTreeMaterializationError::UnsupportedRename);
                }
            };
            Ok(MaterializationPreimage {
                image: encode_native_witness(before, after, None, &backend.work)?,
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
                    return Err(NativeTreeMaterializationError::ExternalMutation(
                        backend.work.display(path)?.0,
                    ));
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
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    backend.work.display(path)?.0,
                ));
            }
            if !backup_exists && live_fingerprint != before {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    backend.work.display(path)?.0,
                ));
            }
            if target_expected && target_fingerprint != after {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    backend.work.display(path)?.0,
                ));
            }
            if backup_exists {
                if target_expected && target_exists {
                    backend.remove(&live)?;
                    backend.rename(&target, &live)?;
                } else if target_expected && !live_exists {
                    return Err(NativeTreeMaterializationError::MissingTarget(
                        backend.work.display(path)?.0,
                    ));
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
                    return Err(NativeTreeMaterializationError::MissingTarget(
                        backend.work.display(path)?.0,
                    ));
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
                    return Err(NativeTreeMaterializationError::ExternalMutation(
                        backend.work.display(path)?.0,
                    ));
                }
                let metadata = decode_native_preimage_metadata(&preimage.image)?
                    .ok_or(NativeTreeMaterializationError::InvalidPreimage)?;
                return backend.set_metadata(&live, &metadata, after);
            }
            let backup_current = backend.fingerprint(&backup)?;
            let backup_exists = backup_current.is_some();
            if backup_exists && backup_current != before {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    backend.work.display(path)?.0,
                ));
            }
            if current == before && !backup_exists {
                return Ok(());
            }
            if current != after && !(current.is_none() && backup_exists) {
                return Err(NativeTreeMaterializationError::ExternalMutation(
                    backend.work.display(path)?.0,
                ));
            }
            match before {
                None => backend.remove(&live),
                Some(_) if backup_exists => {
                    backend.remove(&live)?;
                    backend.rename(&backup, &live)
                }
                Some(expected) if current == Some(expected) => Ok(()),
                Some(_) => Err(NativeTreeMaterializationError::MissingPreimage(
                    backend.work.display(path)?.0,
                )),
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
    if crate::native_host::directory_identity_materialization_measured(&directory, work)? != native_stat_identity(metadata) {
        return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
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
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
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
    Ok(writer.image)
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeImageWriter<'a> {
    image: Vec<u8>,
    work: &'a NativeMaterializationWork,
    allocation: NativeAllocation,
    failure: Option<NativeTreeMaterializationError>,
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> NativeImageWriter<'a> {
    fn new(work: &'a NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        Ok(Self { image: Vec::new(), work, allocation: work.claim(0)?, failure: None })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<usize, NativeTreeMaterializationError> {
        let remaining = self.work.remaining()?;
        let length = bytes.len().min(usize::try_from(remaining.bytes_encoded.min(remaining.bytes_copied)).unwrap_or(usize::MAX));
        let delta = crate::WorkCounters { bytes_encoded: length as u64, bytes_copied: length as u64, ..crate::WorkCounters::default() };
        if length == 0 && !bytes.is_empty() {
            self.work.admit(crate::WorkCounters { bytes_encoded: 1, bytes_copied: 1, ..crate::WorkCounters::default() })?;
        }
        self.work.admit(delta)?;
        let end = self.image.len().checked_add(length).ok_or(crate::WorkError::Overflow)?;
        if end > self.image.capacity() {
            let next = self.image.capacity().checked_mul(2).ok_or(crate::WorkError::Overflow)?.max(8).max(end);
            self.allocation.grow(owned_size(next - self.image.capacity(), 1)?)?;
            self.image.try_reserve_exact(next - self.image.len())
                .map_err(|_| NativeTreeMaterializationError::Allocation("native image growth failed".to_owned()))?;
        }
        self.image.extend_from_slice(&bytes[..length]);
        self.work.spent(delta)?;
        Ok(length)
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl std::io::Write for NativeImageWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.append(bytes).map_err(|error| {
            self.failure = Some(error);
            std::io::Error::other("native image work boundary failed")
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
            if actual != native_stat_identity(&metadata) { return Err(NativeTreeMaterializationError::ExternalMutation(String::new())); }
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
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
        }
        return native_hash_frame(directory, &backend.work).map(Some);
    } else if metadata.file_type().is_file() {
        let mut file = open_native_walk_file(parent, leaf, &backend.work)?;
        let opened = crate::native_host::stat_file_with_work(&file, &mut &backend.work)?;
        if native_stat_identity(&opened) != native_stat_identity(metadata) || opened.len() != metadata.len() {
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
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
            return Err(NativeTreeMaterializationError::ExternalMutation(String::new()));
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
) -> Result<Vec<u8>, NativeTreeMaterializationError> {
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
    Ok(writer.image)
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
    MissingTarget(String),
    /// A physical endpoint changed after its durable witness was captured.
    #[error("native materialization observed an external mutation at '{0}'")]
    ExternalMutation(String),
    /// A durable preimage references a missing backup binding.
    #[error("native materialization preimage is missing for '{0}'")]
    MissingPreimage(String),
    /// Persisted preimage bytes are malformed.
    #[error("native materialization preimage is invalid")]
    InvalidPreimage,
    /// A backup exists before its operation journal has been created.
    #[error("native materialization found an unexpected backup for '{0}'")]
    UnexpectedBackup(String),
    /// Native path exchange does not yet expose a rename edit directly.
    #[error("native materialization rename edits are unsupported")]
    UnsupportedRename,
    /// Native filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A changed namespace path cannot be represented by this host publisher.
    #[error("native materialization path is invalid: '{0}'")]
    InvalidPath(String),
    /// Changed paths overlap and cannot be exchanged independently.
    #[error("native materialization paths overlap")]
    OverlappingPaths,
    /// Cooperative cancellation at a host or journal boundary.
    #[error("native materialization cancelled")]
    Cancelled,
    /// A finite work allowance was exhausted before the next operation.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A measured host buffer could not be admitted or allocated.
    #[error("native materialization allocation failed: {0}")]
    Allocation(String),
    /// The original fallible owned buffer reservation failed.
    #[error("native materialization allocation failed: {0}")]
    AllocationFailed(#[source] std::collections::TryReserveError),
    /// Source metadata requires observations this native publisher cannot prove.
    #[error("native materialization cannot verify complete source metadata at '{0}'")]
    UnverifiedSourceMetadata(String),
    /// The actual held checkout allocation differs from the original owner.
    #[error("native materialization root allocation changed: expected {expected:?}, observed {actual:?}")]
    AllocationChanged { expected: crate::NativeRootIdentity, actual: crate::NativeRootIdentity },
    /// A durable mkdir intent lacks a confirmed created allocation identity.
    #[error("native preparation has an indeterminate allocation; retain the original pending journal without adopting the current path")]
    IndeterminatePreparation,
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
) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
{
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, NativeWorkspacePublicationError::Native))?;
    let result = async {
        let from_generation = workspace_result(workspace.generation_measured(publication.from, work.remaining()?, publication.cancellation).await, &work)?;
        let to_generation = workspace_result(workspace.generation_measured(publication.to, work.remaining()?, publication.cancellation).await, &work)?;
        let mut before_effect = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
        publish_native_transition_inner(&from_generation, &to_generation, state, publication, work.clone(), false, &mut before_effect).await
    }.await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
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
) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, NativeWorkspacePublicationError>
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
) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, NativeWorkspacePublicationError> {
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
) -> crate::MeasuredResult<crate::OperationReceipt<Option<MaterializationJournal>>, NativeWorkspacePublicationError> {
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, NativeWorkspacePublicationError::Native))?;
    let result = async {
        let journal = MaterializationJournalStore::load(&state.materialization_with_work(work.clone()), operation_id).await?;
        if let Some(journal) = &journal {
            let _custody = work.retain_journal(journal)?;
            if !valid_journal(journal, operation_id) || !native_check_plan(&journal.plan, &work)? {
                return Err(NativeWorkspacePublicationError::MismatchedJournal);
            }
        }
        Ok(journal)
    }.await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
    }
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
async fn native_generation_transition<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    from_generation: &crate::Generation<A, O>,
    to_generation: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    prepare_only: bool,
) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, NativeWorkspacePublicationError> {
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, NativeWorkspacePublicationError::Native))?;
    let mut before_effect = || std::future::ready(Ok::<(), NativeWorkspacePublicationError>(()));
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), prepare_only, &mut before_effect).await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
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
) -> crate::MeasuredResult<crate::OperationReceipt<MaterializationJournal>, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    let result = publish_native_transition_inner(from_generation, to_generation, state, publication, work.clone(), false, &mut before_effect).await;
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
        Err(error) => Err(crate::OperationFailure::new(error, work.receipt())),
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
    if work.budget != publication.budget {
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
    let (root, mut path_custody) = work.copy_path(root)?;
    let (operation_directory, operation_custody) = work.copy_path(operation_directory)?;
    path_custody.absorb(operation_custody)?;
    let (root, operation_directory, path_custody) = acyclic_native_runtime::run_blocking_io({
        let work = work.clone();
        move || {
            let actual = validate_native_preparation_location(&root, &operation_directory, &work)?;
            if actual != source_root_identity { return Err(NativeTreeMaterializationError::AllocationChanged { expected: source_root_identity, actual }); }
            Ok::<_, NativeTreeMaterializationError>((root, operation_directory, path_custody))
        }
    }).await??;
    if let Some(mut existing) = MaterializationJournalStore::load(&store, operation_id).await? {
        let existing_custody = work.retain_owned(journal_owned_bytes(&existing)?)?;
        if existing.root_identity != Some(source_root_identity.to_bytes()) {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if existing.plan.intent != (MaterializationIntent::GenerationTransition { from, to }) {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if !valid_journal(&existing, operation_id) || !native_check_plan(&existing.plan, &work)? {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        if existing.preparation_progress.is_some() {
            existing = NativeTreeMaterializationBackend::prepare_allocations(&store, existing, &root, &operation_directory, &work).await?;
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
            materializer.verify_allocation(&existing).await?;
            let stage_identity = materializer.backend().reset_target_directory("").await?;
            let (install_paths, _install_custody) = native_install_paths(existing.plan.edits.iter().filter_map(|edit| match edit {
                MaterializationEdit::Install { path, .. } => Some(path.as_str()),
                _ => None,
            }), &work)?;
            let (path_options, _options_custody) = native_stage_options(options, materializer.backend().target.as_path(), stage_identity, &work)?;
            if !install_paths.is_empty() {
                workspace_result(to_generation.materialize_paths(&install_paths, &path_options, work.remaining()?, cancellation).await, &work)?;
            }
        }
        if prepare_only {
            if existing.phase == MaterializationPhase::Prepared { return Ok(existing); }
            drop(existing_custody);
            return materializer.capture_preimages(existing).await.map_err(Into::into);
        }
        drop(existing);
        drop(existing_custody);
        let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
        return materializer
            .recover_with_guard(operation_id, MaterializationRecovery::Complete, &mut guarded)
            .await?
            .ok_or(NativeWorkspacePublicationError::MismatchedJournal);
    }

    let planning_budget = work.remaining()?;
    let changes = from_generation
        .diff_to_bounded(to_generation, u32::MAX, planning_budget, cancellation)
        .await.map_err(|failure| workspace_failure(failure, &work))?;
    let changed = changes
        .changed_paths_bounded(u32::MAX, planning_budget, cancellation)
        .await.map_err(|failure| workspace_failure(failure, &work))?;
    work.spent(changed.work)?;
    drop(changes);
    let changed_bytes = changed.value.iter().try_fold(owned_size(changed.value.capacity(), std::mem::size_of::<crate::ChangedPath>())?, |bytes, change| bytes.checked_add(change.path.retained_bytes()).ok_or(crate::WorkError::Overflow))?;
    let mut changed_custody = work.retain_owned(changed_bytes)?;
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
    for change in changed.value {
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
            let stat = workspace_result(to_generation.stat_measured(&absolute, work.remaining()?, cancellation).await, &work)?;
            let image = encode_native_metadata(&stat.metadata, &work)?;
            path_allocation.absorb(work.retain_owned(image.capacity() as u64)?)?;
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
        plan_custody.absorb(allocation)?;
        native_vec_push(&mut edits, edit, &mut plan_custody)?;
    }
    drop(paths_storage);
    for (edit, allocation) in metadata_edits {
        plan_custody.absorb(allocation)?;
        native_vec_push(&mut edits, edit, &mut plan_custody)?;
    }
    drop(metadata_storage);
    let plan = MaterializationPlan { operation_id, intent: MaterializationIntent::GenerationTransition { from, to }, edits };
    if !native_check_plan(&plan, &work)? { return Err(NativeWorkspacePublicationError::MismatchedJournal); }
    let parent = crate::native_host::HostRoot::open_materialization_measured(operation_directory.parent().ok_or(NativeTreeMaterializationError::InvalidLayout)?, &work)?;
    let journal = MaterializationJournal {
        version: JOURNAL_VERSION, revision: 0, plan,
        root_identity: Some(source_root_identity.to_bytes()), preparation_identity: None,
        preparation_progress: Some(MaterializationPreparationProgress { parent: parent.identity().to_bytes(), operation: None, target: None, backup: None, pending: None }),
        work: work.journal_work()?, preimages: Vec::new(),
        phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
    };
    drop(parent);
    let (stored, original) = compare_next_journal::<_, NativeTreeMaterializationError>(&store, journal).await?;
    if !stored { return Err(NativeWorkspacePublicationError::MismatchedJournal); }
    let prepared = NativeTreeMaterializationBackend::prepare_allocations(&store, original, &root, &operation_directory, &work).await?;
    let backend = acyclic_native_runtime::run_blocking_io({
        let work = work.clone();
        move || NativeTreeMaterializationBackend::open_owned_with_work(root, operation_directory, path_custody, work)
    }).await??;
    let materializer = JournaledMaterializer::new(store, backend);
    materializer.verify_allocation(&prepared).await?;
    let allocated_target = crate::NativeRootIdentity::from_bytes(prepared.preparation_identity.ok_or(NativeTreeMaterializationError::IndeterminatePreparation)?.target);
    let (path_options, _options_custody) = native_stage_options(options, materializer.backend().target.as_path(), allocated_target, &work)?;
    let prepared_custody = plan_custody;
    if !install_paths.is_empty() {
        let staged = to_generation
            .materialize_paths(
                &install_paths,
                &path_options,
                work.remaining()?,
                cancellation,
            )
            .await.map_err(|failure| {
                let merge = work.spent(*failure.work);
                match merge {
                    Ok(()) => NativeWorkspacePublicationError::Workspace(failure.error),
                    Err(error) => NativeWorkspacePublicationError::Native(error),
                }
            })?;
        work.spent(staged.work)?;
    }
    drop(prepared_custody);
    if prepare_only {
        return materializer.capture_preimages(prepared).await.map_err(Into::into);
    }
    let mut guarded = |verify_source| native_before_effect(before_effect(), verify_source, &materializer.store, &materializer.backend().held_root, operation_id, from_generation, &work, cancellation);
    materializer
        .apply_with_guard(prepared.plan, &mut guarded)
        .await
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn workspace_failure(failure: crate::OperationFailure<crate::WorkspaceError>, work: &NativeMaterializationWork) -> NativeWorkspacePublicationError {
    match work.spent(*failure.work) {
        Ok(()) => NativeWorkspacePublicationError::Workspace(failure.error),
        Err(error) => NativeWorkspacePublicationError::Native(error),
    }
}

#[cfg(all(feature = "local", feature = "native-mount", not(target_arch = "wasm32")))]
fn workspace_result<T>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, crate::WorkspaceError>,
    work: &NativeMaterializationWork,
) -> Result<T, NativeWorkspacePublicationError> {
    let receipt = result.map_err(|failure| workspace_failure(failure, work))?;
    work.spent(receipt.work)?;
    Ok(receipt.value)
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
        let _custody = work.retain_journal(&journal)?;
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
        let source = match generation.stat_measured(&absolute, work.remaining()?, cancellation).await {
            Ok(receipt) => { work.spent(receipt.work)?; Some(receipt.value) }
            Err(failure) if matches!(failure.error, crate::WorkspaceError::NotFound) => { work.spent(*failure.work)?; None }
            Err(failure) => return Err(workspace_failure(failure, work)),
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
                            let published = workspace_result(generation.read_symbolic_link_measured(&absolute, work.remaining()?, cancellation).await, work)?;
                            let _published_custody = work.retain_owned(published.len() as u64)?;
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
            return Err(NativeTreeMaterializationError::ExternalMutation(work.display(&path)?.0).into());
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
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata(work.display(path)?.0));
    }
    #[cfg(windows)]
    if authored.posix_mode.is_some() || authored.posix_uid.is_some() || authored.posix_gid.is_some() || authored.changed_ns.is_some() {
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata(work.display(path)?.0));
    }
    #[cfg(unix)]
    if authored.windows_attributes.is_some() {
        return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata(work.display(path)?.0));
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
                crate::kernel::MetadataField::Unavailable => return Err(NativeTreeMaterializationError::UnverifiedSourceMetadata(work.display(path)?.0)),
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
        return Err(NativeTreeMaterializationError::InvalidPath(work.display("<non-Unicode>")?.0));
    };
    let length = path.len().checked_add(1).and_then(|bytes| bytes.checked_add(name_bytes)).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters { bytes_copied: length as u64 - 1, ..crate::WorkCounters::default() };
    work.admit(copied)?;
    let custody = work.claim(length as u64)?;
    let mut output = String::new();
    output.try_reserve_exact(length).map_err(|error| NativeTreeMaterializationError::Allocation(error.to_string()))?;
    output.push_str(path);
    output.push('/');
    if name.append_unicode(&mut output).is_err() {
        return Err(NativeTreeMaterializationError::InvalidPath(work.display("<non-Unicode>")?.0));
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
        return Err(NativeTreeMaterializationError::ExternalMutation(work.display(path)?.0).into());
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
        let mut page = workspace_result(generation
            .list_directory_measured(&directory, after.as_ref(), 1_024, work.remaining()?, cancellation)
            .await, work)?;
        let page_bytes = page.entries.iter().try_fold(owned_size(page.entries.capacity(), std::mem::size_of::<crate::WorkspaceDirectoryEntry>())?, |bytes, entry| bytes.checked_add(entry.name.retained_bytes() as u64).ok_or(crate::WorkError::Overflow))?;
        let mut page_custody = work.retain_owned(page_bytes)?;
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
    let published = workspace_result(generation.read_measured(&absolute, metadata.len(), work.remaining()?, cancellation).await, work)?;
    let _published_allocation = work.retain_owned(published.capacity() as u64)?;
    let mut file = crate::native_host::open_leaf_with_work(&parent, Path::new(leaf), &crate::native_host::NativeOpenOptions::Read, &mut &*work)?;
    let opened = crate::native_host::stat_file_with_work(&file, &mut &*work)?;
    if native_stat_identity(&opened) != native_stat_identity(&metadata) || opened.len() != metadata.len() {
        return Err(NativeTreeMaterializationError::ExternalMutation(work.display(path)?.0).into());
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
    /// Exact work already spent by authenticated generation materialization.
    #[error(transparent)]
    MeasuredWorkspace(#[from] crate::OperationFailure<crate::WorkspaceError>),
    /// Host tree planning or publication failed.
    #[error(transparent)]
    Native(#[from] NativeTreeMaterializationError),
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
}

/// Process-local materialization journal adapter.
#[derive(Default)]
pub struct MemoryMaterializationJournalStore {
    journals: MemoryRecords<OperationId, MaterializationJournal>,
}

impl MaterializationJournalStore for MemoryMaterializationJournalStore {
    type Error = MemoryMaterializationJournalStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.journals
            .load(&operation_id)
            .map_err(|_| MemoryMaterializationJournalStoreError)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<(bool, MaterializationJournal), Self::Error> {
        let stored = self.journals
            .compare_and_swap(operation_id, expected_revision, replacement.clone())
            .map_err(|_| MemoryMaterializationJournalStoreError)?;
        Ok((stored, replacement))
    }
}

/// Process-local materialization journal synchronization failure.
#[derive(Debug, Error)]
#[error("materialization memory store is unavailable")]
pub struct MemoryMaterializationJournalStoreError;

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
    #[error("test backend unavailable")]
    struct BackendError;

    impl MaterializationBackend for Backend {
        type Error = BackendError;

        async fn capture(
            &self,
            _edit: &MaterializationEdit,
        ) -> Result<MaterializationPreimage, Self::Error> {
            Ok(MaterializationPreimage { image: vec![0] })
        }

        async fn apply(
            &self,
            edit: &MaterializationEdit,
            _preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            let MaterializationEdit::Install { image, .. } = edit else {
                return Err(BackendError);
            };
            self.0.lock().map_err(|_| BackendError)?.push(image.clone());
            Ok(())
        }

        async fn restore(
            &self,
            _edit: &MaterializationEdit,
            preimage: &MaterializationPreimage,
        ) -> Result<(), Self::Error> {
            self.0
                .lock()
                .map_err(|_| BackendError)?
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
                return Err(BackendError);
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
                .map_err(|_| BackendError)?
                .get(path)
                .copied())
        }

        fn set(&self, path: &str, value: Option<u8>) -> Result<(), BackendError> {
            let mut tree = self.tree.lock().map_err(|_| BackendError)?;
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

        async fn capture(
            &self,
            edit: &MaterializationEdit,
        ) -> Result<MaterializationPreimage, Self::Error> {
            let value = self.current(endpoints(edit).0)?;
            Ok(MaterializationPreimage {
                image: value.into_iter().collect(),
            })
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

    impl MaterializationJournalStore for CrashingStore<'_> {
        type Error = BackendError;

        async fn load(
            &self,
            operation_id: OperationId,
        ) -> Result<Option<MaterializationJournal>, Self::Error> {
            self.0.load(operation_id).await.map_err(|_| BackendError)
        }

        async fn compare_and_swap(
            &self,
            operation_id: OperationId,
            expected_revision: u64,
            replacement: MaterializationJournal,
        ) -> Result<(bool, MaterializationJournal), Self::Error> {
            let swapped = self
                .0
                .compare_and_swap(operation_id, expected_revision, replacement)
                .await
                .map_err(|_| BackendError)?;
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
                .compare_and_swap(operation_id, 0, journal.clone())
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
                    .expect("retained journal"),
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
                MaterializationJournal {
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
                },
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
                MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: backend.allocation_identity(),
                    preparation_identity: backend.preparation_identity(),
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan: plan.clone(),
                    preimages: vec![preimage.clone()],
                    phase: MaterializationPhase::Prepared,
                    applied: 0,
                    restored: 0,
                },
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
                backend.observe(edit, &preimage).await.expect("observe"),
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
        let backend =
            NativeTreeMaterializationBackend::new(&root, &operation).expect("native backend");
        let edit = MaterializationEdit::Install {
            path: "file.txt".to_owned(),
            image: Vec::new(),
        };

        assert!(matches!(
            backend.capture(&edit).await,
            Err(NativeTreeMaterializationError::MissingTarget(path)) if path == "file.txt"
        ));
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
                Err(NativeTreeMaterializationError::ExternalMutation(path)) if path == "file.txt"
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
        backend.apply(edit, &preimage).await.expect("apply");
        std::fs::write(root.join("file.txt"), b"external").expect("external write");

        assert!(matches!(
            backend.restore(edit, &preimage).await,
            Err(NativeTreeMaterializationError::ExternalMutation(path)) if path == "file.txt"
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
            backend.observe(&edit, &preimage).await.expect("observe"),
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
                MaterializationJournal {
                    version: JOURNAL_VERSION,
                    revision: 1,
                    root_identity: backend.allocation_identity(),
                    preparation_identity: backend.preparation_identity(),
                    preparation_progress: None,
                    work: crate::WorkCounters::default(),
                    plan: plan.clone(),
                    preimages: vec![preimage.clone()],
                    phase: MaterializationPhase::Applying,
                    applied: 0,
                    restored: 0,
                },
            )
            .await
            .expect("journal");
        backend
            .apply(&plan.edits[0], &preimage)
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
        assert!(matches!(failure.error, MaterializationError::Backend(NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "items_examined", .. }))));
        assert_eq!(failure.work.items_examined, 4);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert!(!state_path.exists());
        assert_eq!(std::fs::read_dir(root).expect("untouched root").count(), 0);
        failure.work.verify(budget).expect("actual validation prefix");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_meter_admits_its_actual_state_before_allocation() {
        let cancellation = crate::CancellationToken::new();
        let mut denied = finite_native_budget();
        denied.peak_allocation_bytes = NativeMaterializationWork::accounting_state_bytes() - 1;
        let failure = NativeMaterializationWork::new(denied, &cancellation).err().expect("state admission fails");
        assert!(matches!(failure.error, NativeTreeMaterializationError::Work(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes", .. })));
        assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
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
        backend.apply(&edit, &preimage).await.expect("real readonly mutation");
        assert!(std::fs::metadata(&file).expect("physical metadata").permissions().readonly());
        assert_eq!(backend.observe(&edit, &preimage).await.expect("actual postimage"), MaterializationObservation::Postimage);
        backend.restore(&edit, &preimage).await.expect("restore witnessed attributes");
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
        let inspection = resumed.inspect(id).await.expect("real restart inspection").expect("original journal retained");
        assert_eq!(inspection.journal.plan, original.plan);
        assert_eq!(inspection.journal.phase, MaterializationPhase::Preparing);
        assert_eq!(inspection.root, MaterializationRootObservation::Identical(original.root_identity.expect("root identity")));
        assert_eq!(inspection.preparation, MaterializationPreparationObservation::Identical(original.preparation_identity.expect("cleanup identity")));
        assert_eq!(resumed.backend().reset_target_directory("").await.expect("finish original cleanup").to_bytes(), original.preparation_identity.expect("target identity").target);
        assert_eq!(std::fs::read_dir(operation.join("target")).expect("empty original stage").count(), 0);
        assert!(root.is_dir());
        resumed_work.receipt().verify(full).expect("restart prefix retained");
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    struct FaultingLocalJournal<'a> {
        store: crate::core_state::MeasuredMaterializationStore<'a>,
        fail_revision: u64,
        lose_ack: bool,
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    impl MaterializationJournalStore for FaultingLocalJournal<'_> {
        type Error = crate::LocalCoreStateStoreError;

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
            let fault = FaultingLocalJournal { store: state.materialization_with_work(work.clone()), fail_revision: 5, lose_ack };
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
            assert_eq!(failure.work.source_bytes_read, exact.source_bytes_read);
            failure.work.verify(exact).expect("failure retains exact spent read prefix");
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
        assert_eq!(loaded.value.as_ref(), Some(&prepared.value));
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
        failure.work.verify(budget).expect("all partial invocation work retained");
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
        assert!(matches!(failure.error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_))));
        failure.work.verify(budget).expect("preflight failure work");
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
        failure.work.verify(budget).expect("post-guard root failure work");
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
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_))));
        assert_eq!(std::fs::read(root.join("tree/file")).expect("dirty child untouched"), b"dirty untouched");
        assert_eq!(std::fs::read(root.join("tree/user")).expect("unknown child untouched"), b"unknown untouched");
        std::fs::remove_file(root.join("tree/user")).expect("remove fixture unknown child");
        let error = native_preflight_path(&held, "tree", &generation, true, true, &work, &cancellation).await.expect_err("matching names alone cannot authorize changed descendant bytes");
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_))));
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
                let _plan_custody = work.retain_owned(plan_owned_bytes(&plan).expect("plan size")).expect("original plan custody");
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
                let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), journal).await.expect("first original intent").1;
                assert!(!operation.exists());
                let fault = FaultingLocalJournal { store: state.materialization_with_work(work.clone()), fail_revision, lose_ack };
                let error = NativeTreeMaterializationBackend::prepare_allocations(&fault, initial, &root, &operation, &work).await.expect_err("original allocation checkpoint failure");
                assert!(matches!(error, MaterializationError::Store(_)));
                work.receipt().verify(budget).expect("all native and failed journal work retained");
                assert_eq!(std::fs::read(root.join("file")).expect("source unchanged"), b"source remains");
                drop(fault);
                drop(state);
                let restarted = crate::LocalCoreStateStore::new(&state_path);
                let current = load_native_materialization_journal(&restarted, operation_id, budget, &cancellation).await.expect("root independent original progress query").value.expect("retained original intent");
                assert_eq!(current.root_identity, Some(source.to_bytes()));
                assert_eq!(current.phase, MaterializationPhase::Preparing);
                assert!(current.preimages.is_empty());
                assert!(current.is_valid_for(operation_id));
                let unknown = current.preparation_progress.is_some_and(|progress| progress.pending.is_some());
                let resumed_work = NativeMaterializationWork::new(budget, &cancellation).expect("restart meter");
                if unknown {
                    let progress = current.preparation_progress.expect("indeterminate allocation prefix");
                    let before = std::fs::read_dir(temporary.path()).expect("fixture names").count();
                    let error = NativeTreeMaterializationBackend::prepare_allocations(&restarted.materialization_with_work(resumed_work.clone()), current, &root, &operation, &resumed_work).await.expect_err("unknown mkdir cannot be adopted from a current path");
                    assert!(matches!(error, MaterializationError::Backend(NativeTreeMaterializationError::IndeterminatePreparation)));
                    assert_eq!(std::fs::read_dir(temporary.path()).expect("no repair or adoption").count(), before);
                    let retained = load_native_materialization_journal(&restarted, operation_id, budget, &cancellation).await.expect("still original pending decision").value.expect("retained");
                    assert_eq!(retained.preparation_progress, Some(progress));
                    assert!(retained.preparation_identity.is_none());
                } else if current.preparation_progress.is_some() {
                    let completed = NativeTreeMaterializationBackend::prepare_allocations(&restarted.materialization_with_work(resumed_work.clone()), current, &root, &operation, &resumed_work).await.expect("continue only recorded known original allocations");
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
        async fn verify_source(&self, _: GenerationId) -> Result<bool, Self::Error> { Ok(self.valid.load(std::sync::atomic::Ordering::SeqCst)) }
        async fn capture(&self, _: &MaterializationEdit) -> Result<MaterializationPreimage, Self::Error> {
            self.valid.store(false, std::sync::atomic::Ordering::SeqCst);
            Ok(MaterializationPreimage { image: vec![1] })
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
        fn retain_journal(&self, journal: &MaterializationJournal) -> Result<Option<NativeAllocation>, Self::Error> { MaterializationBackend::retain_journal(&self.inner, journal) }
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
        assert!(matches!(error, NativeWorkspacePublicationError::Native(NativeTreeMaterializationError::ExternalMutation(_))));
        assert_eq!(owners.load(std::sync::atomic::Ordering::SeqCst), 1);
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
    impl MaterializationJournalStore for AllocationBoundaryJournal<'_> {
        type Error = crate::LocalCoreStateStoreError;
        async fn load(&self, operation: OperationId) -> Result<Option<MaterializationJournal>, Self::Error> { self.store.load(operation).await }
        async fn compare_and_swap(&self, operation: OperationId, expected: u64, journal: MaterializationJournal) -> Result<(bool, MaterializationJournal), Self::Error> {
            let revision = journal.revision;
            if revision == 3 { *self.boundary.lock().expect("boundary fixture") = Some(self.work.receipt()); }
            let result = self.store.compare_and_swap(operation, expected, journal).await?;
            if result.0 && self.cancel_after == Some(revision) { self.work.cancellation.cancel(); }
            Ok(result)
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn allocation_fixture_journal(operation: OperationId, root: crate::NativeRootIdentity, parent: crate::NativeRootIdentity) -> MaterializationJournal {
        MaterializationJournal {
            version: JOURNAL_VERSION, revision: 0,
            plan: MaterializationPlan { operation_id: operation,
                intent: MaterializationIntent::GenerationTransition {
                    from: GenerationId::new(Digest::from_bytes([1; 32])), to: GenerationId::new(Digest::from_bytes([2; 32])),
                }, edits: vec![MaterializationEdit::Remove { path: "file".to_owned() }],
            },
            root_identity: Some(root.to_bytes()), preparation_identity: None,
            preparation_progress: Some(MaterializationPreparationProgress { parent: parent.to_bytes(), operation: None, target: None, backup: None, pending: None }),
            work: crate::WorkCounters::UNCHARGED, preimages: Vec::new(), phase: MaterializationPhase::Preparing, applied: 0, restored: 0,
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
        let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state, allocation_fixture_journal(probe_id, source, parent)).await.expect("original fixture intent").1;
        let work = NativeMaterializationWork::new(budget, &cancellation).expect("probe actual allocation meter");
        let boundary = std::sync::Arc::new(std::sync::Mutex::new(None));
        let store = AllocationBoundaryJournal { store: state.materialization_with_work(work.clone()), work: work.clone(), boundary: boundary.clone(), cancel_after: None };
        NativeTreeMaterializationBackend::prepare_allocations(&store, initial, &root, &operation, &work).await.expect("measure actual first-mkdir boundary");
        let before_actual_id_cas = boundary.lock().expect("actual boundary").expect("actual first mkdir and identity reads");
        std::fs::remove_dir_all(&operation).expect("reset owned fixture only");
        let replay_id = OperationId::new();
        let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state, allocation_fixture_journal(replay_id, source, parent)).await.expect("independent original fixture intent").1;
        let mut exact = budget;
        exact.metadata_operations = before_actual_id_cas.metadata_operations;
        let replay = NativeMaterializationWork::new(exact, &cancellation).expect("exact finite boundary");
        NativeTreeMaterializationBackend::prepare_allocations(&state.materialization_with_work(replay.clone()), initial, &root, &operation, &replay).await.expect_err("actual-ID journal work cannot pass the first-mkdir metadata boundary");
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
            let initial = compare_next_journal::<_, NativeTreeMaterializationError>(&state.materialization_with_work(work.clone()), allocation_fixture_journal(operation_id, source, parent)).await.expect("same original intent").1;
            let store = AllocationBoundaryJournal { store: state.materialization_with_work(work.clone()), work: work.clone(), boundary: std::sync::Arc::default(), cancel_after: Some(cancel_after) };
            NativeTreeMaterializationBackend::prepare_allocations(&store, initial, &root, &operation, &work).await.expect_err("shared cancellation after actual original journal acknowledgment");
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
