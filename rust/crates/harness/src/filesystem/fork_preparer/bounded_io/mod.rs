//! Finite work and owned-byte limits for filesystem preparation.
use super::*;
use crate::conversation::FileRef;
use acyclic_fs::kernel::{allocation::AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use acyclic_fs::{
    CancellationToken, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkspaceError,
};
use std::sync::Mutex;
mod codec;
mod history;
mod quota;
mod records;
mod sdk;
#[cfg(test)]
mod tests;
use quota::finish_decode;
use quota::finish_encoding;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use quota::native_decoded;
pub(crate) use quota::{DecodeReservation, retain_decoded};
pub(crate) use records::{PreparationRecord, UploadRecord, preparation_slot_bytes};
use records::{decode_allowance_error, decode_allowance_with_code};
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) use sdk::publish_native;
pub(in crate::filesystem) use sdk::{
    apply_generation, apply_generation_task_guarded, content_workspace, create, fork, generation,
    generation_ref, mutation, open, operation_generation, operation_key, publish_generation, read,
    resolve, retain, volume_storage_name,
};

/// Work and retained ownership for one preparation attempt.
pub enum PreparationIo {
    /// Standalone work and byte allowances.
    Owned {
        /// Limits for this attempt.
        budget: WorkBudget,
        /// Spent work and live owned bytes.
        work: Mutex<(WorkCounters, AllocationLedger)>,
        /// Cancellation for this attempt.
        cancellation: CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    /// The original invocation's accounting context.
    Native {
        /// Original work and execution lease.
        work: acyclic_fs::NativeMaterializationWork,
        /// Retained metadata on the same ledger.
        custody: Mutex<acyclic_fs::NativeAllocation>,
    },
}

impl PreparationIo {
    pub(crate) fn new(
        request: &crate::resource_preparation::ResourcePreparationRequest,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        request.validate(&mut None)?;
        Self::from_budget(
            acyclic_fs::performance::work_budget(request.maximum_work, request.maximum_bytes),
            cancellation,
        )
    }
    /// Starts finite preparation accounting with the caller's cancellation token.
    pub fn from_budget(budget: WorkBudget, cancellation: &CancellationToken) -> Result<Self> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid(
                "measured content requires finite work allowances".into(),
            ));
        }
        cancellation
            .check()
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        Ok(Self::Owned {
            budget,
            work: Mutex::new((WorkCounters::default(), AllocationLedger::default())),
            cancellation: cancellation.clone(),
        })
    }
    pub(crate) fn for_unmeasured(cancellation: &CancellationToken) -> Result<Self> {
        cancellation
            .check()
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        Ok(Self::Owned {
            budget: WorkBudget::UNBOUNDED,
            work: Mutex::new((WorkCounters::default(), AllocationLedger::default())),
            cancellation: cancellation.clone(),
        })
    }
    /// Uses the original native invocation's ledger and execution lease.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        work.remaining()?;
        Ok(Self::Native {
            work: work.clone(),
            custody: Mutex::new(work.claim(0, 0, WorkCounters::default())?),
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_custody(custody: acyclic_fs::NativeAllocation) -> Self {
        Self::Native {
            work: custody.work().clone(),
            custody: Mutex::new(custody),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_native_custody(self, retained: u64) -> Result<acyclic_fs::NativeAllocation> {
        let Self::Native { custody, .. } = self else {
            return Err(self.invalid("native custody requires the original native ledger"));
        };
        let mut custody = custody
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Err(cause) = custody.resize(retained) {
            return Err(Error::NativeWork {
                cause,
                custody: Some(custody),
            });
        }
        Ok(custody)
    }
    pub(crate) fn original_native_work(&self) -> crate::NativeWork<'_> {
        match self {
            Self::Owned { .. } => None,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => Some(work),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn absorb_native_custody(
        &self,
        mut incoming: acyclic_fs::NativeAllocation,
    ) -> Result<()> {
        let Self::Native { custody, .. } = self else {
            return Err(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
                custody: Some(incoming),
            });
        };
        custody
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .absorb(&mut incoming)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(incoming),
            })
    }
    #[cfg(not(target_arch = "wasm32"))]
    /// Transfers retained output storage to its caller.
    pub fn take_native_custody(&self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        let Self::Native { custody, .. } = self else {
            return Err(self.invalid("native custody requires the original native ledger"));
        };
        custody
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .split_off(bytes)
            .map_err(Into::into)
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        match self {
            Self::Owned { cancellation, .. } => cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.cancellation(),
        }
    }
    pub(crate) fn check_cancellation(&self) -> Result<()> {
        self.cancellation().check().map_err(|cause| {
            #[cfg(not(target_arch = "wasm32"))]
            if self.original_native_work().is_some() {
                return Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                    custody: None,
                };
            }
            Error::Storage(cause.to_string())
        })
    }
    pub(crate) fn invalid(&self, reason: &'static str) -> Error {
        stock_boundary(Some(self), crate::wire::ErrorCode::Invalid, reason)
    }
    pub(crate) fn receipt(&self) -> WorkCounters {
        match self {
            Self::Owned { work, .. } => {
                work.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.receipt(),
        }
    }
    pub(crate) fn owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Owned { work, .. } => work
                .lock()
                .map(|state| state.1.live_bytes())
                .map_err(|_| self.invalid("preparation accounting is poisoned")),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { custody, .. } => Ok(custody
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .bytes()),
        }
    }
    fn account<T>(
        &self,
        operation: impl FnOnce(&mut KernelAccounting<'_>) -> std::result::Result<T, acyclic_fs::FsError>,
    ) -> std::result::Result<T, acyclic_fs::FsError> {
        match self {
            Self::Owned {
                budget,
                work,
                cancellation,
            } => {
                let mut state = work
                    .lock()
                    .map_err(|_| acyclic_fs::FsError::PendingMutationAllocationFailed)?;
                let (spent, allocations) = &mut *state;
                operation(&mut KernelAccounting::borrowed_standalone(
                    allocations,
                    spent,
                    *budget,
                    cancellation,
                ))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => operation(&mut KernelAccounting::NativeOriginal {
                work,
                cancellation: work.cancellation(),
            }),
        }
    }
    pub(crate) fn preflight(
        &self,
        mut next: WorkCounters,
    ) -> std::result::Result<(), acyclic_fs::FsError> {
        self.account(|accounting| {
            if let Some((allocations, _, _, _)) = accounting.standalone_parts() {
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(allocations.live_bytes())
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
            }
            accounting.admit(next)
        })
    }
    pub(crate) fn spent(
        &self,
        mut next: WorkCounters,
    ) -> std::result::Result<(), acyclic_fs::FsError> {
        self.account(|accounting| {
            if let Some((allocations, work, _, _)) = accounting.standalone_parts() {
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(allocations.live_bytes())
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
                *work = work.checked_add(next)?;
                Ok(())
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                if let Self::Native { work, .. } = self {
                    return work.spent(next).map_err(Into::into);
                }
                unreachable!()
            }
        })
    }
    pub(crate) fn charge(
        &self,
        mut next: WorkCounters,
    ) -> std::result::Result<(), acyclic_fs::FsError> {
        self.account(|accounting| {
            if let Some((allocations, _, _, _)) = accounting.standalone_parts() {
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(allocations.live_bytes())
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
            }
            accounting.charge(next)
        })
    }
    pub(crate) fn remaining(&self) -> std::result::Result<WorkBudget, acyclic_fs::FsError> {
        self.account(|accounting| accounting.remaining())
    }
    pub(crate) fn retain_owned(&self, bytes: u64) -> std::result::Result<(), acyclic_fs::FsError> {
        let claim = self.account(|accounting| accounting.claim_owned(bytes, 0))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let KernelOwnedAllocation::NativeOriginal(mut claim) = claim {
            let Self::Native { custody, .. } = self else {
                unreachable!()
            };
            custody
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .absorb(&mut claim)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = claim;
        Ok(())
    }
    pub(crate) fn release(&self, bytes: u64) -> std::result::Result<(), acyclic_fs::FsError> {
        match self {
            Self::Owned {
                budget,
                work,
                cancellation,
            } => {
                let mut state = work
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (spent, allocations) = &mut *state;
                KernelAccounting::borrowed_standalone(allocations, spent, *budget, cancellation)
                    .release_owned(&mut KernelOwnedAllocation::Standalone, bytes)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { custody, .. } => custody
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .release(bytes)
                .map_err(Into::into),
        }
    }
    pub(crate) fn allocation_attempt(&self, bytes: u64) -> Result<()> {
        self.charged(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            ..Default::default()
        })
    }
    pub(crate) fn reserve_decode(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        self.retain_owned(bytes)?;
        Ok(DecodeReservation { io: self, bytes })
    }
    fn reserve_typed(
        &self,
        owned: u64,
        diagnostics: u64,
        code: Option<crate::wire::ErrorCode>,
    ) -> Result<DecodeReservation<'_>> {
        self.reserve_decode(
            owned.checked_add(diagnostics).ok_or_else(|| {
                decode_allowance_error("typed diagnostic reservation overflow", code)
            })?,
        )
    }
    pub(crate) fn guard_owned(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        if bytes > self.owned_bytes()? {
            return Err(self.invalid("owned buffer exceeds retained quota"));
        }
        Ok(DecodeReservation { io: self, bytes })
    }
    pub(crate) fn guard_bytes(
        &self,
        bytes: &acyclic_stream::OwnedBytes,
    ) -> Result<Option<DecodeReservation<'_>>> {
        if self.original_native_work().is_some() {
            return Ok(None);
        }
        self.guard_owned(bytes.retained_capacity().unwrap_or(0) as u64)
            .map(Some)
    }
    pub(crate) fn receive<T>(
        &self,
        result: acyclic_fs::MeasuredResult<OperationReceipt<T>, WorkspaceError>,
    ) -> Result<T> {
        match result {
            Ok(receipt) => {
                self.charge(receipt.work)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                self.spent(*failure.work)?;
                #[cfg(not(target_arch = "wasm32"))]
                if self.original_native_work().is_some() {
                    return Err(match failure.error {
                        WorkspaceError::Work(cause) => Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Work(cause),
                            custody: None,
                        },
                        WorkspaceError::Cancelled(_) => Error::NativeWork {
                            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                            custody: None,
                        },
                        source => map_error(source),
                    });
                }
                Err(Error::from(failure.error))
            }
        }
    }
    pub(crate) fn receive_content<T>(
        &self,
        result: acyclic_fs::MeasuredResult<OperationReceipt<T>, Error>,
    ) -> Result<T> {
        match result {
            Ok(receipt) => {
                self.charge(receipt.work)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                self.spent(*failure.work)?;
                Err(failure.error)
            }
        }
    }
    pub(crate) fn finish<T>(
        &self,
        result: Result<T>,
    ) -> acyclic_fs::MeasuredResult<OperationReceipt<T>, Error> {
        result
            .map(|value| OperationReceipt {
                value,
                work: self.receipt(),
            })
            .map_err(|error| OperationFailure::new(error, self.receipt()))
    }
}

pub(in crate::filesystem) use codec::{clone_bytes, hex_key};
impl PreparationIo {
    pub(crate) fn reserve<T>(
        values: &mut Vec<T>,
        capacity: usize,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if capacity <= values.capacity() {
            return Ok(());
        }
        (capacity - values.capacity())
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "vector capacity overflow",
                )
            })?;
        let Some(io) = io else {
            return values
                .try_reserve_exact(capacity - values.len())
                .map_err(Error::Allocation);
        };
        use crate::store::history::HistoryIo as _;
        let copied = (values.len() * std::mem::size_of::<T>()) as u64;
        let mut observer = io.history_io();
        observer.admit_copy(copied)?;
        crate::store::history::reserve(values, capacity, &mut observer)?;
        observer.copy_bytes(copied)
    }
}
impl PreparationIo {
    pub(crate) fn capability<T: Serialize>(&self, value: &T, prefix: &str) -> Result<String> {
        let hash = crate::executor::preparation_digest(value, Some(self))?;
        let encoding = WorkCounters {
            bytes_encoded: if self.original_native_work().is_some() {
                64
            } else {
                0
            },
            ..Default::default()
        };
        self.preflight(encoding)?;
        let hex = blake3::Hash::from_bytes(hash).to_hex();
        self.charge(encoding)?;
        crate::executor::with_history_io(Some(self), |work| {
            crate::contract::preparation_format(format_args!("{prefix}{hex}"), work)
        })
    }
    pub(crate) fn contains_capability<T: Serialize>(
        &self,
        grants: &crate::Capabilities,
        value: &T,
        prefix: &str,
    ) -> Result<bool> {
        let text = self.capability(value, prefix)?;
        let held = self.guard_owned(text.capacity() as u64)?;
        let granted = grants.contains(&text);
        drop(text);
        drop(held);
        Ok(granted)
    }
}

use crate::executor::stock_boundary;
