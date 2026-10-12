//! Finite work and owned-byte limits for filesystem preparation.
use super::*;
use crate::conversation::FileRef;
use acyclic_fs::{
    CancellationToken, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkspaceError,
};
use std::sync::Mutex;
mod codec;
mod history;
mod metadata;
mod quota;
mod records;
mod sdk;
#[cfg(test)]
mod tests;
pub(in crate::filesystem) use metadata::*;
use quota::finish_decode;
#[cfg(not(target_arch = "wasm32"))]
use quota::finish_encoding;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use quota::native_decoded;
pub(crate) use quota::{DecodeReservation, retain_decoded};
pub(crate) use records::{PreparationRecord, UploadRecord, preparation_slot_bytes};
use records::{decode_allowance_error, decode_allowance_with_code};
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) use sdk::publish_native;
pub(in crate::filesystem) use sdk::{
    apply_generation, apply_generation_task_guarded, create, fork, generation, generation_ref,
    open, operation_generation, publish_generation, read, resolve, retain,
};

/// Work and retained ownership for one preparation attempt.
pub enum PreparationIo {
    Owned {
        budget: WorkBudget,
        work: Mutex<(WorkCounters, u64)>,
        cancellation: CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        work: acyclic_fs::NativeMaterializationWork,
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
    pub(crate) fn from_budget(
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
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
            work: Mutex::new((WorkCounters::default(), 0)),
            cancellation: cancellation.clone(),
        })
    }
    pub(crate) fn for_unmeasured(cancellation: &CancellationToken) -> Result<Self> {
        cancellation
            .check()
            .map_err(|cause| Error::Storage(cause.to_string()))?;
        Ok(Self::Owned {
            budget: WorkBudget::UNBOUNDED,
            work: Mutex::new((WorkCounters::default(), 0)),
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
        if let Err(cause) = custody.substitute(retained) {
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
    pub(crate) fn take_native_custody(&self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
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
        failure(Some(self), crate::wire::ErrorCode::Invalid, reason)
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
                .map(|state| state.1)
                .map_err(|_| self.invalid("preparation accounting is poisoned")),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { custody, .. } => Ok(custody
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .bytes()),
        }
    }
    fn preflight_workspace(
        &self,
        mut next: WorkCounters,
    ) -> std::result::Result<(), WorkspaceError> {
        self.cancellation().check()?;
        match self {
            Self::Owned { budget, work, .. } => {
                let state = work.lock().map_err(|_| {
                    WorkspaceError::Engine("preparation accounting is poisoned".into())
                })?;
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(state.1)
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
                state.0.checked_add(next)?.verify(*budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.admit(next).map_err(|cause| {
                WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))
            }),
        }
    }
    pub(crate) fn preflight(&self, next: WorkCounters) -> Result<()> {
        self.check_cancellation()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.original_native_work() {
            return work.admit(next).map_err(Into::into);
        }
        self.preflight_workspace(next).map_err(Into::into)
    }
    fn spent_workspace(&self, mut next: WorkCounters) -> std::result::Result<(), WorkspaceError> {
        match self {
            Self::Owned { work, .. } => {
                let mut state = work.lock().map_err(|_| {
                    WorkspaceError::Engine("preparation accounting is poisoned".into())
                })?;
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(state.1)
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
                state.0 = state.0.checked_add(next)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.spent(next).map_err(|cause| {
                WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))
            }),
        }
    }
    pub(crate) fn spent(&self, next: WorkCounters) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.original_native_work() {
            return work.spent(next).map_err(Into::into);
        }
        self.spent_workspace(next).map_err(Into::into)
    }
    pub(crate) fn charge(&self, next: WorkCounters) -> Result<()> {
        self.spent(next)?;
        match self {
            Self::Owned { budget, .. } => self
                .receipt()
                .verify(*budget)
                .map_err(|cause| Error::Invalid(cause.to_string())),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.remaining().map(|_| ()).map_err(Into::into),
        }
    }
    fn remaining_workspace(&self) -> std::result::Result<WorkBudget, WorkspaceError> {
        self.cancellation().check()?;
        match self {
            Self::Owned { budget, work, .. } => {
                let state = work.lock().map_err(|_| {
                    WorkspaceError::Engine("preparation accounting is poisoned".into())
                })?;
                let mut budget = state.0.remaining(*budget)?;
                budget.peak_allocation_bytes = budget
                    .peak_allocation_bytes
                    .checked_sub(state.1)
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
                Ok(budget)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.remaining().map_err(|cause| {
                WorkspaceError::Filesystem(acyclic_fs::FsError::NativeKernel(cause))
            }),
        }
    }
    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
        self.check_cancellation()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.original_native_work() {
            return work.remaining().map_err(Into::into);
        }
        self.remaining_workspace().map_err(Into::into)
    }
    pub(crate) fn retain_owned(&self, bytes: u64) -> Result<()> {
        self.check_cancellation()?;
        match self {
            Self::Owned { budget, work, .. } => {
                let mut state = work
                    .lock()
                    .map_err(|_| self.invalid("preparation accounting is poisoned"))?;
                let live = state
                    .1
                    .checked_add(bytes)
                    .ok_or_else(|| self.invalid("preparation ownership overflow"))?;
                let next = state
                    .0
                    .checked_add(WorkCounters {
                        peak_allocation_bytes: live,
                        ..Default::default()
                    })
                    .map_err(WorkspaceError::from)?;
                next.verify(*budget).map_err(WorkspaceError::from)?;
                state.0 = next;
                state.1 = live;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { custody, .. } => {
                let mut custody = custody
                    .lock()
                    .map_err(|_| self.invalid("preparation accounting is poisoned"))?;
                let live = custody
                    .bytes()
                    .checked_add(bytes)
                    .ok_or_else(|| self.invalid("preparation ownership overflow"))?;
                custody.resize_retained(live).map_err(Into::into)
            }
        }
    }
    pub(crate) fn release(&self, bytes: u64) -> Result<()> {
        match self {
            Self::Owned { work, .. } => {
                let mut state = work
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.1 = state
                    .1
                    .checked_sub(bytes)
                    .ok_or_else(|| self.invalid("preparation ownership underflow"))?;
                Ok(())
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
