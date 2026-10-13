//! Preparation work on the SDK allocation ledger.
use super::*;
use acyclic_fs::kernel::{AllocationLedger, KernelAccounting, KernelOwnedAllocation};
use acyclic_fs::{CancellationToken, FsError, WorkBudget, WorkCounters};
use std::sync::Mutex;

/// The caller's finite budget or original native invocation.
pub enum PreparationIo {
    /// Standalone preparation accounting.
    Standalone {
        /// Work and allocation state.
        state: Mutex<(WorkCounters, AllocationLedger)>,
        /// Admitted work limits.
        budget: WorkBudget,
        /// Original cancellation token.
        cancellation: CancellationToken,
    },
    #[cfg(not(target_arch = "wasm32"))]
    /// The original native invocation and retained allocation.
    Native {
        /// Original SDK work.
        work: acyclic_fs::NativeMaterializationWork,
        /// Metadata retained by this context.
        allocation: Mutex<acyclic_fs::NativeAllocation>,
    },
}

impl PreparationIo {
    pub(crate) fn capability<T: Serialize>(&self, value: &T, prefix: &str) -> Result<String> {
        let digest = crate::executor::preparation_digest(value, Some(self))?;
        self.charge(acyclic_fs::WorkCounters {
            bytes_encoded: 64,
            bytes_copied: 64,
            ..Default::default()
        })?;
        let digest = blake3::Hash::from_bytes(digest).to_hex();
        crate::executor::with_history_io(Some(self), |work| {
            crate::contract::preparation_format(format_args!("{prefix}{digest}"), work)
        })
    }
    pub(crate) fn contains_capability<T: Serialize>(
        &self,
        grants: &crate::Capabilities,
        value: &T,
        prefix: &str,
    ) -> Result<bool> {
        let capability = self.capability(value, prefix)?;
        let held = self.guard_owned(capability.capacity() as u64)?;
        let allowed = grants.contains(&capability);
        drop(capability);
        drop(held);
        Ok(allowed)
    }

    pub(crate) fn receive<T, E: Into<Error>>(
        &self,
        result: acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>,
    ) -> Result<T> {
        match result {
            Ok(receipt) => {
                self.charge(receipt.work)?;
                Ok(receipt.value)
            }
            Err(failure) => {
                self.spent(*failure.work)?;
                Err(failure.error.into())
            }
        }
    }
    pub(crate) fn finish<T>(
        &self,
        result: Result<T>,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, Error> {
        let work = self.receipt();
        result
            .map(|value| acyclic_fs::OperationReceipt { value, work })
            .map_err(|error| acyclic_fs::OperationFailure::new(error, work))
    }
    pub(crate) fn allocation_attempt(&self, bytes: u64) -> Result<()> {
        self.charge(WorkCounters {
            allocation_operations: u64::from(bytes != 0),
            ..Default::default()
        })
    }
    pub(crate) fn reserve<T>(
        values: &mut Vec<T>,
        capacity: usize,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if capacity <= values.capacity() {
            return Ok(());
        }
        let bytes = (capacity - values.capacity())
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "vector capacity overflow",
                )
            })? as u64;
        if let Some(io) = io {
            io.preflight(WorkCounters {
                bytes_copied: bytes,
                ..Default::default()
            })?;
            crate::store::history::reserve(values, capacity, &mut io.history_io())?;
            io.charge(WorkCounters {
                bytes_copied: bytes,
                ..Default::default()
            })
        } else {
            values
                .try_reserve_exact(capacity - values.len())
                .map_err(Error::Allocation)
        }
    }

    pub(crate) fn for_unmeasured(cancellation: &CancellationToken) -> Result<Self> {
        cancellation.check().map_err(FsError::from)?;
        Ok(Self::Standalone {
            state: Mutex::new((WorkCounters::default(), AllocationLedger::default())),
            budget: WorkBudget::UNBOUNDED,
            cancellation: cancellation.clone(),
        })
    }
    pub(crate) fn invalid(&self, reason: &'static str) -> Error {
        crate::executor::stock_boundary(Some(self), crate::wire::ErrorCode::Invalid, reason)
    }
    pub(crate) fn check_cancellation(&self) -> Result<()> {
        self.accounting(|accounting| accounting.check())
    }
    pub(crate) fn history_io(&self) -> &Self {
        self
    }
    pub(crate) fn reserve_decode(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        self.retain_owned(bytes)?;
        Ok(DecodeReservation { io: self, bytes })
    }
    pub(crate) fn guard_owned(&self, bytes: u64) -> Result<DecodeReservation<'_>> {
        if bytes > self.owned_bytes()? {
            return Err(self.invalid("storage was not admitted"));
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
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn from_native_custody(allocation: acyclic_fs::NativeAllocation) -> Self {
        Self::Native {
            work: allocation.work().clone(),
            allocation: Mutex::new(allocation),
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn absorb_native_custody(
        &self,
        mut incoming: acyclic_fs::NativeAllocation,
    ) -> Result<()> {
        let Self::Native { allocation, .. } = self else {
            return Err(self.invalid("allocation requires original work"));
        };
        allocation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .absorb(&mut incoming)
            .map_err(Into::into)
    }
    /// Transfers storage without another allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn take_native_custody(&self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        let Self::Native { allocation, .. } = self else {
            return Err(self.invalid("allocation requires original work"));
        };
        allocation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .split_off(bytes)
            .map_err(Into::into)
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn into_native_custody(self, bytes: u64) -> Result<acyclic_fs::NativeAllocation> {
        let Self::Native { allocation, .. } = self else {
            return Err(self.invalid("allocation requires original work"));
        };
        let mut allocation = allocation
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        allocation.resize(bytes)?;
        Ok(allocation)
    }

    /// Starts a finite preparation budget.
    pub fn from_budget(budget: WorkBudget, cancellation: &CancellationToken) -> Result<Self> {
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid(
                "preparation requires finite allowances".into(),
            ));
        }
        cancellation.check().map_err(FsError::from)?;
        Ok(Self::Standalone {
            state: Mutex::new((WorkCounters::default(), AllocationLedger::default())),
            budget,
            cancellation: cancellation.clone(),
        })
    }
    /// Uses the original SDK invocation without creating another ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_native_work(work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        Ok(Self::Native {
            work: work.clone(),
            allocation: Mutex::new(work.claim(0, 0, WorkCounters::default())?),
        })
    }
    pub(crate) fn original_native_work(&self) -> crate::NativeWork<'_> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Self::Native { work, .. } = self {
            return Some(work);
        }
        None
    }
    pub(crate) fn cancellation(&self) -> &CancellationToken {
        match self {
            Self::Standalone { cancellation, .. } => cancellation,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.cancellation(),
        }
    }
    fn accounting<T>(
        &self,
        operation: impl FnOnce(&mut KernelAccounting<'_>) -> std::result::Result<T, FsError>,
    ) -> Result<T> {
        let result = match self {
            Self::Standalone {
                state,
                budget,
                cancellation,
            } => {
                let mut state = state
                    .lock()
                    .map_err(|_| FsError::PendingMutationAllocationFailed)?;
                let (work, allocations) = &mut *state;
                operation(&mut KernelAccounting::borrowed_standalone(
                    allocations,
                    work,
                    *budget,
                    cancellation,
                ))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => operation(&mut KernelAccounting::NativeOriginal {
                work,
                cancellation: work.cancellation(),
            }),
        };
        result.map_err(Into::into)
    }
    pub(crate) fn preflight(&self, next: WorkCounters) -> Result<()> {
        next.verify(self.remaining()?).map_err(FsError::from)?;
        Ok(())
    }
    pub(crate) fn charge(&self, mut next: WorkCounters) -> Result<()> {
        self.accounting(|accounting| {
            if let Some((allocations, _, _, _)) = accounting.standalone_parts() {
                next.peak_allocation_bytes = next
                    .peak_allocation_bytes
                    .checked_add(allocations.live_bytes())
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
            }
            accounting.charge(next)
        })
    }
    pub(crate) fn spent(&self, mut completed: WorkCounters) -> Result<()> {
        self.accounting(|accounting| {
            if let Some((allocations, work, _, _)) = accounting.standalone_parts() {
                completed.peak_allocation_bytes = completed
                    .peak_allocation_bytes
                    .checked_add(allocations.live_bytes())
                    .ok_or(acyclic_fs::WorkError::Overflow)?;
                work.try_add_assign(&completed).map_err(Into::into)
            } else {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = accounting.native_work() {
                    return work.spent(completed).map_err(Into::into);
                }
                unreachable!()
            }
        })
    }
    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
        self.accounting(|accounting| accounting.remaining())
    }
    pub(crate) fn receipt(&self) -> WorkCounters {
        match self {
            Self::Standalone { state, .. } => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .0
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { work, .. } => work.receipt(),
        }
    }
    pub(crate) fn retain_owned(&self, bytes: u64) -> Result<()> {
        self.accounting(|accounting| {
            let allocation = accounting.claim_owned(bytes, 0)?;
            #[cfg(not(target_arch = "wasm32"))]
            if let KernelOwnedAllocation::NativeOriginal(mut incoming) = allocation {
                let Self::Native { allocation, .. } = self else {
                    unreachable!()
                };
                allocation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .absorb(&mut incoming)?;
            }
            Ok(())
        })
    }
    pub(crate) fn release(&self, bytes: u64) -> Result<()> {
        match self {
            Self::Standalone {
                state,
                budget,
                cancellation,
            } => {
                let mut state = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let (work, allocations) = &mut *state;
                KernelAccounting::borrowed_standalone(allocations, work, *budget, cancellation)
                    .release_owned(&mut KernelOwnedAllocation::Standalone, bytes)
                    .map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { allocation, .. } => allocation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .release(bytes)
                .map_err(Into::into),
        }
    }
    pub(crate) fn owned_bytes(&self) -> Result<u64> {
        match self {
            Self::Standalone { state, .. } => Ok(state
                .lock()
                .map_err(|_| FsError::PendingMutationAllocationFailed)?
                .1
                .live_bytes()),
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native { allocation, .. } => Ok(allocation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .bytes()),
        }
    }
}

impl acyclic_fs::facade::FsWork for &PreparationIo {
    type Output<T, E> = Result<std::result::Result<T, E>>;
    type OwnedOutput<T> = Self::Output<T, FsError>;
    fn accounting<'a>(&'a self, cancellation: &'a CancellationToken) -> KernelAccounting<'a> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = self.original_native_work() {
            return KernelAccounting::NativeOriginal { work, cancellation };
        }
        KernelAccounting::standalone(
            self.remaining()
                .unwrap_or_else(|_| acyclic_fs::performance::work_budget(0, 0)),
            cancellation,
        )
    }
    fn finish<T, E>(
        &self,
        result: std::result::Result<T, E>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::Output<T, E> {
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(result);
        }
        let recorded = self.spent(accounting.spent()).and_then(|()| {
            if result.is_ok() {
                self.remaining().map(|_| ())
            } else {
                Ok(())
            }
        });
        recorded.map(|()| result)
    }
    fn finish_owned<T>(
        &self,
        result: std::result::Result<(T, KernelOwnedAllocation), FsError>,
        accounting: &KernelAccounting<'_>,
    ) -> Self::OwnedOutput<T> {
        let result = result.and_then(|(value, allocation)| {
            #[cfg(not(target_arch = "wasm32"))]
            if let KernelOwnedAllocation::NativeOriginal(mut incoming) = allocation {
                let PreparationIo::Native { allocation, .. } = self else {
                    drop(value);
                    return Err(acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into());
                };
                if let Err(error) = allocation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .absorb(&mut incoming)
                {
                    drop(value);
                    return Err(error.into());
                }
            }
            Ok(value)
        });
        self.finish(result, accounting)
    }
}
