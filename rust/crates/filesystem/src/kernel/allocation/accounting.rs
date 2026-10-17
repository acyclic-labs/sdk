use super::*;

impl<'a> KernelAccounting<'a> {
    pub(crate) fn standalone(
        budget: WorkBudget,
        cancellation: &'a crate::CancellationToken,
    ) -> Self {
        Self::Standalone {
            allocations: AllocationLedger::default(),
            work: WorkCounters::UNCHARGED,
            budget,
            cancellation,
        }
    }

    /// Borrows the caller's accounting state.
    #[inline]
    pub fn borrowed_standalone(
        allocations: &'a mut AllocationLedger,
        work: &'a mut WorkCounters,
        budget: WorkBudget,
        cancellation: &'a crate::CancellationToken,
    ) -> Self {
        Self::BorrowedStandalone {
            allocations,
            work,
            budget,
            cancellation,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native(
        work: &'a crate::NativeMaterializationWork,
        cancellation: &'a crate::CancellationToken,
    ) -> Self {
        Self::NativeOriginal { work, cancellation }
    }

    #[inline]
    pub(crate) fn check(&self) -> Result<(), crate::FsError> {
        match self {
            Self::Standalone { cancellation, .. }
            | Self::BorrowedStandalone { cancellation, .. } => {
                cancellation.check().map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, cancellation } => {
                work.admit(WorkCounters::UNCHARGED)?;
                cancellation.check()?;
                Ok(())
            }
        }
    }

    pub(crate) fn with_allocations<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, crate::FsError>,
    ) -> Result<T, crate::FsError> {
        self.check()?;
        operation(self)
    }

    /// Borrows standalone allocation and work state.
    #[inline]
    pub fn standalone_parts(
        &mut self,
    ) -> Option<(
        &mut AllocationLedger,
        &mut WorkCounters,
        WorkBudget,
        &crate::CancellationToken,
    )> {
        match self {
            Self::Standalone {
                allocations,
                work,
                budget,
                cancellation,
            } => Some((allocations, work, *budget, cancellation)),
            Self::BorrowedStandalone {
                allocations,
                work,
                budget,
                cancellation,
            } => Some((allocations, work, *budget, cancellation)),
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { .. } => None,
        }
    }

    /// Claims owned storage before allocation.
    pub fn claim_owned(
        &mut self,
        bytes: u64,
        operations: u64,
    ) -> Result<KernelOwnedAllocation, crate::FsError> {
        self.check()?;
        match self {
            Self::Standalone {
                allocations,
                work,
                budget,
                ..
            } => {
                allocations
                    .claim_bytes(bytes, operations, work, *budget)
                    .map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                Ok(KernelOwnedAllocation::Standalone)
            }
            Self::BorrowedStandalone {
                allocations,
                work,
                budget,
                ..
            } => {
                allocations
                    .claim_bytes(bytes, operations, work, *budget)
                    .map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                Ok(KernelOwnedAllocation::Standalone)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, .. } => work
                .claim(bytes, operations, crate::WorkCounters::UNCHARGED)
                .map(KernelOwnedAllocation::NativeOriginal)
                .map_err(Into::into),
        }
    }

    pub(crate) fn record_actual_owned(
        &mut self,
        bytes: u64,
    ) -> Result<(KernelOwnedAllocation, Result<(), crate::FsError>), crate::FsError> {
        match self {
            Self::Standalone {
                allocations,
                work,
                budget,
                cancellation,
            } => {
                allocations
                    .record_owned_bytes(bytes, work)
                    .map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                let verified = work.verify(*budget).map_err(crate::FsError::from);
                Ok((
                    KernelOwnedAllocation::Standalone,
                    verified.and_then(|()| cancellation.check().map_err(Into::into)),
                ))
            }
            Self::BorrowedStandalone {
                allocations,
                work,
                budget,
                cancellation,
            } => {
                allocations
                    .record_owned_bytes(bytes, work)
                    .map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                let verified = work.verify(*budget).map_err(crate::FsError::from);
                Ok((
                    KernelOwnedAllocation::Standalone,
                    verified.and_then(|()| cancellation.check().map_err(Into::into)),
                ))
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, cancellation } => {
                let (allocation, result) = work.record_completed_allocation(bytes);
                Ok((
                    KernelOwnedAllocation::NativeOriginal(allocation),
                    result
                        .map_err(crate::FsError::from)
                        .and_then(|()| cancellation.check().map_err(Into::into)),
                ))
            }
        }
    }

    pub(crate) fn reconcile_owned(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        requested: u64,
        actual: u64,
    ) -> Result<(), crate::FsError> {
        match (self, allocation) {
            (
                Self::Standalone {
                    allocations,
                    work,
                    budget,
                    cancellation,
                },
                KernelOwnedAllocation::Standalone,
            ) => {
                if actual >= requested {
                    allocations
                        .record_owned_bytes(actual - requested, work)
                        .map_err(|error| {
                            error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                        })?;
                } else {
                    allocations.release(requested - actual).map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                }
                work.verify(*budget)?;
                cancellation.check().map_err(Into::into)
            }
            (
                Self::BorrowedStandalone {
                    allocations,
                    work,
                    budget,
                    cancellation,
                },
                KernelOwnedAllocation::Standalone,
            ) => {
                if actual >= requested {
                    allocations
                        .record_owned_bytes(actual - requested, work)
                        .map_err(|error| {
                            error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                        })?;
                } else {
                    allocations.release(requested - actual).map_err(|error| {
                        error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                    })?;
                }
                work.verify(*budget)?;
                cancellation.check().map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            (
                Self::NativeOriginal { work, cancellation },
                KernelOwnedAllocation::NativeOriginal(allocation),
            ) => {
                if !allocation.work().shares_ledger(work) {
                    return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                }
                let actual_owned = allocation
                    .bytes()
                    .checked_sub(requested)
                    .and_then(|other| other.checked_add(actual))
                    .ok_or(WorkError::Overflow)?;
                allocation.reconcile_capacity(actual_owned)?;
                cancellation.check().map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            _ => Err(AllocationError::ReleaseInvariant
                .into_work_or(crate::FsError::PendingMutationAllocationFailed)),
        }
    }

    /// Releases previously claimed owned storage.
    pub fn release_owned(
        &mut self,
        allocation: &mut KernelOwnedAllocation,
        bytes: u64,
    ) -> Result<(), crate::FsError> {
        match (self, allocation) {
            (Self::Standalone { allocations, .. }, KernelOwnedAllocation::Standalone) => {
                allocations.release(bytes).map_err(|error| {
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                })
            }
            (Self::BorrowedStandalone { allocations, .. }, KernelOwnedAllocation::Standalone) => {
                allocations.release(bytes).map_err(|error| {
                    error.into_work_or(crate::FsError::PendingMutationAllocationFailed)
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            (
                Self::NativeOriginal { work, .. },
                KernelOwnedAllocation::NativeOriginal(allocation),
            ) => {
                if !allocation.work().shares_ledger(work) {
                    return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
                }
                allocation.release(bytes).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            _ => Err(AllocationError::ReleaseInvariant
                .into_work_or(crate::FsError::PendingMutationAllocationFailed)),
        }
    }

    pub(crate) fn record_completed(&mut self, delta: WorkCounters) -> Result<(), crate::FsError> {
        match self {
            Self::Standalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                work.try_add_assign(&delta)?;
                work.verify(*budget)?;
                cancellation.check().map_err(Into::into)
            }
            Self::BorrowedStandalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                work.try_add_assign(&delta)?;
                work.verify(*budget)?;
                cancellation.check().map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, cancellation } => {
                work.record_admitted(delta)?;
                work.admit(WorkCounters::UNCHARGED)?;
                cancellation.check().map_err(Into::into)
            }
        }
    }

    /// Records and checks bounded work.
    #[inline]
    pub fn charge(&mut self, delta: WorkCounters) -> Result<(), crate::FsError> {
        #[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
        charge_tests::redundant_allocation();
        match self {
            Self::Standalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                cancellation.check()?;
                work.charge(&delta, budget).map_err(Into::into)
            }
            Self::BorrowedStandalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                cancellation.check()?;
                work.charge(&delta, budget).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, cancellation } => {
                #[cfg(all(test, acyclic_fs_loom))]
                if charge_model::split_charge() {
                    work.admit(WorkCounters::UNCHARGED)?;
                    cancellation.check()?;
                    work.admit(delta)?;
                    loom::thread::yield_now();
                    return work.record_admitted(delta).map_err(Into::into);
                }
                work.state(|state| {
                    // Preserve ledger failure, caller cancellation, proposed
                    // work failure priority while keeping the leaf held.
                    let native = crate::materializer::native_materialization_work_error;
                    state.admit(WorkCounters::UNCHARGED).map_err(native)?;
                    cancellation.check()?;
                    state.charge(delta).map_err(native).map_err(Into::into)
                })
            }
        }
    }

    /// Checks work before performing it.
    #[inline]
    pub fn admit(&self, delta: WorkCounters) -> Result<(), crate::FsError> {
        match self {
            Self::Standalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                cancellation.check()?;
                work.admit(&delta, budget).map_err(Into::into)
            }
            Self::BorrowedStandalone {
                work,
                budget,
                cancellation,
                ..
            } => {
                cancellation.check()?;
                work.admit(&delta, budget).map_err(Into::into)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, cancellation } => work.state(|state| {
                // Keep existing ledger failures before caller cancellation,
                // and caller cancellation before the proposed work's limit.
                let native = crate::materializer::native_materialization_work_error;
                state.admit(WorkCounters::UNCHARGED).map_err(native)?;
                cancellation.check()?;
                state.admit(delta).map_err(native).map_err(Into::into)
            }),
        }
    }

    /// Returns the remaining bounded allowances.
    pub fn remaining(&self) -> Result<WorkBudget, crate::FsError> {
        self.check()?;
        match self {
            Self::Standalone {
                work,
                allocations,
                budget,
                ..
            } => {
                let mut remaining = work.remaining(*budget)?;
                remaining.peak_allocation_bytes = remaining
                    .peak_allocation_bytes
                    .checked_sub(allocations.live_bytes())
                    .ok_or(WorkError::Overflow)?;
                Ok(remaining)
            }
            Self::BorrowedStandalone {
                work,
                allocations,
                budget,
                ..
            } => {
                let mut remaining = work.remaining(*budget)?;
                remaining.peak_allocation_bytes = remaining
                    .peak_allocation_bytes
                    .checked_sub(allocations.live_bytes())
                    .ok_or(WorkError::Overflow)?;
                Ok(remaining)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, .. } => work.remaining().map_err(Into::into),
        }
    }

    pub(crate) fn spent(&self) -> WorkCounters {
        match self {
            Self::Standalone { work, .. } => *work,
            Self::BorrowedStandalone { work, .. } => **work,
            #[cfg(not(target_arch = "wasm32"))]
            Self::NativeOriginal { work, .. } => work.receipt(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn native_work(&self) -> Option<&'a crate::NativeMaterializationWork> {
        match self {
            Self::Standalone { .. } | Self::BorrowedStandalone { .. } => None,
            Self::NativeOriginal { work, .. } => Some(*work),
        }
    }
}

#[cfg(test)]
#[path = "accounting_tests.rs"]
mod tests;

#[cfg(all(test, not(target_arch = "wasm32"), not(acyclic_fs_loom)))]
mod charge_tests;

#[cfg(all(test, acyclic_fs_loom))]
mod charge_model;
