use super::*;

impl<'a, 'operation> OriginalReadAccounting<'a, 'operation> {
    pub(crate) fn new(accounting: &'a mut KernelAccounting<'operation>) -> Result<Self, FsError> {
        let allocation = accounting.claim_owned(0, 0)?;
        #[cfg(not(target_arch = "wasm32"))]
        let credit = crate::kernel::allocation::effects::EffectCredit::for_accounting(accounting);
        Ok(Self {
            accounting,
            allocation,
            owned_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            credit,
        })
    }

    pub(crate) fn observe(
        &mut self,
        step: super::super::codec::ReadAllocationStep,
    ) -> Result<(), FsError> {
        use super::super::codec::ReadAllocationStep;
        match step {
            ReadAllocationStep::Allocate { bytes, operations } => {
                let next = self
                    .owned_bytes
                    .checked_add(bytes)
                    .ok_or(WorkError::Overflow)?;
                let mut allocation = self.accounting.claim_owned(bytes, operations)?;
                self.allocation.absorb(&mut allocation)?;
                self.owned_bytes = next;
            }
            ReadAllocationStep::ReserveFailed(_error) => {
                #[cfg(not(target_arch = "wasm32"))]
                if self.accounting.native_work().is_some() {
                    return Err(
                        crate::NativeTreeMaterializationError::AllocationFailed(_error).into(),
                    );
                }
                return Err(super::super::CanonicalDecodeError::AllocationFailed.into());
            }
            ReadAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes,
            } => {
                let next = self
                    .owned_bytes
                    .checked_sub(admitted_bytes)
                    .and_then(|bytes| bytes.checked_add(actual_bytes))
                    .ok_or(WorkError::Overflow)?;
                let reconciled = self.accounting.reconcile_owned(
                    &mut self.allocation,
                    admitted_bytes,
                    actual_bytes,
                );
                self.owned_bytes = next;
                reconciled?;
            }
            ReadAllocationStep::CheckCopy(bytes) => {
                self.accounting.admit(WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            ReadAllocationStep::BeforeCopy(bytes) => {
                self.admit(WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            ReadAllocationStep::Copied(bytes) => {
                self.record_completed(WorkCounters {
                    bytes_copied: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            ReadAllocationStep::BeforeHash(bytes) => {
                self.admit(WorkCounters {
                    bytes_hashed: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            ReadAllocationStep::Hashed(bytes) => {
                self.record_completed(WorkCounters {
                    bytes_hashed: bytes,
                    ..WorkCounters::UNCHARGED
                })?;
                self.accounting.check()?;
            }
            ReadAllocationStep::BeforeItem => {
                self.admit(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
            }
            ReadAllocationStep::Item => {
                self.record_completed(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn into_allocation(mut self) -> KernelOwnedAllocation {
        std::mem::replace(&mut self.allocation, KernelOwnedAllocation::Standalone)
    }

    pub(crate) fn into_parts(mut self) -> (KernelOwnedAllocation, u64) {
        (
            std::mem::replace(&mut self.allocation, KernelOwnedAllocation::Standalone),
            self.owned_bytes,
        )
    }

    pub(crate) fn admit(&mut self, delta: WorkCounters) -> Result<(), FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut self.credit {
            return credit.reserve(delta, false);
        }
        self.accounting.admit(delta)
    }

    pub(crate) fn record_completed(&mut self, delta: WorkCounters) -> Result<(), FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut self.credit {
            return credit.complete(delta);
        }
        self.accounting.record_completed(delta)
    }

    pub(crate) fn release_failed(mut self) -> Result<(), FsError> {
        self.accounting
            .release_owned(&mut self.allocation, self.owned_bytes)
    }
}

#[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
mod model_checking;

#[cfg(all(test, not(acyclic_fs_loom), not(target_arch = "wasm32")))]
mod tests;
