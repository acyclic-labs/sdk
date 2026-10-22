use super::{EncodeAllocationStep, EncodeWorkStep, usize_to_u64};
use crate::kernel::allocation;

pub(crate) struct EncodeAccounting<'a, 'root> {
    accounting: &'a mut allocation::KernelAccounting<'root>,
    owned_bytes: u64,
    allocation: Option<allocation::KernelOwnedAllocation>,
    #[cfg(not(target_arch = "wasm32"))]
    credit: Option<allocation::effects::EffectCredit<'root>>,
}

impl<'a, 'root> EncodeAccounting<'a, 'root> {
    pub(crate) fn new(accounting: &'a mut allocation::KernelAccounting<'root>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let credit = allocation::effects::EffectCredit::for_accounting(accounting);
        Self {
            accounting,
            owned_bytes: 0,
            allocation: None,
            #[cfg(not(target_arch = "wasm32"))]
            credit,
        }
    }

    pub(crate) fn observe(&mut self, step: &EncodeWorkStep) -> Result<(), crate::FsError> {
        #[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
        model_checking::completed_effect(step);
        match step {
            EncodeWorkStep::Check => self.accounting.check(),
            EncodeWorkStep::Admit(delta) => self.admit(*delta),
            EncodeWorkStep::Completed(delta) => self.record_completed(*delta),
            EncodeWorkStep::Allocation(EncodeAllocationStep::Reserve { capacity }) => {
                let requested = usize_to_u64(*capacity);
                let next_owned = self
                    .owned_bytes
                    .checked_add(requested)
                    .ok_or(crate::WorkError::Overflow)?;
                let mut allocation = self
                    .accounting
                    .claim_owned(requested, u64::from(*capacity != 0))?;
                self.owned_bytes = next_owned;
                if let Some(owned) = self.allocation.as_mut() {
                    owned.absorb(&mut allocation)?;
                } else {
                    self.allocation = Some(allocation);
                }
                Ok(())
            }
            EncodeWorkStep::Allocation(EncodeAllocationStep::Capacity {
                admitted_bytes,
                actual_bytes,
            }) => {
                self.owned_bytes = self
                    .owned_bytes
                    .checked_sub(*admitted_bytes)
                    .and_then(|other| other.checked_add(*actual_bytes))
                    .ok_or(crate::WorkError::Overflow)?;
                let allocation = self.allocation.as_mut().ok_or(crate::WorkError::Overflow)?;
                self.accounting
                    .reconcile_owned(allocation, *admitted_bytes, *actual_bytes)
            }
            EncodeWorkStep::Allocation(EncodeAllocationStep::BeforeHeader { bytes }) => {
                self.admit(crate::WorkCounters {
                    bytes_encoded: *bytes,
                    ..crate::WorkCounters::default()
                })
            }
            EncodeWorkStep::Allocation(EncodeAllocationStep::HeaderWritten { bytes }) => self
                .record_completed(crate::WorkCounters {
                    bytes_encoded: *bytes,
                    ..crate::WorkCounters::default()
                }),
        }
    }

    pub(crate) fn finish(
        mut self,
        result: Result<Vec<u8>, crate::FsError>,
    ) -> Result<(Vec<u8>, allocation::KernelOwnedAllocation), crate::FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        drop(self.credit.take());
        match result {
            Ok(bytes) => {
                if let Err(error) = self.accounting.check() {
                    drop(bytes);
                    self.retire()?;
                    return Err(error);
                }
                let Some(allocation) = self.allocation.take() else {
                    drop(bytes);
                    return Err(crate::WorkError::Overflow.into());
                };
                self.owned_bytes = 0;
                Ok((bytes, allocation))
            }
            Err(error) => {
                self.retire()?;
                Err(error)
            }
        }
    }

    fn admit(&mut self, delta: crate::WorkCounters) -> Result<(), crate::FsError> {
        #[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
        if allocation::effects::broken_reservation() {
            return self.accounting.admit(delta);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut self.credit {
            // Canonical writers announce a whole record before its header and
            // fields. Those nested checks cover the same outstanding effects;
            // only the deficit above existing credit needs another reservation.
            #[cfg(all(test, acyclic_fs_loom))]
            let reuse = !model_checking::additive_reservation();
            #[cfg(not(all(test, acyclic_fs_loom)))]
            let reuse = true;
            return credit.reserve(delta, reuse);
        }
        self.accounting.admit(delta)
    }

    fn record_completed(&mut self, delta: crate::WorkCounters) -> Result<(), crate::FsError> {
        #[cfg(all(test, acyclic_fs_loom, not(target_arch = "wasm32")))]
        if allocation::effects::broken_reservation() {
            return self.accounting.record_completed(delta);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut self.credit {
            return credit.complete(delta);
        }
        self.accounting.record_completed(delta)
    }

    fn retire(&mut self) -> Result<(), crate::FsError> {
        let Some(allocation) = self.allocation.take() else {
            return Ok(());
        };
        let bytes = std::mem::take(&mut self.owned_bytes);
        match allocation {
            allocation::KernelOwnedAllocation::Standalone => match self.accounting {
                allocation::KernelAccounting::Standalone { allocations, .. } => {
                    allocations.release(bytes)
                }
                allocation::KernelAccounting::BorrowedStandalone { allocations, .. } => {
                    allocations.release(bytes)
                }
                #[cfg(not(target_arch = "wasm32"))]
                allocation::KernelAccounting::NativeOriginal { .. } => {
                    return Err(crate::WorkError::Overflow.into());
                }
            }
            .map_err(|error| error.into_work_or(crate::FsError::PendingMutationAllocationFailed)),
            #[cfg(not(target_arch = "wasm32"))]
            allocation::KernelOwnedAllocation::NativeOriginal(allocation) => {
                drop(allocation);
                Ok(())
            }
        }
    }
}

impl Drop for EncodeAccounting<'_, '_> {
    fn drop(&mut self) {
        #[cfg(not(target_arch = "wasm32"))]
        drop(self.credit.take());
        let retired = self.retire();
        debug_assert!(
            retired.is_ok(),
            "canonical encoder funding must retire its exact live capacity"
        );
    }
}

#[cfg(test)]
#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
mod model_checking;
#[cfg(test)]
#[cfg(not(acyclic_fs_loom))]
mod tests;
