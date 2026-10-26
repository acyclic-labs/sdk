//! Owned custody at asynchronous boundaries, using the original ledger.
use super::*;
use std::sync::{Arc, Mutex};
mod shared;
pub(crate) use shared::{RetainedArc, RetainedWeak};

/// A handoff keeps the same counters, budget and live-byte owner. The caller
/// must stop charging this accounting branch until its completion is returned.
pub(crate) struct OwnedAccounting {
    state: OwnedAccountingState,
    cancellation: crate::CancellationToken,
}

/// Private operation lifetime retained by an owned authority continuation.
/// Request cancellation owns reply delivery; this token owns sealed durability.
/// Its allocation stays on the original ledger through its last native alias.
#[derive(Clone)]
pub(crate) struct AuthorityLifetime {
    cancellation: crate::CancellationToken,
}

impl AuthorityLifetime {
    pub(crate) fn admit(accounting: &mut KernelAccounting<'_>) -> Result<Self, crate::FsError> {
        accounting.check()?;
        Ok(Self {
            cancellation: crate::CancellationToken::for_authority(accounting)?,
        })
    }

    pub(crate) fn cancellation(&self) -> &crate::CancellationToken {
        &self.cancellation
    }
}

enum OwnedAccountingState {
    Standalone(Retained<Box<StandaloneAccounting>>),
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeMaterializationWork),
}

struct StandaloneAccounting {
    allocations: AllocationLedger,
    work: WorkCounters,
    budget: WorkBudget,
    baseline: WorkCounters,
}

impl OwnedAccounting {
    pub(crate) fn cancellation(&self) -> crate::CancellationToken {
        self.cancellation.clone()
    }
    pub(crate) fn take_branch(
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, crate::FsError> {
        accounting.check()?;
        cancellation.check()?;
        let lifetime = AuthorityLifetime::admit(accounting)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = accounting.native_work() {
            return Ok(Self {
                state: OwnedAccountingState::Native(work.authority_publication_view(&lifetime)?),
                cancellation: lifetime.cancellation().clone(),
            });
        }
        let allocation = RetainedAllocation::claim(
            std::mem::size_of::<StandaloneAccounting>() as u64,
            1,
            accounting,
        )?;
        let (allocations, work, budget, _) =
            accounting.standalone_parts().ok_or(WorkError::Overflow)?;
        let allocations = allocations.handoff(work, budget)?;
        let state = StandaloneAccounting {
            allocations,
            work: *work,
            budget,
            baseline: *work,
        };
        Ok(Self {
            state: OwnedAccountingState::Standalone(Retained {
                value: Box::new(state),
                allocation,
            }),
            cancellation: lifetime.cancellation().clone(),
        })
    }

    pub(crate) fn borrow(&mut self) -> KernelAccounting<'_> {
        match &mut self.state {
            OwnedAccountingState::Standalone(state) => {
                let StandaloneAccounting {
                    allocations,
                    work,
                    budget,
                    ..
                } = &mut *state.value;
                KernelAccounting::borrowed_standalone(
                    allocations,
                    work,
                    *budget,
                    &self.cancellation,
                )
            }
            #[cfg(not(target_arch = "wasm32"))]
            OwnedAccountingState::Native(work) => {
                KernelAccounting::native(work, &self.cancellation)
            }
        }
    }

    pub(crate) fn restore(
        self,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), crate::FsError> {
        match self.state {
            OwnedAccountingState::Standalone(state) => {
                let Retained { value, allocation } = state;
                let StandaloneAccounting {
                    allocations,
                    work,
                    budget,
                    baseline,
                } = *value;
                drop(allocation);
                let (caller_ledger, caller_work, caller_budget, _) = accounting
                    .standalone_parts()
                    .ok_or(crate::FsError::AccountingHandoffActive)?;
                if caller_budget != budget || *caller_work != baseline {
                    return Err(crate::FsError::AccountingHandoffActive);
                }
                caller_ledger.resume(allocations)?;
                *caller_work = work;
                caller_work.verify(budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            OwnedAccountingState::Native(work) => {
                if !accounting
                    .native_work()
                    .is_some_and(|caller| caller.shares_ledger(&work))
                {
                    return Err(crate::FsError::AccountingHandoffActive);
                }
                Ok(())
            }
        }
    }
}

pub(crate) enum RetainedAllocation {
    Standalone {
        _lease: StandaloneLease,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Native {
        _allocation: crate::NativeAllocation,
    },
}

pub(crate) struct StandaloneLease {
    ledger: Arc<Mutex<SharedAllocationBytes>>,
    bytes: u64,
}

impl Drop for StandaloneLease {
    fn drop(&mut self) {
        let mut state = self
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.release_retained(self.bytes).unwrap_or_else(|_| {
            unreachable!("a retained lease releases only its own original credit")
        });
    }
}

impl RetainedAllocation {
    pub(super) fn standalone(ledger: Arc<Mutex<SharedAllocationBytes>>, bytes: u64) -> Self {
        Self::Standalone {
            _lease: StandaloneLease { ledger, bytes },
        }
    }

    pub(crate) fn claim(
        bytes: u64,
        operations: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        let mut allocation = accounting.claim_owned(bytes, operations)?;
        match Self::transfer(&mut allocation, bytes, accounting) {
            Ok(lease) => Ok(lease),
            Err(error) => {
                accounting.release_owned(&mut allocation, bytes)?;
                Err(error)
            }
        }
    }

    fn transfer(
        allocation: &mut KernelOwnedAllocation,
        bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        match allocation {
            KernelOwnedAllocation::Standalone => {
                let (ledger, work, budget, _) =
                    accounting.standalone_parts().ok_or(WorkError::Overflow)?;
                ledger.retain(bytes, work, budget)
            }
            #[cfg(not(target_arch = "wasm32"))]
            KernelOwnedAllocation::NativeOriginal(_) => {
                let KernelOwnedAllocation::NativeOriginal(guard) =
                    std::mem::replace(allocation, KernelOwnedAllocation::Standalone)
                else {
                    unreachable!("matched native allocation")
                };
                Ok(Self::Native { _allocation: guard })
            }
        }
    }
}

/// Value fields are destroyed before the lease that funds their backing.
pub(crate) struct Retained<T> {
    pub(crate) value: T,
    pub(crate) allocation: RetainedAllocation,
}

impl<T> Retained<T> {
    pub(crate) fn from_owned(
        mut owned: crate::kernel::Owned<T>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        match RetainedAllocation::transfer(&mut owned.allocation, owned.bytes, accounting) {
            Ok(allocation) => Ok(Self {
                value: owned.value,
                allocation,
            }),
            Err(error) => {
                owned.retire(accounting)?;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn owned_accounting_handoff_fences_the_caller_and_returns_the_same_budget()
    -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        let mut caller = KernelAccounting::standalone(
            WorkBudget {
                items_examined: 2,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancel,
        );
        caller.charge_items(1)?;
        let mut owned = OwnedAccounting::take_branch(&mut caller, &cancel)?;
        cancel.cancel();
        let baseline = caller.spent();
        assert!(matches!(
            caller.charge_items(1),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        assert!(matches!(
            caller.admit(WorkCounters::UNCHARGED),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        assert!(matches!(
            caller.claim_owned(1, 1),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        assert!(matches!(
            caller.remaining(),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        assert_eq!(caller.spent(), baseline);
        owned.borrow().charge_items(1)?;
        assert!(
            owned.borrow().charge_items(1).is_err(),
            "handoff granted a fresh item budget"
        );
        owned.restore(&mut caller)?;
        assert_eq!(caller.spent().items_examined, 2);
        assert!(
            caller.check().is_err(),
            "restoring accounting changed caller cancellation"
        );
        Ok(())
    }

    #[test]
    fn owned_accounting_return_cannot_activate_another_callers_ledger() -> Result<(), crate::FsError>
    {
        let cancel = crate::CancellationToken::new();
        let mut first = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let mut second = KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let first_owned = OwnedAccounting::take_branch(&mut first, &cancel)?;
        let second_owned = OwnedAccounting::take_branch(&mut second, &cancel)?;
        assert!(matches!(
            first_owned.restore(&mut second),
            Err(crate::FsError::AccountingHandoffActive)
        ));
        assert!(first.remaining().is_err());
        assert!(second.remaining().is_err());
        second_owned.restore(&mut second)?;
        assert!(second.remaining().is_ok());
        Ok(())
    }

    struct Observe {
        ledger: Arc<Mutex<SharedAllocationBytes>>,
        minimum: u64,
        observed: Arc<AtomicBool>,
    }

    impl Drop for Observe {
        fn drop(&mut self) {
            self.observed.store(
                self.ledger
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .allocations
                    .live_bytes
                    >= self.minimum,
                Ordering::SeqCst,
            );
        }
    }

    #[test]
    fn retained_standalone_value_keeps_original_credit_after_caller_drop()
    -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let allocation = RetainedAllocation::claim(73, 1, &mut accounting)?;
        let ledger = match &allocation {
            RetainedAllocation::Standalone { _lease: lease } => Arc::clone(&lease.ledger),
            #[cfg(not(target_arch = "wasm32"))]
            RetainedAllocation::Native { .. } => unreachable!("ordinary accounting"),
        };
        let minimum = ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .allocations
            .live_bytes;
        let observed = Arc::new(AtomicBool::new(false));
        let retained = Retained {
            value: Observe {
                ledger: Arc::clone(&ledger),
                minimum,
                observed: Arc::clone(&observed),
            },
            allocation,
        };
        drop(accounting);
        assert_eq!(
            ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .allocations
                .live_bytes,
            minimum
        );
        drop(retained);
        assert!(
            observed.load(Ordering::SeqCst),
            "value outlived its original funding"
        );
        assert_eq!(
            ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .allocations
                .live_bytes,
            minimum - 73
        );
        Ok(())
    }

    #[test]
    fn retained_standalone_lease_and_caller_share_one_live_peak() -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let first = RetainedAllocation::claim(73, 1, &mut accounting)?;
        let baseline = accounting
            .standalone_parts()
            .ok_or(WorkError::Overflow)?
            .0
            .live_bytes();
        let second = RetainedAllocation::claim(127, 1, &mut accounting)?;
        assert_eq!(accounting.spent().peak_allocation_bytes, baseline + 127);
        assert_eq!(
            accounting.spent().allocation_operations,
            3,
            "the ledger control is funded exactly once"
        );
        drop(first);
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or(WorkError::Overflow)?
                .0
                .live_bytes(),
            baseline + 127 - 73
        );
        drop(second);
        assert_eq!(
            accounting
                .standalone_parts()
                .ok_or(WorkError::Overflow)?
                .0
                .live_bytes(),
            baseline - 73
        );
        Ok(())
    }

    #[test]
    fn retained_standalone_control_is_funded_before_promotion() -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(
            WorkBudget {
                allocation_operations: 1,
                ..crate::performance::BOUNDARY_BUDGET
            },
            &cancel,
        );
        assert!(RetainedAllocation::claim(73, 1, &mut accounting).is_err());
        assert_eq!(accounting.spent().allocation_operations, 1);
        let ledger = accounting.standalone_parts().ok_or(WorkError::Overflow)?.0;
        assert_eq!(ledger.live_bytes(), 0);
        assert!(
            matches!(ledger.state, LedgerState::Inline(_)),
            "rejected control allocation promoted the ledger"
        );
        Ok(())
    }
}
