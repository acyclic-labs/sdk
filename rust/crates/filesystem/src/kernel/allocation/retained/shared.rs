//! Arc backing stays funded until both strong and weak handles retire.
use super::*;

// Fields drop in declaration order: release the value's backing before its
// funding, including when a failed upgrade retires the final weak handle.
pub(crate) struct RetainedArc<T> {
    value: Arc<T>,
    allocation: Arc<RetainedAllocation>,
}

pub(crate) struct RetainedWeak<T> {
    value: std::sync::Weak<T>,
    allocation: Option<Arc<RetainedAllocation>>,
}

impl<T> RetainedArc<T> {
    pub(crate) fn new(
        value: T,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, crate::FsError> {
        let value_bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<T>())
            .map_err(|_| WorkError::Overflow)?;
        let funding_bytes =
            crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<RetainedAllocation>())
                .map_err(|_| WorkError::Overflow)?;
        let bytes = value_bytes
            .checked_add(funding_bytes)
            .ok_or(WorkError::Overflow)?;
        let allocation = RetainedAllocation::claim(bytes as u64, 2, accounting)?;
        Ok(Self {
            value: Arc::new(value),
            allocation: Arc::new(allocation),
        })
    }

    pub(crate) fn downgrade(&self) -> RetainedWeak<T> {
        RetainedWeak {
            value: Arc::downgrade(&self.value),
            allocation: Some(Arc::clone(&self.allocation)),
        }
    }
}

impl<T> Clone for RetainedArc<T> {
    fn clone(&self) -> Self {
        Self {
            value: Arc::clone(&self.value),
            allocation: Arc::clone(&self.allocation),
        }
    }
}

impl<T> std::ops::Deref for RetainedArc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> Default for RetainedWeak<T> {
    fn default() -> Self {
        Self {
            value: std::sync::Weak::new(),
            allocation: None,
        }
    }
}

impl<T> RetainedWeak<T> {
    #[cfg(test)]
    pub(crate) fn strong_count(&self) -> usize {
        self.value.strong_count()
    }

    pub(crate) fn as_ptr(&self) -> *const T {
        self.value.as_ptr()
    }

    pub(crate) fn upgrade(&self) -> Option<RetainedArc<T>> {
        Some(RetainedArc {
            value: self.value.upgrade()?,
            allocation: Arc::clone(self.allocation.as_ref()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_arc_weak_backing_outlives_the_caller_and_value() -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::performance::BOUNDARY_BUDGET, &cancel);
        let value = RetainedArc::new(73_u64, &mut accounting)?;
        let ledger = match &*value.allocation {
            RetainedAllocation::Standalone { _lease } => Arc::clone(&_lease.ledger),
            #[cfg(not(target_arch = "wasm32"))]
            RetainedAllocation::Native { .. } => unreachable!("standalone accounting"),
        };
        let funded = ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .allocations
            .live_bytes;
        let weak = value.downgrade();
        let upgraded = weak.upgrade().ok_or(crate::FsError::InvalidDiff)?;
        assert_eq!(*upgraded, 73);
        drop(accounting);
        drop(value);
        assert_eq!(
            ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .allocations
                .live_bytes,
            funded
        );
        drop(upgraded);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .allocations
                .live_bytes,
            funded,
            "dead weak backing lost its original credit"
        );
        drop(weak);
        assert!(
            ledger
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .allocations
                .live_bytes
                < funded
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn retained_arc_every_strong_weak_retirement_order_keeps_original_native_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        for a in 0..4 {
            for b in 0..4 {
                for c in 0..4 {
                    for d in 0..4 {
                        let order = [a, b, c, d];
                        if (0..4).any(|i| order[..i].contains(&order[i])) {
                            continue;
                        }
                        let cancel = crate::CancellationToken::new();
                        let original = crate::NativeMaterializationWork::new(
                            crate::performance::BOUNDARY_BUDGET,
                            &cancel,
                        )
                        .map_err(|(error, _)| error)?;
                        let baseline = original.live_allocation_bytes();
                        let value = RetainedArc::new(
                            73_u64,
                            &mut KernelAccounting::native(&original, &cancel),
                        )?;
                        let funded = original.live_allocation_bytes();
                        assert!(funded > baseline);
                        let mut strong = [Some(value.clone()), Some(value)];
                        let mut weak = [
                            Some(
                                strong[0]
                                    .as_ref()
                                    .ok_or("missing first strong handle")?
                                    .downgrade(),
                            ),
                            Some(
                                strong[1]
                                    .as_ref()
                                    .ok_or("missing second strong handle")?
                                    .downgrade(),
                            ),
                        ];
                        for (step, index) in order.into_iter().enumerate() {
                            if index < 2 {
                                drop(strong[index].take());
                            } else {
                                drop(weak[index - 2].take());
                            }
                            for handle in weak.iter().flatten() {
                                assert_eq!(
                                    handle.upgrade().is_some(),
                                    strong.iter().any(Option::is_some)
                                );
                            }
                            assert_eq!(
                                original.live_allocation_bytes(),
                                if step == 3 { baseline } else { funded },
                                "funding mismatch for retirement order {order:?}, step {step}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn retained_arc_rejected_backing_admission_releases_all_credit() -> Result<(), crate::FsError> {
        let cancel = crate::CancellationToken::new();
        for operations in 0..3 {
            let mut accounting = KernelAccounting::standalone(
                WorkBudget {
                    allocation_operations: operations,
                    ..crate::performance::BOUNDARY_BUDGET
                },
                &cancel,
            );
            assert!(RetainedArc::new(73_u64, &mut accounting).is_err());
            assert_eq!(
                accounting
                    .standalone_parts()
                    .ok_or(WorkError::Overflow)?
                    .0
                    .live_bytes(),
                0
            );
        }
        assert!(RetainedWeak::<u64>::default().upgrade().is_none());
        Ok(())
    }
}
