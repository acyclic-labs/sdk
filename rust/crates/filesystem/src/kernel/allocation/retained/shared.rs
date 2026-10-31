//! Arc backing stays funded until both strong and weak handles retire.
use super::*;

// Retirement takes the original token out of its last funding Arc before
// releasing it, so that Arc's own backing has also been deallocated.
pub(crate) struct RetainedArc<T> {
    value: Option<Arc<T>>,
    allocation: Option<Arc<RetainedAllocation>>,
}

/// Original backing admission, before its value is moved under shared custody.
pub(crate) struct RetainedArcAdmission<T> {
    allocation: RetainedAllocation,
    value: std::marker::PhantomData<T>,
}

impl<T> RetainedArcAdmission<T> {
    pub(crate) fn into_value(self, value: T) -> RetainedArc<T> {
        RetainedArc {
            value: Some(Arc::new(value)),
            allocation: Some(Arc::new(self.allocation)),
        }
    }
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
        Ok(Self::admit(accounting)?.into_value(value))
    }

    pub(crate) fn admit(
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<RetainedArcAdmission<T>, crate::FsError> {
        let value_bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<T>())
            .map_err(|_| WorkError::Overflow)?;
        let funding_bytes =
            crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<RetainedAllocation>())
                .map_err(|_| WorkError::Overflow)?;
        let bytes = value_bytes
            .checked_add(funding_bytes)
            .ok_or(WorkError::Overflow)?;
        let allocation = RetainedAllocation::claim(bytes as u64, 2, accounting)?;
        Ok(RetainedArcAdmission {
            allocation,
            value: std::marker::PhantomData,
        })
    }

    pub(crate) fn downgrade(&self) -> RetainedWeak<T> {
        RetainedWeak {
            value: match &self.value {
                Some(value) => Arc::downgrade(value),
                None => unreachable!("a retained value is present until retirement"),
            },
            allocation: self.allocation.clone(),
        }
    }
}

impl<T> Clone for RetainedArc<T> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            allocation: self.allocation.clone(),
        }
    }
}

impl<T> std::ops::Deref for RetainedArc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        match &self.value {
            Some(value) => value,
            None => unreachable!("a retained value is present until retirement"),
        }
    }
}

impl<T> Drop for RetainedArc<T> {
    fn drop(&mut self) {
        drop(self.value.take());
        drop(self.allocation.take().and_then(Arc::into_inner));
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
            value: Some(self.value.upgrade()?),
            allocation: Some(Arc::clone(self.allocation.as_ref()?)),
        })
    }
}

impl<T> Drop for RetainedWeak<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.value));
        drop(self.allocation.take().and_then(Arc::into_inner));
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
        let ledger = match value.allocation.as_deref() {
            Some(RetainedAllocation::Standalone { _lease }) => Arc::clone(&_lease.ledger),
            #[cfg(not(target_arch = "wasm32"))]
            Some(RetainedAllocation::Native { .. }) => unreachable!("standalone accounting"),
            None => unreachable!("funded retained value"),
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
