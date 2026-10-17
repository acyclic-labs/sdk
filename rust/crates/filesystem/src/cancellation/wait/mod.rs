use super::*;

pub(super) struct WaitMeter<'a> {
    pub(super) allocations: &'a mut crate::kernel::allocation::AllocationLedger,
    pub(super) work: &'a mut crate::WorkCounters,
    pub(super) budget: crate::WorkBudget,
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled<'a> {
    pub(super) token: CancellationToken,
    pub(super) waiter: Option<Arc<CancellationWaiter>>,
    pub(super) meter: Option<WaitMeter<'a>>,
    pub(super) waiter_bytes: u64,
}

#[derive(Debug, Error)]
pub(crate) enum CancellationWaitError {
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::NativeTreeMaterializationError),
}

impl Cancelled<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn detach_meter(mut self) -> Cancelled<'static> {
        self.meter.take();
        Cancelled {
            token: self.token.clone(),
            waiter: self.waiter.take(),
            meter: None,
            waiter_bytes: std::mem::take(&mut self.waiter_bytes),
        }
    }

    pub(super) fn register(
        &mut self,
        #[cfg(not(target_arch = "wasm32"))] original: Option<&crate::NativeMaterializationWork>,
        #[cfg(target_arch = "wasm32")] _original: Option<&()>,
    ) -> Result<(), CancellationWaitError> {
        if self.waiter.is_some() || self.token.is_cancelled() {
            return Ok(());
        }
        let mut head = self
            .token
            .state
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.token.is_cancelled() {
            return Ok(());
        }
        let bytes = std::mem::size_of::<CancellationWaiter>() as u64;
        #[cfg(not(target_arch = "wasm32"))]
        let (allocation, cleanup) = if let Some(original) = original {
            original.admit(crate::WorkCounters {
                allocation_operations: 1,
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            original.admit_retained(bytes)?;
            let cleanup = original.reserve_work(crate::WorkCounters {
                items_examined: 3,
                ..crate::WorkCounters::UNCHARGED
            })?;
            (
                Some(original.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?),
                Some(cleanup),
            )
        } else {
            (None, None)
        };
        if let Some(meter) = &mut self.meter {
            let peak = meter
                .allocations
                .live_bytes()
                .checked_add(bytes)
                .ok_or(crate::WorkError::Overflow)?;
            meter.work.admit(
                &crate::WorkCounters {
                    allocation_operations: 1,
                    peak_allocation_bytes: peak,
                    items_examined: 2,
                    ..crate::WorkCounters::UNCHARGED
                },
                &meter.budget,
            )?;
            meter
                .allocations
                .claim_bytes(bytes, 1, meter.work, meter.budget)
                .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            self.waiter_bytes = bytes;
        }
        let next = head.upgrade();
        let waiter = Arc::new(CancellationWaiter {
            links: Mutex::new(Some((Weak::new(), head.clone()))),
            #[cfg(not(target_arch = "wasm32"))]
            cleanup: parking_lot::Mutex::new(cleanup),
            #[cfg(not(target_arch = "wasm32"))]
            _native_allocation: allocation,
            ..CancellationWaiter::default()
        });
        if let Some(next) = &next {
            next.links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_mut()
                .expect("active head")
                .0 = Arc::downgrade(&waiter);
        }
        *head = Arc::downgrade(&waiter);
        self.waiter = Some(waiter);
        if let Some(meter) = &mut self.meter {
            meter.work.charge_items(1, &meter.budget)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = original {
            original.record_admitted(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
        }
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Cancelled<'static> {
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn cleanup_items(&self) -> u64 {
        u64::from(self.waiter.is_some())
    }

    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn retire_waker(&mut self) {
        let waker = self.waiter.as_ref().and_then(|waiter| {
            waiter
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        });
        drop(waker);
    }

    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn finish(
        mut self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<(), CancellationWaitError> {
        work.admit(
            &crate::WorkCounters {
                items_examined: self.cleanup_items(),
                ..crate::WorkCounters::UNCHARGED
            },
            &budget,
        )?;
        let future = Cancelled {
            token: self.token.clone(),
            waiter: self.waiter.take(),
            waiter_bytes: std::mem::take(&mut self.waiter_bytes),
            meter: Some(WaitMeter {
                allocations,
                work,
                budget,
            }),
        };
        drop(future);
        Ok(())
    }
}

impl Future for Cancelled<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        this.register(None)
            .expect("unbounded cancellation registration");
        let Some(waiter) = &this.waiter else {
            return Poll::Ready(());
        };
        let replace = !waiter
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()));
        if replace {
            // Waker clone/drop can reenter cancellation; no locks span them.
            let replacement = context.waker().clone();
            let previous = waiter
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .replace(replacement);
            drop(previous);
        }
        if this.token.is_cancelled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        {
            let mut head = self
                .token
                .state
                .waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some((previous, next)) = waiter
                .links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                if let Some(previous) = previous.upgrade() {
                    previous
                        .links
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .as_mut()
                        .expect("active predecessor")
                        .1 = next.clone();
                } else {
                    *head = next.clone();
                }
                if let Some(next) = next.upgrade() {
                    next.links
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .as_mut()
                        .expect("active successor")
                        .0 = previous;
                }
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(credit) = &mut *waiter.cleanup.lock() {
            credit.record_deferred(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            });
        }
        let waker = waiter
            .waker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(waiter);
        if let Some(meter) = &mut self.meter {
            meter
                .work
                .charge_items(1, &meter.budget)
                .expect("admitted unregister");
            meter
                .allocations
                .release(self.waiter_bytes)
                .expect("owned cancellation waiter");
        }
        if let Err(panic) = catch_unwind(AssertUnwindSafe(|| drop(waker))) {
            std::panic::resume_unwind(panic);
        }
    }
}
