use super::*;

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled<'a> {
    pub(in crate::cancellation) token: CancellationToken,
    pub(in crate::cancellation) waiter: Option<Arc<CancellationWaiter>>,
    pub(in crate::cancellation) registered: Option<usize>,
    pub(in crate::cancellation) meter: Option<WaitMeter<'a>>,
    pub(in crate::cancellation) waiter_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::cancellation) native_original: Option<&'a crate::NativeMaterializationWork>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::cancellation) native_cleanup: Option<crate::materializer::NativeWorkReservation>,
}

#[derive(Debug, Error)]
pub(crate) enum CancellationWaitError {
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    #[error("cancellation waiter allocation failed")]
    AllocationFailed,
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::NativeTreeMaterializationError),
}

impl Cancelled<'static> {
    pub(crate) fn cleanup_items(&self) -> u64 {
        u64::from(self.registered.is_some())
    }
    pub(crate) fn retained_waiter_bytes(&self) -> u64 {
        self.waiter_bytes
    }
    pub(crate) fn same_token(&self, token: &CancellationToken) -> bool {
        self.token.same_state(token)
    }

    /// Breaks terminal-task waker ownership before its meter is reclaimed.
    /// Call outside that meter's lock. Registration and reserved cleanup remain
    /// owned by this waiter until `finish`; no allocation claim is released here.
    pub(crate) fn retire_waker(&mut self) {
        let waker = self.waiter.as_ref().and_then(|waiter| {
            waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        drop(waker);
    }

    pub(crate) fn finish(
        mut self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        admission: &mut CancellationWaitAdmission,
    ) -> Result<(), CancellationWaitError> {
        work.admit(
            &crate::WorkCounters {
                items_examined: self.cleanup_items(),
                ..crate::WorkCounters::default()
            },
            &budget,
        )?;
        let future = Cancelled {
            token: self.token.clone(),
            waiter: self.waiter.take(),
            registered: self.registered.take(),
            waiter_bytes: std::mem::take(&mut self.waiter_bytes),
            meter: Some(WaitMeter {
                allocations,
                work,
                budget,
                admission,
                temporary_registry_bytes: 0,
                pending_registry_bytes: 0,
                cleanup_reserved: true,
            }),
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: self.native_cleanup.take(),
        };
        drop(future);
        Ok(())
    }
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut().poll_result(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => panic!("unbounded cancellation wait failed: {error}"),
        }
    }
}

impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        if self.waiter.is_none() && self.registered.is_none() && self.meter.is_none() {
            return;
        }
        let mut waiters = self
            .token
            .state
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(not(target_arch = "wasm32"))]
        let mut retired_backing = None;
        #[cfg(not(target_arch = "wasm32"))]
        let mut retired_lease = None;
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if let Some(index) = self.registered {
            if let Some(meter) = &mut self.meter {
                meter
                    .work
                    .charge(
                        &crate::WorkCounters {
                            items_examined: 1,
                            ..crate::WorkCounters::default()
                        },
                        &meter.budget,
                    )
                    .expect("cancellation cleanup allowance was reserved");
            }
            if let (Some(slot), Some(waiter)) = (waiters.get_mut(index), &self.waiter)
                && slot
                    .as_ref()
                    .is_some_and(|slot| Weak::ptr_eq(slot, &Arc::downgrade(waiter)))
            {
                *slot = None;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = waiter
                    .registry_lease
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    && lease.registered.fetch_sub(1, Ordering::AcqRel) == 1
                {
                    retired_backing = Some(std::mem::take(&mut *waiters));
                    self.token
                        .state
                        .registry_generation
                        .fetch_add(1, Ordering::AcqRel);
                    retired_lease = Some(lease.clone());
                }
            }
        }
        drop(waiters);
        #[cfg(not(target_arch = "wasm32"))]
        {
            drop(retired_backing);
            if let Some(lease) = retired_lease {
                lease.retire_backing_deferred();
            }
            if self.registered.is_some()
                && let Some(credit) = &mut self.native_cleanup
            {
                credit.record_deferred(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                });
            }
        }
        let waker = self.waiter.take().and_then(|waiter| {
            let waker = waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(waiter);
            waker
        });
        if let Some(meter) = &mut self.meter {
            meter.release(self.waiter_bytes);
            self.waiter_bytes = 0;
            let temporary = std::mem::take(&mut meter.temporary_registry_bytes);
            let pending = std::mem::take(&mut meter.pending_registry_bytes);
            meter.release(temporary);
            meter.release(pending);
        }
        // A waker's destructor may cancel this very token. No registry/waker
        // lock is held, and its newly freed backing must join the same receipt.
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(waker)));
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if let Err(panic) = dropped {
            std::panic::resume_unwind(panic);
        }
    }
}

mod poll;
