//! Runtime-independent cooperative cancellation for native and browser work.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use thiserror::Error;

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Option<Weak<CancellationWaiter>>>>,
    registry_generation: AtomicU64,
}

#[derive(Default)]
struct CancellationWaiter {
    waker: Mutex<Option<Waker>>,
}

/// Cloneable cancellation source and observation token.
///
/// Cancellation is monotonic. It is an execution fact only and never durable
/// filesystem authority. Backends check it before externally visible work and
/// async waits wake promptly when cancellation wins a race.
#[derive(Clone, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

impl CancellationToken {
    /// Creates one live token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn new_measured(
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<Self, crate::WorkError> {
        allocations.claim_elements::<CancellationState>(1, work, budget)
            .map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
        Ok(Self::new())
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<CancellationState>() as u64
    }

    pub(crate) fn same_state(&self, other: &Self) -> bool { Arc::ptr_eq(&self.state, &other.state) }

    /// Atomically marks the token cancelled and wakes every registered waiter.
    pub fn cancel(&self) {
        self.cancel_inner(None).expect("unmetered cancellation cannot fail");
    }

    pub(crate) fn cancel_measured(
        &self, work: &mut crate::WorkCounters, budget: crate::WorkBudget,
    ) -> Result<(), crate::WorkError> {
        self.cancel_inner(Some((work, budget)))
    }

    /// Only the physical, non-callback commit belongs inside this closure.
    /// Cancellation and this commit use the original token's existing lock.
    #[cfg(feature = "native-mount")]
    pub(crate) fn commit_if_live<T>(
        &self, commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        let _serial = self.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.check()?;
        Ok(commit())
    }

    fn cancel_inner(
        &self, admission: Option<(&mut crate::WorkCounters, crate::WorkBudget)>,
    ) -> Result<(), crate::WorkError> {
        let waiters = {
            let mut waiters = self.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if self.state.cancelled.load(Ordering::Acquire) { return Ok(()); }
            if let Some((work, budget)) = admission {
                let delta = crate::WorkCounters {
                    items_examined: waiters.len() as u64, ..crate::WorkCounters::UNCHARGED
                };
                work.charge(&delta, &budget)?;
            }
            self.state.cancelled.store(true, Ordering::Release);
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters.into_iter().flatten() {
            let Some(waiter) = waiter.upgrade() else {
                continue;
            };
            let waker = match waiter.waker.lock() {
                Ok(mut waker) => waker.take(),
                Err(poisoned) => poisoned.into_inner().take(),
            };
            if let Some(waker) = waker {
                let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
            }
        }
        // The drained backing remains alive while arbitrary wake callbacks run.
        // Publish its release only after the iterator has actually dropped it.
        self.state.registry_generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Returns whether cancellation has occurred.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }

    /// Fails if cancellation has already occurred.
    ///
    /// # Errors
    ///
    /// Returns [`CancellationError`] after this token is cancelled.
    pub fn check(&self) -> Result<(), CancellationError> {
        if self.is_cancelled() {
            Err(CancellationError)
        } else {
            Ok(())
        }
    }

    /// Resolves once cancellation occurs.
    #[must_use]
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            waiter: None,
            registered: None,
            meter: None,
        }
    }

    /// Uses the invocation's existing ledger; construction and precancellation
    /// allocate nothing. The waiter and registry grow only when polled.
    pub(crate) fn cancelled_measured<'a>(
        &'a self,
        allocations: &'a mut crate::kernel::allocation::AllocationLedger,
        work: &'a mut crate::WorkCounters,
        budget: crate::WorkBudget,
        admission: &'a mut CancellationWaitAdmission,
    ) -> MeasuredCancelled<'a> {
        MeasuredCancelled(Cancelled {
            token: self,
            waiter: None,
            registered: None,
            meter: Some(WaitMeter {
                allocations, work, budget, admission, waiter_bytes: 0,
                temporary_registry_bytes: 0, pending_registry_bytes: 0, cleanup_reserved: false,
            }),
        })
    }

    /// Prepares the same registration while the worker's meter is still
    /// available. Polling this owned future never allocates. The caller reserves
    /// `cleanup_items()` until `finish` receives that same meter back.
    pub(crate) fn prepare_cancelled_measured(
        &self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        admission: &mut CancellationWaitAdmission,
    ) -> Result<PreparedCancelled, CancellationWaitError> {
        let mut future = self.cancelled_measured(allocations, work, budget, admission);
        let mut context = Context::from_waker(Waker::noop());
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }
        let waiter = future.0.waiter.take();
        if let Some(waiter) = &waiter {
            *waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
        let registered = future.0.registered.take();
        let meter = future.0.meter.take().expect("prepared cancellation meter");
        Ok(PreparedCancelled {
            token: self.clone(),
            waiter,
            registered,
            waiter_bytes: meter.waiter_bytes,
        })
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    waiter: Option<Arc<CancellationWaiter>>,
    registered: Option<usize>,
    meter: Option<WaitMeter<'a>>,
}

/// Only the backing allocation this invocation actually created. A generation
/// identifies replacements without allocator pointer-reuse ambiguity.
#[derive(Default)]
pub(crate) struct CancellationWaitAdmission {
    registry: Option<(u64, u64)>,
}

impl CancellationWaitAdmission {
    pub(crate) fn freed_registry_bytes(&self, token: &CancellationToken) -> u64 {
        self.registry.filter(|(generation, _)|
            *generation != token.state.registry_generation.load(Ordering::Acquire))
            .map_or(0, |(_, bytes)| bytes)
    }

    pub(crate) fn reconcile_registry(
        &mut self,
        token: &CancellationToken,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
    ) -> Result<(), crate::WorkError> {
        let bytes = self.freed_registry_bytes(token);
        if bytes != 0 {
            allocations.release(bytes).map_err(|error| error.into_work_or(crate::WorkError::Overflow))?;
            self.registry = None;
        }
        Ok(())
    }
}

struct WaitMeter<'a> {
    allocations: &'a mut crate::kernel::allocation::AllocationLedger,
    work: &'a mut crate::WorkCounters,
    budget: crate::WorkBudget,
    admission: &'a mut CancellationWaitAdmission,
    waiter_bytes: u64,
    temporary_registry_bytes: u64,
    pending_registry_bytes: u64,
    cleanup_reserved: bool,
}

#[derive(Debug, Error)]
pub(crate) enum CancellationWaitError {
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    #[error("cancellation waiter allocation failed")]
    AllocationFailed,
}

/// A registration already admitted by the worker's original invocation ledger.
pub(crate) struct PreparedCancelled {
    token: CancellationToken,
    waiter: Option<Arc<CancellationWaiter>>,
    registered: Option<usize>,
    waiter_bytes: u64,
}

impl PreparedCancelled {
    pub(crate) fn cleanup_items(&self) -> u64 { u64::from(self.registered.is_some()) }
    pub(crate) fn retained_waiter_bytes(&self) -> u64 { self.waiter_bytes }
    pub(crate) fn same_token(&self, token: &CancellationToken) -> bool { self.token.same_state(token) }

    /// Breaks terminal-task waker ownership before its meter is reclaimed.
    /// Call outside that meter's lock. Registration and reserved cleanup remain
    /// owned by this waiter until `finish`; no allocation claim is released here.
    pub(crate) fn retire_waker(&mut self) {
        let waker = self.waiter.as_ref().and_then(|waiter| {
            waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
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
        work.admit(&crate::WorkCounters { items_examined: self.cleanup_items(), ..crate::WorkCounters::default() }, &budget)?;
        let future = Cancelled {
            token: &self.token,
            waiter: self.waiter.take(),
            registered: self.registered.take(),
            meter: Some(WaitMeter {
                allocations, work, budget, admission,
                waiter_bytes: std::mem::take(&mut self.waiter_bytes),
                temporary_registry_bytes: 0,
                pending_registry_bytes: 0,
                cleanup_reserved: true,
            }),
        };
        drop(future);
        Ok(())
    }
}

impl Future for PreparedCancelled {
    type Output = ();
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut future = Cancelled {
            token: &this.token,
            waiter: this.waiter.take(),
            registered: this.registered.take(),
            meter: None,
        };
        let result = Pin::new(&mut future).poll(context);
        this.waiter = future.waiter.take();
        this.registered = future.registered.take();
        result
    }
}

impl Drop for PreparedCancelled {
    fn drop(&mut self) {
        drop(Cancelled {
            token: &self.token,
            waiter: self.waiter.take(),
            registered: self.registered.take(),
            meter: None,
        });
    }
}

impl WaitMeter<'_> {
    fn allowance(&self) -> Result<crate::WorkBudget, CancellationWaitError> {
        let mut budget = self.budget;
        if self.cleanup_reserved {
            budget.items_examined = budget.items_examined.checked_sub(1).ok_or(crate::WorkError::Overflow)?;
        }
        Ok(budget)
    }

    fn charge(&mut self, delta: crate::WorkCounters) -> Result<(), CancellationWaitError> {
        let budget = self.allowance()?;
        self.work.charge(&delta, &budget)?;
        Ok(())
    }

    fn claim(&mut self, bytes: u64, operations: u64) -> Result<(), CancellationWaitError> {
        let budget = self.allowance()?;
        self.allocations.claim_bytes(bytes, operations, self.work, budget)
            .map_err(|error| CancellationWaitError::Work(error.into_work_or(crate::WorkError::Overflow)))
    }

    fn release(&mut self, bytes: u64) {
        self.allocations.release(bytes).expect("cancellation allocation ownership invariant");
    }

    fn observe_registry(&mut self, generation: u64) {
        if let Some((owned_generation, bytes)) = self.admission.registry {
            if owned_generation != generation {
                self.release(bytes);
                self.admission.registry = None;
            }
        }
    }
}

/// Budget-aware projection of the same canonical cancellation wait engine.
pub(crate) struct MeasuredCancelled<'a>(Cancelled<'a>);

impl Future for MeasuredCancelled<'_> {
    type Output = Result<(), CancellationWaitError>;
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.poll_result(context)
    }
}

impl Cancelled<'_> {
    fn poll_result(&mut self, context: &mut Context<'_>) -> Poll<Result<(), CancellationWaitError>> {
        if self.token.is_cancelled() { return Poll::Ready(Ok(())); }
        let mut waiters = self.token.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if self.token.is_cancelled() { return Poll::Ready(Ok(())); }
        if self.registered.is_none() {
            let mut free = None;
            for (index, entry) in waiters.iter().enumerate() {
                if let Some(meter) = &mut self.meter {
                    if let Err(error) = meter.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }) {
                        return Poll::Ready(Err(error));
                    }
                }
                if entry.is_none() { free = Some(index); break; }
            }
            let grows = free.is_none() && waiters.len() == waiters.capacity();
            let old_bytes = (waiters.capacity() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64;
            let count = if grows {
                match waiters.capacity().max(1).checked_mul(2) {
                    Some(count) => count,
                    None => return Poll::Ready(Err(crate::WorkError::Overflow.into())),
                }
            } else { 0 };
            let new_bytes = match count.checked_mul(std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) {
                Some(bytes) => bytes as u64,
                None => return Poll::Ready(Err(crate::WorkError::Overflow.into())),
            };
            let waiter_bytes = std::mem::size_of::<CancellationWaiter>() as u64;
            if let Some(meter) = &mut self.meter {
                // Admit the complete first-poll allocation/registration,
                // including its eventual O(1) unregister, before any allocation.
                let adopted = if grows && meter.admission.registry.is_none() { old_bytes } else { 0 };
                let peak = meter.allocations.live_bytes().checked_add(waiter_bytes)
                    .and_then(|bytes| bytes.checked_add(adopted))
                    .and_then(|bytes| bytes.checked_add(new_bytes));
                let Some(peak) = peak else { return Poll::Ready(Err(crate::WorkError::Overflow.into())); };
                let delta = crate::WorkCounters {
                    allocation_operations: 1 + u64::from(grows),
                    peak_allocation_bytes: peak,
                    bytes_copied: if grows { (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64 } else { 0 },
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                };
                if let Err(error) = meter.work.admit(&delta, &meter.budget) { return Poll::Ready(Err(error.into())); }
                meter.cleanup_reserved = true;
                if let Err(error) = meter.claim(waiter_bytes, 1) { return Poll::Ready(Err(error)); }
                meter.waiter_bytes = waiter_bytes;
            }
            let waiter = Arc::new(CancellationWaiter::default());
            if grows {
                if let Some(meter) = &mut self.meter {
                    if meter.admission.registry.is_none() {
                        if let Err(error) = meter.claim(old_bytes, 0) { return Poll::Ready(Err(error)); }
                        meter.temporary_registry_bytes = old_bytes;
                    }
                    if let Err(error) = meter.claim(new_bytes, 1) { return Poll::Ready(Err(error)); }
                    meter.pending_registry_bytes = new_bytes;
                }
                let mut replacement = Vec::new();
                if replacement.try_reserve_exact(count).is_err() {
                    if let Some(meter) = &mut self.meter {
                        meter.release(new_bytes);
                        meter.pending_registry_bytes = 0;
                    }
                    return Poll::Ready(Err(CancellationWaitError::AllocationFailed));
                }
                if let Some(meter) = &mut self.meter {
                    if let Err(error) = meter.charge(crate::WorkCounters {
                        bytes_copied: (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64,
                        ..crate::WorkCounters::default()
                    }) { return Poll::Ready(Err(error)); }
                }
                replacement.append(&mut waiters);
                *waiters = replacement;
                let generation = self.token.state.registry_generation.fetch_add(1, Ordering::AcqRel) + 1;
                if let Some(meter) = &mut self.meter {
                    meter.release(old_bytes);
                    meter.temporary_registry_bytes = 0;
                    meter.pending_registry_bytes = 0;
                    meter.admission.registry = Some((generation, new_bytes));
                }
            }
            let weak = Some(Arc::downgrade(&waiter));
            let index = if let Some(index) = free {
                *waiters.get_mut(index).expect("free cancellation slot remains valid") = weak;
                index
            } else {
                waiters.push(weak);
                waiters.len() - 1
            };
            self.registered = Some(index);
            self.waiter = Some(waiter);
        }
        let waiter = self.waiter.as_ref().expect("registered cancellation waiter");
        let mut waker = waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !waker.as_ref().is_some_and(|waker| waker.will_wake(context.waker())) {
            *waker = Some(context.waker().clone());
        }
        Poll::Pending
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
        if self.waiter.is_none() && self.registered.is_none() && self.meter.is_none() { return; }
        let mut waiters = self.token.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if let Some(index) = self.registered {
            if let Some(meter) = &mut self.meter {
                meter.work.charge(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, &meter.budget)
                    .expect("cancellation cleanup allowance was reserved");
            }
            if let (Some(slot), Some(waiter)) = (waiters.get_mut(index), &self.waiter) {
                if slot.as_ref().is_some_and(|slot| Weak::ptr_eq(slot, &Arc::downgrade(waiter))) { *slot = None; }
            }
        }
        drop(waiters);
        let waker = self.waiter.take().and_then(|waiter| {
            let waker = waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            drop(waiter);
            waker
        });
        if let Some(meter) = &mut self.meter {
            meter.release(meter.waiter_bytes);
            meter.waiter_bytes = 0;
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
        if let Err(panic) = dropped { std::panic::resume_unwind(panic); }
    }
}

/// Stable cooperative-cancellation outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("filesystem operation was cancelled")]
pub struct CancellationError;

#[cfg(test)]
#[path = "tests/cancellation.rs"]
mod tests;
