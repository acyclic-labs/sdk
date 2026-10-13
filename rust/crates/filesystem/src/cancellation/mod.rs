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
    parent: Option<CancellationToken>,
    parent_waiter: Mutex<Option<Cancelled<'static>>>,
    // The real state and its registry drop before their original native
    // allocation guard, including when a token clone outlives its creator.
    #[cfg(not(target_arch = "wasm32"))]
    _native_allocation: Option<crate::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
struct ChildCancellationWake {
    child: Weak<CancellationState>,
    _native_allocation: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl std::task::Wake for ChildCancellationWake {
    fn wake(self: Arc<Self>) {
        if let Some(state) = self.child.upgrade() {
            CancellationToken { state }.cancel();
        }
    }
}

#[derive(Default)]
struct CancellationWaiter {
    waker: Mutex<Option<Waker>>,
    #[cfg(not(target_arch = "wasm32"))]
    registry_lease: Mutex<Option<Arc<NativeRegistryLease>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_allocation: Option<crate::NativeAllocation>,
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

/// Owns the original drained registry until physical wake and backing drop.
/// Separating dispatch lets callers release their invocation meter first.
pub(crate) struct PreparedCancellation {
    state: Arc<CancellationState>,
    waiters: Option<Vec<Option<Weak<CancellationWaiter>>>>,
    parent_registration: Option<Cancelled<'static>>,
    #[cfg(not(target_arch = "wasm32"))]
    registry_lease: Option<Arc<NativeRegistryLease>>,
}

impl PreparedCancellation {
    pub(crate) fn finish(self) {
        drop(self);
    }
}

impl Drop for PreparedCancellation {
    fn drop(&mut self) {
        let Some(waiters) = self.waiters.take() else {
            return;
        };
        for slot in waiters {
            if let Some(waiter) = slot.and_then(|waiter| waiter.upgrade()) {
                let waker = waiter
                    .waker
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(waker) = waker {
                    let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(lease) = &self.registry_lease
                && let Some(credit) = &mut *lease.cancellation_visits.lock()
            {
                credit.record_deferred(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                });
            }
        }
        // The iterator and original buffer have physically dropped.
        self.state
            .registry_generation
            .fetch_add(1, Ordering::AcqRel);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(lease) = &self.registry_lease {
            lease.retire_backing_deferred();
        }
        // Detach the parent's canonical subscription only after this child's
        // physical drain, outside all token-state and accounting locks.
        drop(self.parent_registration.take());
    }
}

impl CancellationToken {
    /// Creates one live token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        let allocation = original.claim(
            std::mem::size_of::<CancellationState>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        // The allocator is deliberately outside the original accounting leaf.
        Ok(Self {
            state: Arc::new(CancellationState {
                _native_allocation: Some(allocation),
                ..CancellationState::default()
            }),
        })
    }

    /// Links a child stop without letting it cancel its parent.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn linked_child_with_native_work(
        parent: &Self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        parent
            .check()
            .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        if !parent.same_state(original.cancellation()) {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        original.admit(crate::WorkCounters {
            allocation_operations: 2,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let mut registered = parent
            .prepare_cancelled_with_native_work(original)
            .map_err(|error| match error {
                CancellationWaitError::Native(error) => error,
                CancellationWaitError::Work(error) => error.into(),
                CancellationWaitError::AllocationFailed => {
                    crate::NativeTreeMaterializationError::Io(
                        std::io::ErrorKind::OutOfMemory.into(),
                    )
                }
            })?;
        let allocation = original.claim(
            std::mem::size_of::<CancellationState>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let child = Self {
            state: Arc::new(CancellationState {
                parent: Some(parent.clone()),
                _native_allocation: Some(allocation),
                ..CancellationState::default()
            }),
        };
        let allocation = original.claim(
            std::mem::size_of::<ChildCancellationWake>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let wake = Waker::from(Arc::new(ChildCancellationWake {
            child: Arc::downgrade(&child.state),
            _native_allocation: allocation,
        }));
        if Pin::new(&mut registered)
            .poll(&mut Context::from_waker(&wake))
            .is_ready()
        {
            child.cancel();
        } else {
            *child
                .state
                .parent_waiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(registered);
        }
        original.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(child)
    }

    pub(crate) fn same_state(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Atomically marks the token cancelled and wakes every registered waiter.
    pub fn cancel(&self) {
        self.cancel_inner(None)
            .expect("unmetered cancellation cannot fail");
    }

    pub(crate) fn cancel_measured(
        &self,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<(), crate::WorkError> {
        self.cancel_inner(Some((work, budget)))
    }

    /// Serializes a physical commit with cancellation.
    #[cfg(any(feature = "native-mount", feature = "native-watch"))]
    pub(crate) fn commit_if_live<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        self.commit_with_parents(None, None, commit)
    }

    /// Serializes the original invocation and source lifetime without a new
    /// gate. Shared tokens take their one lock; distinct locks have a stable
    /// address order and are held only for the non-callback physical commit.
    #[cfg(feature = "native-mount")]
    pub(crate) fn commit_if_both_live<T>(
        &self,
        other: &Self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        self.commit_with_parents(Some(other), None, commit)
    }

    #[cfg(any(feature = "native-mount", feature = "native-watch"))]
    fn commit_with_parents<T>(
        &self,
        other: Option<&Self>,
        after: Option<usize>,
        commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        // Find the next distinct state in address order without allocating a
        // temporary lock list. Common ancestors are locked exactly once.
        let mut next: Option<&CancellationState> = None;
        for root in [Some(self), other].into_iter().flatten() {
            let mut token = Some(root);
            while let Some(current) = token {
                let state = current.state.as_ref();
                let address = state as *const CancellationState as usize;
                if after.is_none_or(|previous| address > previous)
                    && next.is_none_or(|selected| {
                        address < selected as *const CancellationState as usize
                    })
                {
                    next = Some(state);
                }
                token = state.parent.as_ref();
            }
        }
        let Some(state) = next else {
            return Ok(commit());
        };
        let _serial = state
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.cancelled.load(Ordering::Acquire) {
            return Err(CancellationError);
        }
        self.commit_with_parents(
            other,
            Some(state as *const CancellationState as usize),
            commit,
        )
    }

    fn cancel_inner(
        &self,
        admission: Option<(&mut crate::WorkCounters, crate::WorkBudget)>,
    ) -> Result<(), crate::WorkError> {
        self.prepare_cancel(admission)?.finish();
        Ok(())
    }

    fn prepare_cancel(
        &self,
        admission: Option<(&mut crate::WorkCounters, crate::WorkBudget)>,
    ) -> Result<PreparedCancellation, crate::WorkError> {
        let mut waiters = self
            .state
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.cancelled.load(Ordering::Acquire) {
            return Ok(PreparedCancellation {
                state: Arc::clone(&self.state),
                waiters: None,
                parent_registration: None,
                #[cfg(not(target_arch = "wasm32"))]
                registry_lease: None,
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        let lease = {
            let mut visits = 0;
            let lease = registry_lease(&waiters, || {
                visits += 1;
                Ok::<(), crate::WorkError>(())
            })?;
            if let Some(lease) = &lease
                && let Some(credit) = &mut *lease.cancellation_visits.lock()
            {
                // Record the actual lookup, including empty/dead slots,
                // on this backing's original prepaid terminal credit.
                credit.record_deferred(crate::WorkCounters {
                    items_examined: visits,
                    ..crate::WorkCounters::UNCHARGED
                });
            }
            lease
        };
        if let Some((work, budget)) = admission {
            #[cfg(not(target_arch = "wasm32"))]
            let ordinary = lease.is_none();
            #[cfg(target_arch = "wasm32")]
            let ordinary = true;
            if ordinary {
                work.charge(
                    &crate::WorkCounters {
                        items_examined: waiters.len() as u64,
                        ..crate::WorkCounters::UNCHARGED
                    },
                    &budget,
                )?;
            }
        }
        Ok(self.prepare_cancel_locked(
            &mut waiters,
            #[cfg(not(target_arch = "wasm32"))]
            lease,
        ))
    }

    fn prepare_cancel_locked(
        &self,
        waiters: &mut Vec<Option<Weak<CancellationWaiter>>>,
        #[cfg(not(target_arch = "wasm32"))] registry_lease: Option<Arc<NativeRegistryLease>>,
    ) -> PreparedCancellation {
        self.state.cancelled.store(true, Ordering::Release);
        PreparedCancellation {
            state: Arc::clone(&self.state),
            waiters: Some(std::mem::take(waiters)),
            parent_registration: self
                .state
                .parent_waiter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
            #[cfg(not(target_arch = "wasm32"))]
            registry_lease,
        }
    }

    /// Uses only this registry's original before-birth terminal credit. The
    /// independent token controls waiting, not namespace or task authority.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancel_with_cleanup_wait(
        &self,
        wait: &CancellationToken,
    ) -> Result<PreparedCancellation, crate::NativeTreeMaterializationError> {
        self.prepare_cancel_cleanup(Some(wait))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancel_accepted_cleanup(
        &self,
    ) -> Result<PreparedCancellation, crate::NativeTreeMaterializationError> {
        self.prepare_cancel_cleanup(None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_cancel_cleanup(
        &self,
        wait: Option<&CancellationToken>,
    ) -> Result<PreparedCancellation, crate::NativeTreeMaterializationError> {
        if let Some(wait) = wait {
            wait.check()
                .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let mut waiters = self
            .state
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.cancelled.load(Ordering::Acquire) {
            return Ok(PreparedCancellation {
                state: Arc::clone(&self.state),
                waiters: None,
                parent_registration: None,
                registry_lease: None,
            });
        }
        if let Some(wait) = wait {
            wait.check()
                .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let mut visits = 0;
        let lease = registry_lease(&waiters, || {
            visits += 1;
            Ok::<(), crate::NativeTreeMaterializationError>(())
        })?;
        if let Some(lease) = &lease {
            let mut credit = lease.cancellation_visits.lock();
            credit
                .as_mut()
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
                .record_completed(crate::WorkCounters {
                    items_examined: visits,
                    ..crate::WorkCounters::UNCHARGED
                })?;
        } else if !waiters.is_empty() {
            // A preexisting ordinary registry has no original native credit.
            // Its source owner must retain custody and report uncertainty.
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(self.prepare_cancel_locked(&mut waiters, lease))
    }

    /// Returns whether cancellation has occurred.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        let mut token = Some(self);
        while let Some(current) = token {
            if current.state.cancelled.load(Ordering::Acquire) {
                return true;
            }
            token = current.state.parent.as_ref();
        }
        false
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
            token: self.clone(),
            waiter: None,
            registered: None,
            meter: None,
            waiter_bytes: 0,
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
        }
    }

    /// Uses the invocation's existing ledger; construction and precancellation
    /// allocate nothing. The waiter and registry grow only when polled.
    fn cancelled_measured<'a>(
        &'a self,
        allocations: &'a mut crate::kernel::allocation::AllocationLedger,
        work: &'a mut crate::WorkCounters,
        budget: crate::WorkBudget,
        admission: &'a mut CancellationWaitAdmission,
    ) -> Cancelled<'a> {
        let mut future = self.cancelled();
        future.meter = Some(WaitMeter {
            allocations,
            work,
            budget,
            admission,
            temporary_registry_bytes: 0,
            pending_registry_bytes: 0,
            cleanup_reserved: false,
        });
        future
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
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        let mut future = self.cancelled_measured(allocations, work, budget, admission);
        let mut context = Context::from_waker(Waker::noop());
        match future.poll_result(&mut context) {
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }
        let waiter = future.waiter.take();
        if let Some(waiter) = &waiter {
            *waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        }
        let registered = future.registered.take();
        future.meter.take();
        Ok(Cancelled {
            token: self.clone(),
            waiter,
            registered,
            meter: None,
            waiter_bytes: std::mem::take(&mut future.waiter_bytes),
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
        })
    }

    /// Prepares the same canonical waiter without running its allocator under
    /// the original ledger's short scalar lock.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancelled_with_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        let mut future = self.cancelled();
        future.native_original = Some(original);
        let mut context = Context::from_waker(Waker::noop());
        match future.poll_result(&mut context) {
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }
        let waiter = future.waiter.take();
        if let Some(waiter) = &waiter {
            let waker = waiter
                .waker
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(waker);
        }
        Ok(Cancelled {
            token: self.clone(),
            waiter,
            registered: future.registered.take(),
            meter: None,
            waiter_bytes: 0,
            native_original: None,
            native_cleanup: future.native_cleanup.take(),
        })
    }
}

/// Stable cooperative-cancellation outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("filesystem operation was cancelled")]
pub struct CancellationError;

#[cfg(test)]
#[path = "../tests/cancellation.rs"]
mod tests;

mod budget;
pub(crate) use budget::CancellationWaitAdmission;
use budget::WaitMeter;
mod wait;
pub(crate) use wait::CancellationWaitError;
pub use wait::Cancelled;
#[cfg(not(target_arch = "wasm32"))]
mod registry;
#[cfg(not(target_arch = "wasm32"))]
use registry::{NativeRegistryLease, registry_lease};
#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_tests;
