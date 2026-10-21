//! Runtime-independent cooperative cancellation for native and browser work.

use crate::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, PoisonError, Weak};
use std::task::{Context, Poll, Waker};
use thiserror::Error;

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    // List head precedes every waiter-links lock. Linked-token commit gates
    // acquire distinct ancestor heads in ascending address order.
    waiters: Mutex<Weak<CancellationWaiter>>,
    parent: Option<CancellationToken>,
    // Acquired alone; parent registration is destroyed after releasing it.
    parent_waiter: Mutex<Option<Cancelled<'static>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_allocation: Option<crate::NativeAllocation>,
}

#[derive(Default)]
struct CancellationWaiter {
    // Acquired without the head or links; no locks span waker clone/drop/calls.
    waker: Mutex<Option<Waker>>,
    // State lock protects list edits; the waiting future owns the node.
    links: Mutex<Option<(Weak<Self>, Weak<Self>)>>,
    #[cfg(not(target_arch = "wasm32"))]
    // Head may precede cleanup; links and waker guards never span cleanup.
    cleanup: parking_lot::Mutex<Option<crate::materializer::NativeWorkReservation>>,
    // Last: the node and waker retire before their original allocation.
    #[cfg(not(target_arch = "wasm32"))]
    _native_allocation: Option<crate::NativeAllocation>,
}

/// Cloneable, monotonic cancellation source and observation token.
#[derive(Clone, Default)]
pub struct CancellationToken {
    state: Arc<CancellationState>,
}

pub(crate) struct PreparedCancellation {
    state: Option<Arc<CancellationState>>,
    parent_registration: Option<Cancelled<'static>>,
}

impl PreparedCancellation {
    pub(crate) fn finish(self) {
        drop(self);
    }
}

impl Drop for PreparedCancellation {
    fn drop(&mut self) {
        while let Some(state) = &self.state {
            let waiter = {
                let mut head = state.waiters.lock().unwrap_or_else(PoisonError::into_inner);
                let Some(waiter) = head.upgrade() else {
                    break;
                };
                let (_, next) = waiter
                    .links
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .expect("active cancellation head");
                if let Some(next) = next.upgrade() {
                    next.links
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .as_mut()
                        .expect("active successor")
                        .0 = Weak::new();
                }
                *head = next;
                waiter
            };
            let waker = waiter
                .waker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(waker) = waker {
                let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(credit) = &mut *waiter.cleanup.lock() {
                credit.record_deferred(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                });
            }
        }
        drop(self.parent_registration.take());
    }
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

impl CancellationToken {
    /// Creates one live token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn new_with_native_work(
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        let allocation = original.claim(
            std::mem::size_of::<CancellationState>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Self {
            state: Arc::new(CancellationState {
                _native_allocation: Some(allocation),
                ..CancellationState::default()
            }),
        })
    }

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
                .unwrap_or_else(PoisonError::into_inner) = Some(registered);
        }
        original.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(child)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn same_state(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }

    /// Marks the token cancelled and wakes every registered waiter.
    pub fn cancel(&self) {
        let prepared = {
            let _head = self
                .state
                .waiters
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            self.detach()
        };
        prepared.finish();
    }

    fn detach(&self) -> PreparedCancellation {
        let first = !self.state.cancelled.swap(true, Ordering::AcqRel);
        PreparedCancellation {
            state: first.then(|| self.state.clone()),
            parent_registration: self
                .state
                .parent_waiter
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn prepare_cancel_cleanup(
        &self,
        wait: Option<&Self>,
    ) -> Result<PreparedCancellation, crate::NativeTreeMaterializationError> {
        if let Some(wait) = wait {
            wait.check()
                .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let head = self
            .state
            .waiters
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(wait) = wait {
            wait.check()
                .map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let mut current = if self.state.cancelled.load(Ordering::Acquire) {
            None
        } else {
            head.upgrade()
        };
        while let Some(waiter) = current {
            waiter
                .cleanup
                .lock()
                .as_mut()
                .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
                .record_completed(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
            current = waiter
                .links
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .as_ref()
                .and_then(|(_, next)| next.upgrade());
        }
        Ok(self.detach())
    }

    #[cfg(any(feature = "native-mount", feature = "native-watch"))]
    pub(crate) fn commit_if_live<T>(
        &self,
        commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        self.commit_with_parents(None, None, commit)
    }

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
        // Lock distinct ancestors in address order, without scratch allocation.
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
        let _serial = state.waiters.lock().unwrap_or_else(PoisonError::into_inner);
        if state.cancelled.load(Ordering::Acquire) {
            return Err(CancellationError);
        }
        self.commit_with_parents(
            other,
            Some(state as *const CancellationState as usize),
            commit,
        )
    }

    /// Returns whether this token or its parent has been cancelled.
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

    /// Fails after cancellation.
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
            meter: None,
            waiter_bytes: 0,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(any(test, feature = "native-mount"))]
    pub(crate) fn prepare_cancelled_measured(
        &self,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        let mut future = self.cancelled();
        future.meter = Some(WaitMeter {
            allocations,
            work,
            budget,
        });
        future.register(None)?;
        Ok(future.detach_meter())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancelled_with_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Cancelled<'static>, CancellationWaitError> {
        let mut future = self.cancelled();
        future.register(Some(original))?;
        Ok(future.detach_meter())
    }
}

/// Stable cooperative-cancellation outcome.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("filesystem operation was cancelled")]
pub struct CancellationError;

mod wait;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use wait::CancellationWaitError;
pub use wait::Cancelled;
#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "native-mount")))]
use wait::WaitMeter;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod native_tests;
#[cfg(test)]
#[path = "../tests/cancellation.rs"]
mod tests;
