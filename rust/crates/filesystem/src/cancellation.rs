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
    parent_waiter: Mutex<Option<PreparedCancelled>>,
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

/// Custody of a backing of the existing registry. Only active waiter/future
/// nodes own this lease; the token owns only their existing Weak slots.
#[cfg(not(target_arch = "wasm32"))]
struct NativeRegistryLease {
    state: Weak<CancellationState>,
    generation: u64,
    registry_bytes: AtomicU64,
    registered: AtomicU64,
    // Logical highwater admitted before allocating this backing. Allocator
    // capacity rounding must never expose unfunded slots.
    funded_slots: usize,
    original: crate::NativeMaterializationWork,
    cancellation_visits: parking_lot::Mutex<Option<crate::materializer::NativeWorkReservation>>,
    allocation: parking_lot::Mutex<crate::NativeAllocation>,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeRegistryLease {
    fn retire_backing(&self) -> Result<(), crate::NativeTreeMaterializationError> {
        let visits = self.cancellation_visits.lock().take();
        drop(visits);
        let bytes = self.registry_bytes.swap(0, Ordering::AcqRel);
        self.allocation.lock()
            .release(bytes)
    }

    fn retire_backing_deferred(&self) {
        let visits = self.cancellation_visits.lock().take();
        drop(visits);
        let bytes = self.registry_bytes.swap(0, Ordering::AcqRel);
        self.allocation.lock()
            .release_deferred(bytes);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn registry_lease<E>(
    waiters: &[Option<Weak<CancellationWaiter>>],
    mut visit: impl FnMut() -> Result<(), E>,
) -> Result<Option<Arc<NativeRegistryLease>>, E> {
    for slot in waiters {
        visit()?;
        let Some(slot) = slot else { continue; };
        if let Some(waiter) = slot.upgrade() {
            let lease = waiter.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
            if lease.is_some() { return Ok(lease); }
        }
    }
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for NativeRegistryLease {
    fn drop(&mut self) {
        let detached = self.state.upgrade().and_then(|state| {
            if state.registry_generation.load(Ordering::Acquire) != self.generation { return None; }
            let mut waiters = state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.registry_generation.load(Ordering::Acquire) != self.generation { return None; }
            debug_assert!(waiters.iter().flatten().all(|slot| slot.strong_count() == 0));
            let detached = std::mem::take(&mut *waiters);
            state.registry_generation.fetch_add(1, Ordering::AcqRel);
            drop(waiters);
            Some(detached)
        });
        drop(detached);
        self.retire_backing_deferred();
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
    parent_registration: Option<PreparedCancelled>,
    #[cfg(not(target_arch = "wasm32"))]
    registry_lease: Option<Arc<NativeRegistryLease>>,
}

impl PreparedCancellation {
    pub(crate) fn finish(self) { drop(self); }
}

impl Drop for PreparedCancellation {
    fn drop(&mut self) {
        let Some(waiters) = self.waiters.take() else { return; };
        for slot in waiters {
            if let Some(waiter) = slot.and_then(|waiter| waiter.upgrade()) {
                let waker = waiter.waker.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner).take();
                if let Some(waker) = waker {
                    let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(lease) = &self.registry_lease {
                if let Some(credit) = &mut *lease.cancellation_visits.lock()
                {
                    credit.record_deferred(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED });
                }
            }
        }
        // The iterator and original buffer have physically dropped.
        self.state.registry_generation.fetch_add(1, Ordering::AcqRel);
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
            std::mem::size_of::<CancellationState>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        // The allocator is deliberately outside the original accounting leaf.
        Ok(Self { state: Arc::new(CancellationState {
            _native_allocation: Some(allocation),
            ..CancellationState::default()
        }) })
    }

    /// Links a child stop without letting it cancel its parent.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn linked_child_with_native_work(
        parent: &Self, original: &crate::NativeMaterializationWork,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        parent.check().map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        if !parent.same_state(original.cancellation()) {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        original.admit(crate::WorkCounters {
            allocation_operations: 2, ..crate::WorkCounters::UNCHARGED
        })?;
        let mut registered = parent.prepare_cancelled_with_native_work(original).map_err(|error| match error {
            CancellationWaitError::Native(error) => error,
            CancellationWaitError::Work(error) => error.into(),
            CancellationWaitError::AllocationFailed => crate::NativeTreeMaterializationError::Io(
                std::io::ErrorKind::OutOfMemory.into(),
            ),
        })?;
        let allocation = original.claim(std::mem::size_of::<CancellationState>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let child = Self { state: Arc::new(CancellationState {
            parent: Some(parent.clone()), _native_allocation: Some(allocation),
            ..CancellationState::default()
        }) };
        let allocation = original.claim(std::mem::size_of::<ChildCancellationWake>() as u64, 1, crate::WorkCounters::UNCHARGED)?;
        let wake = Waker::from(Arc::new(ChildCancellationWake {
            child: Arc::downgrade(&child.state), _native_allocation: allocation,
        }));
        if Pin::new(&mut registered).poll(&mut Context::from_waker(&wake)).is_ready() {
            child.cancel();
        } else {
            *child.state.parent_waiter.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                = Some(registered);
        }
        original.admit(crate::WorkCounters::UNCHARGED)?;
        Ok(child)
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

    /// Serializes a physical commit with cancellation.
    #[cfg(any(feature = "native-mount", feature = "native-watch"))]
    pub(crate) fn commit_if_live<T>(
        &self, commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        self.commit_with_parents(None, None, commit)
    }

    /// Serializes the original invocation and source lifetime without a new
    /// gate. Shared tokens take their one lock; distinct locks have a stable
    /// address order and are held only for the non-callback physical commit.
    #[cfg(feature = "native-mount")]
    pub(crate) fn commit_if_both_live<T>(
        &self, other: &Self, commit: impl FnOnce() -> T,
    ) -> Result<T, CancellationError> {
        self.commit_with_parents(Some(other), None, commit)
    }

    #[cfg(any(feature = "native-mount", feature = "native-watch"))]
    fn commit_with_parents<T>(
        &self, other: Option<&Self>, after: Option<usize>, commit: impl FnOnce() -> T,
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
                    && next.is_none_or(|selected| address < selected as *const CancellationState as usize)
                {
                    next = Some(state);
                }
                token = state.parent.as_ref();
            }
        }
        let Some(state) = next else { return Ok(commit()); };
        let _serial = state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.cancelled.load(Ordering::Acquire) { return Err(CancellationError); }
        self.commit_with_parents(other, Some(state as *const CancellationState as usize), commit)
    }

    fn cancel_inner(
        &self, admission: Option<(&mut crate::WorkCounters, crate::WorkBudget)>,
    ) -> Result<(), crate::WorkError> {
        self.prepare_cancel(admission)?.finish();
        Ok(())
    }

    fn prepare_cancel(
        &self, admission: Option<(&mut crate::WorkCounters, crate::WorkBudget)>,
    ) -> Result<PreparedCancellation, crate::WorkError> {
        let mut waiters = self.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.cancelled.load(Ordering::Acquire) {
            return Ok(PreparedCancellation {
                state: Arc::clone(&self.state), waiters: None,
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
            if let Some(lease) = &lease {
                if let Some(credit) = &mut *lease.cancellation_visits.lock()
                {
                    // Record the actual lookup, including empty/dead slots,
                    // on this backing's original prepaid terminal credit.
                    credit.record_deferred(crate::WorkCounters { items_examined: visits, ..crate::WorkCounters::UNCHARGED });
                }
            }
            lease
        };
        if let Some((work, budget)) = admission {
            #[cfg(not(target_arch = "wasm32"))]
            let ordinary = lease.is_none();
            #[cfg(target_arch = "wasm32")]
            let ordinary = true;
            if ordinary {
                work.charge(&crate::WorkCounters {
                    items_examined: waiters.len() as u64, ..crate::WorkCounters::UNCHARGED
                }, &budget)?;
            }
        }
        Ok(self.prepare_cancel_locked(&mut waiters,
            #[cfg(not(target_arch = "wasm32"))]
            lease,
        ))
    }

    fn prepare_cancel_locked(
        &self, waiters: &mut Vec<Option<Weak<CancellationWaiter>>>,
        #[cfg(not(target_arch = "wasm32"))]
        registry_lease: Option<Arc<NativeRegistryLease>>,
    ) -> PreparedCancellation {
        self.state.cancelled.store(true, Ordering::Release);
        PreparedCancellation {
            state: Arc::clone(&self.state), waiters: Some(std::mem::take(waiters)),
            parent_registration: self.state.parent_waiter.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).take(),
            #[cfg(not(target_arch = "wasm32"))]
            registry_lease,
        }
    }

    /// Uses only this registry's original before-birth terminal credit. The
    /// independent token controls waiting, not namespace or task authority.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancel_with_cleanup_wait(
        &self, wait: &CancellationToken,
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
        &self, wait: Option<&CancellationToken>,
    ) -> Result<PreparedCancellation, crate::NativeTreeMaterializationError> {
        if let Some(wait) = wait {
            wait.check().map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let mut waiters = self.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.state.cancelled.load(Ordering::Acquire) {
            return Ok(PreparedCancellation {
                state: Arc::clone(&self.state), waiters: None,
                parent_registration: None, registry_lease: None,
            });
        }
        if let Some(wait) = wait {
            wait.check().map_err(|_| crate::NativeTreeMaterializationError::Cancelled)?;
        }
        let mut visits = 0;
        let lease = registry_lease(&waiters, || {
            visits += 1;
            Ok::<(), crate::NativeTreeMaterializationError>(())
        })?;
        if let Some(lease) = &lease {
            let mut credit = lease.cancellation_visits.lock();
            credit.as_mut().ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?
                .record_completed(crate::WorkCounters { items_examined: visits, ..crate::WorkCounters::UNCHARGED })?;
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
            if current.state.cancelled.load(Ordering::Acquire) { return true; }
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
            token: self,
            waiter: None,
            registered: None,
            meter: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
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
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
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
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
        })
    }

    /// Prepares the same canonical waiter without running its allocator under
    /// the original ledger's short scalar lock.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn prepare_cancelled_with_native_work(
        &self, original: &crate::NativeMaterializationWork,
    ) -> Result<PreparedCancelled, CancellationWaitError> {
        let mut future = Cancelled {
            token: self, waiter: None, registered: None, meter: None,
            native_original: Some(original), native_cleanup: None,
        };
        let mut context = Context::from_waker(Waker::noop());
        match future.poll_result(&mut context) {
            Poll::Ready(Err(error)) => return Err(error),
            Poll::Ready(Ok(())) | Poll::Pending => {}
        }
        let waiter = future.waiter.take();
        if let Some(waiter) = &waiter {
            let waker = waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            drop(waker);
        }
        Ok(PreparedCancelled {
            token: self.clone(), waiter, registered: future.registered.take(),
            waiter_bytes: 0, native_cleanup: future.native_cleanup.take(),
        })
    }
}

/// Future returned by [`CancellationToken::cancelled`].
pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    waiter: Option<Arc<CancellationWaiter>>,
    registered: Option<usize>,
    meter: Option<WaitMeter<'a>>,
    #[cfg(not(target_arch = "wasm32"))]
    native_original: Option<&'a crate::NativeMaterializationWork>,
    #[cfg(not(target_arch = "wasm32"))]
    native_cleanup: Option<crate::materializer::NativeWorkReservation>,
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
    #[cfg(not(target_arch = "wasm32"))]
    #[error(transparent)]
    Native(#[from] crate::NativeTreeMaterializationError),
}

/// A registration already admitted by the worker's original invocation ledger.
pub(crate) struct PreparedCancelled {
    token: CancellationToken,
    waiter: Option<Arc<CancellationWaiter>>,
    registered: Option<usize>,
    waiter_bytes: u64,
    #[cfg(not(target_arch = "wasm32"))]
    native_cleanup: Option<crate::materializer::NativeWorkReservation>,
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
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: self.native_cleanup.take(),
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
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: None,
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
            #[cfg(not(target_arch = "wasm32"))]
            native_original: None,
            #[cfg(not(target_arch = "wasm32"))]
            native_cleanup: self.native_cleanup.take(),
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
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = self.native_original {
                    if let Err(error) = original.charge(crate::WorkCounters {
                        items_examined: 1, ..crate::WorkCounters::UNCHARGED
                    }) { return Poll::Ready(Err(error.into())); }
                }
                if entry.is_none() { free = Some(index); break; }
            }
            #[cfg(not(target_arch = "wasm32"))]
            let previous_lease = match registry_lease(&waiters, || {
                self.native_original.map_or(Ok(()), |original| original.charge(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }))
            }) {
                Ok(lease) => lease,
                Err(error) => return Poll::Ready(Err(error.into())),
            };
            #[cfg(not(target_arch = "wasm32"))]
            let mut cancellation_visits = None;
            #[cfg(not(target_arch = "wasm32"))]
            let inherited_original = previous_lease.as_ref().map(|lease| lease.original.clone());
            #[cfg(not(target_arch = "wasm32"))]
            let native_original = inherited_original.as_ref().or(self.native_original);
            #[cfg(not(target_arch = "wasm32"))]
            let funded_slots = previous_lease.as_ref().map_or(waiters.capacity(), |lease| lease.funded_slots);
            #[cfg(target_arch = "wasm32")]
            let funded_slots = waiters.capacity();
            let grows = free.is_none() && waiters.len() == funded_slots;
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
            #[cfg(not(target_arch = "wasm32"))]
            let mut growth_visits = None;
            #[cfg(not(target_arch = "wasm32"))]
            let native_waiter_allocation = if let Some(original) = native_original {
                let registry_metadata = if grows { std::mem::size_of::<NativeRegistryLease>() as u64 } else { 0 };
                let retained = waiter_bytes.checked_add(new_bytes)
                    .and_then(|bytes| bytes.checked_add(registry_metadata));
                let Some(retained) = retained else { return Poll::Ready(Err(crate::WorkError::Overflow.into())); };
                let cancellation_items = if grows {
                    match (count as u64).checked_mul(2) {
                        Some(items) => items,
                        None => return Poll::Ready(Err(crate::WorkError::Overflow.into())),
                    }
                } else { 0 };
                if let Err(error) = original.admit(crate::WorkCounters {
                    allocation_operations: 1 + 2 * u64::from(grows),
                    bytes_copied: if grows { (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64 } else { 0 },
                    items_examined: 2 + if grows { waiters.len() as u64 + cancellation_items } else { 0 },
                    ..crate::WorkCounters::UNCHARGED
                }).and_then(|()| original.admit_retained(retained)) {
                    return Poll::Ready(Err(error.into()));
                }
                self.native_cleanup = match original.reserve_work(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED }) {
                    Ok(credit) => Some(credit),
                    Err(error) => return Poll::Ready(Err(error.into())),
                };
                if grows {
                    growth_visits = match original.reserve_work(crate::WorkCounters { items_examined: waiters.len() as u64, ..crate::WorkCounters::UNCHARGED }) {
                        Ok(credit) => Some(credit),
                        Err(error) => return Poll::Ready(Err(error.into())),
                    };
                    cancellation_visits = match original.reserve_work(crate::WorkCounters { items_examined: cancellation_items, ..crate::WorkCounters::UNCHARGED }) {
                        Ok(credit) => Some(credit),
                        Err(error) => return Poll::Ready(Err(error.into())),
                    };
                }
                match original.claim(waiter_bytes, 1, crate::WorkCounters::UNCHARGED) {
                    Ok(allocation) => Some(allocation),
                    Err(error) => return Poll::Ready(Err(error.into())),
                }
            } else { None };
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
            let waiter_value = CancellationWaiter {
                #[cfg(not(target_arch = "wasm32"))]
                _native_allocation: native_waiter_allocation,
                #[cfg(not(target_arch = "wasm32"))]
                registry_lease: std::sync::Mutex::new(previous_lease.clone()),
                ..CancellationWaiter::default()
            };
            let waiter = Arc::new(waiter_value);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = native_original {
                if let Err(error) = original.record_admitted(crate::WorkCounters {
                    items_examined: 1, ..crate::WorkCounters::UNCHARGED
                }) { return Poll::Ready(Err(error.into())); }
            }
            if grows {
                if let Some(meter) = &mut self.meter {
                    if meter.admission.registry.is_none() {
                        if let Err(error) = meter.claim(old_bytes, 0) { return Poll::Ready(Err(error)); }
                        meter.temporary_registry_bytes = old_bytes;
                    }
                    if let Err(error) = meter.claim(new_bytes, 1) { return Poll::Ready(Err(error)); }
                    meter.pending_registry_bytes = new_bytes;
                }
                #[cfg(not(target_arch = "wasm32"))]
                let mut native_registry_allocation = if let Some(original) = native_original {
                    match original.claim(new_bytes, 1, crate::WorkCounters::UNCHARGED) {
                        Ok(allocation) => Some(allocation),
                        Err(error) => return Poll::Ready(Err(error.into())),
                    }
                } else { None };
                let mut replacement = Vec::new();
                if let Err(_error) = replacement.try_reserve_exact(count) {
                    if let Some(meter) = &mut self.meter {
                        meter.release(new_bytes);
                        meter.pending_registry_bytes = 0;
                    }
                    #[cfg(not(target_arch = "wasm32"))]
                    if native_original.is_some() {
                        return Poll::Ready(Err(CancellationWaitError::Native(
                            crate::NativeTreeMaterializationError::AllocationFailed(_error),
                        )));
                    }
                    return Poll::Ready(Err(CancellationWaitError::AllocationFailed));
                }
                if let Some(meter) = &mut self.meter {
                    if let Err(error) = meter.charge(crate::WorkCounters {
                        bytes_copied: (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64,
                        ..crate::WorkCounters::default()
                    }) { return Poll::Ready(Err(error)); }
                }
                #[cfg(not(target_arch = "wasm32"))]
                let new_lease = if let Some(original) = native_original {
                    let actual = (replacement.capacity() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64;
                    if let Err(error) = native_registry_allocation.as_mut().expect("native registry allocation")
                        .reconcile_capacity(actual)
                    {
                        drop(waiters);
                        drop(replacement);
                        return Poll::Ready(Err(error.into()));
                    }
                    let generation = match self.token.state.registry_generation.load(Ordering::Acquire).checked_add(1) {
                        Some(generation) => generation,
                        None => {
                            drop(waiters);
                            drop(replacement);
                            return Poll::Ready(Err(crate::WorkError::Overflow.into()));
                        }
                    };
                    let mut metadata = match original.claim(std::mem::size_of::<NativeRegistryLease>() as u64, 1, crate::WorkCounters::UNCHARGED) {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            drop(waiters);
                            drop(replacement);
                            return Poll::Ready(Err(error.into()));
                        }
                    };
                    if let Err(error) = metadata.absorb(native_registry_allocation.as_mut().expect("native registry allocation")) {
                        drop(waiters);
                        drop(replacement);
                        return Poll::Ready(Err(error.into()));
                    }
                    Some(Arc::new(NativeRegistryLease {
                        state: Arc::downgrade(&self.token.state), generation,
                        registry_bytes: AtomicU64::new(actual), registered: AtomicU64::new(0),
                        funded_slots: count, original: original.clone(),
                        cancellation_visits: parking_lot::Mutex::new(cancellation_visits),
                        allocation: parking_lot::Mutex::new(metadata),
                    }))
                } else { None };
                let _copied_bytes = (waiters.len() * std::mem::size_of::<Option<Weak<CancellationWaiter>>>()) as u64;
                replacement.append(&mut waiters);
                let old_backing = std::mem::replace(&mut *waiters, replacement);
                let generation = self.token.state.registry_generation.fetch_add(1, Ordering::AcqRel) + 1;
                if let Some(meter) = &mut self.meter {
                    // The old backing remains physically alive until unlock.
                    meter.temporary_registry_bytes = 0;
                    meter.pending_registry_bytes = 0;
                    meter.admission.registry = Some((generation, new_bytes));
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    // Only Arc aliases of this one backing are changed. Keep
                    // the old lease alive until its physical Vec is dropped.
                    for slot in waiters.iter() {
                        if let Some(existing) = slot.as_ref().and_then(Weak::upgrade) {
                            *existing.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = new_lease.clone();
                            if let Some(lease) = &new_lease { lease.registered.fetch_add(1, Ordering::AcqRel); }
                        }
                        if let Some(credit) = &mut growth_visits {
                            credit.record_deferred(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED });
                        }
                    }
                    *waiter.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = new_lease.clone();
                }
                let index = if let Some(index) = free {
                    waiters[index] = Some(Arc::downgrade(&waiter));
                    index
                } else {
                    waiters.push(Some(Arc::downgrade(&waiter)));
                    waiters.len() - 1
                };
                self.registered = Some(index);
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(lease) = waiter.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
                    lease.registered.fetch_add(1, Ordering::AcqRel);
                }
                self.waiter = Some(waiter.clone());
                drop(waiters);
                drop(old_backing);
                if let Some(meter) = &mut self.meter { meter.release(old_bytes); }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let copied = native_original.map_or(Ok(()), |original|
                        original.record_admitted(crate::WorkCounters {
                            bytes_copied: _copied_bytes, ..crate::WorkCounters::UNCHARGED
                        }));
                    let retired = previous_lease.as_ref().map_or(Ok(()), |lease| lease.retire_backing());
                    if let Err(error) = copied.and(retired) { return Poll::Ready(Err(error.into())); }
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = native_original {
                    if let Err(error) = original.admit(crate::WorkCounters::UNCHARGED) {
                        return Poll::Ready(Err(error.into()));
                    }
                }
                waiters = self.token.state.waiters.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                // Cancellation may have drained the newly installed registry
                // while its old backing was being physically retired.
                if self.token.is_cancelled() { return Poll::Ready(Ok(())); }
            }
            if self.registered.is_none() {
            let weak = Some(Arc::downgrade(&waiter));
            let index = if let Some(index) = free {
                *waiters.get_mut(index).expect("free cancellation slot remains valid") = weak;
                index
            } else {
                waiters.push(weak);
                waiters.len() - 1
            };
            self.registered = Some(index);
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(lease) = waiter.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
                lease.registered.fetch_add(1, Ordering::AcqRel);
            }
            self.waiter = Some(waiter);
            }
        }
        let waiter = self.waiter.as_ref().expect("registered cancellation waiter");
        drop(waiters);
        let replace = {
            let waker = waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            !waker.as_ref().is_some_and(|waker| waker.will_wake(context.waker()))
        };
        if replace {
            // RawWaker clone/drop may run arbitrary executor code, including
            // cancellation of this same token. Neither registry nor waiter
            // locks may span those callbacks.
            let replacement = context.waker().clone();
            let previous = waiter.waker.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .replace(replacement);
            drop(previous);
        }
        // A cancellation that won before replacement may already have drained
        // this slot. Observing it here closes that lost-wakeup window; one that
        // wins afterward takes the newly installed waker from this same node.
        if self.token.is_cancelled() { Poll::Ready(Ok(())) } else { Poll::Pending }
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
        #[cfg(not(target_arch = "wasm32"))]
        let mut retired_backing = None;
        #[cfg(not(target_arch = "wasm32"))]
        let mut retired_lease = None;
        if let Some(meter) = &mut self.meter {
            meter.observe_registry(self.token.state.registry_generation.load(Ordering::Acquire));
        }
        if let Some(index) = self.registered {
            if let Some(meter) = &mut self.meter {
                meter.work.charge(&crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }, &meter.budget)
                    .expect("cancellation cleanup allowance was reserved");
            }
            if let (Some(slot), Some(waiter)) = (waiters.get_mut(index), &self.waiter) {
                if slot.as_ref().is_some_and(|slot| Weak::ptr_eq(slot, &Arc::downgrade(waiter))) {
                    *slot = None;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(lease) = waiter.registry_lease.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
                        if lease.registered.fetch_sub(1, Ordering::AcqRel) == 1 {
                            retired_backing = Some(std::mem::take(&mut *waiters));
                            self.token.state.registry_generation.fetch_add(1, Ordering::AcqRel);
                            retired_lease = Some(lease.clone());
                        }
                    }
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
            if self.registered.is_some() {
                if let Some(credit) = &mut self.native_cleanup {
                    credit.record_deferred(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED });
                }
            }
        }
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_registry_cleanup_controls {
    use super::*;

    fn budget(items: u64) -> crate::WorkBudget {
        crate::WorkBudget {
            items_examined: items, allocation_operations: 64,
            bytes_hashed: 0,
            bytes_copied: 4096,
            peak_allocation_bytes: 16384, ..crate::WorkBudget::UNBOUNDED
        }
    }



    #[test]
    fn cancelled_cleanup_wait_preserves_live_registry_and_original_prefix() {
        let root = CancellationToken::new();
        let token = CancellationToken::new();
        let original = crate::NativeMaterializationWork::new(budget(64), &root)
            .map_err(|(error, _)| error).unwrap();
        let registered = token.prepare_cancelled_with_native_work(&original).unwrap();
        root.cancel();
        let wait = CancellationToken::new();
        wait.cancel();
        let before = original.receipt();
        assert!(matches!(token.prepare_cancel_with_cleanup_wait(&wait),
            Err(crate::NativeTreeMaterializationError::Cancelled)));
        assert!(!token.is_cancelled());
        assert_eq!(original.receipt(), before);
        drop(registered);
    }


    #[test]
    fn ordinary_preexisting_registry_cannot_fabricate_native_delivery_credit() {
        let token = CancellationToken::new();
        let mut ordinary = token.cancelled();
        assert!(Pin::new(&mut ordinary).poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert!(matches!(token.prepare_cancel_with_cleanup_wait(&CancellationToken::new()),
            Err(crate::NativeTreeMaterializationError::InvalidLayout)));
        assert!(!token.is_cancelled());
        drop(ordinary);
    }
}
