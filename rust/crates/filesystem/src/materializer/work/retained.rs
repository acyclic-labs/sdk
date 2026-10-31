//! FIFO capacity admission on the original retained-byte ledger.
use super::*;
use crate::demand::DemandWork;
use std::{
    future::Future,
    pin::Pin,
    sync::{Mutex, PoisonError, Weak},
    task::{Context, Poll, Waker},
};

#[derive(Default)]
pub(crate) struct RetainedClaims {
    generation: u64,
    open: bool,
    head: Weak<Waiter>,
    tail: Weak<Waiter>,
    wake: Option<Waker>,
    advancing: bool,
}

impl RetainedClaims {
    #[cfg(all(test, not(loom)))]
    pub(crate) fn wake_enabled() -> bool {
        !tests::OMIT_WAKE.with(std::cell::Cell::get)
    }
    pub(crate) fn take_wake(&mut self) -> Option<Waker> {
        self.wake.take()
    }
}

struct Waiter {
    generation: u64,
    work: NativeMaterializationWork,
    bytes: u64,
    operations: u64,
    metadata_bytes: u64,
    delta: WorkCounters,
    state: Mutex<WaitState>,
}

#[derive(Default)]
struct WaitState {
    linked: bool,
    previous: Weak<Waiter>,
    next: Weak<Waiter>,
    waker: Option<Waker>,
    result: Option<Result<NativeAllocation, NativeTreeMaterializationError>>,
}

/// Exclusive lifecycle custody for one admitted memory producer generation.
/// The runtime/session owner retains this non-clone lease. Closing an old lease
/// never cancels another operation or a successor generation's original work.
pub struct NativeRetainedProducer {
    work: NativeMaterializationWork,
    generation: u64,
}

impl Drop for NativeRetainedProducer {
    fn drop(&mut self) {
        self.work.state(|state| {
            if state.retained_claims.generation == self.generation {
                state.retained_claims.open = false;
                state.advance_retained_claim();
            }
        });
    }
}

/// A single exact retained-capacity claim. Registration/cancellation are O(1).
/// Its queue node and waker storage are funded before registration by the same
/// original ledger; inability to admit that storage returns a typed refusal.
pub struct NativeRetainedClaim {
    waiter: Option<Arc<Waiter>>,
    immediate: Option<Result<NativeAllocation, NativeTreeMaterializationError>>,
    cancellation: Option<crate::Cancelled<'static>>,
    // Queue state and physical node are destroyed before their original credit.
    _storage: Option<NativeAllocation>,
}

impl NativeMaterializationWork {
    /// Acquires the original meter's producer generation. This must be retained
    /// by the actual runtime/session lifecycle owner, never by its waiters.
    pub fn retained_producer(
        &self,
    ) -> Result<NativeRetainedProducer, NativeTreeMaterializationError> {
        let generation = self
            .state(|state| {
                state.check()?;
                if state.retained_claims.open {
                    return Err(crate::DemandError::InvalidRequest);
                }
                let generation = state
                    .retained_claims
                    .generation
                    .checked_add(1)
                    .ok_or(crate::WorkError::Overflow)?;
                state.retained_claims.generation = generation;
                state.retained_claims.open = true;
                // Older waiters keep their original generation and terminate even
                // if their first poll follows this successor's admission.
                state.advance_retained_claim();
                Ok::<_, crate::DemandError>(generation)
            })
            .map_err(native_materialization_work_error)?;
        Ok(NativeRetainedProducer {
            work: self.clone(),
            generation,
        })
    }

    /// Claims exact capacity now or waits for its original producer to release
    /// it. The complete requested amount is reserved under the original leaf
    /// before the waiter is woken, so another direct claimant cannot steal it.
    pub fn claim_when_released(
        &self,
        bytes: u64,
        operations: u64,
        delta: WorkCounters,
    ) -> Result<NativeRetainedClaim, NativeTreeMaterializationError> {
        let initial = self
            .state(|state| {
                state.check()?;
                if !state.retained_claims.open {
                    return Err(crate::DemandError::NativeWorkUnavailable);
                }
                // The original ledger header cannot be released while a claim
                // exists. Refuse an amount which cannot coexist even with it.
                let maximum = state
                    .original_budget()
                    .peak_allocation_bytes
                    .saturating_sub(Self::accounting_state_bytes());
                if bytes > maximum {
                    return Err(crate::WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes",
                        observed: bytes,
                        maximum,
                    }
                    .into());
                }
                let generation = state.retained_claims.generation;
                match state.claim_allocation(bytes, operations, delta) {
                    Ok(()) => Ok((generation, true)),
                    Err(error) if capacity_error(&error) => {
                        if let crate::DemandError::Work(crate::WorkError::BudgetExceeded {
                            maximum,
                            ..
                        }) = &error
                            && bytes > *maximum
                        {
                            return Err(error);
                        }
                        Ok((generation, false))
                    }
                    Err(error) => Err(error),
                }
            })
            .map_err(native_materialization_work_error)?;
        if initial.1 {
            return Ok(NativeRetainedClaim {
                waiter: None,
                immediate: Some(Ok(NativeAllocation {
                    meter: self.allocation_work(),
                    bytes,
                })),
                cancellation: None,
                _storage: None,
            });
        }
        let cancellation = self.cancellation_wait()?;
        let cancellation_bytes = cancellation.retained_storage_bytes();
        let storage_bytes = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<Waiter>())
            .map_err(|_| crate::WorkError::Overflow)? as u64;
        let storage = self.claim(storage_bytes, 1, WorkCounters::UNCHARGED)?;
        // A pending node stays live through the grant. This exact metadata is
        // admission overhead, not a source-size estimate or a new memory pool.
        let maximum = self
            .state(|state| state.original_budget().peak_allocation_bytes)
            .saturating_sub(Self::accounting_state_bytes())
            .saturating_sub(storage_bytes)
            .saturating_sub(cancellation_bytes);
        if bytes > maximum {
            return Err(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes",
                observed: bytes,
                maximum,
            }
            .into());
        }
        Ok(NativeRetainedClaim {
            waiter: Some(Arc::new(Waiter {
                generation: initial.0,
                work: self.clone(),
                bytes,
                operations,
                metadata_bytes: storage_bytes + cancellation_bytes,
                delta,
                state: Mutex::new(WaitState::default()),
            })),
            immediate: None,
            cancellation: Some(cancellation),
            _storage: Some(storage),
        })
    }
}

fn capacity_error(error: &crate::DemandError) -> bool {
    matches!(
        error,
        crate::DemandError::Work(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes",
            ..
        })
    )
}

fn impossible_with_metadata(error: &crate::DemandError, waiter: &Waiter) -> bool {
    match error {
        crate::DemandError::Work(crate::WorkError::BudgetExceeded { maximum, .. }) => {
            waiter.bytes > maximum.saturating_sub(waiter.metadata_bytes)
        }
        _ => false,
    }
}

impl DemandWork {
    // The no-waiter release path is this single branch. No atomics, extra lock,
    // allocation or event operation is performed for an ordinary release.
    pub(crate) fn advance_retained_claim(&mut self) {
        if self.retained_claims.advancing {
            return;
        }
        let Some(head) = self.retained_claims.head.upgrade() else {
            return;
        };
        if self.retained_claims.wake.is_some() {
            return;
        }
        self.scope(
            |state| &mut state.retained_claims.advancing,
            true,
            |state| {
                state.advance_retained_head(&head);
            },
        );
    }

    fn advance_retained_head(&mut self, head: &Arc<Waiter>) {
        let mut waiting = head.state.lock().unwrap_or_else(PoisonError::into_inner);
        if waiting.result.is_some() {
            return;
        }
        let result =
            if !self.retained_claims.open || head.generation != self.retained_claims.generation {
                Err(crate::DemandError::NativeWorkUnavailable)
            } else {
                head.work.with_leaf_attribution(self, |state| {
                    state.claim_allocation(head.bytes, head.operations, head.delta)
                })
            };
        match result {
            Err(error) if capacity_error(&error) && !impossible_with_metadata(&error, head) => {}
            result => {
                waiting.result = Some(
                    result
                        .map(|()| NativeAllocation {
                            meter: head.work.allocation_work(),
                            bytes: head.bytes,
                        })
                        .map_err(native_materialization_work_error),
                );
                self.retained_claims.wake = waiting.waker.take();
            }
        }
    }

    fn unlink_retained(&mut self, waiter: &Arc<Waiter>) {
        let mut waiting = waiter.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !waiting.linked {
            return;
        }
        let previous = waiting.previous.upgrade();
        let next = waiting.next.upgrade();
        match &previous {
            Some(previous) => {
                previous
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .next = waiting.next.clone()
            }
            None => self.retained_claims.head = waiting.next.clone(),
        }
        match &next {
            Some(next) => {
                next.state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .previous = waiting.previous.clone()
            }
            None => self.retained_claims.tail = waiting.previous.clone(),
        }
        waiting.linked = false;
    }
}

impl Future for NativeRetainedClaim {
    type Output = Result<NativeAllocation, NativeTreeMaterializationError>;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(result) = self.immediate.take() {
            return Poll::Ready(result);
        }
        if self
            .cancellation
            .as_mut()
            .is_some_and(|cancel| Pin::new(cancel).poll(context).is_ready())
        {
            let waiter = self.waiter.take().expect("live retained claim");
            let abandoned = waiter.work.state(|state| {
                state.unlink_retained(&waiter);
                let abandoned = waiter
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .result
                    .take();
                state.advance_retained_claim();
                abandoned
            });
            drop(abandoned);
            self.cancellation.take();
            return Poll::Ready(Err(NativeTreeMaterializationError::Cancelled));
        }
        let waiter = self
            .waiter
            .as_ref()
            .expect("retained claim polled after completion")
            .clone();
        // Waker cloning/dropping can execute user code, so do it outside leaf.
        let waker = context.waker().clone();
        let (result, old_waker) = waiter.work.state(|state| {
            let mut waiting = waiter.state.lock().unwrap_or_else(PoisonError::into_inner);
            let old_waker = waiting.waker.replace(waker);
            let mut registered = false;
            if !waiting.linked && waiting.result.is_none() {
                waiting.previous = state.retained_claims.tail.clone();
                if let Some(tail) = waiting.previous.upgrade() {
                    tail.state
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .next = Arc::downgrade(&waiter);
                } else {
                    state.retained_claims.head = Arc::downgrade(&waiter);
                }
                state.retained_claims.tail = Arc::downgrade(&waiter);
                waiting.linked = true;
                registered = true;
            }
            drop(waiting);
            if registered {
                state.advance_retained_claim();
                return (None, old_waker);
            }
            let result = waiter
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .result
                .take();
            if result.is_some() {
                state.unlink_retained(&waiter);
                state.advance_retained_claim();
            }
            (result, old_waker)
        });
        drop(old_waker);
        match result {
            Some(result) => {
                self.waiter.take();
                Poll::Ready(result)
            }
            None => Poll::Pending,
        }
    }
}

impl Drop for NativeRetainedClaim {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        let result = waiter.work.state(|state| {
            state.unlink_retained(&waiter);
            let result = waiter
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .result
                .take();
            state.advance_retained_claim();
            result
        });
        // A granted but abandoned allocation returns credit outside the leaf.
        drop(result);
    }
}

#[cfg(all(test, not(loom)))]
mod tests;
