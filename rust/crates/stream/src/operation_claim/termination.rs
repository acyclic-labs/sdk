//! One retained event for one exact live operation generation.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};
use tokio::sync::Notify;

/// Physical retirement of the observed operation owner, not a durable result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationTermination {
    /// The producer explicitly retired its ownership after finishing its work.
    Released,
    /// The producer disappeared without explicitly completing ownership.
    ProducerDropped,
}

pub(crate) struct Signal {
    terminal: AtomicU8,
    changed: Notify,
}

impl Signal {
    pub(crate) fn new() -> Self {
        Self {
            terminal: AtomicU8::new(0),
            changed: Notify::new(),
        }
    }

    pub(crate) fn is_terminated(&self) -> bool {
        self.terminal.load(Ordering::Acquire) != 0
    }

    pub(crate) fn publish(&self, terminal: OperationTermination) {
        let published = self
            .terminal
            .compare_exchange(
                0,
                match terminal {
                    OperationTermination::Released => 1,
                    OperationTermination::ProducerDropped => 2,
                },
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok();
        // Every registered observer must progress even if another stops polling.
        // Fan-out visits live observers once; observers do not relay notifications.
        if published {
            self.changed.notify_waiters();
        }
    }

    pub(crate) async fn terminated(&self) -> OperationTermination {
        let notified = self.changed.notified();
        tokio::pin!(notified);
        // Register before reading retained state, including concurrent retirement.
        notified.as_mut().enable();
        if !self.is_terminated() {
            notified.await;
        }
        match self.terminal.load(Ordering::Acquire) {
            1 => OperationTermination::Released,
            2 => OperationTermination::ProducerDropped,
            _ => unreachable!("only terminal publication wakes an operation observer"),
        }
    }
}

trait RetainedSignal: Send + Sync {
    fn signal(&self) -> &Signal;
}

struct FundedSignal<H> {
    signal: Signal,
    // Physical state is destroyed before its original admission token.
    _storage: H,
}

impl<H: Send + Sync> RetainedSignal for FundedSignal<H> {
    fn signal(&self) -> &Signal {
        &self.signal
    }
}

/// Pins the exact predecessor under the claim lock, including before first poll.
/// Replacing the operation key cannot redirect this observation to its successor.
#[derive(Clone)]
pub struct OperationObservation(Arc<dyn RetainedSignal>);

impl std::fmt::Debug for OperationObservation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OperationObservation")
            .finish_non_exhaustive()
    }
}

impl OperationObservation {
    pub(super) fn create<H: Send + Sync + 'static, E>(
        admit: &mut impl FnMut(u64, u64) -> Result<H, E>,
    ) -> Result<Self, E> {
        let storage = admit(
            acyclic_collections::arc_allocation_bytes::<FundedSignal<H>>() as u64,
            1,
        )?;
        Ok(Self(Arc::new(FundedSignal {
            signal: Signal::new(),
            _storage: storage,
        })))
    }

    pub(super) fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(super) fn publish(&self, terminal: OperationTermination) {
        self.0.signal().publish(terminal);
    }

    /// Waits once for this exact producer's terminal event. There is no stream
    /// requery, polling, global release notification or successor lookup.
    /// Retirement wakes every registered observer independently; wake work is
    /// proportional to live observers, with constant work per observer.
    pub async fn terminated(&self) -> OperationTermination {
        self.0.signal().terminated().await
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::atomic::AtomicUsize,
        task::{Context, Poll},
    };

    struct Wake(AtomicUsize);

    impl futures::task::ArcWake for Wake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn independent_observers_survive_replacement_and_cancellation() {
        for cancel_first in [false, true] {
            let claims = crate::OperationClaims::default();
            let owner = claims
                .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .unwrap();
            let observation = match claims
                .claim(
                    [1; 48],
                    || Ok::<_, ()>(()),
                    |_, _| -> Result<(), ()> { panic!("duplicate allocated") },
                )
                .unwrap()
            {
                crate::OperationAttempt::Observed(observation) => observation,
                crate::OperationAttempt::Owned(_) => panic!("duplicate became a producer"),
            };
            let first_wake = Arc::new(Wake(AtomicUsize::new(0)));
            let second_wake = Arc::new(Wake(AtomicUsize::new(0)));
            let first_waker = futures::task::waker(first_wake.clone());
            let second_waker = futures::task::waker(second_wake.clone());
            let mut first_context = Context::from_waker(&first_waker);
            let mut second_context = Context::from_waker(&second_waker);
            let mut first = Box::pin(observation.terminated());
            let mut second = Box::pin(observation.terminated());
            assert!(first.as_mut().poll(&mut first_context).is_pending());
            assert!(second.as_mut().poll(&mut second_context).is_pending());
            let unrelated = claims
                .claim([2; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .unwrap();
            drop(unrelated);
            assert_eq!(first_wake.0.load(Ordering::SeqCst), 0);
            assert_eq!(second_wake.0.load(Ordering::SeqCst), 0);
            drop(owner);
            assert_eq!(first_wake.0.load(Ordering::SeqCst), 1);
            assert_eq!(
                second_wake.0.load(Ordering::SeqCst),
                1,
                "retained operation observer blocked another observer's retirement"
            );
            // Hold a replacement live while every old observer must terminate.
            let replacement = claims
                .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .unwrap();
            assert_eq!(
                second.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::ProducerDropped)
            );
            if cancel_first {
                drop(first);
            } else {
                assert_eq!(
                    first.as_mut().poll(&mut first_context),
                    Poll::Ready(OperationTermination::ProducerDropped)
                );
            }
            let mut late = Box::pin(observation.terminated());
            assert_eq!(
                late.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::ProducerDropped)
            );
            let released = replacement.observe();
            let mut first = Box::pin(released.terminated());
            let mut second = Box::pin(released.terminated());
            assert!(first.as_mut().poll(&mut first_context).is_pending());
            assert!(second.as_mut().poll(&mut second_context).is_pending());
            let first_before = first_wake.0.load(Ordering::SeqCst);
            let second_before = second_wake.0.load(Ordering::SeqCst);
            replacement.release();
            assert_eq!(first_wake.0.load(Ordering::SeqCst), first_before + 1);
            assert_eq!(second_wake.0.load(Ordering::SeqCst), second_before + 1);
            assert_eq!(
                second.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::Released)
            );
            drop(first);
            let mut released = Box::pin(released.terminated());
            assert_eq!(
                released.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::Released)
            );
        }
    }

    #[test]
    fn forgotten_producer_is_terminated_when_its_provider_owner_drops() {
        let claims = crate::OperationClaims::default();
        let owner = claims
            .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
            .unwrap()
            .into_owner()
            .unwrap();
        let observation = owner.observe();
        std::mem::forget(owner);
        drop(claims);
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut wait = Box::pin(observation.terminated());
        assert_eq!(
            wait.as_mut().poll(&mut context),
            Poll::Ready(OperationTermination::ProducerDropped)
        );
    }
}
