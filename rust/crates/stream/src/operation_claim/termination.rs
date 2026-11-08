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

pub(super) struct Signal {
    terminal: AtomicU8,
    changed: Notify,
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
            signal: Signal {
                terminal: AtomicU8::new(0),
                changed: Notify::new(),
            },
            _storage: storage,
        })))
    }

    pub(super) fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(super) fn publish(&self, terminal: OperationTermination) {
        let published = self
            .0
            .signal()
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
        // One primitive wake per retirement. Consumers pass this retained wake
        // onward, so producer work is independent of the number of observers.
        if published {
            self.0.signal().changed.notify_one();
        }
    }

    /// Waits once for this exact producer's terminal event. There is no stream
    /// requery, polling, global release notification or successor lookup.
    pub async fn terminated(&self) -> OperationTermination {
        let signal = self.0.signal();
        let notified = signal.changed.notified();
        tokio::pin!(notified);
        // Registration precedes the state read. Tokio's intrusive notification
        // transfers a notification when its selected waiter is cancelled.
        notified.as_mut().enable();
        if signal.terminal.load(Ordering::Acquire) == 0 {
            notified.await;
        }
        let terminal = match signal.terminal.load(Ordering::Acquire) {
            1 => OperationTermination::Released,
            2 => OperationTermination::ProducerDropped,
            _ => unreachable!("only terminal publication wakes an operation observer"),
        };
        signal.changed.notify_one();
        terminal
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
    fn exact_generation_survives_replacement_and_cancelled_notification_handoff() {
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
            // Hold a replacement live while every old observer must terminate.
            let replacement = claims
                .claim([1; 48], || Ok::<_, ()>(()), |_, _| Ok(()))
                .unwrap()
                .into_owner()
                .unwrap();
            if cancel_first {
                drop(first);
            } else {
                assert_eq!(
                    first.as_mut().poll(&mut first_context),
                    Poll::Ready(OperationTermination::ProducerDropped)
                );
            }
            assert!(second_wake.0.load(Ordering::SeqCst) > 0);
            assert_eq!(
                second.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::ProducerDropped)
            );
            let mut late = Box::pin(observation.terminated());
            assert_eq!(
                late.as_mut().poll(&mut second_context),
                Poll::Ready(OperationTermination::ProducerDropped)
            );
            let released = replacement.observe();
            replacement.release();
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
