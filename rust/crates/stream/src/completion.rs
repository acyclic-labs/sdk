//! One retained result, independent of the caller waiting for it.
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

struct State<T, H, I> {
    identity: I,
    result: OnceLock<T>,
    finished: AtomicBool,
    changed: Notify,
    // The result and event retire before their original admission token.
    storage: H,
}

/// The sole producer disappeared before publishing its result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerDropped;

/// Retains one producer's exact result and original funding.
pub struct Completion<T, H = (), I = ()>(Arc<State<T, H, I>>);

impl<T, H, I> Clone for Completion<T, H, I> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// The sole producer. Dropping it terminates every observer, before first poll
/// as well as after submission. Dropping observers never cancels the producer.
pub struct Producer<T, H = (), I = ()>(Arc<State<T, H, I>>);

impl<T, H, I> Producer<T, H, I> {
    /// Publishes the original result without copying it or its allocation owner.
    pub fn publish(self, result: T) {
        assert!(self.0.result.set(result).is_ok());
    }
}

impl<T, H, I> Drop for Producer<T, H, I> {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        self.0.changed.notify_one();
    }
}

impl<T, H, I> Completion<T, H, I> {
    /// Actual shared control allocation, including the result slot and funding
    /// token. Payload backing is supplied by its own original retained owner.
    pub const fn allocation_bytes() -> usize {
        acyclic_collections::arc_allocation_bytes::<State<T, H, I>>()
    }

    pub(crate) fn prepare(identity: I, storage: H) -> (Self, Producer<T, H, I>) {
        let state = Arc::new(State {
            identity,
            result: OnceLock::new(),
            finished: AtomicBool::new(false),
            changed: Notify::new(),
            storage,
        });
        (Self(Arc::clone(&state)), Producer(state))
    }

    pub(crate) fn identity(&self) -> &I {
        &self.0.identity
    }

    /// Borrows the original result. Cancellation cannot consume it or redirect
    /// the wait to another producer. Each waiter passes one terminal wake onward.
    pub async fn result(&self) -> Result<&T, ProducerDropped> {
        let state = &self.0;
        let notified = state.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !state.finished.load(Ordering::Acquire) {
            notified.await;
        }
        let result = state.result.get().ok_or(ProducerDropped);
        state.changed.notify_one();
        result
    }

    /// Moves the exact result and funding to the sole remaining observer.
    /// A live producer or another observer leaves this handle intact.
    pub fn try_take(self) -> Result<(I, Option<T>, H), Self> {
        if !self.0.finished.load(Ordering::Acquire) {
            return Err(self);
        }
        match Arc::try_unwrap(self.0) {
            Ok(state) => Ok((state.identity, state.result.into_inner(), state.storage)),
            Err(state) => Err(Self(state)),
        }
    }
}

/// Admits one bounded result slot before allocation. There is one producer,
/// one allocation and no queue, retry, timer or registration allocation.
pub fn channel<T, H, E>(
    admit: impl FnOnce(usize) -> Result<H, E>,
) -> Result<(Completion<T, H>, Producer<T, H>), E> {
    let storage = admit(Completion::<T, H>::allocation_bytes())?;
    Ok(Completion::prepare((), storage))
}
