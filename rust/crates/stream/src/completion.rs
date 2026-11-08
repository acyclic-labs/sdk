//! One retained result, independent of the caller waiting for it.
use crate::OperationTermination;
use crate::operation_claim::Signal;
use std::sync::{Arc, OnceLock};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

struct State<T, H, I> {
    identity: I,
    result: OnceLock<T>,
    terminal: Signal,
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

    /// Admits and owns one complete physical operation before starting it.
    /// The boxed future is destroyed before its original admission retires.
    /// Submit the returned future to the caller's executor; dropping observers
    /// stops delivery without cancelling that operation or its continuation.
    pub fn operation<F, A: Unpin, E>(
        self,
        admit: impl FnOnce(usize) -> Result<A, E>,
        start: impl FnOnce() -> Result<F, E>,
    ) -> Result<Operation<F, H, A, I>, E>
    where
        F: Future<Output = T>,
    {
        let storage = admit(std::mem::size_of::<F>())?;
        Ok(Operation {
            future: Box::pin(start()?),
            producer: Some(self),
            _storage: storage,
        })
    }
}

/// One physical future and its original producer and backing admission.
/// Admission and start callbacks are consumed before this owner is returned.
pub struct Operation<F: Future, H, A, I> {
    future: Pin<Box<F>>,
    producer: Option<Producer<F::Output, H, I>>,
    // Physical state retires before the credit that funds its backing.
    _storage: A,
}

impl<F: Future, H, A: Unpin, I> Future for Operation<F, H, A, I> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let value = std::task::ready!(self.future.as_mut().poll(cx));
        if let Some(producer) = self.producer.take() {
            producer.publish(value);
        }
        Poll::Ready(())
    }
}

impl<T, H, I> Drop for Producer<T, H, I> {
    fn drop(&mut self) {
        self.0.terminal.publish(if self.0.result.get().is_some() {
            OperationTermination::Released
        } else {
            OperationTermination::ProducerDropped
        });
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
            terminal: Signal::new(),
            storage,
        });
        (Self(Arc::clone(&state)), Producer(state))
    }

    pub(crate) fn identity(&self) -> &I {
        &self.0.identity
    }

    /// Borrows the original result. Cancellation cannot consume it or redirect
    /// the wait to another producer. All registered observers wake independently.
    pub async fn result(&self) -> Result<&T, ProducerDropped> {
        self.0.terminal.terminated().await;
        self.0.result.get().ok_or(ProducerDropped)
    }

    /// Moves the exact result and funding to the sole remaining observer.
    /// A live producer or another observer leaves this handle intact.
    pub fn try_take(self) -> Result<(I, Option<T>, H), Self> {
        if !self.0.terminal.is_terminated() {
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
