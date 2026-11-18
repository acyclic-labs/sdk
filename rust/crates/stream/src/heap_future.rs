//! Futures built on the heap, so callers hold a pointer instead of them.
//!
//! A future holds every future it is awaiting, so an operation whose layers
//! await each other is one value as large as its deepest path, and each poll
//! frame reserves room for the futures it builds and awaits. A native mount
//! callback polls a whole operation on the driver's thread, so that size
//! becomes stack depth: before this boundary existed, one lazy rename used
//! 1.8 MiB of a 2 MiB FUSE thread in an unoptimized build.
//!
//! [`in_heap`] builds a future in a short frame of its own and moves it into
//! a box. An async function whose body runs through it is a pointer to its
//! callers, and none of its state enters their futures or frames.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// The future and everything it captures die before its allocation charge.
pub struct OwnedFuture<F: ?Sized, A> {
    future: Pin<Box<F>>,
    allocation: A,
}

impl<F: Future + ?Sized, A: Unpin> Future for OwnedFuture<F, A> {
    type Output = F::Output;

    // Boxing construction alone does not isolate polling: a concrete F's
    // poll can be inlined back into every awaiting caller. Keep one shared
    // poll boundary as well, so sibling phases cannot combine their frames.
    #[inline(never)]
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(context)
    }
}

impl<F: ?Sized, A: Unpin> OwnedFuture<F, A> {
    /// The future pinned again, for a caller that drives it in place.
    pub fn as_mut(&mut self) -> Pin<&mut Self> {
        Pin::new(self)
    }

    /// The boxed future behind this allocation, for size and address checks.
    pub fn as_ref(&self) -> Pin<&F> {
        self.future.as_ref()
    }
}

impl<F: Future, A> OwnedFuture<F, A> {
    /// Erases the body at a provider boundary without moving or reallocating
    /// its storage, so admission still covers the concrete future's bytes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn erase_send<'a>(self) -> OwnedFuture<dyn Future<Output = F::Output> + Send + 'a, A>
    where
        F: Send + 'a,
    {
        OwnedFuture {
            future: self.future,
            allocation: self.allocation,
        }
    }
}

/// A heap future erased to the platform executor's bound: `Send` natively,
/// event-loop owned in browsers, as `acyclic_stream::ProviderTask` is.
#[cfg(not(target_arch = "wasm32"))]
pub type TaskFuture<'a, T> = dyn Future<Output = T> + Send + 'a;
#[cfg(target_arch = "wasm32")]
pub type TaskFuture<'a, T> = dyn Future<Output = T> + 'a;

impl<F: Future, A> OwnedFuture<F, A> {
    /// Erases the body to [`TaskFuture`] without moving its storage.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn erase_task<'a>(self) -> OwnedFuture<TaskFuture<'a, F::Output>, A>
    where
        F: Send + 'a,
    {
        self.erase_send()
    }

    /// Erases the body to [`TaskFuture`] without moving its storage.
    #[cfg(target_arch = "wasm32")]
    pub fn erase_task<'a>(self) -> OwnedFuture<TaskFuture<'a, F::Output>, A>
    where
        F: 'a,
    {
        OwnedFuture {
            future: self.future,
            allocation: self.allocation,
        }
    }
}

impl<F> OwnedFuture<F, ()> {
    /// Attaches the allocation charge the box was admitted for.
    pub fn with_allocation<A>(self, allocation: A) -> OwnedFuture<F, A> {
        OwnedFuture {
            future: self.future,
            allocation,
        }
    }
}

/// Builds `create`'s future directly in a heap allocation.
///
/// The future is built in this function's own frame, which returns before the
/// future is first polled, so no poll frame ever holds it.
#[inline(never)]
pub fn in_heap<F: Future>(create: impl FnOnce() -> F) -> OwnedFuture<F, ()> {
    OwnedFuture {
        future: Box::pin(create()),
        allocation: (),
    }
}
