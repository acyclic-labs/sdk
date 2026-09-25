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

/// Builds `create`'s future directly in a heap allocation.
///
/// The future is built in this function's own frame, which returns before the
/// future is first polled, so no poll frame ever holds it.
#[inline(never)]
pub(crate) fn in_heap<F: Future>(create: impl FnOnce() -> F) -> Pin<Box<F>> {
    Box::pin(create())
}
