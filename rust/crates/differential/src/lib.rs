//! Thread-local operation traces shared by every crate in a test build.
//!
//! Production crates record observation points here behind their
//! `test-support` feature, and test captures (`acyclic-test-support`) read
//! them back. Keeping the storage in this one published crate lets a capture
//! in a dependent crate observe points recorded by its dependencies; a
//! `cfg(test)` gate would compile them out of every dependency.
//!
//! # Rule for published crates
//!
//! `cfg(test)` is set only for the crate whose own tests are being built, so
//! a hook behind `cfg(test)` is invisible to every other crate's tests: a
//! capture there silently observes nothing. Hooks consumed across crates
//! therefore use the producing crate's `test-support` feature and record
//! through this module, never through `cfg(test)` alone.
//!
//! The test-only crate `acyclic-test-support` is `publish = false`, so a
//! published crate may name it only as a dev-dependency; even an optional
//! normal dependency breaks `cargo package`. Trace storage lives here, in a
//! published leaf crate with no dependencies, for that reason: production
//! crates name it as an optional dependency of their `test-support` feature,
//! and `acyclic-test-support` names it without depending on any crate whose
//! tests use it, so no dev-dependency cycle forms.
//! `scripts/publish-cargo-crates.mjs check` enforces the dependency half of
//! this rule in CI, and `scripts/check-dev-dependency-cycles.mjs` the cycle
//! half.

use std::{any::Any, cell::RefCell};

thread_local! {
    static TRACE: RefCell<Option<Box<dyn Any>>> = const { RefCell::new(None) };
}

/// Records an event into an active capture of the same event type. Inactive
/// recording, or an event of another type, does nothing: independent
/// observation points share code paths, and a capture subscribes to exactly
/// one of them by its type.
pub fn record<T: 'static>(event: T) {
    let mut pending = Some(event);
    TRACE.with_borrow_mut(|trace| {
        if let Some(events) = trace
            .as_mut()
            .and_then(|trace| trace.downcast_mut::<Vec<T>>())
            && let Some(event) = pending.take()
        {
            events.push(event);
        }
    });
    // Inactive events may themselves record from Drop.
    drop(pending);
}

/// Whether a capture is active on this thread.
pub fn capturing() -> bool {
    TRACE.with_borrow(Option::is_some)
}

/// Starts capturing events of type `T` on this thread. Callers check
/// [`capturing`] first, so an active capture is never replaced.
pub fn begin<T: 'static>() {
    TRACE.with_borrow_mut(|trace| *trace = Some(Box::new(Vec::<T>::new())));
}

/// Ends this thread's capture and returns its events, if one was active.
/// Event destructors may record, so the caller drops the result outside the
/// trace borrow.
pub fn end() -> Option<Box<dyn Any>> {
    TRACE.with_borrow_mut(Option::take)
}
