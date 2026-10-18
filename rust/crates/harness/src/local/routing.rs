//! Routing a prompt submitted beneath a parent turn.
//!
//! Routing observes the continuation, checkpoint, parent outcome and existing
//! admission, then continues or parents the turn. Observation and action share
//! one admission-gate critical section: a concurrent submission of the same
//! operation either finishes first, and is observed, or waits.
use super::*;

/// How a prompt beneath `parent` is admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Route {
    /// A new root bound to the parent's selected checkpoint. `history`
    /// captures the parent's whole history first when nothing is selected.
    Continue { history: bool },
    /// An ordinary child of a live parent.
    Child,
}

/// Decides the route from one gated observation.
pub(super) fn route(
    parent: TaskId,
    continuation: bool,
    selected: bool,
    settled: bool,
    existing: Option<Option<TaskId>>,
) -> Result<Route> {
    if continuation {
        return Ok(Route::Continue { history: false });
    }
    if selected && existing.is_some_and(|retained| retained != Some(parent)) {
        return Err(Error::Conflict(
            "task admission belongs to another conversation".into(),
        ));
    }
    // A completed conversation is continued, never parented.
    Ok(if settled && existing.is_none() {
        Route::Continue { history: !selected }
    } else {
        Route::Child
    })
}

/// Observes and acts under one hold of the admission gate.
pub(super) async fn routed<R, T>(
    gate: &crate::sync::owned::Arc<crate::sync::owned::Mutex<()>>,
    observe: impl AsyncFnOnce() -> Result<R>,
    act: impl AsyncFnOnce(R) -> Result<T>,
) -> Result<T> {
    #[cfg(all(test, acyclic_harness_loom))]
    if model::observes_outside_the_gate() {
        let observed = observe().await?;
        return super::admission::serialized(gate, async || act(observed).await).await;
    }
    super::admission::serialized(gate, async || act(observe().await?).await).await
}

#[cfg(all(test, acyclic_harness_loom))]
mod model;
