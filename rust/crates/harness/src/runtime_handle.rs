//! Persistent handles for FFI-owned runtime tasks.
//!
//! This module is intentionally not wired into the crate yet.  It is the
//! smallest wrapper needed by a future WASM/N-API surface to retain a task
//! result after the underlying [`RuntimeTask`] has been consumed by its first
//! observation.  Scheduling, admission, cancellation, and outcome semantics
//! remain in `runtime.rs`.

use crate::{Error, OperationId, Outcome, Result};
use crate::runtime::{RuntimeTask, TaskCancellation};
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

struct PersistentState<O> {
    task: Option<RuntimeTask<O>>,
    terminal: Option<Arc<Result<Outcome<O>>>>,
    indeterminate: Option<OperationId>,
    observing: bool,
}

/// A stable task handle whose terminal result can be observed repeatedly.
///
/// `RuntimeTask::result` consumes the Rust task value.  FFI callers generally
/// retain one JavaScript/native handle and may ask for its result more than
/// once, so this wrapper stores the first terminal observation behind a shared
/// state cell. The result is returned through an `Arc` to avoid requiring the
/// task output type to implement `Clone`.
pub struct PersistentRuntimeTask<O> {
    identity: String,
    cancellation: TaskCancellation,
    state: Mutex<PersistentState<O>>,
    wake: Notify,
}

impl<O> PersistentRuntimeTask<O> {
    /// Retains one admitted runtime task for an FFI-facing handle.
    #[must_use]
    pub fn new(task: RuntimeTask<O>) -> Self {
        let identity = task.identity();
        let cancellation = task.cancellation();
        Self {
            identity,
            cancellation,
            state: Mutex::new(PersistentState {
                task: Some(task),
                terminal: None,
                indeterminate: None,
                observing: false,
            }),
            wake: Notify::new(),
        }
    }

    /// Returns the stable operation or durable task identity.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.identity
    }
}

impl<O> PersistentRuntimeTask<O>
where
    O: serde::de::DeserializeOwned + Send + 'static,
{
    /// Observes the task once and returns the same terminal result thereafter.
    ///
    /// Successful outcomes, failures, and confirmed cancellation are cached.
    /// An indeterminate observation is deliberately kept out of that terminal
    /// cache: it is reconciliation state, and a caller must reattach through
    /// the owning harness before treating the task as settled. Until then,
    /// later calls return [`Error::Indeterminate`] rather than inventing a
    /// terminal outcome.
    pub async fn result(&self) -> Arc<Result<Outcome<O>>> {
        loop {
            let task = {
                let mut state = self.state.lock().await;
                if let Some(result) = &state.terminal {
                    return Arc::clone(result);
                }
                if let Some(operation_id) = state.indeterminate {
                    return Arc::new(Err(Error::Indeterminate(operation_id)));
                }
                if state.observing {
                    None
                } else {
                    state.observing = true;
                    state.task.take()
                }
            };
            let Some(task) = task else {
                self.wake.notified().await;
                continue;
            };

            let result = Arc::new(task.result().await);
            let mut state = self.state.lock().await;
            state.observing = false;
            match result.as_ref() {
                Ok(Outcome::Indeterminate { operation_id })
                | Err(Error::Indeterminate(operation_id)) => {
                    state.indeterminate = Some(*operation_id);
                }
                _ => {
                    state.terminal = Some(Arc::clone(&result));
                }
            }
            self.wake.notify_waiters();
            return result;
        }
    }

    /// Requests cancellation while the underlying task is still available.
    ///
    /// The cancellation capability is independent of result observation and
    /// remains valid after the underlying `RuntimeTask` has been consumed.
    pub async fn cancel(&self) -> Result<()> {
        self.cancellation.cancel().await
    }
}
