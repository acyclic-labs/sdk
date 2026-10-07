//! Persistent handles for FFI-owned runtime tasks.
//!
//! This module provides the smallest stable wrapper needed by a WASM/N-API
//! surface to retain a task result after the underlying [`RuntimeTask`] has
//! been consumed by its first observation. Scheduling, admission, cancellation,
//! and outcome semantics remain in `runtime.rs`.

use crate::distributed::{MAX_TASK_EVENT_PAGE, TaskEventPage};
use crate::runtime::{RuntimeTask, TaskCancellation, TaskStateProvider};
use crate::{Error, OperationId, Outcome, Result, TaskId};
use futures::future::{BoxFuture, FutureExt, Shared};
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedResult<O> = Shared<BoxFuture<'static, Arc<Result<Outcome<O>>>>>;

struct PersistentState<O> {
    task: Option<RuntimeTask<O>>,
    pending: Option<SharedResult<O>>,
    terminal: Option<Arc<Result<Outcome<O>>>>,
    indeterminate: Option<OperationId>,
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
    event_source: Option<(TaskId, OperationId, Arc<dyn TaskStateProvider>)>,
    state: Mutex<PersistentState<O>>,
}

impl<O> PersistentRuntimeTask<O> {
    /// Retains one admitted runtime task for an FFI-facing handle.
    #[must_use]
    pub fn new(task: RuntimeTask<O>) -> Self {
        let identity = task.identity();
        let cancellation = task.cancellation();
        let event_source = task.scheduler_event_source();
        Self {
            identity,
            cancellation,
            event_source,
            state: Mutex::new(PersistentState {
                task: Some(task),
                pending: None,
                terminal: None,
                indeterminate: None,
            }),
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
    /// the owning harness before treating the task as settled. Reattachment
    /// creates a fresh handle from the owner-provided task with the same
    /// identity; this handle never swaps a different task into its state.
    /// Until then, later calls return [`Error::Indeterminate`] rather than
    /// inventing a terminal outcome.
    pub async fn result(&self) -> Arc<Result<Outcome<O>>> {
        let pending = {
            let mut state = self.state.lock().await;
            if let Some(result) = &state.terminal {
                return Arc::clone(result);
            }
            if let Some(operation_id) = state.indeterminate {
                return Arc::new(Err(Error::Indeterminate(operation_id)));
            }
            if let Some(pending) = &state.pending {
                pending.clone()
            } else {
                let Some(task) = state.task.take() else {
                    return Arc::new(Err(Error::Invalid(
                        "persistent runtime task has no result future".into(),
                    )));
                };
                let pending = async move { Arc::new(task.result().await) }
                    .boxed()
                    .shared();
                state.pending = Some(pending.clone());
                pending
            }
        };

        let result = pending.await;
        let mut state = self.state.lock().await;
        match result.as_ref() {
            Ok(Outcome::Indeterminate { operation_id })
            | Err(Error::Indeterminate(operation_id)) => {
                state.indeterminate = Some(*operation_id);
            }
            _ => {
                state.terminal = Some(Arc::clone(&result));
            }
        }
        state.pending = None;
        result
    }

    /// Requests cancellation through the retained task capability.
    ///
    /// The cancellation capability is independent of result observation and
    /// remains valid after the underlying `RuntimeTask` has been consumed.
    pub async fn cancel(&self) -> Result<()> {
        self.cancellation.cancel().await
    }

    /// Replays one authenticated page of durable scheduler history.
    ///
    /// The page cursor is the coordinator revision. Live tasks have no
    /// durable event source and return `Unsupported`.
    pub async fn scheduler_events(&self, after_revision: u64, limit: u32) -> Result<TaskEventPage> {
        if limit == 0 || limit > MAX_TASK_EVENT_PAGE {
            return Err(Error::Invalid(
                "task event page limit is out of bounds".into(),
            ));
        }
        let Some((task_id, operation_id, host)) = &self.event_source else {
            return Err(Error::Unsupported(
                "live task event replay is not durable".into(),
            ));
        };
        let page = host
            .scheduler_events_for(*task_id, *operation_id, after_revision, limit)
            .await?;
        page.validate_for(*operation_id, after_revision, limit)?;
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::PersistentRuntimeTask;
    use crate::{Error, OperationId, Outcome, live::TaskGroup, runtime::RuntimeTask};
    use std::sync::Arc;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn cancellation_survives_result_observation() {
        let group = TaskGroup::new(1);
        let (started, started_rx) = oneshot::channel();
        let task = group
            .spawn(async move {
                let _ = started.send(());
                std::future::pending::<Result<i32, Error>>().await
            })
            .await;
        let handle = Arc::new(PersistentRuntimeTask::new(RuntimeTask::Live(task)));
        let waiting_handle = Arc::clone(&handle);
        let waiter = tokio::spawn(async move { waiting_handle.result().await });

        assert!(started_rx.await.is_ok(), "task did not start");
        assert!(handle.cancel().await.is_ok(), "cancellation request failed");

        let observed = waiter.await;
        assert!(observed.is_ok(), "result observer task failed");
        if let Ok(observed) = observed {
            assert!(matches!(observed.as_ref(), Ok(Outcome::Cancelled)));
        }
    }

    #[tokio::test]
    async fn indeterminate_observation_is_not_cached_as_terminal() {
        let operation_id = OperationId::new();
        let task = TaskGroup::new(1)
            .spawn(async move { Err::<i32, Error>(Error::Indeterminate(operation_id)) })
            .await;
        let handle = PersistentRuntimeTask::new(RuntimeTask::Live(task));

        let first = handle.result().await;
        assert!(matches!(
            first.as_ref(),
            Ok(Outcome::Indeterminate { operation_id: observed })
                if observed == &operation_id
        ));

        let second = handle.result().await;
        assert!(matches!(
            second.as_ref(),
            Err(Error::Indeterminate(observed)) if observed == &operation_id
        ));
    }

    #[tokio::test]
    async fn concurrent_observers_share_terminal_result() {
        let group = TaskGroup::new(1);
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let task = group
            .spawn(async move {
                let _ = started.send(());
                let _ = release_rx.await;
                Ok::<i32, Error>(7)
            })
            .await;
        let handle = PersistentRuntimeTask::new(RuntimeTask::Live(task));

        let releaser = tokio::spawn(async move {
            let _ = started_rx.await;
            let _ = release.send(());
        });
        let observed = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(handle.result(), handle.result())
        })
        .await;
        assert!(observed.is_ok(), "concurrent observers did not complete");
        if let Ok((first, second)) = observed {
            assert!(matches!(
                first.as_ref(),
                Ok(Outcome::Succeeded(value)) if *value == 7
            ));
            assert!(matches!(
                second.as_ref(),
                Ok(Outcome::Succeeded(value)) if *value == 7
            ));
        }
        assert!(releaser.await.is_ok(), "release task failed");
    }

    #[tokio::test]
    async fn dropping_result_observer_keeps_task_observable() {
        let group = TaskGroup::new(1);
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let task = group
            .spawn(async move {
                let _ = started.send(());
                let _ = release_rx.await;
                Ok::<i32, Error>(7)
            })
            .await;
        let handle = Arc::new(PersistentRuntimeTask::new(RuntimeTask::Live(task)));
        let first_handle = Arc::clone(&handle);
        let first = tokio::spawn(async move { first_handle.result().await });

        assert!(started_rx.await.is_ok(), "task did not start");
        first.abort();
        assert!(
            first.await.is_err(),
            "aborted observer unexpectedly completed"
        );
        assert!(release.send(()).is_ok(), "task release failed");

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), handle.result()).await;
        assert!(
            second.is_ok(),
            "task was lost when its first observer dropped"
        );
        if let Ok(second) = second {
            assert!(matches!(
                second.as_ref(),
                Ok(Outcome::Succeeded(value)) if *value == 7
            ));
        }
    }

    #[tokio::test]
    async fn dropped_observer_can_cancel_before_later_result() {
        let group = TaskGroup::new(1);
        let (started, started_rx) = oneshot::channel();
        let task = group
            .spawn(async move {
                let _ = started.send(());
                std::future::pending::<Result<i32, Error>>().await
            })
            .await;
        let handle = Arc::new(PersistentRuntimeTask::new(RuntimeTask::Live(task)));
        let first_handle = Arc::clone(&handle);
        let first = tokio::spawn(async move { first_handle.result().await });

        assert!(started_rx.await.is_ok(), "task did not start");
        first.abort();
        assert!(
            first.await.is_err(),
            "aborted observer unexpectedly completed"
        );
        assert!(handle.cancel().await.is_ok(), "cancellation request failed");

        let second = tokio::time::timeout(std::time::Duration::from_secs(1), handle.result()).await;
        assert!(second.is_ok(), "cancelled task was not observable");
        if let Ok(second) = second {
            assert!(matches!(second.as_ref(), Ok(Outcome::Cancelled)));
        }
    }
}
