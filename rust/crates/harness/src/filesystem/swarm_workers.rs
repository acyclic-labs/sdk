//! Live ownership only. Durable admission, budgets and outcomes remain in Harness journals.

use super::LocalForkOutcome;
use crate::{Admission, Error, Result, TaskGroup, TaskHandle, TaskId};
use std::{collections::BTreeMap, future::Future};
use tokio::sync::Mutex;

#[derive(Default)]
struct Workers {
    closed: bool,
    handles: BTreeMap<TaskId, TaskHandle<Result<LocalForkOutcome>>>,
}

pub(super) struct LocalChildWorkers {
    group: TaskGroup,
    workers: Mutex<Workers>,
}

impl Drop for LocalChildWorkers {
    fn drop(&mut self) {
        // The async shutdown path joins every handle. A synchronous drop may
        // not await, but it can still close admission and abort the owned
        // futures before the mutex releases their handles.
        self.group.cancel();
    }
}

impl Default for LocalChildWorkers {
    fn default() -> Self {
        Self {
            // Whole-agent semaphore permits would deadlock a parent waiting
            // for descendants. Session budget admission owns resource bounds;
            // this group only owns live futures and their cancellation.
            group: TaskGroup::new(tokio::sync::Semaphore::MAX_PERMITS),
            workers: Mutex::new(Workers::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::sync::oneshot;

    struct DropNotice(Option<oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    fn pending_worker(
        started: oneshot::Sender<()>,
        dropped: oneshot::Sender<()>,
    ) -> impl Future<Output = Result<LocalForkOutcome>> + Send + 'static {
        async move {
            let _notice = DropNotice(Some(dropped));
            let _ = started.send(());
            std::future::pending::<Result<LocalForkOutcome>>().await
        }
    }

    #[tokio::test]
    async fn duplicate_child_is_rejected_before_the_new_future_starts()
    -> Result<(), Box<dyn std::error::Error>> {
        let workers = LocalChildWorkers::default();
        let task = TaskId::new();
        let (started, started_observed) = oneshot::channel();
        let (dropped, dropped_observed) = oneshot::channel();
        workers
            .enqueue(task, pending_worker(started, dropped))
            .await?;
        started_observed.await?;

        let second_started = Arc::new(AtomicBool::new(false));
        let second_started_for_future = Arc::clone(&second_started);
        let error = workers
            .enqueue(task, async move {
                second_started_for_future.store(true, Ordering::Release);
                Err(Error::Conflict("duplicate future ran".into()))
            })
            .await
            .expect_err("duplicate child was admitted");
        assert!(matches!(error, Error::Conflict(_)));
        assert!(!second_started.load(Ordering::Acquire));

        workers.shutdown().await;
        tokio::time::timeout(Duration::from_secs(1), dropped_observed).await??;
        Ok(())
    }

    #[tokio::test]
    async fn dropping_the_composition_registry_cancels_owned_workers()
    -> Result<(), Box<dyn std::error::Error>> {
        let workers = LocalChildWorkers::default();
        let task = TaskId::new();
        let (started, started_observed) = oneshot::channel();
        let (dropped, dropped_observed) = oneshot::channel();
        workers
            .enqueue(task, pending_worker(started, dropped))
            .await?;
        started_observed.await?;
        drop(workers);
        tokio::time::timeout(Duration::from_secs(1), dropped_observed).await??;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_joins_workers_and_closes_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let workers = LocalChildWorkers::default();
        let task = TaskId::new();
        let (started, started_observed) = oneshot::channel();
        let (dropped, dropped_observed) = oneshot::channel();
        workers
            .enqueue(task, pending_worker(started, dropped))
            .await?;
        started_observed.await?;

        workers.shutdown().await;
        tokio::time::timeout(Duration::from_secs(1), dropped_observed).await??;
        let second_started = Arc::new(AtomicBool::new(false));
        let second_started_for_future = Arc::clone(&second_started);
        let error = workers
            .enqueue(task, async move {
                second_started_for_future.store(true, Ordering::Release);
                Err(Error::Conflict("closed future ran".into()))
            })
            .await
            .expect_err("closed child registry admitted a worker");
        assert!(matches!(error, Error::Conflict(_)));
        assert!(!second_started.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_shutdown_future_keeps_handles_owned()
    -> Result<(), Box<dyn std::error::Error>> {
        let workers = Arc::new(LocalChildWorkers::default());
        let task = TaskId::new();
        let (started, started_observed) = oneshot::channel();
        let (dropped, dropped_observed) = oneshot::channel();
        workers
            .enqueue(task, pending_worker(started, dropped))
            .await?;
        started_observed.await?;

        // Hold the registry mutex so shutdown is pending before it can take
        // ownership of the handle map. Cancelling that future must not detach
        // the worker; dropping the composition still owns its cleanup.
        let lock = workers.workers.lock().await;
        let shutdown_workers = Arc::clone(&workers);
        let shutdown = tokio::spawn(async move { shutdown_workers.shutdown().await });
        tokio::task::yield_now().await;
        shutdown.abort();
        assert!(shutdown.await.expect_err("shutdown unexpectedly completed").is_cancelled());
        assert!(!dropped_observed.is_closed());
        drop(lock);
        drop(workers);
        tokio::time::timeout(Duration::from_secs(1), dropped_observed).await??;
        Ok(())
    }

    #[tokio::test]
    async fn worker_registry_has_no_strong_arc_cycle()
    -> Result<(), Box<dyn std::error::Error>> {
        let workers = Arc::new(LocalChildWorkers::default());
        let weak = Arc::downgrade(&workers);
        let task = TaskId::new();
        let (started, started_observed) = oneshot::channel();
        let (dropped, dropped_observed) = oneshot::channel();
        let worker_weak = weak.clone();
        workers
            .enqueue(task, async move {
                let _ = worker_weak.strong_count();
                let _notice = DropNotice(Some(dropped));
                let _ = started.send(());
                std::future::pending::<Result<LocalForkOutcome>>().await
            })
            .await?;
        started_observed.await?;
        drop(workers);
        assert!(weak.upgrade().is_none());
        tokio::time::timeout(Duration::from_secs(1), dropped_observed).await??;
        Ok(())
    }
}

impl LocalChildWorkers {
    pub(super) async fn ensure_open(&self) -> Result<()> {
        if self.workers.lock().await.closed {
            return Err(Error::Conflict("local child workers are shut down".into()));
        }
        Ok(())
    }

    pub(super) async fn contains(&self, task: TaskId) -> bool {
        self.workers.lock().await.handles.contains_key(&task)
    }

    pub(super) async fn enqueue(
        &self,
        task: TaskId,
        future: impl Future<Output = Result<LocalForkOutcome>> + Send + 'static,
    ) -> Result<()> {
        let mut workers = self.workers.lock().await;
        if workers.closed {
            return Err(Error::Conflict("local child workers are shut down".into()));
        }
        if workers.handles.contains_key(&task) {
            return Err(Error::Conflict(
                "local child already has a live owner".into(),
            ));
        }
        match self.group.try_spawn(future).await {
            Admission::Accepted(handle) => {
                workers.handles.insert(task, handle);
                Ok(())
            }
            Admission::Rejected { reason } => Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => Err(Error::Indeterminate(operation_id)),
        }
    }

    pub(super) async fn shutdown(&self) {
        let handles = {
            let mut workers = self.workers.lock().await;
            workers.closed = true;
            self.group.cancel();
            std::mem::take(&mut workers.handles)
        };
        for handle in handles.into_values() {
            // Join cancelled futures before returning. Durable admitted
            // effects remain subject to their provider recovery guarantee.
            let _ = handle.result().await;
        }
    }
}
