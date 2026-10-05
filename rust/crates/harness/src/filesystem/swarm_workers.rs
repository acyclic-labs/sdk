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
