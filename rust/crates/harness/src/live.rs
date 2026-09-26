//! Live-only bounded execution for arbitrary Rust futures.

use crate::{Admission, OperationId, Outcome};
use futures::{StreamExt, stream, stream::BoxStream};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::{
    sync::{Semaphore, oneshot},
    task::{AbortHandle, JoinHandle},
};

struct ActiveGuard {
    id: OperationId,
    group: Arc<GroupState>,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.group
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(&self.id);
    }
}

struct GroupAdmission {
    closed: bool,
    active: BTreeMap<OperationId, AbortHandle>,
}

struct GroupState {
    semaphore: Arc<Semaphore>,
    admission: Mutex<GroupAdmission>,
    children: Mutex<Vec<Weak<GroupState>>>,
}

/// Controls admission and lifetime for related live tasks.
#[derive(Clone)]
pub struct TaskGroup {
    state: Arc<GroupState>,
}

impl TaskGroup {
    /// Creates a task group with bounded concurrency.
    #[must_use]
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            state: Arc::new(GroupState {
                semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
                admission: Mutex::new(GroupAdmission {
                    closed: false,
                    active: BTreeMap::new(),
                }),
                children: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Creates an independently bounded child retained in this cancellation tree.
    #[must_use]
    pub fn child(&self, max_concurrency: usize) -> Self {
        let child = Self::new(max_concurrency);
        let admission = self
            .state
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::downgrade(&child.state));
        let closed = admission.closed;
        drop(admission);
        if closed {
            child.cancel();
        }
        child
    }

    /// Admits one live task.
    pub async fn spawn<T, F>(&self, future: F) -> TaskHandle<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        match self.try_spawn(future).await {
            Admission::Accepted(handle) => handle,
            Admission::Rejected { reason } => TaskHandle {
                id: OperationId::new(),
                join: Some(tokio::spawn(
                    async move { Outcome::Failed { message: reason } },
                )),
            },
            Admission::Indeterminate { operation_id } => TaskHandle {
                id: operation_id,
                join: Some(tokio::spawn(async move {
                    Outcome::Indeterminate { operation_id }
                })),
            },
        }
    }

    /// Returns an explicit rejection when cancellation has closed admission.
    pub async fn try_spawn<T, F>(&self, future: F) -> Admission<TaskHandle<T>>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let id = OperationId::new();
        let semaphore = Arc::clone(&self.state.semaphore);
        let guard = ActiveGuard {
            id,
            group: Arc::clone(&self.state),
        };
        let (start, admitted) = oneshot::channel();
        let join = tokio::spawn(async move {
            let _guard = guard;
            let _ = admitted.await;
            match semaphore.acquire_owned().await {
                Ok(_permit) => Outcome::Succeeded(future.await),
                Err(_) => Outcome::Failed {
                    message: "task group closed".into(),
                },
            }
        });
        {
            let mut admission = self
                .state
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if admission.closed {
                join.abort();
                return Admission::Rejected {
                    reason: "task group is closed".into(),
                };
            }
            admission.active.insert(id, join.abort_handle());
        }
        let _ = start.send(());
        Admission::Accepted(TaskHandle {
            id,
            join: Some(join),
        })
    }

    /// Closes admission and requests cancellation of this group and descendants.
    pub fn cancel(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            let active = {
                let mut admission = group
                    .admission
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                admission.closed = true;
                std::mem::take(&mut admission.active)
            };
            for handle in active.values() {
                handle.abort();
            }
            let children = {
                let mut retained = group
                    .children
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let children = retained
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>();
                retained.retain(|child| child.strong_count() > 0);
                children
            };
            pending.extend(children);
        }
    }

    /// Closes admission throughout the group tree while accepted work drains.
    pub fn close(&self) {
        let mut pending = vec![Arc::clone(&self.state)];
        while let Some(group) = pending.pop() {
            group
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed = true;
            let mut retained = group
                .children
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.extend(retained.iter().filter_map(Weak::upgrade));
            retained.retain(|child| child.strong_count() > 0);
        }
    }

    /// Admits many independent tasks without serially awaiting results.
    pub async fn spawn_many<T, F, I>(&self, futures: I) -> Vec<TaskHandle<T>>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        I: IntoIterator<Item = F>,
    {
        stream::iter(futures)
            .then(|future| self.spawn(future))
            .collect()
            .await
    }

    /// Preserves one admission result per input.
    pub async fn admit_many<T, F, I>(&self, futures: I) -> Vec<Admission<TaskHandle<T>>>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        I: IntoIterator<Item = F>,
    {
        stream::iter(futures)
            .then(|future| self.try_spawn(future))
            .collect()
            .await
    }
}

/// Addressable handle for an admitted live task.
pub struct TaskHandle<T> {
    id: OperationId,
    join: Option<JoinHandle<Outcome<T>>>,
}

impl<T> TaskHandle<T> {
    /// Returns the operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        if let Some(join) = &self.join {
            join.abort();
        }
    }

    /// Waits for a terminal outcome.
    pub async fn result(mut self) -> Outcome<T> {
        let Some(join) = self.join.take() else {
            return Outcome::Failed {
                message: "task handle has no join".into(),
            };
        };
        match join.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_cancelled() => Outcome::Cancelled,
            Err(error) => Outcome::Failed {
                message: error.to_string(),
            },
        }
    }
}

/// Collects outcomes in input order.
pub async fn join_all<T>(handles: Vec<TaskHandle<T>>) -> Vec<Outcome<T>> {
    stream::iter(handles)
        .then(TaskHandle::result)
        .collect()
        .await
}

/// Streams outcomes in completion order.
pub fn completion_stream<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
) -> BoxStream<'static, (OperationId, Outcome<T>)> {
    let concurrency = handles.len().max(1);
    stream::iter(handles)
        .map(|handle| async move {
            let id = handle.id;
            (id, handle.result().await)
        })
        .buffer_unordered(concurrency)
        .boxed()
}

/// Returns the first terminal task; cancellation remains an explicit group action.
pub async fn race<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
) -> Option<(OperationId, Outcome<T>)> {
    completion_stream(handles).next().await
}

/// Folds terminal outcomes in admission order regardless of completion order.
pub async fn ordered_reduce<T, A, F>(handles: Vec<TaskHandle<T>>, initial: A, reducer: F) -> A
where
    F: FnMut(A, Outcome<T>) -> A,
{
    join_all(handles).await.into_iter().fold(initial, reducer)
}

/// Returns the first success or all failures.
pub async fn first_success<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
) -> std::result::Result<T, Vec<Outcome<T>>> {
    let mut completions = completion_stream(handles);
    let mut failures = Vec::new();
    while let Some((_id, outcome)) = completions.next().await {
        match outcome {
            Outcome::Succeeded(value) => return Ok(value),
            outcome => failures.push(outcome),
        }
    }
    Err(failures)
}

/// Collects the first `required` successes.
pub async fn quorum<T: Send + 'static>(
    handles: Vec<TaskHandle<T>>,
    required: usize,
) -> std::result::Result<Vec<T>, Vec<Outcome<T>>> {
    if required == 0 {
        return Ok(Vec::new());
    }
    let mut completions = completion_stream(handles);
    let mut successes = Vec::with_capacity(required);
    let mut failures = Vec::new();
    while let Some((_id, outcome)) = completions.next().await {
        match outcome {
            Outcome::Succeeded(value) => {
                successes.push(value);
                if successes.len() == required {
                    return Ok(successes);
                }
            }
            outcome => failures.push(outcome),
        }
    }
    Err(failures)
}

/// Recursively sums input with balanced live fork-join decomposition.
pub async fn recursive_sum(group: TaskGroup, values: Vec<u64>, leaf_size: usize) -> Outcome<u64> {
    if values.len() <= leaf_size.max(1) {
        return group
            .spawn(async move { values.into_iter().sum() })
            .await
            .result()
            .await;
    }
    let midpoint = values.len() / 2;
    let (left, right) = values.split_at(midpoint);
    let (left, right) = (left.to_vec(), right.to_vec());
    let (left_result, right_result) = tokio::join!(
        recursive_sum_boxed(group.clone(), left, leaf_size),
        recursive_sum_boxed(group, right, leaf_size),
    );
    match (left_result, right_result) {
        (Outcome::Succeeded(left), Outcome::Succeeded(right)) => Outcome::Succeeded(left + right),
        (Outcome::Failed { message }, _) | (_, Outcome::Failed { message }) => {
            Outcome::Failed { message }
        }
        (Outcome::Indeterminate { operation_id }, _)
        | (_, Outcome::Indeterminate { operation_id }) => Outcome::Indeterminate { operation_id },
        _ => Outcome::Cancelled,
    }
}

fn recursive_sum_boxed(
    group: TaskGroup,
    values: Vec<u64>,
    leaf_size: usize,
) -> std::pin::Pin<Box<dyn Future<Output = Outcome<u64>> + Send>> {
    Box::pin(recursive_sum(group, values, leaf_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn recursive_work_joins_to_expected_value() {
        assert_eq!(
            recursive_sum(TaskGroup::new(8), (1..=16).collect(), 2).await,
            Outcome::Succeeded(136)
        );
    }

    #[tokio::test]
    async fn cancellation_is_explicit() {
        let handle = TaskGroup::new(1).spawn(std::future::pending::<u64>()).await;
        handle.cancel();
        assert_eq!(handle.result().await, Outcome::Cancelled);
    }

    #[tokio::test]
    async fn first_success_observes_without_implicitly_cancelling_losers()
    -> Result<(), Box<dyn std::error::Error>> {
        struct MarkDropped(Arc<AtomicBool>);
        impl Drop for MarkDropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let (started, observed) = tokio::sync::oneshot::channel();
        let flag = Arc::clone(&dropped);
        let group = TaskGroup::new(2);
        let loser = group
            .spawn(async move {
                let _guard = MarkDropped(flag);
                let _ = started.send(());
                std::future::pending::<u64>().await
            })
            .await;
        observed.await?;
        let winner = group.spawn(async { 7_u64 }).await;
        assert_eq!(first_success(vec![loser, winner]).await, Ok(7));
        assert!(!dropped.load(Ordering::SeqCst));
        group.cancel();
        for _ in 0..16 {
            if dropped.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(dropped.load(Ordering::SeqCst));
        Ok(())
    }

    #[tokio::test]
    async fn ordered_reduction_uses_admission_order() -> Result<(), Box<dyn std::error::Error>> {
        let group = TaskGroup::new(2);
        let (release, wait) = tokio::sync::oneshot::channel();
        let first = group
            .spawn(async move {
                let _ = wait.await;
                1_u64
            })
            .await;
        let second = group.spawn(async { 2_u64 }).await;
        release.send(()).map_err(|_| "first task stopped waiting")?;
        let values = ordered_reduce(vec![first, second], Vec::new(), |mut values, outcome| {
            values.push(outcome);
            values
        })
        .await;
        assert_eq!(values, vec![Outcome::Succeeded(1), Outcome::Succeeded(2)]);
        Ok(())
    }
}
