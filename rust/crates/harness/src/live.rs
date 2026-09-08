//! Live-only bounded execution for arbitrary Rust futures.

use crate::{Admission, OperationId, Outcome};
use futures::{StreamExt, stream, stream::BoxStream};
use std::{future::Future, sync::Arc};
use tokio::{sync::Semaphore, task::JoinHandle};

/// Controls admission and lifetime for related live tasks.
#[derive(Clone)]
pub struct TaskGroup {
    semaphore: Arc<Semaphore>,
}

impl TaskGroup {
    /// Creates a task group with bounded concurrency.
    #[must_use]
    pub fn new(max_concurrency: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
        }
    }

    /// Admits one live task.
    pub async fn spawn<T, F>(&self, future: F) -> TaskHandle<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let id = OperationId::new();
        let semaphore = Arc::clone(&self.semaphore);
        let join = tokio::spawn(async move {
            match semaphore.acquire_owned().await {
                Ok(_permit) => Outcome::Succeeded(future.await),
                Err(_) => Outcome::Failed {
                    message: "task group closed".into(),
                },
            }
        });
        TaskHandle { id, join }
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
        self.spawn_many(futures)
            .await
            .into_iter()
            .map(Admission::Accepted)
            .collect()
    }
}

/// Addressable handle for an admitted live task.
pub struct TaskHandle<T> {
    id: OperationId,
    join: JoinHandle<Outcome<T>>,
}

impl<T> TaskHandle<T> {
    /// Returns the operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation.
    pub fn cancel(&self) {
        self.join.abort();
    }

    /// Waits for a terminal outcome.
    pub async fn result(self) -> Outcome<T> {
        match self.join.await {
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
    let (left, right) = (values[..midpoint].to_vec(), values[midpoint..].to_vec());
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
}
