//! A small composable Tokio harness for bounded recursive fork-join work.

use acyclic_contracts::{OperationId, Outcome};
use futures::{StreamExt, stream};
use std::{future::Future, sync::Arc};
use tokio::{sync::Semaphore, task::JoinHandle};

/// Controls admission and lifetime for related tasks.
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

    /// Admits one task and returns its handle before completion.
    pub async fn spawn<T, F>(&self, future: F) -> TaskHandle<T>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let id = OperationId::new();
        let semaphore = Arc::clone(&self.semaphore);
        let join = tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await;
            match permit {
                Ok(_permit) => Outcome::Succeeded(future.await),
                Err(_) => Outcome::Failed {
                    message: "task group closed".into(),
                },
            }
        });
        TaskHandle { id, join }
    }

    /// Admits many independent tasks without serially awaiting their results.
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
}

/// Addressable handle for an admitted task.
pub struct TaskHandle<T> {
    id: OperationId,
    join: JoinHandle<Outcome<T>>,
}

impl<T> TaskHandle<T> {
    /// Returns the durable operation identity.
    #[must_use]
    pub fn id(&self) -> &OperationId {
        &self.id
    }

    /// Requests cancellation of local execution.
    pub fn cancel(&self) {
        self.join.abort();
    }

    /// Waits for a terminal typed outcome.
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

/// Recursively sums input using balanced fork-join decomposition.
pub async fn recursive_sum(group: TaskGroup, values: Vec<u64>, leaf_size: usize) -> Outcome<u64> {
    if values.len() <= leaf_size.max(1) {
        return group
            .spawn(async move { values.into_iter().sum() })
            .await
            .result()
            .await;
    }
    let midpoint = values.len() / 2;
    let right = values[midpoint..].to_vec();
    let left = values[..midpoint].to_vec();
    let left_group = group.clone();
    let right_group = group.clone();
    let (left_result, right_result) = tokio::join!(
        recursive_sum_boxed(left_group, left, leaf_size),
        recursive_sum_boxed(right_group, right, leaf_size),
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
