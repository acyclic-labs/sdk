use super::*;

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
            (id, handle.wait().await)
        })
        .buffer_unordered(concurrency)
        .boxed()
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
