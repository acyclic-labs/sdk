use super::*;

/// Local or durable typed handle; durable observation is host-authoritative.

pub enum RuntimeTask<O> {
    /// Process-local task; its future is never persisted.
    Live(TaskHandle<Result<O>>),
    /// Host-backed task with pinned output validation.
    Durable {
        /// Stable task address at the durable host.
        task_id: TaskId,
        /// Provider that resolves the task after process recovery.
        host: Arc<dyn TaskStateProvider>,
        /// Pinned schema for the task result.
        output_schema: Value,
        /// Native extension implementations retained by this admission.
        extensions: Option<Vec<crate::extension::ExtensionLease>>,
    },
}

impl<O> RuntimeTask<O> {
    /// Stable identity of the admitted local operation or durable task.
    #[must_use]
    pub fn identity(&self) -> String {
        match self {
            Self::Live(handle) => handle.id().to_string(),
            Self::Durable { task_id, .. } => task_id.to_string(),
        }
    }
}

/// Observes every admitted task in input order without cancelling siblings.

pub async fn join_runtime<O: DeserializeOwned + Send + 'static>(
    tasks: Vec<RuntimeTask<O>>,
) -> Vec<Result<Outcome<O>>> {
    let concurrency = tasks.len().clamp(1, 64);
    stream::iter(tasks)
        .map(|task| async move { task.result().await })
        .buffered(concurrency)
        .collect()
        .await
}

/// An impossible quorum retains both partial successes and non-successes.

pub struct QuorumFailure<O> {
    /// Caller-specified threshold that was not met.
    pub required: usize,
    /// Successful values with their task IDs, in completion order.
    pub successes: Vec<(String, O)>,
    /// Failed, cancelled, uncertain, or unobservable members.
    pub others: Vec<(String, Result<Outcome<O>>)>,
}

impl<O: DeserializeOwned> RuntimeTask<O> {
    /// Requests cancellation without claiming a confirmed cancelled outcome.
    pub async fn cancel(&self) -> Result<()> {
        match self {
            Self::Live(handle) => {
                handle.cancel();
                Ok(())
            }
            Self::Durable { task_id, host, .. } => host.cancel(*task_id).await,
        }
    }

    /// Waits for a terminal outcome, preserving uncertainty and cancellation.
    pub async fn result(self) -> Result<Outcome<O>> {
        match self {
            Self::Live(handle) => Ok(match handle.result().await {
                Outcome::Succeeded(Ok(value)) => Outcome::Succeeded(value),
                Outcome::Succeeded(Err(Error::Indeterminate(operation_id))) => {
                    Outcome::Indeterminate { operation_id }
                }
                Outcome::Succeeded(Err(error)) => Outcome::Failed {
                    message: error.to_string(),
                },
                Outcome::Failed { message } => Outcome::Failed { message },
                Outcome::Cancelled => Outcome::Cancelled,
                Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
            }),
            Self::Durable {
                task_id,
                host,
                output_schema,
                extensions: _,
            } => match host.wait_outcome(task_id).await? {
                Outcome::Succeeded(value) => {
                    crate::contract::validate_json_schema_value(&output_schema, &value, "task output", &mut None)?;
                    let value = serde_json::from_value(value)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    Ok(Outcome::Succeeded(value))
                }
                Outcome::Failed { message } => Ok(Outcome::Failed { message }),
                Outcome::Cancelled => Ok(Outcome::Cancelled),
                Outcome::Indeterminate { operation_id } => {
                    Ok(Outcome::Indeterminate { operation_id })
                }
            },
        }
    }
}
