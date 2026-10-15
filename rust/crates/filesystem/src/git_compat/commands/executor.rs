use super::*;

/// Backend-neutral execution boundary for Git-shaped filesystem work.
pub trait GitFilesystemExecutor: Send + Sync {
    /// Executor failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Confirms that the caller still owns any writer lease governing this command.
    fn validate(&self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Executes or recovers one exact action.
    fn execute(
        &self,
        operation_id: OperationId,
        action: &GitFilesystemAction,
    ) -> impl Future<Output = Result<GitFilesystemResult, Self::Error>> + Send;
}
