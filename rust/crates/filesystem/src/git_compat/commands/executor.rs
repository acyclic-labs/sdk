use super::*;

/// Backend-neutral execution boundary for Git-shaped filesystem work.
///
/// Every method returns an erased future: the command engine holds a pointer
/// to the backend's work, so neither its layout nor its auto-trait proof
/// grows with what a backend does.
pub trait GitFilesystemExecutor: acyclic_stream::ProviderPlatform {
    /// Executor failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Confirms that the caller still owns any writer lease governing this command.
    fn validate(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }

    /// Validates current conflict resolutions and returns their exact resulting tree and tracked paths.
    fn validate_merge_resolution<'a>(
        &'a self,
        operation: OperationId,
        action: &'a GitFilesystemAction,
        tree: GitTreeRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>>;

    /// Executes or recovers one exact action.
    fn execute<'a>(
        &'a self,
        operation_id: OperationId,
        action: &'a GitFilesystemAction,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>>;
}
