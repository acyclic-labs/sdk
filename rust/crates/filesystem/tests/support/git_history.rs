use acyclic_fs::git_compat::{
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitTreeRef,
};
use acyclic_fs::{GenerationId, WorkspaceId};
use acyclic_fs_format::identity::{Digest, OperationId};
use std::sync::Mutex;
use thiserror::Error;

#[derive(Debug, Error)]
#[error("test executor failure")]
pub(crate) struct TestExecutorError;

pub(crate) struct TestExecutor {
    result: Result<GitFilesystemResult, TestExecutorError>,
    pub(crate) operations: Mutex<Vec<OperationId>>,
}

impl TestExecutor {
    pub(crate) fn returning(result: GitFilesystemResult) -> Self {
        Self {
            result: Ok(result),
            operations: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn failing() -> Self {
        Self {
            result: Err(TestExecutorError),
            operations: Mutex::new(Vec::new()),
        }
    }
}

impl GitFilesystemExecutor for TestExecutor {
    type Error = TestExecutorError;

    fn validate_merge_resolution<'a>(
        &'a self,
        _operation: OperationId,
        action: &'a GitFilesystemAction,
        tree: GitTreeRef,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>> {
        Box::pin(std::future::ready(
            self.result
                .as_ref()
                .map(|_| GitFilesystemResult::Applied {
                    tree: Some(tree),
                    tracked_paths: match action {
                        GitFilesystemAction::Join { tracked_paths, .. } => {
                            Some(tracked_paths.clone())
                        }
                        _ => None,
                    },
                })
                .map_err(|_| TestExecutorError),
        ))
    }

    fn execute<'a>(
        &'a self,
        operation_id: OperationId,
        _action: &'a GitFilesystemAction,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<GitFilesystemResult, Self::Error>> {
        let result = self
            .operations
            .lock()
            .map_err(|_| TestExecutorError)
            .and_then(|mut operations| {
                operations.push(operation_id);
                self.result
                    .as_ref()
                    .map(Clone::clone)
                    .map_err(|_| TestExecutorError)
            });
        Box::pin(std::future::ready(result))
    }
}

pub(crate) fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

pub(crate) fn workspace() -> WorkspaceId {
    WorkspaceId::from_bytes([9; 16])
}

pub(crate) fn tree(byte: u8) -> GitTreeRef {
    GitTreeRef::exact(workspace(), generation(byte))
}
