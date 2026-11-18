use acyclic_fs::git_compat::{
    GitFilesystemAction, GitFilesystemExecutor, GitFilesystemResult, GitTreeRef,
};
use acyclic_fs::{
    CancellationToken, Digest, GenerationId, OperationId, WorkspaceId, WorkspaceName,
};
use acyclic_fs_work::performance::WorkBudget;
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
        self.operations
            .lock()
            .expect("operation lock")
            .push(operation_id);
        Box::pin(std::future::ready(
            self.result
                .as_ref()
                .map(Clone::clone)
                .map_err(|_| TestExecutorError),
        ))
    }
}

pub(crate) fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

pub(crate) fn workspace() -> WorkspaceId {
    workspace_identity([9; 16], "repo")
}

pub(crate) fn workspace_identity(namespace: [u8; 16], name: &str) -> WorkspaceId {
    let name = WorkspaceName::new(name, &WorkBudget::UNBOUNDED, &CancellationToken::new())
        .expect("workspace name")
        .value;
    WorkspaceId::derive(
        namespace,
        &name,
        &WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value
}

pub(crate) fn tree(byte: u8) -> GitTreeRef {
    GitTreeRef::exact(workspace(), generation(byte))
}
