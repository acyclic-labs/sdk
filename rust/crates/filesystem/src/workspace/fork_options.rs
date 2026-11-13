use super::*;

/// Exact source selection for a cheap workspace fork.
#[derive(Clone)]
pub struct ForkOptions<A: 'static, O: 'static> {
    pub(crate) generation: Generation<A, O>,
    pub(crate) idempotency_key: IdempotencyKey,
    pub(crate) paths: Option<Vec<String>>,
}

impl<A, O> ForkOptions<A, O> {
    /// Forks from one exact immutable generation.
    pub fn from_generation(generation: Generation<A, O>, idempotency_key: IdempotencyKey) -> Self {
        Self {
            generation,
            idempotency_key,
            paths: None,
        }
    }

    /// Carries only explicitly selected files or directory subtrees. Selection
    pub fn inherit_paths(mut self, paths: Vec<String>) -> Self {
        self.paths = Some(paths);
        self
    }
}

/// Custody of an ephemeral fork made by [`Workspace::fork_ephemeral`]. The
/// fork exists exactly as long as this custody: dropping or settling it, or
/// losing the process, removes the fork's authority with no durable trace.
/// Until then it pins the fork's base generation against collection.
#[must_use = "dropping the custody ends the fork"]
pub struct EphemeralFork {
    pub(crate) lease: acyclic_stream::EphemeralLease,
}

impl EphemeralFork {
    /// Ends the fork and waits until its authority is gone.
    pub async fn settle(self) -> Result<(), WorkspaceError> {
        let Self { lease } = self;
        lease
            .settle()
            .await
            .map_err(|error| FsError::from(crate::AuthorityStoreError::NativeStream(error)).into())
    }
}

impl std::fmt::Debug for EphemeralFork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EphemeralFork")
            .field("root", self.lease.root())
            .finish_non_exhaustive()
    }
}
