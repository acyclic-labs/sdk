use super::*;

/// Exact source selection for a cheap workspace fork.
#[derive(Clone)]
pub struct ForkOptions<A, O> {
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
