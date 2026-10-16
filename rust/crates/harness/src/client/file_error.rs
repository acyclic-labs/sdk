use crate::Error;

/// Submission backpressure is distinct from malformed input and provider failure.
#[derive(Debug, thiserror::Error)]
pub enum FileClientError {
    /// The pending command count has reached its configured bound.
    #[error("outbox command quota reached ({limit})")]
    CommandQuota {
        /// Configured maximum pending commands.
        limit: usize,
    },
    /// The pending bytes plus this request exceed the configured bound.
    #[error("outbox byte quota reached ({limit})")]
    ByteQuota {
        /// Configured maximum pending payload bytes.
        limit: u64,
    },
    /// Input, history, identity or provider failure, retaining its classification.
    #[error(transparent)]
    Harness(#[from] Error),
}
