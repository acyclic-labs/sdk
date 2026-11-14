use crate::{AuthorityStoreError, FilesystemCommit};
use thiserror::Error;

/// Original storage and codec failures of stream-backed companion state.
#[derive(Error)]
pub enum StreamStateStoreError {
    /// The provider failed or could not resolve an exact append.
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    /// The stream primitive rejected a path or request.
    #[error(transparent)]
    Stream(#[from] acyclic_stream::StreamError),
    /// The companion codec refused state.
    #[error(transparent)]
    Codec(#[from] serde_json::Error),
    /// An unresolved publication retains its exact request and custody.
    #[error("stream-backed companion publication remains unresolved: {error}")]
    Unknown {
        /// Returns the original sealed publication without reconstruction or
        /// re-fencing. Erase its custody graph at the error boundary so other
        /// workspace failures do not expand it during auto-trait checking.
        #[cfg(not(target_arch = "wasm32"))]
        commit: Box<dyn FnOnce() -> FilesystemCommit + Send + Sync>,
        /// The browser retains the same owner on its original event loop.
        #[cfg(target_arch = "wasm32")]
        commit: Box<dyn FnOnce() -> FilesystemCommit>,
        /// Original submission failure.
        #[source]
        error: AuthorityStoreError,
    },
    /// A record cannot represent this workspace or revision.
    #[error("incompatible stream-backed companion state")]
    Incompatible,
}

impl std::fmt::Debug for StreamStateStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamStateStoreError")
            .field("cause", &format_args!("{self}"))
            .finish_non_exhaustive()
    }
}
