use crate::{AuthorityStoreError, FilesystemCommit};
use thiserror::Error;

/// Retry custody of an unresolved companion publication: the original sealed
/// commit, without reconstruction or re-fencing. Its custody graph is erased
/// here so other workspace failures do not expand it during auto-trait checks.
#[cfg(not(target_arch = "wasm32"))]
pub type UnresolvedCommit = Box<dyn FnOnce() -> FilesystemCommit + Send + Sync>;
/// The browser retains the same owner on its original event loop.
#[cfg(target_arch = "wasm32")]
pub type UnresolvedCommit = Box<dyn FnOnce() -> FilesystemCommit>;

/// Original storage and codec failures of stream-backed companion state.
///
/// `C` is the retry custody of an unresolved publication. Custody may own
/// thread-affine browser handles, so it never travels as a cause;
/// [`StreamStateStoreError::into_cause`] keeps the failure without it.
#[derive(Error)]
pub enum StreamStateStoreError<C = UnresolvedCommit> {
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
        /// Returns the original sealed publication; `()` in a cause.
        commit: C,
        /// Original submission failure.
        #[source]
        error: AuthorityStoreError,
    },
    /// A record cannot represent this workspace or revision.
    #[error("incompatible stream-backed companion state")]
    Incompatible,
}

/// A companion-state failure without retry custody, `Send + Sync` on every
/// target, so it can be carried as a source by any other error.
pub type StreamStateStoreCause = StreamStateStoreError<()>;

impl StreamStateStoreError {
    /// This failure as a cause. An unresolved publication keeps its original
    /// submission failure; its sealed commit, the only retry custody, is
    /// dropped here, so callers that may retry must match before converting.
    #[must_use]
    pub fn into_cause(self) -> StreamStateStoreCause {
        match self {
            Self::Authority(error) => StreamStateStoreError::Authority(error),
            Self::Stream(error) => StreamStateStoreError::Stream(error),
            Self::Codec(error) => StreamStateStoreError::Codec(error),
            Self::Unknown { error, .. } => StreamStateStoreError::Unknown { commit: (), error },
            Self::Incompatible => StreamStateStoreError::Incompatible,
        }
    }
}

impl<C> std::fmt::Debug for StreamStateStoreError<C> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamStateStoreError")
            .field("cause", &format_args!("{self}"))
            .finish_non_exhaustive()
    }
}
