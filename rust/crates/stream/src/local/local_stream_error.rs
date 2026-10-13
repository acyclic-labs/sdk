/// Local provider initialization or durable-publication failure.
#[derive(Debug, Error)]
pub enum LocalStreamError {
    /// An accepted write stopped before its complete frame.
    #[error("incomplete accepted journal frame")]
    IncompleteFrame,
    /// Filesystem operation failed.
    #[error("local Stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Another process owns the local provider root.
    #[error("local Stream root is already open")]
    AlreadyOpen,
    /// Stored bytes or configuration do not match the canonical local format.
    #[error("local Stream journal is corrupt or incompatible")]
    Corrupt,
    /// Configured bounds are zero or cannot represent the canonical format.
    #[error("local Stream limits are invalid")]
    InvalidLimits,
    /// Durable history cannot be reconstructed through the canonical state machine.
    #[error("local Stream replay failed: {0}")]
    Replay(StreamError),
    /// Native blocking execution could not complete.
    #[error("local Stream executor is unavailable")]
    Executor,
}

impl obs::ErrorKind for LocalStreamError {
    fn kind(&self) -> &'static str {
        match self {
            Self::IncompleteFrame => "incomplete_frame",
            Self::Io(_) => "io",
            Self::AlreadyOpen => "already_open",
            Self::Corrupt => "corrupt",
            Self::InvalidLimits => "invalid_limits",
            Self::Replay(error) => error.code(),
            Self::Executor => "executor",
        }
    }
}
