use serde::{Deserialize, Serialize};

/// Platform-neutral process failure, retained and propagated without translation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessError<M = String> {
    /// Executable was absent.
    NotFound {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// The process exceeded its deadline.
    Timeout {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// Host permissions prevented execution or capture.
    PermissionDenied {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// Other process IO failed.
    Io {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// The process exited unsuccessfully.
    Exit {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// Structured stdout failed validation.
    InvalidOutput {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// The caller cancelled execution.
    Cancelled {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
    /// Capture or retained output exhausted its byte budget.
    OutputLimit {
        /// Observed process exit code, if any.
        exit_code: Option<i32>,
        /// Diagnostic returned by capture or validation.
        message: M,
    },
}

impl<M> ProcessError<M> {
    /// Observed exit code, if capture reached process termination.
    pub fn exit_code(&self) -> Option<i32> {
        *self.parts().0
    }

    /// Diagnostic supplied by capture or validation.
    pub fn message(&self) -> &M {
        self.parts().1
    }

    fn parts(&self) -> (&Option<i32>, &M) {
        match self {
            Self::NotFound { exit_code, message }
            | Self::Timeout { exit_code, message }
            | Self::PermissionDenied { exit_code, message }
            | Self::Io { exit_code, message }
            | Self::Exit { exit_code, message }
            | Self::InvalidOutput { exit_code, message }
            | Self::Cancelled { exit_code, message }
            | Self::OutputLimit { exit_code, message } => (exit_code, message),
        }
    }

    pub(crate) fn set_exit_code(&mut self, code: Option<i32>) {
        match self {
            Self::NotFound { exit_code, .. }
            | Self::Timeout { exit_code, .. }
            | Self::PermissionDenied { exit_code, .. }
            | Self::Io { exit_code, .. }
            | Self::Exit { exit_code, .. }
            | Self::InvalidOutput { exit_code, .. }
            | Self::Cancelled { exit_code, .. }
            | Self::OutputLimit { exit_code, .. } => *exit_code = code,
        }
    }

    /// Fallibly change message ownership without converting the cause.
    pub fn try_map_message<N, E>(
        self,
        map: impl FnOnce(M) -> std::result::Result<N, E>,
    ) -> std::result::Result<ProcessError<N>, E> {
        Ok(match self {
            Self::NotFound { exit_code, message } => ProcessError::NotFound {
                exit_code,
                message: map(message)?,
            },
            Self::Timeout { exit_code, message } => ProcessError::Timeout {
                exit_code,
                message: map(message)?,
            },
            Self::PermissionDenied { exit_code, message } => ProcessError::PermissionDenied {
                exit_code,
                message: map(message)?,
            },
            Self::Io { exit_code, message } => ProcessError::Io {
                exit_code,
                message: map(message)?,
            },
            Self::Exit { exit_code, message } => ProcessError::Exit {
                exit_code,
                message: map(message)?,
            },
            Self::InvalidOutput { exit_code, message } => ProcessError::InvalidOutput {
                exit_code,
                message: map(message)?,
            },
            Self::Cancelled { exit_code, message } => ProcessError::Cancelled {
                exit_code,
                message: map(message)?,
            },
            Self::OutputLimit { exit_code, message } => ProcessError::OutputLimit {
                exit_code,
                message: map(message)?,
            },
        })
    }

    /// Change message ownership while preserving the complete cause.
    pub fn map_message<N>(self, map: impl FnOnce(M) -> N) -> ProcessError<N> {
        match self {
            Self::NotFound { exit_code, message } => ProcessError::NotFound {
                exit_code,
                message: map(message),
            },
            Self::Timeout { exit_code, message } => ProcessError::Timeout {
                exit_code,
                message: map(message),
            },
            Self::PermissionDenied { exit_code, message } => ProcessError::PermissionDenied {
                exit_code,
                message: map(message),
            },
            Self::Io { exit_code, message } => ProcessError::Io {
                exit_code,
                message: map(message),
            },
            Self::Exit { exit_code, message } => ProcessError::Exit {
                exit_code,
                message: map(message),
            },
            Self::InvalidOutput { exit_code, message } => ProcessError::InvalidOutput {
                exit_code,
                message: map(message),
            },
            Self::Cancelled { exit_code, message } => ProcessError::Cancelled {
                exit_code,
                message: map(message),
            },
            Self::OutputLimit { exit_code, message } => ProcessError::OutputLimit {
                exit_code,
                message: map(message),
            },
        }
    }
}

impl<M: std::fmt::Display> std::fmt::Display for ProcessError<M> {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message().fmt(out)
    }
}

impl<M: std::fmt::Debug + std::fmt::Display> std::error::Error for ProcessError<M> {}
