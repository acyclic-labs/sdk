/// First terminal failure of one local provider incarnation.
///
/// This contains only static operation/class names and OS error codes. It never
/// retains paths, record bytes, credentials or an error's free-form message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalStreamFailure {
    /// The operation that made the incarnation unavailable.
    pub operation: &'static str,
    /// The original error's stable class.
    pub cause: &'static str,
    /// Original I/O classification, when the failure came from I/O.
    pub io_kind: Option<std::io::ErrorKind>,
    /// Original OS code, when supplied by the OS.
    pub raw_os_error: Option<i32>,
}

impl LocalStreamFailure {
    fn new(operation: &'static str, cause: &'static str) -> Self {
        Self {
            operation,
            cause,
            io_kind: None,
            raw_os_error: None,
        }
    }

    fn io(operation: &'static str, error: &std::io::Error) -> Self {
        Self {
            operation,
            cause: "io",
            io_kind: Some(error.kind()),
            raw_os_error: error.raw_os_error(),
        }
    }

    fn local(
        operation: &'static str,
        error: &LocalStreamError,
        work: Option<&crate::native::Work>,
    ) -> Self {
        if let Some(failure) = work.and_then(|work| work.failure(operation)) {
            return failure;
        }
        match error {
            LocalStreamError::Io(error) => Self::io(operation, error),
            _ => Self::new(operation, obs::ErrorKind::kind(error)),
        }
    }
}

impl std::fmt::Display for LocalStreamFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "local Stream {}: {}", self.operation, self.cause)?;
        if let Some(kind) = self.io_kind {
            write!(formatter, " ({kind:?}")?;
            if let Some(code) = self.raw_os_error {
                write!(formatter, ", os error {code}")?;
            }
            write!(formatter, ")")?;
        }
        Ok(())
    }
}
