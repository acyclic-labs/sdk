//! Span helpers; see `docs/observability.md`.

use std::io;
use tracing::Span;

pub(crate) use tracing::field::Empty;

/// Records `outcome` and, on failure, `error.kind` on the current span.
pub(crate) fn finish<T>(result: io::Result<T>) -> io::Result<T> {
    record(&Span::current(), &result);
    result
}

/// Records `outcome` and, on failure, `error.kind` on `span`.
pub(crate) fn record<T>(span: &Span, result: &io::Result<T>) {
    match result {
        Ok(_) => span.record("outcome", "ok"),
        Err(error) => span
            .record("outcome", "err")
            .record("error.kind", kind(error)),
    };
}

/// A span for one native file operation, with its size fields left for
/// [`OperationSpan::sized`].
macro_rules! file_span {
    ($level:ident, $name:literal) => {
        tracing::span!(
            tracing::Level::$level,
            $name,
            batch_len = $crate::obs::Empty,
            bytes = $crate::obs::Empty,
            outcome = $crate::obs::Empty,
            error.kind = $crate::obs::Empty,
        )
    };
}
pub(crate) use file_span;

/// An operation's own span, and the span its worker enters: the operation
/// span, or the caller's when the operation span is filtered out, so work
/// nested in the operation keeps its caller as parent either way.
#[derive(Clone)]
pub(crate) struct OperationSpan {
    pub(crate) span: Span,
    pub(crate) context: Span,
}

impl OperationSpan {
    /// Captures `span` and the caller's context; call it on the caller's thread.
    pub(crate) fn new(span: Span) -> Self {
        let context = if span.is_disabled() {
            Span::current()
        } else {
            span.clone()
        };
        Self { span, context }
    }

    /// Records a batch's length and byte count, summing only when recorded.
    pub(crate) fn sized(self, batch_len: usize, bytes: impl FnOnce() -> usize) -> Self {
        if !self.span.is_disabled() {
            self.span
                .record("batch_len", batch_len)
                .record("bytes", bytes());
        }
        self
    }
}

/// A stable, path-free name for an I/O failure.
pub(crate) fn kind(error: &io::Error) -> &'static str {
    use io::ErrorKind as Kind;
    if crate::is_uncertain_io_error(error) {
        return "uncertain";
    }
    match error.kind() {
        Kind::NotFound => "not_found",
        Kind::PermissionDenied => "permission_denied",
        Kind::AlreadyExists => "already_exists",
        Kind::WouldBlock => "would_block",
        Kind::InvalidInput => "invalid_input",
        Kind::InvalidData => "invalid_data",
        Kind::TimedOut => "timed_out",
        Kind::Interrupted => "interrupted",
        Kind::Unsupported => "unsupported",
        Kind::UnexpectedEof => "unexpected_eof",
        Kind::OutOfMemory => "out_of_memory",
        Kind::BrokenPipe => "broken_pipe",
        Kind::FileTooLarge => "file_too_large",
        Kind::StorageFull => "storage_full",
        _ => "other",
    }
}
