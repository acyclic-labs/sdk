//! Span helpers; see `docs/observability.md`.

use std::io;
use tracing::Span;

pub(crate) use tracing::field::Empty;

/// Records `outcome` and, on failure, `error.kind` on the current span.
pub(crate) fn finish<T>(result: io::Result<T>) -> io::Result<T> {
    let span = Span::current();
    match &result {
        Ok(_) => span.record("outcome", "ok"),
        Err(error) => span
            .record("outcome", "err")
            .record("error.kind", kind(error)),
    };
    result
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
