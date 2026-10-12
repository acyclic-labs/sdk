//! Operation context shared by native producers.

use std::io;
use tracing::{Dispatch, Span};

mod scoped;
pub use scoped::Scoped;
pub(crate) use tracing::field::Empty;

macro_rules! span {
    ($level:ident, $name:literal $(, $($fields:tt)*)?) => {
        $crate::obs::OperationSpan::new(tracing::span!(tracing::Level::$level, $name, $($($fields)*)?))
    };
}
pub(crate) use span;

macro_rules! file_span {
    ($level:ident, $name:literal) => {
        $crate::obs::span!($level, $name, batch_len = $crate::obs::Empty,
            bytes = $crate::obs::Empty, outcome = $crate::obs::Empty, error.kind = $crate::obs::Empty)
    };
}
pub(crate) use file_span;

/// An operation span and the originating context for its work.
pub struct OperationSpan {
    span: Span,
    context: Span,
    dispatch: Dispatch,
}

impl OperationSpan {
    /// Captures the operation and its creating dispatcher.
    pub fn new(span: Span) -> Self {
        let dispatch = span.with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        let context = if span.is_disabled() { Span::current() } else { span.clone() };
        Self { span, context, dispatch }
    }
    /// Borrows the operation's poll/drop context, including a filtered caller.
    pub fn context(&self) -> &Span { &self.context }
    /// Runs work under its originating dispatcher and context.
    pub fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || self.context.in_scope(work))
    }
    /// Retains a producer's context through polling and destruction.
    pub fn scope<T>(&self, inner: T) -> Scoped<T> {
        Scoped::with_context(inner, Some(self.clone()))
    }
    /// Records a path-free result on the operation, including when filtered.
    pub fn record_error(&self, error: Option<&io::Error>) {
        self.span.record("outcome", if error.is_some() { "err" } else { "ok" });
        if let Some(error) = error { self.span.record("error.kind", kind(error)); }
    }
    /// Records completion and returns the unchanged result.
    pub fn finish<T>(&self, result: io::Result<T>) -> io::Result<T> {
        self.record_error(result.as_ref().err());
        result
    }
    /// Records observed batch size, calculating bytes only for an enabled span.
    pub fn sized(self, batch_len: usize, bytes: impl FnOnce() -> usize) -> Self {
        if !self.span.is_disabled() { self.span.record("batch_len", batch_len).record("bytes", bytes()); }
        self
    }
}
impl Clone for OperationSpan {
    fn clone(&self) -> Self {
        tracing::dispatcher::with_default(&self.dispatch, || Self {
            span: self.span.clone(), context: self.context.clone(), dispatch: self.dispatch.clone(),
        })
    }
}
impl std::ops::Deref for OperationSpan {
    type Target = Span;
    fn deref(&self) -> &Span { &self.span }
}
impl Drop for OperationSpan {
    fn drop(&mut self) {
        tracing::dispatcher::with_default(&self.dispatch, || {
            self.span = Span::none();
            self.context = Span::none();
        });
    }
}

pub(crate) fn kind(error: &io::Error) -> &'static str {
    use io::ErrorKind as Kind;
    if crate::is_uncertain_io_error(error) { return "uncertain"; }
    match error.kind() {
        Kind::NotFound => "not_found", Kind::PermissionDenied => "permission_denied",
        Kind::AlreadyExists => "already_exists", Kind::WouldBlock => "would_block",
        Kind::InvalidInput => "invalid_input", Kind::InvalidData => "invalid_data",
        Kind::TimedOut => "timed_out", Kind::Interrupted => "interrupted",
        Kind::Unsupported => "unsupported", Kind::UnexpectedEof => "unexpected_eof",
        Kind::OutOfMemory => "out_of_memory", Kind::BrokenPipe => "broken_pipe",
        Kind::FileTooLarge => "file_too_large", Kind::StorageFull => "storage_full", _ => "other",
    }
}

#[cfg(test)]
mod tests;
