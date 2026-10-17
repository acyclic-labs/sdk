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
        $crate::obs::span!(
            $level,
            $name,
            batch_len = $crate::obs::Empty,
            bytes = $crate::obs::Empty,
            outcome = $crate::obs::Empty,
            error.kind = $crate::obs::Empty
        )
    };
}
pub(crate) use file_span;

/// An operation span and the originating context for its work.
pub struct OperationSpan {
    span: Span,
    context: Span,
    dispatch: Dispatch,
}

#[cfg(not(test))]
use tracing::dispatcher::with_default as with_dispatch;

#[cfg(test)]
#[inline]
fn with_dispatch<T>(dispatch: &Dispatch, work: impl FnOnce() -> T) -> T {
    tests::DISPATCH_INSTALLS.with(|count| count.set(count.get() + 1));
    tracing::dispatcher::with_default(dispatch, work)
}

impl OperationSpan {
    /// Captures the operation and its creating dispatcher.
    pub fn new(span: Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        let context = if span.is_disabled() {
            Span::current()
        } else {
            span.clone()
        };
        Self {
            span,
            context,
            dispatch,
        }
    }
    /// Captures an explicitly rooted span without retaining a filtered caller.
    pub fn new_root(span: Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        Self {
            context: span.clone(),
            span,
            dispatch,
        }
    }
    /// Adds a causal edge only when both spans share the same registry.
    /// Span IDs from unrelated or unknown subscriber implementations are never
    /// passed to another subscriber. Unknown implementations omit this edge.
    pub fn follows_from_same_subscriber(&self, cause: &Self) {
        let registry = self.dispatch.downcast_ref::<tracing_subscriber::Registry>();
        let origin = cause
            .dispatch
            .downcast_ref::<tracing_subscriber::Registry>();
        if let (Some(registry), Some(origin)) = (registry, origin)
            && std::ptr::eq(registry, origin)
        {
            self.span.follows_from(cause.context.id());
        }
    }
    /// Borrows the operation's poll/drop context, including a filtered caller.
    pub fn context(&self) -> &Span {
        &self.context
    }
    /// Runs work under its originating dispatcher and context.
    pub fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        // Preserve isolation when an unobserved producer is later polled under
        // a foreign subscriber, but install no dispatcher when both ends are idle.
        if self.dispatch.is::<tracing::subscriber::NoSubscriber>()
            && tracing::dispatcher::get_default(|current| {
                current.is::<tracing::subscriber::NoSubscriber>()
            })
        {
            return work();
        }
        with_dispatch(&self.dispatch, || self.context.in_scope(work))
    }
    /// Retains a producer's context through polling and destruction.
    pub fn scope<T>(&self, inner: T) -> Scoped<T> {
        Scoped::with_context(inner, Some(self.clone()))
    }
    /// Records a path-free result on the operation, including when filtered.
    pub fn record_error(&self, error: Option<&io::Error>) {
        self.span
            .record("outcome", if error.is_some() { "err" } else { "ok" });
        if let Some(error) = error {
            self.span.record("error.kind", kind(error));
        }
    }
    /// Records completion and returns the unchanged result.
    pub fn finish<T>(&self, result: io::Result<T>) -> io::Result<T> {
        self.record_error(result.as_ref().err());
        result
    }
    /// Records observed batch size, calculating bytes only for an enabled span.
    pub fn sized(self, batch_len: usize, bytes: impl FnOnce() -> usize) -> Self {
        if !self.span.is_disabled() {
            self.span
                .record("batch_len", batch_len)
                .record("bytes", bytes());
        }
        self
    }
}
impl Clone for OperationSpan {
    fn clone(&self) -> Self {
        let clone = || Self {
            span: self.span.clone(),
            context: self.context.clone(),
            dispatch: self.dispatch.clone(),
        };
        if self.span.is_none() && self.context.is_none() {
            clone()
        } else {
            with_dispatch(&self.dispatch, clone)
        }
    }
}
impl std::ops::Deref for OperationSpan {
    type Target = Span;
    fn deref(&self) -> &Span {
        &self.span
    }
}
impl Drop for OperationSpan {
    fn drop(&mut self) {
        if self.span.is_none() && self.context.is_none() {
            return;
        }
        with_dispatch(&self.dispatch, || {
            self.span = Span::none();
            self.context = Span::none();
        });
    }
}

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

#[cfg(test)]
mod tests;
