//! Operation context shared by native producers.

use std::io;
use tracing::{Dispatch, Span};

mod scoped;
pub use scoped::Scoped;
mod report_port;
mod wait_report;
pub use report_port::{ReportAck, ReportOutcome, ReportPort, ReportRequest};
mod waiting;
pub use acyclic_observability::{Metrics, Operations, Query, QueryError, Snapshot};
pub(crate) use tracing::field::Empty;
pub use wait_report::{WaitReport, WaitSnapshot};
pub use waiting::{Waiting, waiting, waiting_sync};

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
    caller: Span,
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
        let caller = if dispatch.is::<tracing::subscriber::NoSubscriber>() {
            Span::none()
        } else {
            with_dispatch(&dispatch, Span::current)
        };
        Self {
            span,
            caller,
            dispatch,
        }
    }
    /// Captures an explicitly rooted span without retaining a filtered caller.
    pub fn new_root(span: Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        Self {
            caller: Span::none(),
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
            self.span.follows_from(cause.context().id());
        }
    }
    /// Borrows the operation's poll/drop context, including a filtered caller.
    pub fn context(&self) -> &Span {
        if self.span.is_disabled() {
            &self.caller
        } else {
            &self.span
        }
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
        // A registry span can be enabled while one of its layers filters it.
        // Enter the retained caller as well so that layer keeps its ancestry.
        with_dispatch(&self.dispatch, || {
            self.caller.in_scope(|| self.span.in_scope(work))
        })
    }
    /// Release a completed operation while retaining its originating caller for
    /// later cleanup. Call after leaving the operation's entered scope.
    pub fn release_operation(&mut self) {
        if !self.span.is_none() {
            with_dispatch(&self.dispatch, || self.span = Span::none());
        }
    }
    /// Retains a producer's context through polling and destruction.
    pub fn scope<T>(&self, inner: T) -> Scoped<T> {
        Scoped::with_context(inner, Some(self.clone()))
    }
    /// Records a path-free result on the operation, including when filtered.
    pub fn record_error(&self, error: Option<&io::Error>) {
        if !self.span.is_disabled() {
            acyclic_observability::record_outcome(&self.span, error.map(kind));
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
            caller: self.caller.clone(),
            dispatch: self.dispatch.clone(),
        };
        if self.span.is_none() && self.caller.is_none() {
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
        if self.span.is_none() && self.caller.is_none() {
            return;
        }
        with_dispatch(&self.dispatch, || {
            self.span = Span::none();
            self.caller = Span::none();
        });
    }
}

/// Classifies the original I/O error without exposing paths or losing uncertainty.
pub fn kind(error: &io::Error) -> &'static str {
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
