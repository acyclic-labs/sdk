//! Span helpers; see `docs/observability.md`.

use std::io;
use tracing::{Dispatch, Span};

pub(crate) use tracing::field::Empty;

/// Captures operation identity before work can cross a thread or dispatch.
macro_rules! span {
    ($level:ident, $name:literal $(, $($fields:tt)*)?) => {
        $crate::obs::OperationSpan::new(tracing::span!(
            tracing::Level::$level, $name,
            $($($fields)*)?
        ))
    };
}
pub(crate) use span;

/// A span for one native file operation, with its size fields left for [`OperationSpan::sized`].
macro_rules! file_span {
    ($level:ident, $name:literal) => {
        $crate::obs::span!(
            $level,
            $name,
            batch_len = $crate::obs::Empty,
            bytes = $crate::obs::Empty,
            outcome = $crate::obs::Empty,
            error.kind = $crate::obs::Empty,
        )
    };
}
pub(crate) use file_span;

/// An operation's context and originating dispatcher.
#[derive(Clone)]
pub(crate) struct OperationSpan {
    span: Span,
    observed: bool,
    dispatch: Dispatch,
}

impl Drop for OperationSpan {
    fn drop(&mut self) {
        let Self { span, dispatch, .. } = self;
        // Subscriber parent-reference cleanup can consult the default dispatch.
        tracing::dispatcher::with_default(dispatch, || {
            *span = Span::none();
        });
    }
}

impl OperationSpan {
    /// Captures `span` and the caller's context; call it on the caller's thread.
    pub(crate) fn new(span: Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        let observed = !span.is_disabled();
        Self { span: if observed { span } else { Span::current() }, observed, dispatch }
    }

    /// Executes under the originating dispatcher and operation/caller context.
    pub(crate) fn scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || self.span.in_scope(work))
    }

    /// Records only on this operation, including when its span is filtered out.
    pub(crate) fn record(&self, field: &'static str, value: impl tracing::field::Value) {
        if self.observed { self.span.record(field, value); }
    }

    pub(crate) fn record_error(&self, error: Option<&io::Error>) {
        self.record("outcome", if error.is_some() { "err" } else { "ok" });
        if let Some(error) = error { self.record("error.kind", kind(error)); }
    }

    /// Completion reporting belongs to the work, independently of its observer.
    pub(crate) fn finish<T>(&self, result: io::Result<T>) -> io::Result<T> {
        self.record_error(result.as_ref().err());
        result
    }

    /// Records a batch's length and byte count, summing only when recorded.
    pub(crate) fn sized(self, batch_len: usize, bytes: impl FnOnce() -> usize) -> Self {
        if self.observed {
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    use std::task::{Context, Poll, Waker};
    use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

    // Worker ancestry, caller mutation, and caller retirement.
    struct Capture(Arc<[AtomicBool; 3]>);
    impl<S> tracing_subscriber::Layer<S> for Capture
    where S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> {
        fn on_new_span(
            &self, attributes: &tracing::span::Attributes<'_>, id: &tracing::span::Id,
            context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if attributes.metadata().name() == "test.worker_nested" {
                self.0[0].store(context.span(id).unwrap().scope()
                    .any(|span| span.name() == "test.caller"), Ordering::Release);
            }
        }
        fn on_record(&self, id: &tracing::span::Id, _: &tracing::span::Record<'_>, context: tracing_subscriber::layer::Context<'_, S>) {
            if context.span(id).unwrap().name() == "test.caller" { self.0[1].store(true, Ordering::Release); }
        }
        fn on_close(&self, id: tracing::span::Id, context: tracing_subscriber::layer::Context<'_, S>) {
            if context.span(&id).unwrap().name() == "test.caller" { self.0[2].store(true, Ordering::Release); }
        }
    }

    #[test]
    fn admitted_work_keeps_observation_after_observer_drop() {
        use std::future::Future;
        for (native_control, filtered) in [(false, false), (true, false), (false, true), (true, true)] {
            let temporary = tempfile::tempfile().unwrap();
            let native = crate::NativeFile::from_file(temporary.try_clone().unwrap(), None)
                .map_err(|(error, _)| error).unwrap();
            let seen = Arc::new(std::array::from_fn(|_| AtomicBool::new(false)));
            let origin = tracing::Dispatch::new(tracing_subscriber::registry().with(Capture(seen.clone()).with_filter(
                tracing_subscriber::filter::filter_fn(move |metadata| !filtered || *metadata.level() == tracing::Level::INFO))));
            let foreign = tracing::Dispatch::new(tracing_subscriber::registry());
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let mut observer: Box<dyn Future<Output = std::io::Result<()>> + Unpin> =
                tracing::dispatcher::with_default(&origin, || tracing::info_span!("test.caller", outcome = "sentinel", error.kind = "sentinel").in_scope(|| {
                    let work = move || {
                        started_tx.send(()).unwrap();
                        release_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
                        drop(tracing::info_span!("test.worker_nested"));
                    };
                    if native_control {
                        Box::new(native.control_async(move |file, _| {
                            work();
                            let result = file.set_len(73);
                            done_tx.send(()).unwrap();
                            result
                        }, None).expect("unmetered control construction cannot fail")) as Box<dyn Future<Output = std::io::Result<()>> + Unpin>
                    } else {
                        Box::new(crate::run_blocking_io(move || { work(); done_tx.send(()).unwrap(); }, None).expect("unmetered task construction cannot fail"))
                    }
                }));
            assert!(matches!(std::pin::Pin::new(&mut observer).poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
            started_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            drop(observer);
            tracing::dispatcher::with_default(&foreign, || release_tx.send(()).unwrap());
            done_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !seen[2].load(Ordering::Acquire) {
                assert!(std::time::Instant::now() < deadline);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(seen[0].load(Ordering::Acquire));
            assert!(!seen[1].load(Ordering::Acquire));
            if native_control { assert_eq!(temporary.metadata().unwrap().len(), 73); }
        }
    }
}
