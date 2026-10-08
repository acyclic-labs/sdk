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

/// A span for one native file operation, with its size fields left for
/// [`OperationSpan::sized`].
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

/// An operation's own span, and the span its worker enters: the operation
/// span, or the caller's when the operation span is filtered out, so work
/// nested in the operation keeps its caller as parent either way.
#[derive(Clone)]
pub(crate) struct OperationSpan {
    span: Span,
    context: Span,
    dispatch: Dispatch,
}

impl Drop for OperationSpan {
    fn drop(&mut self) {
        let Self {
            span,
            context,
            dispatch,
        } = self;
        // Subscriber parent-reference cleanup can consult the default dispatch.
        tracing::dispatcher::with_default(dispatch, || {
            *span = Span::none();
            *context = Span::none();
        });
    }
}

impl OperationSpan {
    /// Captures `span` and the caller's context; call it on the caller's thread.
    pub(crate) fn new(span: Span) -> Self {
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

    /// Executes under the originating dispatcher and operation/caller context.
    pub(crate) fn scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || self.context.in_scope(work))
    }

    /// Records only on this operation, including when its span is filtered out.
    pub(crate) fn record(&self, field: &'static str, value: impl tracing::field::Value) {
        self.span.record(field, value);
    }

    pub(crate) fn record_result<T>(&self, result: &io::Result<T>) {
        match result {
            Ok(_) => self.record("outcome", "ok"),
            Err(error) => {
                self.record("outcome", "err");
                self.record("error.kind", kind(error));
            }
        }
    }

    /// Completion reporting belongs to the work, independently of its observer.
    pub(crate) fn finish<T>(&self, result: io::Result<T>) -> io::Result<T> {
        self.record_result(&result);
        result
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

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "capture and channel failures must fail these adversarial tests"
)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::layer::{Context, SubscriberExt as _};

    type Seen = Arc<Mutex<Vec<(&'static str, &'static str, String)>>>;
    struct Capture(Seen);
    struct Fields<'a>(
        &'static str,
        &'a mut Vec<(&'static str, &'static str, String)>,
    );
    impl Visit for Fields<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.1.push((self.0, field.name(), value.to_owned()));
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.1.push((self.0, field.name(), format!("{value:?}")));
        }
    }
    impl<S> tracing_subscriber::Layer<S> for Capture
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            id: &tracing::span::Id,
            context: Context<'_, S>,
        ) {
            let name = attributes.metadata().name();
            let mut seen = self.0.lock().unwrap();
            seen.push((name, "new", String::new()));
            if let Some(parent) = context.span(id).and_then(|span| span.parent()) {
                seen.push((name, "parent", parent.name().to_owned()));
            }
            attributes.record(&mut Fields(name, &mut seen));
        }
        fn on_record(
            &self,
            id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            context: Context<'_, S>,
        ) {
            values.record(&mut Fields(
                context.span(id).unwrap().name(),
                &mut self.0.lock().unwrap(),
            ));
        }
        fn on_close(&self, id: tracing::span::Id, context: Context<'_, S>) {
            self.0.lock().unwrap().push((
                context.span(&id).unwrap().name(),
                "close",
                String::new(),
            ));
        }
        fn on_event(&self, event: &tracing::Event<'_>, context: Context<'_, S>) {
            let name = event.metadata().name();
            let mut seen = self.0.lock().unwrap();
            seen.push((name, "event", String::new()));
            if let Some(parent) = context.event_span(event) {
                seen.push((name, "parent", parent.name().to_owned()));
            }
            event.record(&mut Fields(name, &mut seen));
        }
    }

    fn capture(filtered: bool) -> (Dispatch, Seen) {
        let seen = Seen::default();
        let dispatch = Dispatch::new(tracing_subscriber::registry().with(
            Capture(Arc::clone(&seen)).with_filter(tracing_subscriber::filter::filter_fn(
                move |metadata| {
                    !filtered
                        || !matches!(metadata.name(), "test.filtered" | "acyclic.runtime.control")
                },
            )),
        ));
        (dispatch, seen)
    }

    #[test]
    fn filtered_completion_preserves_parent_and_safe_fields() {
        let (dispatch, seen) = capture(true);
        let _other = Dispatch::new(tracing_subscriber::registry());
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info_span!("test.caller", outcome = "sentinel", error.kind = "sentinel")
                .in_scope(|| {
                    let operation =
                        span!(DEBUG, "test.filtered", outcome = Empty, error.kind = Empty);
                    assert!(operation.finish(Ok(())).is_ok());
                    let error =
                        io::Error::new(io::ErrorKind::PermissionDenied, "SECRET_PATH_ENV_PAYLOAD");
                    assert!(operation.finish::<()>(Err(error)).is_err());
                    let operation =
                        span!(DEBUG, "test.visible", outcome = Empty, error.kind = Empty);
                    assert!(
                        operation
                            .finish::<()>(Err(io::Error::new(
                                io::ErrorKind::PermissionDenied,
                                "SECRET_PATH_ENV_PAYLOAD"
                            )))
                            .is_err()
                    );
                });
        });
        let seen = seen.lock().unwrap();
        for field in ["outcome", "error.kind"] {
            let values: Vec<_> = seen
                .iter()
                .filter(|(name, key, _)| *name == "test.caller" && *key == field)
                .map(|(_, _, value)| value.as_str())
                .collect();
            assert_eq!(values, ["sentinel"], "{seen:?}");
        }
        assert!(
            seen.iter()
                .any(|(name, field, value)| *name == "test.visible"
                    && *field == "error.kind"
                    && value == "permission_denied")
        );
        assert!(!seen.iter().any(|(_, _, value)| value.contains("SECRET")));
        assert!(!seen.iter().any(|(name, _, _)| *name == "test.filtered"));
    }

    #[test]
    fn scope_restores_originating_dispatch_for_visible_and_filtered_operations() {
        let (origin, seen) = capture(true);
        let (foreign, foreign_seen) = capture(false);
        for filtered in [false, true] {
            let (caller, operation) = tracing::dispatcher::with_default(&origin, || {
                let caller = tracing::info_span!("test.caller");
                let operation = caller.in_scope(|| {
                    if filtered {
                        span!(DEBUG, "test.filtered")
                    } else {
                        span!(DEBUG, "test.visible")
                    }
                });
                (caller, operation)
            });
            tracing::dispatcher::with_default(&foreign, || {
                operation.scope(|| {
                    drop(tracing::info_span!("test.nested"));
                });
                drop(operation);
            });
            drop(caller);
        }
        let seen = seen.lock().unwrap();
        let parents: Vec<_> = seen
            .iter()
            .filter(|(name, field, _)| *name == "test.nested" && *field == "parent")
            .map(|(_, _, value)| value.as_str())
            .collect();
        assert_eq!(parents, ["test.visible", "test.caller"], "{seen:?}");
        assert!(foreign_seen.lock().unwrap().is_empty());
    }

    #[test]
    fn admitted_work_keeps_observation_after_observer_drop() {
        use std::future::Future;
        use std::task::{Context as TaskContext, Poll, Waker};
        use std::time::{Duration, Instant};
        for native_control in [false, true] {
            let temporary = tempfile::tempfile().unwrap();
            let native = crate::NativeFile::from_file(temporary.try_clone().unwrap()).unwrap();
            let operation_name = if native_control {
                "acyclic.runtime.control"
            } else {
                "acyclic.runtime.blocking_io"
            };
            let (origin, seen) = capture(false);
            let (foreign, foreign_seen) = capture(false);
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let mut observer: Box<dyn Future<Output = io::Result<()>> + Unpin> =
                tracing::dispatcher::with_default(&origin, || {
                    let work = move || {
                        started_tx.send(()).unwrap();
                        release_rx
                            .recv_timeout(Duration::from_secs(10))
                            .expect("release admitted work");
                        drop(tracing::info_span!("test.worker_nested"));
                    };
                    if native_control {
                        Box::new(native.control_async(move |file| {
                            work();
                            file.set_len(73)
                        }))
                            as Box<dyn Future<Output = io::Result<()>> + Unpin>
                    } else {
                        Box::new(crate::run_blocking_io(work))
                    }
                });
            tracing::dispatcher::with_default(&origin, || {
                let mut context = TaskContext::from_waker(Waker::noop());
                assert!(matches!(
                    std::pin::Pin::new(&mut observer).poll(&mut context),
                    Poll::Pending
                ));
            });
            started_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("worker admitted");
            drop(observer);
            assert!(
                !seen
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(_, field, _)| *field == "outcome")
            );
            tracing::dispatcher::with_default(&foreign, || release_tx.send(()).unwrap());
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if seen
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(name, field, _)| *name == operation_name && *field == "close")
                {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "admitted operation did not close"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            let seen = seen.lock().unwrap();
            assert_eq!(
                seen.iter()
                    .filter(|(name, field, value)| *name == operation_name
                        && *field == "outcome"
                        && value == "ok")
                    .count(),
                1,
                "{seen:?}"
            );
            assert_eq!(
                seen.iter()
                    .filter(|(name, field, _)| *name == operation_name && *field == "close")
                    .count(),
                1,
                "{seen:?}"
            );
            assert!(
                seen.iter()
                    .any(|(name, field, value)| *name == "test.worker_nested"
                        && *field == "parent"
                        && value == operation_name)
            );
            assert!(foreign_seen.lock().unwrap().is_empty());
            if native_control {
                assert_eq!(temporary.metadata().unwrap().len(), 73);
            }
        }
    }

    #[test]
    fn pending_file_fence_observes_originating_dispatch() {
        use std::future::Future as _;
        use std::task::{Context as TaskContext, Poll, Waker};
        use std::time::{Duration, Instant};
        let native = crate::NativeFile::from_file(tempfile::tempfile().unwrap()).unwrap();
        let (origin, seen) = capture(false);
        let (foreign, foreign_seen) = capture(false);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut first = tracing::dispatcher::with_default(&origin, || {
            native.control_async(move |_| {
                started_tx.send(()).unwrap();
                release_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release predecessor");
                Ok(())
            })
        });
        let mut context = TaskContext::from_waker(Waker::noop());
        let first_pending = tracing::dispatcher::with_default(&origin, || {
            matches!(
                std::pin::Pin::new(&mut first).poll(&mut context),
                Poll::Pending
            )
        });
        started_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("predecessor admitted");
        let mut second = tracing::dispatcher::with_default(&origin, || {
            native.control_async(|_| panic!("dropped fence waiter must never be admitted"))
        });
        let second_pending = tracing::dispatcher::with_default(&foreign, || {
            matches!(
                std::pin::Pin::new(&mut second).poll(&mut context),
                Poll::Pending
            )
        });
        drop(second);
        drop(first);
        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if seen
                .lock()
                .unwrap()
                .iter()
                .filter(|(name, field, _)| *name == "acyclic.runtime.control" && *field == "close")
                .count()
                == 2
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "predecessor/waiter did not close"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(first_pending && second_pending);
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.iter()
                .filter(|(name, field, value)| *name == "acyclic.runtime.fence_wait"
                    && *field == "parent"
                    && value == "acyclic.runtime.control")
                .count(),
            1,
            "{seen:?}"
        );
        assert_eq!(
            seen.iter()
                .filter(|(name, field, value)| *name == "acyclic.runtime.control"
                    && *field == "outcome"
                    && value == "ok")
                .count(),
            1,
            "{seen:?}"
        );
        assert_eq!(
            seen.iter()
                .filter(|(name, field, _)| *name == "acyclic.runtime.control" && *field == "outcome")
                .count(),
            1,
            "a dropped, unadmitted waiter must not report completion: {seen:?}"
        );
        assert!(foreign_seen.lock().unwrap().is_empty());
    }

    #[test]
    fn uncertain_file_rejection_records_exact_operation_before_admission() {
        use std::future::Future as _;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::task::{Context as TaskContext, Poll, Waker};
        for filtered in [false, true] {
            let temporary = tempfile::tempfile().unwrap();
            temporary.set_len(11).unwrap();
            let native = crate::NativeFile::from_file(temporary.try_clone().unwrap()).unwrap();
            let executed = Arc::new(AtomicBool::new(false));
            let work_executed = Arc::clone(&executed);
            let (origin, seen) = capture(filtered);
            let (foreign, foreign_seen) = capture(false);
            let (caller, mut observer) = tracing::dispatcher::with_default(&origin, || {
                let caller = tracing::info_span!(
                    "test.caller",
                    outcome = "sentinel",
                    error.kind = "sentinel"
                );
                let observer = caller.in_scope(|| {
                    native.control_async(move |file| {
                        work_executed.store(true, Ordering::Release);
                        file.set_len(73)
                    })
                });
                (caller, observer)
            });
            native.uncertain.store(true, Ordering::Release);
            let rejected = tracing::dispatcher::with_default(&foreign, || {
                let mut context = TaskContext::from_waker(Waker::noop());
                matches!(
                    std::pin::Pin::new(&mut observer).poll(&mut context),
                    Poll::Ready(Err(_))
                )
            });
            drop(observer);
            drop(caller);
            assert!(rejected);
            assert!(!executed.load(Ordering::Acquire));
            assert_eq!(temporary.metadata().unwrap().len(), 11);
            assert!(foreign_seen.lock().unwrap().is_empty());
            let seen = seen.lock().unwrap();
            for field in ["outcome", "error.kind"] {
                let values: Vec<_> = seen
                    .iter()
                    .filter(|(name, key, _)| *name == "test.caller" && *key == field)
                    .map(|(_, _, value)| value.as_str())
                    .collect();
                assert_eq!(values, ["sentinel"], "{seen:?}");
            }
            if filtered {
                assert!(
                    !seen
                        .iter()
                        .any(|(name, _, _)| *name == "acyclic.runtime.control")
                );
            } else {
                for (field, value) in [("outcome", "err"), ("error.kind", "other"), ("close", "")] {
                    let values: Vec<_> = seen
                        .iter()
                        .filter(|(name, key, _)| {
                            *name == "acyclic.runtime.control" && *key == field
                        })
                        .map(|(_, _, recorded)| recorded.as_str())
                        .collect();
                    assert_eq!(values, [value], "{seen:?}");
                }
            }
        }
    }
}
