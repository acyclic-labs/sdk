//! Span helpers; see `docs/observability.md`.

use std::io;
use std::pin::Pin;
use tracing::Span;

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

/// Owns a span and its creating dispatcher through polling and destruction.
pub struct OperationSpan {
    span: Span,
    dispatch: tracing::Dispatch,
    // Context only: completion fields always record on `span`, never this caller.
    /// Caller ancestry retained only for a filtered operation.
    pub caller: Option<Span>,
}
impl OperationSpan {
    /// Captures the operation and its creating dispatcher.
    pub fn new(span: Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        let caller = span.is_disabled().then(Span::current);
        Self {
            span,
            dispatch,
            caller,
        }
    }
    /// Runs synchronous work under the originating context.
    pub fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || {
            self.caller.as_ref().unwrap_or(&self.span).in_scope(work)
        })
    }
    /// Polls and drops a producer under the originating context.
    pub fn scope<T>(&self, inner: T) -> Scoped<T> {
        Scoped { inner: Some(inner), context: Some(self.clone()) }
    }
    /// Records a native I/O result on the operation span.
    pub fn record_error(&self, error: Option<&io::Error>) {
        self.span.record("outcome", if error.is_some() { "err" } else { "ok" });
        if let Some(error) = error { self.span.record("error.kind", kind(error)); }
    }

    /// Records completion while returning the original result.
    pub fn finish<T>(&self, result: io::Result<T>) -> io::Result<T> {
        self.record_error(result.as_ref().err());
        result
    }

    /// Records sizes only when this operation is observed.
    pub fn sized(self, batch_len: usize, bytes: impl FnOnce() -> usize) -> Self {
        if !self.span.is_disabled() {
            self.span.record("batch_len", batch_len).record("bytes", bytes());
        }
        self
    }
}
impl Clone for OperationSpan {
    fn clone(&self) -> Self {
        tracing::dispatcher::with_default(&self.dispatch, || Self {
            span: self.span.clone(),
            dispatch: self.dispatch.clone(),
            caller: self.caller.clone(),
        })
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
        tracing::dispatcher::with_default(&self.dispatch, || {
            drop(std::mem::replace(&mut self.span, Span::none()));
            drop(self.caller.take());
        });
    }
}

pin_project_lite::pin_project! {
    /// Polls and destroys the pinned inner value under the owned context.
    pub struct Scoped<T> {
        #[pin]
        /// The original producer retained by this owner.
        pub inner: Option<T>,
        /// Originating poll/drop context, absent for consumer-owned streams.
        pub context: Option<OperationSpan>,
    }
    impl<T> PinnedDrop for Scoped<T> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            match this.context.as_ref() {
                Some(context) => context.in_scope(|| this.inner.set(None)),
                None => this.inner.set(None),
            }
        }
    }
}
impl<T> Scoped<T> {
    /// Retains a producer under its originating dispatcher.
    pub fn new(inner: T, dispatch: tracing::Dispatch) -> Self {
        Self {
            inner: Some(inner),
            context: Some(OperationSpan { span: Span::none(), dispatch, caller: None }),
        }
    }

    /// Transfers the dispatcher selected by a completed transport to its body.
    pub fn set_dispatch(&mut self, dispatch: tracing::Dispatch) {
        self.context = Some(OperationSpan { span: Span::none(), dispatch, caller: None });
    }
}

impl<T: futures_core::Stream> Scoped<T> {
    /// Polls a body whose owner must remain retained after terminal completion.
    pub fn poll_next_retained(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T::Item>> {
        let this = self.project();
        let poll = || this.inner.as_pin_mut()
            .map_or(std::task::Poll::Ready(None), |inner| inner.poll_next(cx));
        match this.context.as_ref() {
            Some(context) => context.in_scope(poll),
            None => poll(),
        }
    }
}
impl<T: std::future::Future> std::future::Future for Scoped<T> {
    type Output = T::Output;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let Some(mut inner) = this.inner.as_pin_mut() else {
            return std::task::Poll::Pending;
        };
        match this.context.as_ref() {
            Some(context) => context.in_scope(|| inner.as_mut().poll(cx)),
            None => inner.as_mut().poll(cx),
        }
    }
}
impl<T: futures_core::Stream> futures_core::Stream for Scoped<T> {
    type Item = T::Item;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let next = self.as_mut().poll_next_retained(cx);
        if matches!(&next, std::task::Poll::Ready(None)) {
            let mut this = self.project();
            match this.context.as_ref() {
                Some(context) => context.in_scope(|| this.inner.set(None)),
                None => this.inner.set(None),
            }
        }
        next
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
