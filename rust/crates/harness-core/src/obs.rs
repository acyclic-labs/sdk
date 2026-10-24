//! Tracing glue for `docs/observability.md`.

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use acyclic_native_runtime::obs::OperationSpan;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tracing::field::Empty;

/// Records `outcome` and `error.kind` on the current span, then returns `result`.
pub(crate) fn outcome<T>(result: crate::Result<T>) -> crate::Result<T> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let span = tracing::Span::current();
        match &result {
            Ok(_) => span.record("outcome", "ok"),
            Err(error) => span
                .record("outcome", "err")
                .record("error.kind", error.kind()),
        };
    }
    result
}

/// Records extra fields on the current span; expands to nothing on wasm32.
macro_rules! obs_record {
    ($($field:literal = $value:expr),+ $(,)?) => {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let span = ::tracing::Span::current();
            $(span.record($field, $value);)+
        }
    };
}
pub(crate) use obs_record;

/// Opens an owned operation span; WASM does not evaluate fields.
macro_rules! obs_span {
    (INFO, $($rest:tt)+) => { crate::obs::obs_span!(@ INFO, $($rest)+) };
    (@ $level:ident, $name:literal $(, $field:ident = $value:expr)* $(,)?) => {{
        #[cfg(not(target_arch = "wasm32"))]
        let span = crate::obs::OperationSpan::new(::tracing::span!(
            ::tracing::Level::$level,
            $name,
            $($field = $value,)*
            outcome = ::tracing::field::Empty,
            error.kind = ::tracing::field::Empty,
        ));
        #[cfg(target_arch = "wasm32")]
        let span = crate::obs::OperationSpan;
        span
    }};
    ($($rest:tt)+) => { crate::obs::obs_span!(@ DEBUG, $($rest)+) };
}
pub(crate) use obs_span;

/// wasm32 records nothing.
#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy)]
pub(crate) struct OperationSpan;

/// Runs `future` inside `span` and records its outcome there.
#[cfg(not(target_arch = "wasm32"))]
fn traced_frame<T, F: Future<Output = crate::Result<T>>>(
    span: OperationSpan,
    future: F,
) -> impl Future<Output = crate::Result<T>> {
    let context = span.clone();
    context.scope(async move {
        let result = future.await;
        span.record("outcome", if result.is_ok() { "ok" } else { "err" });
        if let Err(error) = &result {
            span.record("error.kind", error.kind());
        }
        result
    })
}

/// Spans are not recorded on wasm32, so the frame is the future itself.
#[cfg(target_arch = "wasm32")]
fn traced_frame<T, F: Future<Output = crate::Result<T>>>(_: OperationSpan, future: F) -> F {
    future
}

/// Boxes a trait method's future inside `span` and records its outcome there.
///
/// `create` builds the future inside the heap allocation's own short frame,
/// so neither this function nor the caller's poll frame ever holds it.
#[inline(never)]
pub(crate) fn traced<'a, T: 'a, F>(
    span: OperationSpan,
    create: impl FnOnce() -> F,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<T>>
where
    F: Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
{
    // Unbudgeted: a caller that holds a live budget uses `traced_on`.
    acyclic_fs::in_heap(move || traced_frame(span, create()))
}

/// [`traced`] where an allocation budget is live: the whole frame is charged
/// on `io` before it is made, and a refused frame is never built.
#[cfg(feature = "filesystem")]
#[inline(never)]
pub(crate) fn traced_on<'a, T: 'a, F>(
    io: Option<&crate::executor::PreparationIo>,
    span: OperationSpan,
    create: impl FnOnce() -> F,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<T>>
where
    F: Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
{
    match crate::charged_box::charged_frame(io, move || traced_frame(span, create())) {
        Ok(frame) => frame,
        // Unbudgeted: a refused frame holds only its error.
        Err(error) => Box::pin(async move { Err(error) }),
    }
}

/// [`traced`] where no allocation budget can exist: without the filesystem
/// there is no budget to charge.
#[cfg(not(feature = "filesystem"))]
pub(crate) fn traced_on<'a, T: 'a, F>(
    _: Option<&crate::executor::PreparationIo>,
    span: OperationSpan,
    create: impl FnOnce() -> F,
) -> acyclic_stream::BoxProviderFuture<'a, crate::Result<T>>
where
    F: Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
{
    // Unbudgeted: no budget exists without the filesystem.
    traced(span, create)
}

#[cfg(any(feature = "filesystem", all(test, not(target_arch = "wasm32"))))]
mod typed;
#[cfg(any(feature = "filesystem", all(test, not(target_arch = "wasm32"))))]
pub(crate) use typed::traced_future;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn traced_future_retains_one_payload_and_records_only_when_ready() {
    capture::sync(|seen| {
        use std::{
            future::Future,
            pin::Pin,
            task::{Context, Poll},
        };

        struct Payload {
            bytes: [u8; 8192],
            ready: bool,
        }

        impl Future for Payload {
            type Output = crate::Result<()>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                assert!(self.bytes.iter().all(|byte| *byte == 7));
                if !self.ready {
                    self.ready = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Err(crate::Error::Invalid("fixture refusal".into())))
                }
            }
        }

        let span = obs_span!(INFO, "acyclic.harness.test.payload");
        let future = traced_future(
            move || span,
            || Payload {
                bytes: [7; 8192],
                ready: false,
            },
        );
        // This is an ownership bound, independent of the particular payload size:
        // the tracing adapter must not retain a second copy of its child future.
        assert!(std::mem::size_of_val(&future) < 2 * std::mem::size_of::<Payload>());
        futures::pin_mut!(future);
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(!capture::has(
            &seen,
            "acyclic.harness.test.payload",
            "outcome",
            "err"
        ));
        assert!(matches!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(Err(crate::Error::Invalid(reason))) if reason == "fixture refusal"
        ));
        assert!(capture::has(
            &seen,
            "acyclic.harness.test.payload",
            "outcome",
            "err"
        ));
    });
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
fn filtered_operation_cannot_record_failure_on_its_parent() {
    capture::sync(|seen| {
        let parent = obs_span!(INFO, "acyclic.harness.test.parent");
        parent.in_scope(|| {
            for boxed in [false, true] {
                let span = OperationSpan::new(tracing::Span::none());
                let future =
                    std::future::ready(Err::<(), _>(crate::Error::Invalid("test failure".into())));
                let result = if boxed {
                    futures::executor::block_on(traced(span, || future))
                } else {
                    futures::executor::block_on(traced_future(|| span, || future))
                };
                assert!(result.is_err());
            }
        });
        assert!(!capture::has(
            &seen,
            "acyclic.harness.test.parent",
            "outcome",
            "err"
        ));
        assert!(capture::has(
            &seen,
            "acyclic.harness.test.parent",
            "outcome",
            ""
        ));
    });
}

/// Span-test capture layer shared by the crate's test modules.
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod capture {
    use std::sync::{Arc, Mutex};
    use tracing::{
        field::{Field, Visit},
        span,
    };
    use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

    /// `(span, field, value)` for every declared and recorded span field.
    pub(crate) type Seen = Arc<Mutex<Vec<(&'static str, &'static str, String)>>>;

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
    impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
        fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
            event.record(&mut Fields(
                event.metadata().name(),
                &mut self.0.lock().unwrap(),
            ));
        }

        fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
            let name = attrs.metadata().name();
            let parent = ctx
                .span(id)
                .and_then(|span| span.parent())
                .map(|span| span.metadata().name());
            // Registry lookup precedes the capture lock, as in on_record.
            let mut seen = self.0.lock().unwrap();
            if let Some(parent) = parent {
                seen.push((name, "parent", parent.to_owned()));
            }
            let fields = attrs.metadata().fields();
            seen.extend(fields.iter().map(|f| (name, f.name(), String::new())));
            attrs.record(&mut Fields(name, &mut seen));
        }
        fn on_record(&self, id: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
            let name = ctx.span(id).unwrap().name();
            values.record(&mut Fields(name, &mut self.0.lock().unwrap()));
        }
    }

    /// Captures synchronous work without exposing a thread guard to async tests.
    pub(crate) fn sync<T>(test: impl FnOnce(Seen) -> T) -> T {
        let (seen, dispatch) = subscriber();
        tracing::dispatcher::with_default(&dispatch, || test(seen))
    }

    fn subscriber() -> (Seen, tracing::Dispatch) {
        use tracing_subscriber::layer::SubscriberExt as _;
        let seen = Seen::default();
        let dispatch = acyclic_test_support::capture::dispatch(
            tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
        );
        (seen, dispatch)
    }

    /// Installs the test's subscriber only while its future is being polled.
    /// Spawned tasks must carry this dispatcher with `with_current_subscriber`.
    pub(crate) async fn run<T, F: Future<Output = T>>(test: impl FnOnce(Seen) -> F) -> T {
        use tracing::instrument::WithSubscriber as _;
        let (seen, dispatch) = subscriber();
        async move { test(seen).await }
            .with_subscriber(dispatch)
            .await
    }

    #[cfg(test)]
    mod tests;

    pub(crate) fn has(seen: &Seen, span: &str, field: &str, value: &str) -> bool {
        let seen = seen.lock().unwrap();
        seen.iter()
            .any(|(s, f, v)| *s == span && *f == field && v == value)
    }

    /// Asserts no span declares a forbidden field or records one of `leaks`.
    pub(crate) fn assert_clean(seen: &Seen, leaks: &[&str]) {
        for (span, field, value) in seen.lock().unwrap().iter() {
            assert!(
                !["path", "token", "content", "body", "authorization"].contains(field),
                "{span} records {field}"
            );
            assert!(
                !leaks.iter().any(|leak| value.contains(leak)),
                "{span}.{field}={value}"
            );
        }
    }
}
