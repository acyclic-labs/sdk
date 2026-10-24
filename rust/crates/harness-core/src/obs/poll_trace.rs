use super::OperationSpan;
#[cfg(not(target_arch = "wasm32"))]
use std::pin::Pin;

/// Own one payload while acquiring its tracing parent on the first poll.
pub(crate) fn traced_on_poll<'a, T: 'a>(
    build: impl FnMut() -> OperationSpan + acyclic_stream::ProviderTask + 'a,
    future: impl Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
) -> impl Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a {
    #[cfg(not(target_arch = "wasm32"))]
    {
        PollTrace {
            future: Some(future),
            span: None,
            build,
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        let _ = build;
        future
    }
}

#[cfg(not(target_arch = "wasm32"))]
pin_project_lite::pin_project! {
    struct PollTrace<F, B> {
        #[pin]
        future: Option<F>,
        span: Option<OperationSpan>,
        build: B,
    }
    impl<F, B> PinnedDrop for PollTrace<F, B> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            match this.span.as_ref() {
                Some(span) => span.in_scope(|| this.future.set(None)),
                None => this.future.set(None),
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T, F, B> Future for PollTrace<F, B>
where
    F: Future<Output = crate::Result<T>>,
    B: FnMut() -> OperationSpan,
{
    type Output = crate::Result<T>;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        let span = this.span.get_or_insert_with(this.build);
        let Some(mut future) = this.future.as_pin_mut() else {
            return std::task::Poll::Pending;
        };
        let result = span.in_scope(|| future.as_mut().poll(cx));
        if let std::task::Poll::Ready(result) = &result {
            span.record("outcome", if result.is_ok() { "ok" } else { "err" });
            if let Err(error) = result {
                span.record("error.kind", error.kind());
            }
        }
        result
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[test]
fn lazy_trace_owns_one_payload_and_keeps_first_poll_context_through_drop() {
    use crate::obs::capture;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    struct Payload([u8; 8192], bool);
    impl Future for Payload {
        type Output = crate::Result<()>;
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            assert!(self.0.iter().all(|byte| *byte == 7));
            if self.1 {
                Poll::Ready(Err(crate::Error::Invalid("fixture refusal".into())))
            } else {
                self.1 = true;
                Poll::Pending
            }
        }
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            obs_record!("dropped" = true);
        }
    }

    let (seen, _capture) = capture::install();
    let future = traced_on_poll(
        || {
            obs_span!(
                INFO,
                "acyclic.harness.test.lazy",
                dropped = crate::obs::Empty
            )
        },
        Payload([7; 8192], false),
    );
    assert!(std::mem::size_of_val(&future) < 2 * std::mem::size_of::<Payload>());
    let mut future = Box::pin(future);
    let first = obs_span!(INFO, "acyclic.harness.test.first_poll");
    let foreign = obs_span!(INFO, "acyclic.harness.test.foreign_poll");
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(
        first
            .in_scope(|| future.as_mut().poll(&mut cx))
            .is_pending()
    );
    assert!(!capture::has(
        &seen,
        "acyclic.harness.test.lazy",
        "outcome",
        "err"
    ));
    assert!(
        foreign
            .in_scope(|| future.as_mut().poll(&mut cx))
            .is_ready()
    );
    foreign.in_scope(|| drop(future));
    assert!(capture::has(
        &seen,
        "acyclic.harness.test.lazy",
        "parent",
        "acyclic.harness.test.first_poll"
    ));
    assert!(capture::has(
        &seen,
        "acyclic.harness.test.lazy",
        "outcome",
        "err"
    ));
    assert!(capture::has(
        &seen,
        "acyclic.harness.test.lazy",
        "dropped",
        "true"
    ));

    drop(traced_on_poll(
        || obs_span!(INFO, "acyclic.harness.test.unpolled"),
        std::future::ready(Ok(())),
    ));
    assert!(!capture::has(
        &seen,
        "acyclic.harness.test.unpolled",
        "outcome",
        ""
    ));
}
