use super::OperationSpan;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    struct Traced<F, S> {
        #[pin]
        future: Option<F>,
        span: Option<OperationSpan>,
        create_span: Option<S>,
    }
    impl<F, S> PinnedDrop for Traced<F, S> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(span) = this.span.as_ref() {
                span.in_scope(|| this.future.set(None));
                return;
            }
            this.future.set(None);
        }
    }
}

// The caller's one trait-erasure box contains the producer itself. Span setup
// waits until polling; the producer is never moved through an async wrapper.
#[inline(never)]
pub(crate) fn traced_future<'a, T: 'a, F, S>(
    create_span: S,
    create: impl FnOnce() -> F,
) -> impl Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a
where
    F: Future<Output = crate::Result<T>> + acyclic_stream::ProviderTask + 'a,
    S: FnOnce() -> OperationSpan + acyclic_stream::ProviderTask + 'a,
{
    Traced {
        future: Some(create()),
        span: None,
        create_span: Some(create_span),
    }
}

impl<T, F, S> Future for Traced<F, S>
where
    F: Future<Output = crate::Result<T>>,
    S: FnOnce() -> OperationSpan,
{
    type Output = crate::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        if let Some(create) = this.create_span.take() {
            *this.span = Some(create());
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let Some(span) = this.span.as_ref() else {
                return Poll::Pending;
            };
            span.in_scope(|| {
                let result = poll_producer(this.future.as_mut(), cx);
                if let Poll::Ready(result) = &result {
                    span.record("outcome", if result.is_ok() { "ok" } else { "err" });
                    if let Err(error) = result {
                        span.record("error.kind", error.kind());
                    }
                }
                result
            })
        }
        #[cfg(target_arch = "wasm32")]
        poll_producer(this.future, cx)
    }
}

// Match await: destroy a completed producer under its context before recording
// the continuation's outcome. Pending producers remain owned until cancellation.
#[inline(never)]
fn poll_producer<F: Future>(
    mut future: Pin<&mut Option<F>>,
    cx: &mut Context<'_>,
) -> Poll<F::Output> {
    let Some(inner) = future.as_mut().as_pin_mut() else {
        return Poll::Pending;
    };
    let result = inner.poll(cx);
    if result.is_ready() {
        future.set(None);
    }
    result
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;
