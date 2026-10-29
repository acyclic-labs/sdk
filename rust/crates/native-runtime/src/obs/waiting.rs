use super::OperationSpan;
use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    /// A primitive wait, retired when its future completes or is dropped.
    pub struct Waiting<F> {
        #[pin]
        inner: Option<F>,
        context: OperationSpan,
    }
    impl<F> PinnedDrop for Waiting<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            if this.inner.is_some() {
                this.context.in_scope(|| this.inner.set(None));
                this.context.record("finished", true);
            }
        }
    }
}

impl<F: Future> Future for Waiting<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let Some(mut inner) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Pending;
        };
        let result = this.context.in_scope(|| inner.as_mut().poll(cx));
        if result.is_ready() {
            this.context.in_scope(|| this.inner.set(None));
            this.context.record("finished", true);
        }
        result
    }
}

/// Names the primitive and opaque operation key a future is waiting for.
///
/// The report ends with the future, even if descendants retain its tracing span.
/// This adds no task, timer, wake or completion flag governing readiness:
/// only the producer determines readiness.
/// Keys must identify operations, never contain prompts or credentials.
pub fn waiting<F: Future>(primitive: &'static str, key: impl Display, future: F) -> Waiting<F> {
    Waiting {
        inner: Some(future),
        context: OperationSpan::new(tracing::info_span!(
            target: "acyclic.wait",
            "waiting",
            primitive,
            key = %key,
            finished = tracing::field::Empty,
        )),
    }
}
