use futures::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(test)]
#[path = "context/tests.rs"]
mod tests;

#[derive(Clone)]
pub(crate) struct OperationSpan {
    span: tracing::Span,
    dispatch: tracing::Dispatch,
}
impl OperationSpan {
    pub(crate) fn new(span: tracing::Span) -> Self {
        let dispatch = span
            .with_subscriber(|(_, dispatch)| dispatch.clone())
            .unwrap_or_else(|| tracing::dispatcher::get_default(Clone::clone));
        Self { span, dispatch }
    }
    pub(crate) fn new_root(span: tracing::Span) -> Self {
        // Physical spans use explicit parent: None and retain only their own
        // dispatch. The browser executor carries no ambient Tokio task context.
        Self::new(span)
    }
    pub(crate) fn follows_from_same_subscriber(&self, cause: &Self) {
        if let (Some(registry), Some(origin)) = (
            self.dispatch.downcast_ref::<tracing_subscriber::Registry>(),
            cause
                .dispatch
                .downcast_ref::<tracing_subscriber::Registry>(),
        ) && std::ptr::eq(registry, origin)
        {
            self.span.follows_from(cause.span.id());
        }
    }
    pub(crate) fn in_scope<T>(&self, work: impl FnOnce() -> T) -> T {
        tracing::dispatcher::with_default(&self.dispatch, || self.span.in_scope(work))
    }
    pub(crate) fn scope<T>(&self, source: T) -> Scoped<T> {
        Scoped {
            source: Some(source),
            context: self.clone(),
        }
    }
}
impl std::ops::Deref for OperationSpan {
    type Target = tracing::Span;
    fn deref(&self) -> &Self::Target {
        &self.span
    }
}
pin_project_lite::pin_project! {
    pub(crate) struct Scoped<T> {
        #[pin]
        source: Option<T>,
        context: OperationSpan,
    }
    impl<T> PinnedDrop for Scoped<T> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            this.context.in_scope(|| this.source.set(None));
        }
    }
}
impl<T: Future> Future for Scoped<T> {
    type Output = T::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.context
            .in_scope(|| this.source.as_pin_mut().expect("live producer").poll(cx))
    }
}
impl<T: Stream> Stream for Scoped<T> {
    type Item = T::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let result = this.context.in_scope(|| {
            this.source
                .as_mut()
                .as_pin_mut()
                .map_or(Poll::Ready(None), |source| source.poll_next(cx))
        });
        if matches!(result, Poll::Ready(None)) {
            this.context.in_scope(|| this.source.set(None));
        }
        result
    }
}
