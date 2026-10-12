use super::{Dispatch, OperationSpan, Span};
use std::{future::Future, pin::Pin, task::{Context, Poll}};

pin_project_lite::pin_project! {
    /// Polls and destroys a producer under its originating context.
    pub struct Scoped<T> {
        #[pin]
        inner: Option<T>,
        context: Option<OperationSpan>,
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
    /// Retains a producer under an optional operation context.
    pub fn with_context(inner: T, context: Option<OperationSpan>) -> Self {
        Self { inner: Some(inner), context }
    }
    /// Borrows the producer before pinning.
    pub fn inner_mut(&mut self) -> Option<&mut T> { self.inner.as_mut() }
    /// Retains a producer under an originating dispatcher.
    pub fn new(inner: T, dispatch: Dispatch) -> Self {
        let mut result = Self::with_context(inner, None);
        result.set_dispatch(dispatch);
        result
    }
    /// Transfers a completed transport's dispatcher to its body.
    pub fn set_dispatch(&mut self, dispatch: Dispatch) {
        self.context = Some(OperationSpan { span: Span::none(), context: Span::none(), dispatch });
    }
}

impl<T: futures_core::Stream> Scoped<T> {
    /// Polls a body that retains its owner beyond EOF.
    pub fn poll_next_retained(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        let this = self.project();
        let poll = || this.inner.as_pin_mut().map_or(Poll::Ready(None), |inner| inner.poll_next(cx));
        match this.context.as_ref() {
            Some(context) => context.in_scope(poll),
            None => poll(),
        }
    }
}
impl<T: Future> Future for Scoped<T> {
    type Output = T::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let Some(mut inner) = this.inner.as_pin_mut() else { return Poll::Pending; };
        match this.context.as_ref() {
            Some(context) => context.in_scope(|| inner.as_mut().poll(cx)),
            None => inner.as_mut().poll(cx),
        }
    }
}
impl<T: futures_core::Stream> futures_core::Stream for Scoped<T> {
    type Item = T::Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = self.as_mut().poll_next_retained(cx);
        if matches!(&next, Poll::Ready(None)) {
            let mut this = self.project();
            match this.context.as_ref() {
                Some(context) => context.in_scope(|| this.inner.set(None)),
                None => this.inner.set(None),
            }
        }
        next
    }
}
