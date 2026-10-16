use futures::{Stream, channel::oneshot};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use wasm_bindgen::{JsCast as _, closure::Closure};

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

struct Timer {
    global: web_sys::WorkerGlobalScope,
    handle: i32,
    _callback: Closure<dyn FnMut()>,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.global.clear_timeout_with_handle(self.handle);
    }
}
pub(crate) async fn delay(milliseconds: u32) {
    // Window and worker globals expose the same timer operations. Cancellation
    // unregisters the callback; no polling task or detached timeout is retained.
    let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    let (sender, receiver) = oneshot::channel();
    let callback = Closure::once(move || {
        let _ = sender.send(());
    });
    let Ok(milliseconds) = i32::try_from(milliseconds) else {
        return;
    };
    let Ok(handle) = global.set_timeout_with_callback_and_timeout_and_arguments_0(
        callback.as_ref().unchecked_ref(),
        milliseconds,
    ) else {
        return;
    };
    let _timer = Timer {
        global,
        handle,
        _callback: callback,
    };
    let _ = receiver.await;
}
