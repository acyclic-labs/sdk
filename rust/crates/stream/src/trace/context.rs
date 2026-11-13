//! Causal custody follows an admitted future's poll, never a worker thread.
use super::SpanId;
use std::{cell::Cell, future::Future};

thread_local! {
    static CURRENT: Cell<Option<SpanId>> = const { Cell::new(None) };
}

pub(super) fn current() -> Option<SpanId> {
    CURRENT.get()
}

struct Restore(Option<SpanId>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.set(self.0);
    }
}

pub(super) async fn within<F: Future>(id: SpanId, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    futures::future::poll_fn(|cx| {
        let _restore = Restore(CURRENT.replace(Some(id)));
        future.as_mut().poll(cx)
    })
    .await
}
