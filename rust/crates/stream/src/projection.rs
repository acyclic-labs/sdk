//! Synchronous stream projection without retaining a per-item future.
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    pub(crate) struct FilterMap<S, F> {
        #[pin]
        source: S,
        project: F,
    }
}

pub(crate) fn filter_map<S, F>(source: S, project: F) -> FilterMap<S, F> {
    FilterMap { source, project }
}

impl<S, F, T> futures::Stream for FilterMap<S, F>
where
    S: futures::Stream,
    F: FnMut(S::Item) -> Option<T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut this = self.project();
        loop {
            match std::task::ready!(this.source.as_mut().poll_next(cx)) {
                Some(item) => {
                    if let Some(projected) = (this.project)(item) {
                        return Poll::Ready(Some(projected));
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }
}
