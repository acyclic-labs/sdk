//! Synchronous canonical data projection, without a retained per-item future.
use super::{Body, Received, StreamError, wire};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    pub(super) struct Data<S> {
        #[pin]
        source: S,
    }
}
impl<S> Data<S> {
    pub(super) fn new(source: S) -> Self {
        Self { source }
    }
}
impl<S: futures::Stream<Item = Result<Received, StreamError>>> futures::Stream for Data<S> {
    type Item = Result<wire::SessionData, StreamError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            return Poll::Ready(
                match std::task::ready!(this.source.as_mut().poll_next(cx)) {
                    Some(Ok(Received::Frame(wire::SessionFrame {
                        body: Some(Body::Watermark(_)),
                        ..
                    }))) => continue,
                    Some(Ok(item)) => Some(item.into_data()),
                    Some(Err(error)) => Some(Err(error)),
                    None => None,
                },
            );
        }
    }
}
