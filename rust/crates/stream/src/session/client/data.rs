//! Canonical data frames without progress, preserving original payload custody.
use super::{Body, Credit, Cursor, Frame, StreamError, wire};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

impl Cursor {
    /// Receive one fresh data frame, retaining its original receive credit.
    /// Calls and sockets cannot carry skipped-cut watermarks.
    pub(super) async fn receive_data(
        &mut self,
    ) -> Result<Option<(wire::SessionData, Credit)>, StreamError> {
        loop {
            let Some(delivery) = self.receive().await? else {
                return Ok(None);
            };
            if delivery.before.is_some() {
                return Err(StreamError::InvalidResponse);
            }
            if !delivery.consume(&self.progress)? {
                continue;
            }
            let data = delivery
                .item
                .into_data()
                .map_err(|_| StreamError::InvalidResponse)?;
            return Ok(Some((data, delivery.credit)));
        }
    }
}

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
impl<S: futures::Stream<Item = Result<Frame, StreamError>>> futures::Stream for Data<S> {
    type Item = Result<Frame, StreamError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            return Poll::Ready(
                match std::task::ready!(this.source.as_mut().poll_next(cx)) {
                    Some(Ok(item)) if matches!(item.wire().body, Some(Body::Watermark(_))) => {
                        continue;
                    }
                    item => item,
                },
            );
        }
    }
}
