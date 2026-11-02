//! Enforce the byte frontier independently of transport chunk boundaries.
use crate::{Error, OperationId, Result};
use bytes::Bytes;
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

pin_project_lite::pin_project! {
    pub(super) struct Bounded<S> {
        #[pin]
        body: S,
        remaining: usize,
        overflow: bool,
        ended: bool,
        operation: OperationId,
    }
}

pub(super) fn bounded<S>(body: S, maximum: u32, operation: OperationId) -> Bounded<S> {
    Bounded {
        body,
        remaining: maximum as usize,
        overflow: false,
        ended: false,
        operation,
    }
}

impl<S: Stream<Item = Result<Bytes>>> Stream for Bounded<S> {
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.ended {
            return Poll::Ready(None);
        }
        if *this.overflow {
            *this.ended = true;
            return Poll::Ready(Some(Err(Error::Indeterminate(*this.operation))));
        }
        match this.body.poll_next(cx) {
            Poll::Ready(Some(Ok(mut chunk))) => {
                if chunk.len() > *this.remaining {
                    *this.overflow = true;
                    if *this.remaining == 0 {
                        *this.ended = true;
                        return Poll::Ready(Some(Err(Error::Indeterminate(*this.operation))));
                    }
                    // Expose the same permitted prefix for every partition.
                    // Bytes::truncate only changes its view; no payload copy.
                    chunk.truncate(*this.remaining);
                }
                *this.remaining -= chunk.len();
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(other) => {
                *this.ended = true;
                Poll::Ready(other)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use std::cell::Cell;

    #[test]
    fn frontier_borrows_prefix_and_settles_without_another_source_poll() {
        let polls = Cell::new(0);
        let bytes = Bytes::from_static(b"permitted and excess");
        let pointer = bytes.as_ptr();
        let operation = OperationId::from_bytes([7; 16]);
        let body = futures::stream::poll_fn(|_| {
            polls.set(polls.get() + 1);
            assert_eq!(polls.get(), 1, "overflow polled the original body again");
            Poll::Ready(Some(Ok(bytes.clone())))
        });
        let body = bounded(body, 9, operation);
        futures::pin_mut!(body);
        futures::executor::block_on(async {
            let prefix = body.next().await.unwrap().unwrap();
            assert_eq!(prefix.as_ref(), b"permitted");
            assert_eq!(prefix.as_ptr(), pointer, "frontier copied payload bytes");
            assert!(
                matches!(body.next().await, Some(Err(Error::Indeterminate(id))) if id == operation)
            );
            assert!(body.next().await.is_none());
        });
        assert_eq!(polls.get(), 1);
    }
}
