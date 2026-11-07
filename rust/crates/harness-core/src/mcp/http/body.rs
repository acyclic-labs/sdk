//! Enforce the byte frontier independently of transport chunk boundaries.
use crate::{Error, OperationId, Result};
use bytes::Bytes;
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Admits the same response prefix regardless of a provider's chunk sizes.
/// Foreign adapters use this before copying bytes into Rust storage.
pub struct ResponseByteFrontier {
    remaining: u32,
    overflow: bool,
}

impl ResponseByteFrontier {
    /// Starts one response's byte allowance, without allocation or I/O.
    pub fn new(maximum: u32) -> Self {
        Self {
            remaining: maximum,
            overflow: false,
        }
    }

    /// Admits a prefix, retaining overflow for the next demand after that prefix.
    /// None means overflow has no remaining prefix; the original operation must
    /// return its typed error without another read or copy.
    pub fn admit(&mut self, length: u64) -> Option<u32> {
        if self.overflow {
            return None;
        }
        let allowed = length.min(u64::from(self.remaining)) as u32;
        self.remaining -= allowed;
        self.overflow = length > u64::from(allowed);
        (!self.overflow || allowed != 0).then_some(allowed)
    }

    /// Whether the next demand must settle overflow without reading its source.
    pub fn overflowed(&self) -> bool {
        self.overflow
    }
}

pin_project_lite::pin_project! {
    pub(super) struct Bounded<S> {
        #[pin]
        body: S,
        frontier: ResponseByteFrontier,
        ended: bool,
        operation: OperationId,
    }
}

pub(super) fn bounded<S>(body: S, maximum: u32, operation: OperationId) -> Bounded<S> {
    Bounded {
        body,
        frontier: ResponseByteFrontier::new(maximum),
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
        if this.frontier.overflowed() {
            *this.ended = true;
            return Poll::Ready(Some(Err(Error::Indeterminate(*this.operation))));
        }
        match this.body.poll_next(cx) {
            Poll::Ready(Some(Ok(mut chunk))) => {
                let Some(allowed) = this.frontier.admit(chunk.len() as u64) else {
                    *this.ended = true;
                    return Poll::Ready(Some(Err(Error::Indeterminate(*this.operation))));
                };
                // Bytes::truncate only changes its view; no payload copy.
                chunk.truncate(allowed as usize);
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

        #[cfg(feature = "test-support")]
        super::super::codec_fuzz::check(include_bytes!(
            "../../../../../codec-fuzz/corpus/harness_codecs/mcp-http-invalid-utf8-frontier"
        ));
    }
}
