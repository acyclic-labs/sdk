use super::*;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork, WorkCounters};
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

struct Body<S> {
    stream: S,
    custody: Option<NativeAllocation>,
}

impl<S: Stream<Item = Result<Bytes>> + Unpin> Stream for Body<S> {
    type Item = Result<Bytes>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(custody) = &this.custody {
            if let Err(cause) = custody.work().remaining() {
                return Poll::Ready(Some(Err(cause.into())));
            }
        }
        let next = Pin::new(&mut this.stream).poll_next(cx);
        match (next, &this.custody) {
            (Poll::Ready(Some(Ok(bytes))), Some(owner)) if !bytes.is_empty() => {
                // Admit the entire received buffer before exposing it to a reader.
                // SSE may suspend at any frame with the remainder still resident.
                let result = owner
                    .work()
                    .claim(
                        bytes.len() as u64,
                        0,
                        WorkCounters {
                            source_bytes_read: bytes.len() as u64,
                            ..WorkCounters::UNCHARGED
                        },
                    )
                    .map_err(acyclic_harness_core::Error::from)
                    .and_then(|custody| super::owned_bytes::owned(bytes, custody));
                Poll::Ready(Some(result))
            }
            (next, _) => next,
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

pub(super) fn body<S>(
    stream: S,
    work: Option<&NativeMaterializationWork>,
) -> Result<BoxProviderStream<'static, Result<Bytes>>>
where
    S: Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
{
    let custody = work
        .map(|work| {
            work.claim(
                std::mem::size_of::<Body<S>>() as u64,
                1,
                WorkCounters::UNCHARGED,
            )
        })
        .transpose()?;
    Ok(Box::pin(Body { stream, custody }))
}
