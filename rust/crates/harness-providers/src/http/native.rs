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
        Pin::new(&mut this.stream).poll_next(cx)
    }
}

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
