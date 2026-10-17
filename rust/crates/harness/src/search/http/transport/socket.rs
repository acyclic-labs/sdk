use crate::{Error, NativeCustody, NativeWork, Result};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// Meter the actual transport, before TLS/HTTP can buffer a complete chunk.
// The failure slot is borrowed by this connection, without another shared store.
pub(super) struct Socket<'a, I> {
    inner: I,
    work: NativeWork<'a>,
    failure: &'a mut Option<Error>,
    held: NativeCustody,
    received: u64,
}

impl<'a, I> Socket<'a, I> {
    pub(super) fn new(
        inner: I,
        work: NativeWork<'a>,
        failure: &'a mut Option<Error>,
    ) -> Result<Self> {
        Ok(Self {
            inner,
            work,
            failure,
            held: crate::tool::edit::allocation(work, 0)?,
            received: 0,
        })
    }
    fn failed(&mut self, error: Error) -> Poll<std::io::Result<()>> {
        *self.failure = Some(error);
        Poll::Ready(Err(std::io::ErrorKind::Other.into()))
    }
}

impl<I: AsyncRead + Unpin> AsyncRead for Socket<'_, I> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let mut maximum = buffer.remaining().min(8192);
        if maximum == 0 {
            return Poll::Ready(Ok(()));
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let reserved = if let Some(work) = this.work {
            let remaining = match work.remaining() {
                Ok(remaining) => remaining,
                Err(error) => return this.failed(error.into()),
            };
            maximum = maximum.min(remaining.source_bytes_read as usize).max(1);
            // Keep all accepted transport bytes and the next read buffer funded
            // until the TLS/HTTP connection drops its retained frames/buffers.
            if let Some(held) = &mut this.held {
                let Some(retained) = this.received.checked_add(buffer.capacity() as u64) else {
                    return this.failed(super::super::super::error(
                        this.work,
                        "search transport allocation overflow",
                    ));
                };
                if let Err(error) = held.resize(retained) {
                    return this.failed(error.into());
                }
            }
            match work.reserve_work(acyclic_fs::WorkCounters {
                source_bytes_read: maximum as u64,
                ..Default::default()
            }) {
                Ok(reserved) => Some(reserved),
                Err(error) => return this.failed(error.into()),
            }
        } else {
            None
        };
        let initialized = buffer.initialized().len() - buffer.filled().len();
        let zeroed = maximum.saturating_sub(initialized);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = this.work {
            if let Err(error) = work.charge(acyclic_fs::WorkCounters {
                bytes_copied: zeroed as u64,
                ..Default::default()
            }) {
                return this.failed(error.into());
            }
        }
        let _ = zeroed;
        let mut limited = ReadBuf::new(buffer.initialize_unfilled_to(maximum));
        let result = Pin::new(&mut this.inner).poll_read(cx, &mut limited);
        let count = limited.filled().len();
        buffer.advance(count);
        this.received += count as u64;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(reserved) = reserved {
            if let Err(error) = reserved.record_completed(acyclic_fs::WorkCounters {
                source_bytes_read: count as u64,
                ..Default::default()
            }) {
                return this.failed(error.into());
            }
        }
        result
    }
}
impl<I: AsyncWrite + Unpin> AsyncWrite for Socket<'_, I> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(all(test, feature = "filesystem"))]
mod tests;
