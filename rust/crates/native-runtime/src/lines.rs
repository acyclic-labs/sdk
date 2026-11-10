//! Bounded line framing shared by native pipes and incremental host output.
//!
//! Tokio's `LinesCodec` is the sole framer (also used by rmcp's JSON-RPC codec).
//! Callers decide whether a malformed frame ends a protocol or is discarded.
use bytes::BytesMut;
use futures_core::Stream;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::AsyncRead;
/// Distinct line-bound and native I/O failures from the shared framer.
pub use tokio_util::codec::LinesCodecError as LineError;
use tokio_util::codec::{Decoder as _, FramedRead, LinesCodec};

fn invalid(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

/// Cancellation-safe line reads: partial input belongs to the retained reader.
pub struct AsyncLines<R> {
    framed: FramedRead<R, LinesCodec>,
}

impl<R: AsyncRead + Unpin> AsyncLines<R> {
    /// A byte bound before the delimiter, with CRLF and trailing EOF support.
    #[must_use]
    pub fn new(input: R, maximum: usize) -> Self {
        Self {
            framed: FramedRead::new(input, LinesCodec::new_with_max_length(maximum)),
        }
    }

    /// Cancelling this future never discards bytes already read from the pipe.
    pub async fn next_line(&mut self) -> Result<Option<String>, LineError> {
        std::future::poll_fn(|cx| Pin::new(&mut *self).poll_next(cx))
            .await
            .transpose()
    }
}

impl<R: AsyncRead + Unpin> Stream for AsyncLines<R> {
    type Item = Result<String, LineError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().framed).poll_next(cx)
    }
}

/// Incremental output from a process whose custody must outlive bad frames.
pub struct LineBuffer {
    codec: LinesCodec,
    bytes: BytesMut,
}

impl LineBuffer {
    /// Use the same byte bound and delimiter semantics as [`AsyncLines`].
    #[must_use]
    pub fn new(maximum: usize) -> Self {
        Self {
            codec: LinesCodec::new_with_max_length(maximum),
            bytes: BytesMut::new(),
        }
    }

    /// Decode complete frames; oversize input is discarded through its newline.
    /// Errors are returned once per malformed frame, and later frames survive.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<io::Result<String>> {
        let mut lines = Vec::new();
        for part in chunk.chunks(8192) {
            self.bytes.extend_from_slice(part);
            loop {
                match self.codec.decode(&mut self.bytes) {
                    Ok(Some(line)) => lines.push(Ok(line)),
                    Ok(None) => break,
                    Err(error) => lines.push(Err(invalid(error))),
                }
            }
        }
        lines
    }

    /// Retained incomplete frame bytes, for resource accounting and diagnostics.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.bytes.len()
    }
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt as _;

    #[test]
    fn an_open_async_peer_is_rejected_without_waiting_for_a_delimiter() {
        struct OpenPeer(usize);
        impl AsyncRead for OpenPeer {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                bytes: &mut tokio::io::ReadBuf<'_>,
            ) -> Poll<io::Result<()>> {
                if self.0 == 0 {
                    return Poll::Pending;
                }
                let count = self.0.min(bytes.remaining());
                bytes.put_slice(&vec![b'x'; count]);
                self.0 -= count;
                Poll::Ready(Ok(()))
            }
        }
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut bounded = AsyncLines::new(OpenPeer(65), 64);
        assert!(matches!(
            Pin::new(&mut bounded).poll_next(&mut cx),
            Poll::Ready(Some(Err(_)))
        ));
        // Actual unbounded codec asks the still-open peer for more input.
        let mut unbounded = AsyncLines::new(OpenPeer(65), usize::MAX);
        assert!(Pin::new(&mut unbounded).poll_next(&mut cx).is_pending());
    }

    #[tokio::test]
    async fn cancelled_read_retains_partial_frame_and_buffered_following_frames() -> io::Result<()>
    {
        let (mut writer, reader) = tokio::io::duplex(128);
        let mut lines = AsyncLines::new(reader, 64);
        writer.write_all(b"first").await?;
        let mut pending = Box::pin(lines.next_line());
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await
        );
        drop(pending);
        writer.write_all(b"\r\nsecond\nthird").await?;
        writer.shutdown().await?;
        assert_eq!(
            lines.next_line().await.map_err(invalid)?.as_deref(),
            Some("first")
        );
        assert_eq!(
            lines.next_line().await.map_err(invalid)?.as_deref(),
            Some("second")
        );
        assert_eq!(
            lines.next_line().await.map_err(invalid)?.as_deref(),
            Some("third")
        );
        assert!(lines.next_line().await.map_err(invalid)?.is_none());
        Ok(())
    }

    #[test]
    fn oversize_and_invalid_frames_cannot_hide_the_next_complete_line() -> io::Result<()> {
        let mut bounded = LineBuffer::new(64);
        let oversized = [b'x'; 8192];
        let rejected = bounded.push(&oversized);
        assert_eq!(rejected.len(), 1);
        assert!(rejected.into_iter().all(|line| line.is_err()));
        assert!(bounded.buffered_bytes() <= 64);
        let lines = bounded.push(b"\n\xff\nvalid\r\n");
        assert_eq!(lines.len(), 2);
        let mut lines = lines.into_iter();
        assert!(lines.next().ok_or(io::ErrorKind::UnexpectedEof)?.is_err());
        assert_eq!(lines.next().ok_or(io::ErrorKind::UnexpectedEof)??, "valid");
        // Removing the bound accepts the same peer's entire oversized frame.
        let mut unbounded = LineBuffer::new(usize::MAX);
        assert!(unbounded.push(&oversized).is_empty());
        assert_eq!(
            unbounded
                .push(b"\n")
                .pop()
                .ok_or(io::ErrorKind::UnexpectedEof)??
                .len(),
            oversized.len()
        );
        Ok(())
    }
}
