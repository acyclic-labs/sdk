//! Transport selection for the one server. TCP retains its native path; local
//! connections arrive only after their owner checked the OS peer credentials.
use futures::stream::BoxStream;
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A local connection admitted by its OS endpoint owner.
pub struct LocalConnection {
    io: Box<dyn Io>,
    prefix: Option<(Arc<[u8]>, usize)>,
}

impl LocalConnection {
    /// Adopt an already authenticated local connection. No bytes supplied by
    /// the peer may stand in for the kernel identity check.
    pub fn new(io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static) -> Self {
        Self {
            io: Box::new(io),
            prefix: None,
        }
    }

    /// Send an immutable in-memory bootstrap before protocol bytes, without
    /// blocking acceptance of other connections on a slow reader.
    pub fn with_prefix(mut self, prefix: Arc<[u8]>) -> Self {
        self.prefix = Some((prefix, 0));
        self
    }

    fn poll_prefix(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some((bytes, position)) = &mut self.prefix {
            while *position < bytes.len() {
                let written =
                    std::task::ready!(Pin::new(&mut *self.io).poll_write(cx, &bytes[*position..]))?;
                if written == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                *position += written;
            }
            self.prefix = None;
        }
        Poll::Ready(Ok(()))
    }
}

impl tonic::transport::server::Connected for LocalConnection {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}

impl AsyncRead for LocalConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_prefix(cx))?;
        Pin::new(&mut *self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for LocalConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        std::task::ready!(self.poll_prefix(cx))?;
        Pin::new(&mut *self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_prefix(cx))?;
        Pin::new(&mut *self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_prefix(cx))?;
        Pin::new(&mut *self.io).poll_shutdown(cx)
    }
}

/// The same session service over a network listener or an owner-authenticated
/// local incoming stream. Local transports do not accept a TLS configuration.
pub enum Listener {
    /// Network connections, protected by the server's existing TLS policy.
    Tcp(tokio::net::TcpListener),
    /// OS-authenticated local connections, including any in-memory bootstrap.
    Local(BoxStream<'static, io::Result<LocalConnection>>),
}

impl From<tokio::net::TcpListener> for Listener {
    fn from(listener: tokio::net::TcpListener) -> Self {
        Self::Tcp(listener)
    }
}
