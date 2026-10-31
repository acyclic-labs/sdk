//! Actor sockets: bidirectional virtual channels on the authenticated session.
//!
//! A socket channel is admitted exactly like an Actors call (`actors/invoke`
//! on the canonical actor resource), shares the session's credit for frames
//! to the peer, and is never replayed on reconnect. Its lifetime is the
//! channel's: removing the channel, a credential revocation or the end of the
//! session drops the owner's [`SocketInbound`], which is the owner's signal
//! that the peer is gone.
use super::*;

/// Frames from the peer of one admitted socket. Dropping it ends the socket.
pub trait SocketInbound: crate::ProviderPlatform {
    /// Accept one peer frame, in the order the peer sent it. This MUST NOT
    /// perform I/O: the session hands it over synchronously, so no socket's
    /// owner can stall the other channels of the shared session. The owner
    /// applies accepted frames in order on its own tasks. An error closes
    /// only this channel.
    fn receive(&self, frame: Bytes) -> Result<(), StreamError>;
}

/// One admitted socket: frames to the peer, and the owner's inbound handle.
pub struct SocketChannel {
    /// Frames to the peer under channel credit; ends after the final frame.
    pub outbound: crate::BoxProviderStream<'static, Result<Bytes, StreamError>>,
    /// Peer frames; dropped when the channel or the session ends.
    pub inbound: Box<dyn SocketInbound>,
}

/// Product-owned socket routing, without another session or transport loop.
pub trait SocketProvider: crate::ProviderPlatform + 'static {
    /// Validate and decode the encoded open request and return its exact
    /// canonical actor resource. This phase MUST NOT perform I/O.
    fn resource(&self, open: &[u8]) -> Result<String, StreamError>;

    /// Install one socket after credential admission. Any I/O runs inside the
    /// returned outbound stream or the owner's own tasks, never here.
    fn open(
        &self,
        open: Bytes,
        authorization: CallAuthorization,
    ) -> Result<SocketChannel, StreamError>;
}

/// The outbound half as a session source: admission first, then one data
/// frame per socket frame, counted from 1.
pub(super) fn source(
    outbound: crate::BoxProviderStream<'static, Result<Bytes, StreamError>>,
    identity: wire::SessionChannel,
) -> Source {
    let opened = stream::once(future::ready(Ok(Event::Opened(None, None))));
    let frames = outbound
        .enumerate()
        .map(|(index, frame)| {
            let bytes = frame?;
            if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
                return Err(StreamError::LimitExceeded);
            }
            Ok(Event::Socket {
                next: index as u64 + 1,
                bytes,
            })
        })
        .chain(stream::once(future::ready(Ok(Event::End))));
    Box::pin(opened.chain(frames).map(move |event| (identity, event)))
}
