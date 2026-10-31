//! Actor sockets on the existing authenticated, credited session pool.
use super::*;

/// One actor socket: a virtual channel on the shared session. Dropping it
/// removes the channel, which the host observes as the peer going away. A
/// lost connection fails it; a socket is never replayed onto another link.
pub struct ActorSocket {
    cursor: Cursor,
}

/// Write projection of one original socket. It cannot reopen or replay it,
/// and stops working when the owning socket is dropped or its session ends.
#[derive(Clone)]
pub struct ActorSocketSender {
    client: Client,
    id: u64,
}

impl Client {
    /// Opens one socket to the actor named by the encoded `open` request.
    /// The host admits it as `actors/invoke` on the actor's resource. Frames
    /// to this peer share the session's credit.
    pub async fn actor_socket(
        &self,
        open: Bytes,
        credential: u32,
    ) -> Result<ActorSocket, StreamError> {
        if open.is_empty() || open.len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        let cursor = self
            .open_cursor(
                Kind::ActorSocket(open),
                cursor(0),
                false,
                false,
                None,
                credential,
            )
            .await?;
        Ok(ActorSocket { cursor })
    }
}

impl ActorSocket {
    /// Retains the original write channel so upload credit never prevents
    /// observing this socket's incoming Close.
    #[must_use]
    pub fn sender(&self) -> ActorSocketSender {
        ActorSocketSender {
            client: self.cursor.client.clone(),
            id: self.cursor.id,
        }
    }

    /// Sends one frame to the host, in order with every earlier frame.
    pub async fn send(&self, frame: Bytes) -> Result<(), StreamError> {
        self.sender().send(frame).await
    }

    /// The next frame from the host; `None` once the host ended the channel.
    pub async fn next(&mut self) -> Result<Option<Bytes>, StreamError> {
        loop {
            let Some(mut delivery) = self.cursor.receive().await? else {
                return Ok(None);
            };
            // Sockets carry no skipped-cut watermarks.
            if delivery.consume_before(&self.cursor.progress)?.is_some() {
                return Err(StreamError::InvalidResponse);
            }
            let fresh = delivery.consume(&self.cursor.progress)?;
            drop(delivery.credit);
            if !fresh {
                continue;
            }
            return match delivery.item {
                Received::Data(wire::SessionData {
                    item: Some(Item::ActorSocket(frame)),
                    ..
                }) => Ok(Some(frame)),
                _ => Err(StreamError::InvalidResponse),
            };
        }
    }
}

impl ActorSocketSender {
    /// Sends once on the original socket, retaining its existing credit and
    /// exact send result. No retry or replacement connection is created.
    pub async fn send(&self, frame: Bytes) -> Result<(), StreamError> {
        if frame.is_empty() || frame.len() > crate::MAX_RECORD_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        let (sent, result) = oneshot::channel();
        self.client
            .pool
            .commands
            .send(Command::Socket {
                id: self.id,
                frame,
                sent,
            })
            .await
            .map_err(|_| StreamError::Unavailable)?;
        result.await.map_err(|_| StreamError::Unavailable)?
    }
}
