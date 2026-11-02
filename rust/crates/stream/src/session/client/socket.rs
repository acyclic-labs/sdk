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
    /// Receive credit is returned before exposing the bytes. Use
    /// [`Self::next_chunk`] to retain credit through their consumption.
    pub async fn next(&mut self) -> Result<Option<Bytes>, StreamError> {
        Ok(self.receive().await?.map(|(bytes, _credit)| bytes))
    }

    /// The next original frame with its receive credit, without a payload copy.
    /// Retaining the returned chunk delays further data credit; it does not
    /// prevent observing the original terminal or dropping this socket.
    pub async fn next_chunk(
        &mut self,
    ) -> Result<Option<crate::body::BodyChunk<impl Sized + use<>>>, StreamError> {
        Ok(self
            .receive()
            .await?
            .map(|(bytes, credit)| crate::body::BodyChunk::new(bytes, credit)))
    }

    async fn receive(&mut self) -> Result<Option<(Bytes, Credit)>, StreamError> {
        let Some((data, credit)) = self.cursor.receive_data().await? else {
            return Ok(None);
        };
        match data.item {
            Some(Item::ActorSocket(frame)) => Ok(Some((frame, credit))),
            _ => Err(StreamError::InvalidResponse),
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
            .map_err(|_| StreamError::unavailable())?;
        result.await.map_err(|_| StreamError::unavailable())?
    }
}
