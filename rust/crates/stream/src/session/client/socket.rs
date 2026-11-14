//! Actor sockets on the existing authenticated, credited session pool.
use super::*;

/// One actor socket: a virtual channel on the shared session. Dropping it
/// removes the channel, which the host observes as the peer going away. A
/// lost connection fails it; a socket is never replayed onto another link.
pub struct ActorSocket {
    cursor: Cursor,
    credential: u32,
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
        Ok(ActorSocket { cursor, credential })
    }
}

impl ActorSocket {
    /// Forward this socket through another authenticated Session without
    /// another protocol driver or per-socket task. Both halves retain this
    /// original channel: dropping either one cancels it. Queued sends retain
    /// their actual result; an asynchronous send failure ends the output.
    pub fn into_channel(self) -> SocketChannel {
        let (results, receive) = mpsc::channel(crate::MAX_ITEMS);
        let inbound = ForwardInput {
            client: self.cursor.client.clone(),
            id: self.cursor.id,
            progress: self.cursor.progress.clone(),
            results,
            credential: self.credential,
        };
        let pending =
            FuturesUnordered::<BoxProviderFuture<'static, Result<(), StreamError>>>::new();
        let outbound = stream::try_unfold(
            (self, receive, pending),
            |(mut socket, mut results, mut pending)| async move {
                loop {
                    tokio::select! {
                        biased;
                        result = pending.next(), if !pending.is_empty() => {
                            result.ok_or(StreamError::Unavailable)??;
                        }
                        result = results.recv(), if pending.len() < crate::MAX_ITEMS => {
                            let result = result.ok_or(StreamError::Unavailable)?;
                            pending.push(Box::pin(async move {
                                result.await.map_err(|_| StreamError::Unavailable)?
                            }));
                        }
                        frame = socket.next() => {
                            return Ok(frame?.map(|frame| (frame, (socket, results, pending))));
                        }
                    }
                }
            },
        );
        SocketChannel {
            outbound: Box::pin(outbound),
            inbound: Box::new(inbound),
        }
    }

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

struct ForwardInput {
    client: Client,
    id: u64,
    progress: Arc<Progress>,
    results: mpsc::Sender<oneshot::Receiver<Result<(), StreamError>>>,
    credential: u32,
}

impl SocketInbound for ForwardInput {
    fn replace_credential(&self, credential: Bytes) -> Result<(), StreamError> {
        if self.progress.cancelled() || self.progress.ended() {
            return Err(StreamError::Unavailable);
        }
        self.client.register_credential(self.credential, credential)
    }

    fn receive(&self, frame: Bytes) -> Result<(), StreamError> {
        if frame.is_empty() || frame.len() > crate::MAX_RECORD_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        if self.progress.cancelled() || self.progress.ended() {
            return Err(StreamError::Unavailable);
        }
        let result = self.results.try_reserve().map_err(queue_error)?;
        let (sent, completion) = oneshot::channel();
        self.client
            .pool
            .commands
            .try_send(Command::Socket {
                id: self.id,
                frame,
                sent,
            })
            .map_err(queue_error)?;
        result.send(completion);
        Ok(())
    }
}

fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> StreamError {
    match error {
        mpsc::error::TrySendError::Full(_) => StreamError::Capacity,
        mpsc::error::TrySendError::Closed(_) => StreamError::Unavailable,
    }
}

impl Drop for ForwardInput {
    fn drop(&mut self) {
        self.client.pool.cancel_interest(self.id, &self.progress);
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
