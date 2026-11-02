//! Finite one-shot replies on the existing authenticated, credited session pool.
use super::*;

impl Client {
    /// Invokes one canonical encoded Actors request on the shared session.
    ///
    /// The caller retains the invocation and its intent throughout this borrow.
    /// A lost connection returns an error; a call is never automatically replayed
    /// or resumed into a newly computed response. Only a complete bounded reply
    /// is returned. The host must authorize `actors/invoke` before dispatch.
    pub async fn actor_call(
        &self,
        invocation: &Bytes,
        credential: u32,
        maximum_reply_bytes: usize,
    ) -> Result<Bytes, StreamError> {
        validate_call(invocation, maximum_reply_bytes)?;
        self.reply(
            Kind::ActorCall(invocation.clone()),
            credential,
            maximum_reply_bytes,
        )
        .await
    }

    /// Opens the same one-shot invocation and returns original reply fragments.
    ///
    /// Each item pairs the immutable total encoded reply length with its next
    /// fragment. Holding a fragment retains its original session receive credit;
    /// dropping the stream cancels its original interest. Source loss or an
    /// incomplete reply is a typed error, never a successful end. The request is
    /// still one complete invocation; this does not stream its input body.
    pub async fn actor_call_chunks(
        &self,
        invocation: &Bytes,
        credential: u32,
        maximum_reply_bytes: usize,
    ) -> Result<
        impl futures::Stream<
            Item = Result<(usize, crate::body::BodyChunk<impl Sized + use<>>), StreamError>,
        >
        + 'static
        + use<>,
        StreamError,
    > {
        validate_call(invocation, maximum_reply_bytes)?;
        self.reply_chunks(
            Kind::ActorCall(invocation.clone()),
            credential,
            maximum_reply_bytes,
        )
        .await
    }

    /// Reads one page of direct children on the shared session, with the
    /// semantics of [`crate::StreamProviderExt::children_page`]. The host must
    /// authorize `streams/list` on the parent; each child is returned only
    /// when the credential may list it, and its tail record only when it may
    /// follow that stream. A page is a pure read: a lost connection reissues
    /// it until its first fragment arrives and fails afterwards.
    pub async fn children_page(
        &self,
        request: crate::ChildrenPageRequest,
        credential: u32,
    ) -> Result<crate::ChildrenPage, StreamError> {
        let reply = self
            .reply(
                Kind::ChildrenPage(wire_codec::children_page_request_to_wire(request)),
                credential,
                wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES,
            )
            .await?;
        wire_codec::children_page_from_response(
            wire::ChildrenPageResponse::decode(reply).map_err(|_| StreamError::InvalidResponse)?,
        )
    }

    /// Observes named heads and children at one cut, on the shared credited session.
    /// A revision-only request checks credential context without a resource
    /// grant; named heads and children retain their resource authorization.
    pub async fn heads(
        &self,
        mut request: crate::HeadsRequest,
        credential: u32,
    ) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        request.at_least = self.revisions.floor(&request.at_least)?;
        let reply = self
            .reply(
                Kind::Heads(wire_codec::heads_request_to_wire(request.clone())),
                credential,
                wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES,
            )
            .await?;
        let view = wire_codec::heads_from_response(
            wire::HeadsResponse::decode(reply).map_err(|_| StreamError::InvalidResponse)?,
        )?;
        view.validate_for(&request)?;
        self.revisions.observe(&view.snapshot);
        Ok(view)
    }

    async fn reply(
        &self,
        kind: Kind,
        credential: u32,
        maximum_reply_bytes: usize,
    ) -> Result<Bytes, StreamError> {
        let source = self
            .reply_chunks(kind, credential, maximum_reply_bytes)
            .await?;
        futures::pin_mut!(source);
        let mut reply = Vec::new();
        let mut total = None;
        while let Some(item) = source.next().await {
            let (length, chunk) = item?;
            if total.is_none() {
                reply
                    .try_reserve_exact(length)
                    .map_err(|_| StreamError::Capacity)?;
                total = Some(length);
            }
            reply.extend_from_slice(chunk.as_ref());
            if reply.len() == length {
                return Ok(reply.into());
            }
        }
        Err(StreamError::Unavailable)
    }

    async fn reply_chunks(
        &self,
        kind: Kind,
        credential: u32,
        maximum_reply_bytes: usize,
    ) -> Result<
        impl futures::Stream<Item = Result<(usize, crate::body::BodyChunk<Credit>), StreamError>>
        + 'static
        + use<>,
        StreamError,
    > {
        // Use the original cursor directly: the generic subscription projection
        // returns delivery credit before yielding its frame.
        let cursor = self
            .open_cursor(kind, cursor(0), false, false, None, credential)
            .await?;
        let operation = cursor.observation.operation.clone();
        Ok(operation.scope(stream::try_unfold(
            (cursor, None, 0_usize, false),
            move |(mut cursor, total, received, complete)| async move {
                if complete {
                    return Ok(None);
                }
                let (data, credit) = cursor
                    .receive_data()
                    .await?
                    .ok_or(StreamError::Unavailable)?;
                let length = wire_codec::session::actor_call_total(&data)?;
                if length > maximum_reply_bytes || total.is_some_and(|old| old != length) {
                    return Err(StreamError::InvalidResponse);
                }
                let bytes = match data.item {
                    Some(
                        wire::session_data::Item::ActorCall(bytes)
                        | wire::session_data::Item::ChildrenPage(bytes)
                        | wire::session_data::Item::Heads(bytes),
                    ) => bytes,
                    _ => return Err(StreamError::InvalidResponse),
                };
                let received = received
                    .checked_add(bytes.len())
                    .filter(|received| *received <= length)
                    .ok_or(StreamError::InvalidResponse)?;
                let chunk = crate::body::BodyChunk::new(bytes, credit);
                Ok(Some((
                    (length, chunk),
                    (cursor, Some(length), received, received == length),
                )))
            },
        )))
    }
}

fn validate_call(invocation: &Bytes, maximum_reply_bytes: usize) -> Result<(), StreamError> {
    if invocation.is_empty()
        || invocation.len() > crate::MAX_COMMAND_BYTES
        || maximum_reply_bytes == 0
        || maximum_reply_bytes > wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES
    {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}
