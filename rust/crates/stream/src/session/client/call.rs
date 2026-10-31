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
        if invocation.is_empty()
            || invocation.len() > crate::MAX_COMMAND_BYTES
            || maximum_reply_bytes == 0
            || maximum_reply_bytes > wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES
        {
            return Err(StreamError::InvalidArgument);
        }
        self.reply(
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
        // No acknowledgement/resume cursor is useful for a one-shot reply.
        // Dropping this cursor still returns all queued frame credit and removes
        // the interest through the same cancellation owner as a follow.
        let source = self
            .open_subscription(kind, cursor(0), false, false, None, credential)
            .await?;
        futures::pin_mut!(source);
        let mut reply = Vec::new();
        let mut total = None;
        while let Some(item) = source.next().await {
            let data = item?
                .into_data()
                .map_err(|_| StreamError::InvalidResponse)?;
            let length = wire_codec::session::actor_call_total(&data)?;
            if length > maximum_reply_bytes || total.is_some_and(|old| old != length) {
                return Err(StreamError::InvalidResponse);
            }
            if total.is_none() {
                reply
                    .try_reserve_exact(length)
                    .map_err(|_| StreamError::Capacity)?;
                total = Some(length);
            }
            let chunk = wire_codec::session::reply_chunk(data.item.as_ref())
                .ok_or(StreamError::InvalidResponse)?;
            if chunk.len() > length - reply.len() {
                return Err(StreamError::InvalidResponse);
            }
            reply.extend_from_slice(chunk);
            if reply.len() == length {
                return Ok(reply.into());
            }
        }
        Err(StreamError::Unavailable)
    }
}
