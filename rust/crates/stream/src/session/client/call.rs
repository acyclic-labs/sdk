//! Finite Actors replies on the existing authenticated, credited session pool.
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
        // No acknowledgement/resume cursor is useful for a non-replayable call.
        // Dropping this cursor still returns all queued frame credit and removes
        // the interest through the same cancellation owner as a follow.
        let source = self
            .open_subscription(
                Kind::ActorCall(invocation.clone()),
                cursor(0),
                false,
                false,
                None,
                credential,
            )
            .await?;
        futures::pin_mut!(source);
        let mut reply = Vec::new();
        let mut total = None;
        while let Some(item) = source.next().await {
            let Received::Data(data) = item? else {
                return Err(StreamError::InvalidResponse);
            };
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
            let Some(Item::ActorCall(chunk)) = data.item else {
                return Err(StreamError::InvalidResponse);
            };
            if chunk.len() > length - reply.len() {
                return Err(StreamError::InvalidResponse);
            }
            reply.extend_from_slice(&chunk);
            if reply.len() == length {
                return Ok(reply.into());
            }
        }
        Err(StreamError::Unavailable)
    }
}
