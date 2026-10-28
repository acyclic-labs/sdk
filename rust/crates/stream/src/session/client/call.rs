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
        maximum_reply_bytes: impl Into<Option<usize>>,
    ) -> Result<Bytes, StreamError> {
        let maximum_reply_bytes = maximum_reply_bytes.into();
        if invocation.is_empty()
            || invocation.len() > crate::MAX_COMMAND_BYTES
            || maximum_reply_bytes == Some(0)
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
            if maximum_reply_bytes.is_some_and(|maximum| length > maximum)
                || total.is_some_and(|old| old != length)
            {
                return Err(StreamError::InvalidResponse);
            }
            if total.is_none() {
                total = Some(length);
            }
            let Some(Item::ActorCall(chunk)) = data.item else {
                return Err(StreamError::InvalidResponse);
            };
            if chunk.len() > length - reply.len() {
                return Err(StreamError::InvalidResponse);
            }
            // Fund only received fragments. The announced total cannot trigger
            // a large speculative allocation before its bytes arrive.
            reply
                .try_reserve(chunk.len())
                .map_err(|_| StreamError::Capacity)?;
            reply.extend_from_slice(&chunk);
            if reply.len() == length {
                return Ok(reply.into());
            }
        }
        Err(StreamError::Unavailable)
    }
}
