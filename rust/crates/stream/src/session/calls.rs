//! Owning Actors dispatch on the same authority and credit boundaries as follows.
use super::*;
use futures::TryStreamExt as _;

/// Verified admission for one canonical Actors invocation. Only session
/// admission constructs this value. It contains no caller-supplied identity.
#[derive(Clone)]
pub struct CallAuthorization(pub(super) ResourceAuthorization);

impl CallAuthorization {
    /// Issuer-attested identity used by the owning host's input staging/control.
    pub fn principal(&self) -> Result<acyclic_auth::contract::Principal, StreamError> {
        self.0.principal()
    }

    /// Recheck current credential/trust and the owner's current actor policy at
    /// dispatch. The canonical resource must remain within the admitted target.
    pub fn authorize(
        &self,
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &acyclic_auth::contract::RecordAttributes,
        policy: acyclic_auth::authority::PolicyDecision,
    ) -> Result<(), StreamError> {
        if resource != self.0.resource.name {
            return Err(StreamError::AccessDenied);
        }
        self.0.authorize(resource, keys, attributes, policy)
    }
}

/// Product-owned decoding/routing, without another session or transport loop.
pub trait CallProvider: crate::ProviderPlatform + 'static {
    /// Validate/decode the canonical Actors invocation and return its exact
    /// canonical actor resource. This phase MUST NOT perform I/O or dispatch.
    fn resource(&self, invocation: &[u8]) -> Result<String, StreamError>;

    /// Dispatch once after credential admission. The owner derives identity from
    /// `authorization`, checks current actor policy before staging/mutation, and
    /// retains any uncertain commit until its encoded result has been exported.
    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> crate::BoxProviderFuture<'static, Result<Bytes, StreamError>>;
}

pub(super) fn source(
    provider: Arc<dyn CallProvider>,
    invocation: Bytes,
    authorization: CallAuthorization,
    identity: wire::SessionChannel,
) -> Source {
    reply(
        identity,
        Item::ActorCall,
        Box::pin(async move { provider.call(invocation, authorization).await }),
    )
}

/// One finite one-shot reply (an Actors call or a children page) on a channel.
pub(super) fn reply(
    identity: wire::SessionChannel,
    item: fn(Bytes) -> Item,
    reply: crate::BoxProviderFuture<'static, Result<Bytes, StreamError>>,
) -> Source {
    // Opening acknowledges admission, not completion. Dispatch itself starts
    // only when the shared scheduler admits the next source poll under credit.
    let opened = stream::once(future::ready(Ok(Event::Opened(None, None))));
    // One result allocation is retained while chunks are exposed. Fragmentation
    // allocates no payload copies and the common session queue enforces credit.
    let result = stream::once(async move {
        let reply = reply.await?;
        if reply.is_empty() || reply.len() > wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok::<_, StreamError>(reply)
    })
    .map_ok(move |reply| {
        let total = reply.len();
        stream::iter(
            (0..reply.len())
                .step_by(crate::MAX_RECORD_BYTES)
                .enumerate()
                .map(move |(index, start)| {
                    let end = (start + crate::MAX_RECORD_BYTES).min(reply.len());
                    Ok(Event::CallChunk {
                        index: index as u64,
                        total: total as u64,
                        bytes: reply.slice(start..end),
                        item,
                    })
                }),
        )
    })
    .try_flatten();
    Box::pin(opened.chain(result).map(move |event| (identity, event)))
}
