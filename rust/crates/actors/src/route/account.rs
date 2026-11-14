//! Account routing on the existing authenticated Actors call channel.
use super::{Envelope, Error, NodeId, Paths, Placement, Rejection, Routed};
use crate::addressing::AccountId;
use acyclic_auth::authority::PolicyDecision;
use acyclic_stream::{
    BoxProviderFuture, CommitCondition, CommitEffect, IdempotencyKey, StreamError, StreamProvider,
    session::{CallAuthorization, CallProvider, CallTarget, Client},
};
use bytes::Bytes;
use prost::Message as _;
use std::sync::Arc;

/// Canonical account relay resource. Message admission is separate from the
/// NodeLink actor namespace, so a peer grant conveys no node lifecycle control.
pub fn relay_resource(account: &AccountId, node: &NodeId) -> String {
    format!(
        "actors:/relay/{}/{}",
        acyclic_auth::encode_component(account.as_bytes()),
        acyclic_auth::encode_component(node.as_bytes())
    )
}

/// Host policy's original admission decision. Every effect must be hard;
/// publication joins it atomically to the message or its rejection Notice.
pub enum Admission {
    /// Membership guards and funded account storage admission.
    Allow(Vec<CommitEffect>),
    /// Storage was exhausted at an observed cut. The owned event must resume
    /// after that cut's drain or reservation change; it must not poll. Keeping
    /// the wait outside preparation lets a concurrent original publication win.
    Credit {
        /// Existing reservation/drain event, captured before returning.
        changed: BoxProviderFuture<'static, Result<(), Error>>,
    },
    /// A definitive refusal with a host-constructed, authorized sender Notice.
    Reject {
        /// Original recipient refusal.
        reason: Rejection,
        /// Ordinary delivery request addressed to the original sender.
        notice: Envelope,
        /// Original membership and storage admission guards/debits.
        effects: Vec<CommitEffect>,
    },
}

/// Application policy, shared by self-hosted and managed account nodes.
/// There is no default policy or implicit peer grant.
pub trait AdmissionPolicy: acyclic_stream::ProviderPlatform + 'static {
    /// Check the current sender-bound append-message grant before provider IO.
    /// The node's namespace grant alone must not stand in for that grant.
    /// Validate the application's request shape too: a peer grant cannot admit
    /// control records, directory operations or caller-selected read conditions.
    fn authorize(
        &self,
        authorization: &CallAuthorization,
        message: &Envelope,
    ) -> Result<(), StreamError>;

    /// Read the current membership cut and prepare account storage admission.
    /// No credit returns its captured drain event, never awaits it here.
    /// Return guards over the observed cut; no admission is published here.
    fn prepare<'a>(
        &'a self,
        authorization: &'a CallAuthorization,
        message: &'a Envelope,
        destination: Option<&'a Placement>,
    ) -> BoxProviderFuture<'a, Result<Admission, Error>>;
}

/// One account's directory/router, for composition into the existing `server::serve`
/// product dispatch. It owns no session roots, connection loop or per-root task.
pub struct AccountService<P: StreamProvider + ?Sized> {
    account: AccountId,
    provider: Arc<P>,
    paths: Paths,
    policy: Arc<dyn AdmissionPolicy>,
}

impl<P: StreamProvider + ?Sized> AccountService<P> {
    pub(super) fn handles(&self, invocation: &[u8]) -> bool {
        super::SourceWire::decode(invocation).is_ok_and(|header| {
            matches!(
                header.domain.as_str(),
                super::CALL_PROTOCOL | super::registration::PROTOCOL
            )
        })
    }

    /// Inject the account's existing provider, routing subtree and host policy.
    pub fn new(
        account: AccountId,
        provider: Arc<P>,
        paths: Paths,
        policy: Arc<dyn AdmissionPolicy>,
    ) -> Self {
        Self {
            account,
            provider,
            paths,
            policy,
        }
    }

    fn message(&self, invocation: &[u8]) -> Result<Envelope, StreamError> {
        let message = Envelope::decode_invocation(invocation).map_err(stream_error)?;
        if message.sender.account() != &self.account {
            return Err(StreamError::AccessDenied);
        }
        Ok(message)
    }
}

fn stream_error(error: Error) -> StreamError {
    match error {
        Error::Stream(error) | Error::Actor(crate::local::Error::Stream(error)) => error,
        Error::Identity(_) | Error::InvalidRecord => StreamError::InvalidArgument,
        _ => StreamError::InvalidResponse,
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub(super) struct Reply {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(bytes = "bytes", tag = "2")]
    key: Bytes,
    #[prost(uint64, optional, tag = "3")]
    relay_seq: Option<u64>,
    #[prost(enumeration = "Rejection", optional, tag = "4")]
    rejection: Option<i32>,
}

impl Reply {
    pub(super) fn new(key: &IdempotencyKey, routed: Routed) -> Result<Self, StreamError> {
        let (relay_seq, rejection) = match routed {
            Routed::Relayed {
                key: admitted,
                relay_seq,
            } if admitted == *key => (Some(relay_seq), None),
            Routed::Rejected(reason) => (None, Some(reason as i32)),
            _ => return Err(StreamError::InvalidResponse),
        };
        Ok(Self {
            version: 1,
            key: Bytes::copy_from_slice(key.as_bytes()),
            relay_seq,
            rejection,
        })
    }

    pub(super) fn routed(self, key: &IdempotencyKey) -> Result<Routed, StreamError> {
        if self.version != 1 || self.key.as_ref() != key.as_bytes() {
            return Err(StreamError::InvalidResponse);
        }
        match (self.relay_seq, self.rejection) {
            (Some(relay_seq), None) => Ok(Routed::Relayed {
                key: key.clone(),
                relay_seq,
            }),
            (None, Some(reason)) => Ok(Routed::Rejected(
                Rejection::try_from(reason).map_err(|_| StreamError::InvalidResponse)?,
            )),
            _ => Err(StreamError::InvalidResponse),
        }
    }
}

impl<P: StreamProvider + ?Sized> CallProvider for AccountService<P> {
    fn target(&self, invocation: &[u8]) -> Result<CallTarget, StreamError> {
        // Credential selection needs only the source header. Decoding the
        // CommitRequest here would repeat validation before authorization.
        let header =
            super::SourceWire::decode(invocation).map_err(|_| StreamError::InvalidArgument)?;
        if header.version != 1 {
            return Err(StreamError::InvalidArgument);
        }
        let source = NodeId::new(&header.source).map_err(|_| StreamError::InvalidArgument)?;
        let resource = match header.domain.as_str() {
            super::registration::PROTOCOL => super::placement_resource(&self.account, &source),
            super::CALL_PROTOCOL => relay_resource(&self.account, &source),
            _ => return Err(StreamError::InvalidArgument),
        };
        CallTarget::new(resource, "actors/invoke")
    }

    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        if super::SourceWire::decode(invocation.as_ref())
            .is_ok_and(|header| header.domain == super::registration::PROTOCOL)
        {
            let account = self.account.clone();
            let provider = self.provider.clone();
            let paths = self.paths.clone();
            return Box::pin(async move {
                super::Registration::apply(
                    &invocation,
                    &account,
                    provider.as_ref(),
                    &paths,
                    &authorization,
                )
                .await
                .map_err(stream_error)
            });
        }
        let message = self.message(&invocation);
        let account = self.account.clone();
        let provider = Arc::clone(&self.provider);
        let paths = self.paths.clone();
        let policy = Arc::clone(&self.policy);
        Box::pin(async move {
            let message = message?;
            let resource = relay_resource(&account, &message.source);
            let attributes = acyclic_auth::contract::RecordAttributes::try_new(Vec::new())
                .map_err(|_| StreamError::InvalidArgument)?;
            let authorize = || {
                authorization.authorize_in_account(
                    account.as_bytes(),
                    &resource,
                    &[],
                    &attributes,
                    PolicyDecision::Allow,
                )?;
                policy.authorize(&authorization, &message)
            };
            authorize()?;
            let sender = Placement::read(provider.as_ref(), &paths, &message.sender)
                .await
                .map_err(stream_error)?;
            if !sender.is_some_and(|placement| placement.node == message.source) {
                return Err(StreamError::AccessDenied);
            }
            let mut refused = None;
            let routed = loop {
                if let Some(original) = message
                    .retained_admission(provider.as_ref(), &paths)
                    .await
                    .map_err(stream_error)?
                {
                    break original;
                }
                let destination = Placement::read(provider.as_ref(), &paths, &message.to)
                    .await
                    .map_err(stream_error)?;
                let decision = policy
                    .prepare(&authorization, &message, destination.as_ref())
                    .await
                    .map_err(stream_error)?;
                // Preparation may have suspended. Recheck the current credential
                // and host grant before publishing or awaiting credit.
                authorize()?;
                let result = match decision {
                    Admission::Credit { changed } => {
                        // The original may have committed between inspection
                        // and the no-credit cut. It needs no second debit.
                        if let Some(original) = message
                            .retained_admission(provider.as_ref(), &paths)
                            .await
                            .map_err(stream_error)?
                        {
                            break original;
                        }
                        changed.await.map_err(stream_error)?;
                        authorize()?;
                        continue;
                    }
                    Admission::Allow(effects) => {
                        let destination =
                            destination.as_ref().ok_or(StreamError::InvalidResponse)?;
                        message
                            .publish_relay(provider.as_ref(), &paths, &destination.node, effects)
                            .await
                    }
                    Admission::Reject {
                        reason,
                        notice,
                        mut effects,
                    } => {
                        if destination.is_none() {
                            if reason != Rejection::NoSuchActor {
                                return Err(StreamError::InvalidResponse);
                            }
                            effects.push(CommitEffect::hard(
                                vec![CommitCondition::Absent {
                                    path: paths.placement(&message.to)?,
                                }],
                                Vec::new(),
                            ));
                        }
                        message
                            .publish_rejection(provider.as_ref(), &paths, reason, &notice, effects)
                            .await
                    }
                };
                match result {
                    // This is a new conditional-commit result, not a readiness
                    // probe or transport retry. Prepare a fresh guarded cut.
                    Err(Error::Actor(crate::local::Error::PreconditionChanged(conflicts))) => {
                        crate::local::observe_refusal(&mut refused, conflicts)
                            .map_err(|error| stream_error(error.into()))?;
                        continue;
                    }
                    other => break other.map_err(stream_error)?,
                }
            };
            authorize()?;
            let reply = Reply::new(&message.request.idempotency_key, routed)?;
            Ok(reply.encode_to_vec().into())
        })
    }
}

/// Admit on the existing authenticated account session. The caller retains
/// the original envelope across uncertain completion; no transport retry runs
/// here. The reply budget is the caller's existing session allocation policy.
pub async fn admit(
    client: &Client,
    credential: u32,
    message: &Envelope,
    maximum_reply_bytes: usize,
) -> Result<Routed, Error> {
    let bytes = client
        .actor_call(
            &message.encode_invocation(),
            credential,
            maximum_reply_bytes,
        )
        .await?;
    let reply = Reply::decode(bytes).map_err(|_| StreamError::InvalidResponse)?;
    Ok(reply.routed(&message.request.idempotency_key)?)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
