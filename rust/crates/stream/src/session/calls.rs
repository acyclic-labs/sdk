//! Owning Actors dispatch on the same authority and credit boundaries as follows.
use super::*;
use futures::TryStreamExt as _;

/// Verified admission for one canonical Actors invocation. Only session
/// admission constructs this value. It contains no caller-supplied identity.
#[derive(Clone)]
pub struct CallAuthorization(
    pub(super) ResourceAuthorization,
    pub(super) tokio::sync::watch::Receiver<super::authority::Credential>,
    pub(super) Arc<acyclic_auth::verify::VerifiedCapability>,
    pub(super) [u8; 32],
    pub(super) bool,
);

impl CallAuthorization {
    /// Whether this original call must contribute live credential custody.
    /// An owner without an enrollment implementation must refuse before effects.
    pub fn enrolling(&self) -> bool {
        self.4
    }
    /// Retain this admitted caller and its original live replacement/loss owner.
    /// The host must still apply its current command policy before issuance and
    /// dispatch. This contributes dependencies; it grants no new resource.
    pub fn contribution(&self) -> Result<CallContribution, StreamError> {
        self.1.has_changed().map_err(|_| StreamError::Unavailable)?;
        let capability = Arc::clone(&self.2);
        let contribution = CallContribution(Arc::new(Contribution {
            authorization: self.0.clone(),
            evidence: capability
                .trust_evidence()
                .map_err(|_| StreamError::AccessDenied)?,
            expires: capability.effective().expires_at_unix_seconds(),
            changed: self.1.clone(),
            filter_digest: self.3,
            capability,
        }));
        contribution.check()?;
        Ok(contribution)
    }
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

struct Contribution {
    capability: Arc<acyclic_auth::verify::VerifiedCapability>,
    authorization: ResourceAuthorization,
    evidence: acyclic_auth::verify::TrustEvidence,
    expires: u64,
    changed: tokio::sync::watch::Receiver<super::authority::Credential>,
    filter_digest: [u8; 32],
}

/// Original session admission retained while a selected source is issued and
/// dispatched. Portable credentials cannot replace this live custody.
#[derive(Clone)]
pub struct CallContribution(Arc<Contribution>);

impl CallContribution {
    /// Original authenticated acting identity, including its complete lineage.
    pub fn principal(&self) -> &acyclic_auth::contract::Principal {
        self.0.capability.effective().principal()
    }
    /// Complete canonical public trust evidence for a signed contribution.
    pub fn evidence(&self) -> &acyclic_auth::verify::TrustEvidence {
        &self.0.evidence
    }
    /// Recheck original admission, captured dependencies and current replacement.
    /// The owning command's current policy remains an independent required fence.
    pub fn check(&self) -> Result<(), StreamError> {
        let authorization = &self.0.authorization;
        self.0
            .changed
            .has_changed()
            .map_err(|_| StreamError::Unavailable)?;
        authorization
            .change_source()
            .check_open()
            .map_err(|_| StreamError::Unavailable)?;
        let capability = self
            .0
            .changed
            .borrow()
            .as_ref()
            .map(Arc::clone)
            .map_err(|_| StreamError::AccessDenied)?;
        if capability.effective().principal() != self.principal()
            || !capability
                .effective()
                .grants()
                .eq(self.0.capability.effective().grants())
            || crate::FollowFilter::authorized(wire::SessionFilter::default(), &capability)?
                .digest()
                != self.0.filter_digest
        {
            return Err(StreamError::AccessDenied);
        }
        let snapshot = authorization.authorize_capability(
            &capability,
            &authorization.resource.name,
            &[],
            &acyclic_auth::contract::RecordAttributes::default(),
            acyclic_auth::authority::PolicyDecision::Allow,
        )?;
        self.0
            .evidence
            .check(snapshot.trust.as_ref(), snapshot.now_unix_seconds)
            .map_err(|_| StreamError::AccessDenied)
    }
    /// Completes on original credential slot or session loss, invalid replacement,
    /// revocation or expiry. Register before dispatch; the slot/trust/clock are
    /// the producers. Dropping any producer returns a typed unavailable error.
    /// No task, readiness probe or independent trust source is started.
    pub fn terminated(&self) -> crate::BoxProviderFuture<'static, StreamError> {
        let inherited = self.0.authorization.custody_terminal();
        let contribution = self.clone();
        let mut changed = self.0.changed.clone();
        let original: crate::BoxProviderFuture<'static, StreamError> = Box::pin(async move {
            let expiry = contribution.0.authorization.expiry(contribution.0.expires);
            futures::pin_mut!(expiry);
            loop {
                let authorization = &contribution.0.authorization;
                let trust = contribution
                    .0
                    .evidence
                    .changed(authorization.change_source());
                if let Err(error) = contribution.check() {
                    return error;
                }
                let owner = async {
                    changed
                        .changed()
                        .await
                        .map_err(|_| acyclic_auth::trust::SourceClosed)
                };
                futures::pin_mut!(owner);
                let publication = future::select(trust, owner);
                futures::pin_mut!(publication);
                match future::select(publication, expiry.as_mut()).await {
                    future::Either::Left((publication, _)) => {
                        let result = match publication {
                            future::Either::Left((result, _))
                            | future::Either::Right((result, _)) => result,
                        };
                        if result.is_err() {
                            return StreamError::Unavailable;
                        }
                    }
                    future::Either::Right((result, _)) => {
                        return match result {
                            Ok(()) => StreamError::AccessDenied,
                            Err(_) => StreamError::Unavailable,
                        };
                    }
                }
            }
        });
        match inherited {
            None => original,
            Some(inherited) => Box::pin(async move {
                match future::select(original, inherited).await {
                    future::Either::Left((error, _)) | future::Either::Right((error, _)) => error,
                }
            }),
        }
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
    ) -> crate::BoxProviderFuture<'static, Result<CallReply, StreamError>>;
}

pub(super) fn source(
    provider: Arc<dyn CallProvider>,
    invocation: Bytes,
    authorization: CallAuthorization,
    identity: wire::SessionChannel,
) -> Source {
    let enrolling = authorization.enrolling();
    reply_with_enrollment(identity, Item::ActorCall, enrolling, async move {
        let caller = enrolling
            .then(|| authorization.contribution())
            .transpose()?;
        let mut reply = provider.call(invocation, authorization).await?;
        if let Some(caller) = caller {
            reply.enrollment = reply.enrollment.map(|enrollment| enrollment.bind(caller));
        }
        Ok(reply)
    })
}

/// One finite one-shot reply (an Actors call or a children page) on a channel.
pub(super) fn reply(
    identity: wire::SessionChannel,
    item: fn(Bytes) -> Item,
    reply: crate::BoxProviderFuture<'static, Result<Bytes, StreamError>>,
) -> Source {
    reply_with_enrollment(identity, item, false, async move {
        reply.await.map(CallReply::new)
    })
}

fn reply_with_enrollment(
    identity: wire::SessionChannel,
    item: fn(Bytes) -> Item,
    enrolling: bool,
    reply: impl std::future::Future<Output = Result<CallReply, StreamError>>
    + crate::ProviderTask
    + 'static,
) -> Source {
    // Opening acknowledges admission, not completion. Dispatch itself starts
    // only when the shared scheduler admits the next source poll under credit.
    let opened = stream::once(future::ready(Ok(Event::Opened(None, None))));
    // One result allocation is retained while chunks are exposed. Fragmentation
    // allocates no payload copies and the common session queue enforces credit.
    let result = stream::once(async move {
        let reply = reply.await?;
        if reply.bytes.is_empty()
            || reply.bytes.len() > wire_codec::session::MAX_ACTOR_CALL_REPLY_BYTES
        {
            return Err(StreamError::LimitExceeded);
        }
        if enrolling != reply.enrollment.is_some() {
            return Err(StreamError::Unsupported);
        }
        Ok::<_, StreamError>(reply)
    })
    .map_ok(move |reply| {
        let enrollment = reply.enrollment;
        let reply = reply.bytes;
        let total = reply.len();
        stream::iter(
            enrollment
                .into_iter()
                .map(|enrollment| Ok(Event::Enroll(enrollment)))
                .chain(
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
                ),
        )
    })
    .try_flatten();
    Box::pin(opened.chain(result).map(move |event| (identity, event)))
}
