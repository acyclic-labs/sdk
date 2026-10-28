//! Public trust, credential slots and immutable subscription ceilings.
#[cfg(any(test, feature = "test-support"))]
#[path = "testing.rs"]
pub mod testing;
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Scope, Target},
    verify::{Trust, VerifyError},
};

/// One authenticated, current public trust view and its trusted clock.
/// No issuer or workload signing secret belongs in this object.
pub struct TrustSnapshot {
    /// Immutable keyring/revocation view; missing or stale state must fail closed.
    pub trust: Arc<dyn Trust + Send + Sync>,
    /// Current UTC Unix seconds supplied by the runtime.
    pub now_unix_seconds: u64,
    /// Monotonic public trust revision used by push observation.
    pub revision: u64,
}

/// Runtime-owned public trust feed. Waiting must be push driven, including
/// expiry; callers must never implement this with periodic trust polling.
pub trait AuthorityState: crate::ProviderPlatform {
    /// Captures current authenticated public state and time.
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError>;
    /// Completes when authenticated trust changes from the observed revision.
    fn changed(&self, revision: u64) -> crate::BoxProviderFuture<'_, ()>;
    /// Completes at the absolute UTC deadline, independently of record credit.
    fn expires_at(&self, unix_seconds: u64) -> crate::BoxProviderFuture<'_, ()>;
}

/// Authenticated recipient and holder binding for one session host.
/// The runtime supplies public trust; the host never receives a root signer.
pub struct Authority {
    pub(super) state: Arc<dyn AuthorityState>,
    verifier: Arc<dyn CredentialVerifier>,
    pub(super) audience: String,
    pub(super) holder: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    Forbidden,
    Revoked,
    Expired,
    Filter,
    Unavailable,
}
impl Rejection {
    pub(super) fn code(self) -> &'static str {
        match self {
            Self::Forbidden => "forbidden",
            Self::Revoked => "revoked",
            Self::Expired => "credential_expired",
            Self::Filter => "filter_invalid",
            Self::Unavailable => "unavailable",
        }
    }
}
impl From<VerifyError> for Rejection {
    fn from(error: VerifyError) -> Self {
        match error {
            VerifyError::Revoked => Self::Revoked,
            VerifyError::Time => Self::Expired,
            VerifyError::Unavailable => Self::Unavailable,
            _ => Self::Forbidden,
        }
    }
}
type Credential = Result<Arc<dyn Admission>, Rejection>;

/// A credential its verifier is still deciding, for one slot. The session
/// keeps serving its channels and enforcing their trust while it waits.
pub(super) struct Verifying {
    pub(super) slot: u32,
    length: usize,
    pub(super) verdict: futures::future::BoxFuture<'static, Credential>,
}

impl Verifying {
    pub(super) const fn length(&self) -> usize {
        self.length
    }
}
impl Slots {
    /// The credential bytes held once `slot` holds `length` of them.
    fn credential_bytes(&self, slot: u32, length: usize) -> Result<usize, StreamError> {
        let previous = self.entries.get(&slot).map_or(0, |entry| entry.1);
        self.credential_bytes
            .checked_sub(previous)
            .and_then(|current| current.checked_add(length))
            .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
            .ok_or(StreamError::Capacity)
    }
}

#[derive(Default)]
pub(super) struct Slots {
    entries: BTreeMap<u32, (u64, usize, Credential)>,
    next_revision: u64,
    credential_bytes: usize,
    expiries: std::collections::BinaryHeap<std::cmp::Reverse<(u64, u64, u32)>>,
}

pub(super) struct Binding {
    credential_revision: u64,
    pub(super) slot: u32,
    resource: String,
    requested: wire::SessionFilter,
    filter_digest: [u8; 32],
    resource_authorization: Option<ResourceAuthorization>,
    additional: Vec<(&'static str, String)>,
}

impl Binding {
    /// Finite one-shot replies (Actors calls and children pages) carry no
    /// record filter and get no credit-free lookahead.
    pub(super) fn is_reply(&self) -> bool {
        self.resource_authorization.as_ref().is_some_and(|value| {
            matches!(
                value.action,
                "actors/invoke" | "streams/list" | "streams/read"
            )
        })
    }
}

pub(super) fn stream_resource(path: Option<&StreamPath>) -> String {
    path.map_or_else(
        || String::from("streams:/"),
        |path| wire_codec::session::stream_resource(path.as_str()),
    )
}
impl Authority {
    /// Binds a canonical authenticated audience and presenting account,
    /// verifying credentials as this SDK's capability envelope
    /// ([`CapabilityVerifier`]).
    ///
    /// # Errors
    /// When `audience` is not a resource or `holder` is empty.
    pub fn new(
        state: Arc<dyn AuthorityState>,
        audience: String,
        holder: Option<String>,
    ) -> Result<Self, StreamError> {
        if !acyclic_auth::valid_resource(&audience)
            || holder.as_ref().is_some_and(|holder| holder.is_empty())
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            state,
            verifier: Arc::new(CapabilityVerifier),
            audience,
            holder,
        })
    }

    /// Verifies every credential a client presents through `verifier`
    /// instead: the one place this session verifies credentials. Calls,
    /// reads and follows are admitted from what it returns, under the same
    /// slots, renewal, expiry, revocation and credit as the stock envelope.
    #[must_use]
    pub fn verified_by(mut self, verifier: Arc<dyn CredentialVerifier>) -> Self {
        self.verifier = verifier;
        self
    }
    pub(super) fn snapshot(&self) -> Result<TrustSnapshot, Rejection> {
        self.state.snapshot().map_err(Into::into)
    }
    /// Verifies `bytes` and registers the result in `slot`.
    pub(super) async fn register(
        &self,
        slots: &mut Slots,
        slot: u32,
        bytes: &[u8],
        maximum: usize,
    ) -> Result<(), StreamError> {
        match self.verifying(slots, slot, Bytes::copy_from_slice(bytes), maximum)? {
            Some(verifying) => {
                let verdict = verifying.verdict.await;
                self.settle(slots, verifying.slot, verifying.length, verdict, maximum)
            }
            None => Ok(()),
        }
    }

    /// Starts verifying `bytes` for `slot`, within the slot and credential
    /// byte bounds; empty `bytes` clear the slot at once. The verdict owns
    /// what it reads, so the session need not wait on it.
    pub(super) fn verifying(
        &self,
        slots: &mut Slots,
        slot: u32,
        bytes: Bytes,
        maximum: usize,
    ) -> Result<Option<Verifying>, StreamError> {
        if usize::try_from(slot).map_err(|_| StreamError::Capacity)? >= maximum {
            return Err(StreamError::Capacity);
        }
        let credential_bytes = slots.credential_bytes(slot, bytes.len())?;
        if bytes.is_empty() {
            slots.entries.remove(&slot);
            slots.credential_bytes = credential_bytes;
            return Ok(None);
        }
        let length = bytes.len();
        let verifier = Arc::clone(&self.verifier);
        let snapshot = self.snapshot();
        Ok(Some(Verifying {
            slot,
            length,
            verdict: Box::pin(async move {
                match snapshot {
                    Ok(snapshot) => verifier.verify(&bytes, &snapshot).await.map_err(Into::into),
                    Err(rejection) => Err(rejection),
                }
            }),
        }))
    }

    /// Registers `credential`, `slot`'s verdict, under a new revision.
    pub(super) fn settle(
        &self,
        slots: &mut Slots,
        slot: u32,
        length: usize,
        credential: Credential,
        maximum: usize,
    ) -> Result<(), StreamError> {
        let credential_bytes = slots.credential_bytes(slot, length)?;
        let revision = slots
            .next_revision
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        slots.next_revision = revision;
        if let Ok(capability) = &credential {
            slots.expiries.push(std::cmp::Reverse((
                capability.expires_at_unix_seconds(),
                revision,
                slot,
            )));
        }
        slots.entries.insert(slot, (revision, length, credential));
        slots.credential_bytes = credential_bytes;
        if slots.expiries.len() > maximum.saturating_mul(2) {
            slots.expiries = slots
                .entries
                .iter()
                .filter_map(|(slot, (revision, _, credential))| {
                    credential.as_ref().ok().map(|cap| {
                        std::cmp::Reverse((cap.expires_at_unix_seconds(), *revision, *slot))
                    })
                })
                .collect();
        }
        Ok(())
    }
    fn authorize(
        &self,
        slots: &Slots,
        binding: &Binding,
        channels: usize,
    ) -> Result<(), Rejection> {
        let capability = slots
            .entries
            .get(&binding.slot)
            .ok_or(Rejection::Forbidden)?
            .2
            .as_ref()
            .map_err(|error| *error)?;
        let action = binding
            .resource_authorization
            .as_ref()
            .map_or("streams/follow", |authorization| authorization.action);
        let scoped = binding.resource_authorization.is_some();
        self.check(&**capability, action, &binding.resource, scoped, channels)?;
        for (action, resource) in &binding.additional {
            self.check(&**capability, action, resource, false, channels)?;
        }
        Ok(())
    }
    /// Admits `action` on one stream resource for an already admitted credential.
    pub(super) fn permits(
        &self,
        capability: &dyn Admission,
        action: &'static str,
        resource: &str,
        channels: usize,
    ) -> Result<(), Rejection> {
        self.check(capability, action, resource, false, channels)
    }
    /// The one admission check for every session action on one resource.
    /// `scoped` follows a product resource through its owner's authorization.
    fn check(
        &self,
        capability: &dyn Admission,
        action: &'static str,
        resource: &str,
        scoped: bool,
        channels: usize,
    ) -> Result<(), Rejection> {
        let snapshot = self.snapshot()?;
        let attributes = acyclic_auth::contract::RecordAttributes::try_new(vec![])
            .map_err(|_| Rejection::Forbidden)?;
        let usage = acyclic_auth::contract::Limits {
            rps: None,
            burst: None,
            bytes_per_s: None,
            channels: Some(channels as u64),
            concurrency: None,
            bytes: None,
            spend_micros: None,
            tokens: None,
        };
        let request = Request {
            target: Target::Resource { action, resource },
            audience: &self.audience,
            holder: self.holder.as_deref(),
            now_unix_seconds: snapshot.now_unix_seconds,
            keys: &[],
            attributes: &attributes,
            usage: &usage,
            cpu_millis: 0,
            memory_bytes: 0,
        };
        let scope = match (action, scoped) {
            ("actors/invoke" | "streams/read", _) => Scope::Exact,
            ("streams/list", _) => Scope::Children,
            (_, true) => Scope::ResourceFollow,
            (_, false) => Scope::Follow,
        };
        capability
            .authorize(
                snapshot.trust.as_ref(),
                &request,
                PolicyDecision::Allow,
                scope,
            )
            .map_err(Rejection::from)
    }
    fn filter(
        &self,
        slots: &Slots,
        binding: &Binding,
        channels: usize,
    ) -> Result<crate::FollowFilter, Rejection> {
        self.authorize(slots, binding, channels)?;
        let capability = slots
            .entries
            .get(&binding.slot)
            .ok_or(Rejection::Forbidden)?
            .2
            .as_ref()
            .map_err(|error| *error)?;
        capability
            .follow_filter(binding.requested.clone())
            .map_err(|_| Rejection::Filter)
    }
    pub(super) fn bind(
        &self,
        slots: &Slots,
        slot: u32,
        path: &StreamPath,
        requested: wire::SessionFilter,
        channels: usize,
    ) -> Result<(Binding, crate::FollowFilter), Rejection> {
        let mut binding = Binding {
            credential_revision: slots.entries.get(&slot).ok_or(Rejection::Forbidden)?.0,
            slot,
            resource: stream_resource(Some(path)),
            requested,
            filter_digest: [0; 32],
            resource_authorization: None,
            additional: Vec::new(),
        };
        let filter = self.filter(slots, &binding, channels)?;
        binding.filter_digest = filter.digest();
        Ok((binding, filter))
    }
    pub(super) fn bind_resource(
        self: &Arc<Self>,
        slots: &Slots,
        slot: u32,
        resource: String,
        requested: wire::SessionFilter,
        channels: usize,
    ) -> Result<(Binding, crate::FollowFilter, ResourceAuthorization), Rejection> {
        if !resource.starts_with("fs:/") || !acyclic_auth::valid_resource(&resource) {
            return Err(Rejection::Forbidden);
        }
        let (revision, _, credential) = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability));
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource,
            requested,
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            additional: Vec::new(),
        };
        let filter = self.filter(slots, &binding, channels)?;
        binding.filter_digest = filter.digest();
        Ok((binding, filter, authorization))
    }

    pub(super) fn validate(
        &self,
        slots: &Slots,
        binding: &mut Binding,
        channels: usize,
    ) -> Result<(), Rejection> {
        let revision = slots
            .entries
            .get(&binding.slot)
            .ok_or(Rejection::Forbidden)?
            .0;
        if revision == binding.credential_revision {
            return self.authorize(slots, binding, channels);
        }
        // Every binding, follow or finite reply, keeps the ceiling it was
        // admitted under: principal and grants through `renew`, record filters
        // and pins through this digest. A changed ceiling closes the channel.
        if self.filter(slots, binding, channels)?.digest() != binding.filter_digest {
            return Err(Rejection::Forbidden);
        }
        if let Some(authorization) = &binding.resource_authorization {
            let capability = slots
                .entries
                .get(&binding.slot)
                .ok_or(Rejection::Forbidden)?
                .2
                .as_ref()
                .map_err(|error| *error)?;
            authorization.renew(Arc::clone(capability))?;
        }
        binding.credential_revision = revision;
        Ok(())
    }
    pub(super) fn bind_call(
        self: &Arc<Self>,
        slots: &Slots,
        slot: u32,
        resource: String,
        channels: usize,
    ) -> Result<(Binding, CallAuthorization), Rejection> {
        if !acyclic_auth::valid_resource(&resource) || !resource.starts_with("actors:/") {
            return Err(Rejection::Forbidden);
        }
        let (revision, _, credential) = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let mut authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability));
        authorization.action = "actors/invoke";
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource,
            requested: wire::SessionFilter::default(),
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            additional: Vec::new(),
        };
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        Ok((binding, CallAuthorization(authorization)))
    }
    /// Admits one children page: `streams/list` on the parent, as
    /// StreamService.ChildrenPage requires. Each returned child and tail
    /// record is then checked through the returned visibility, which reads the
    /// binding's current (renewed) credential when the reply is projected.
    pub(super) fn bind_list(
        self: &Arc<Self>,
        slots: &Slots,
        slot: u32,
        parent: Option<&StreamPath>,
        channels: usize,
    ) -> Result<(Binding, super::pages::Visibility), Rejection> {
        let (revision, _, credential) = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let resource = stream_resource(parent);
        let mut authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability));
        authorization.action = "streams/list";
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource,
            requested: wire::SessionFilter::default(),
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            additional: Vec::new(),
        };
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        Ok((
            binding,
            super::pages::Visibility {
                authority: Arc::clone(self),
                authorization,
                channels,
            },
        ))
    }
    pub(super) fn bind_heads(
        self: &Arc<Self>,
        slots: &Slots,
        slot: u32,
        request: &crate::HeadsRequest,
        channels: usize,
    ) -> Result<
        (
            Binding,
            ResourceAuthorization,
            Option<super::pages::Visibility>,
        ),
        Rejection,
    > {
        let (mut binding, authorization, visibility) = if let Some(page) = &request.children {
            let (binding, visibility) =
                self.bind_list(slots, slot, page.parent.as_ref(), channels)?;
            let authorization = visibility.authorization.clone();
            (binding, authorization, Some(visibility))
        } else {
            let (revision, _, credential) = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
            let capability = credential.as_ref().map_err(|error| *error)?;
            let resource = stream_resource(request.paths.first());
            let mut authorization = ResourceAuthorization::new(
                Arc::clone(self),
                resource.clone(),
                Arc::clone(capability),
            );
            authorization.action = "streams/read";
            let binding = Binding {
                credential_revision: *revision,
                slot,
                resource,
                requested: wire::SessionFilter::default(),
                filter_digest: [0; 32],
                resource_authorization: Some(authorization.clone()),
                additional: Vec::new(),
            };
            (binding, authorization, None)
        };
        binding.additional = request
            .paths
            .iter()
            .map(|path| ("streams/read", stream_resource(Some(path))))
            .collect();
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        Ok((binding, authorization, visibility))
    }

    pub(super) fn deadline(&self, slots: &mut Slots) -> Option<u64> {
        let now = self.snapshot().ok()?.now_unix_seconds;
        loop {
            let &std::cmp::Reverse((expiry, revision, slot)) = slots.expiries.peek()?;
            if expiry > now
                && slots
                    .entries
                    .get(&slot)
                    .is_some_and(|entry| entry.0 == revision)
            {
                return Some(expiry);
            }
            slots.expiries.pop();
        }
    }
    pub(super) async fn changed(&self, revision: u64, deadline: Option<u64>) {
        let changed = self.state.changed(revision);
        if let Some(deadline) = deadline {
            let expiry = self.state.expires_at(deadline);
            let _ = future::select(changed, expiry).await;
        } else {
            changed.await;
        }
    }
}

#[cfg(test)]
#[path = "authority_tests.rs"]
mod tests;

impl<P: StreamProvider + ?Sized> Session<P> {
    pub(super) fn close_authority(
        &mut self,
        identity: wire::SessionChannel,
        reason: Rejection,
    ) -> Result<(), StreamError> {
        self.remove(identity.channel_id)?;
        self.controls
            .retain(|frame| frame.channel_id != identity.channel_id);
        self.controls.push_back(frame(
            identity.channel_id,
            identity.generation,
            Body::Closed(wire::SessionClosed {
                code: reason.code().into(),
                message: String::new(),
            }),
        ));
        Ok(())
    }
    pub(super) fn validate_authority(&mut self, id: u64) -> Result<(), Rejection> {
        let count = self.channels.len();
        let binding = self
            .channels
            .get_mut(&id)
            .and_then(|channel| channel.binding.as_mut())
            .ok_or(Rejection::Forbidden)?;
        self.authority.validate(&self.slots, binding, count)
    }
    pub(super) fn revalidate(&mut self, slot: Option<u32>) -> Result<(), StreamError> {
        let snapshot = self
            .authority
            .snapshot()
            .map_err(|_| StreamError::AccessDenied)?;
        self.trust_revision = snapshot.revision;
        let count = self.channels.len();
        let denied = self
            .channels
            .iter_mut()
            .filter_map(|(id, channel)| {
                let binding = channel.binding.as_mut()?;
                if slot.is_some_and(|slot| slot != binding.slot) {
                    return None;
                }
                self.authority
                    .validate(&self.slots, binding, count)
                    .err()
                    .map(|reason| {
                        (
                            wire::SessionChannel {
                                channel_id: *id,
                                generation: channel.generation,
                            },
                            reason,
                        )
                    })
            })
            .collect::<Vec<_>>();
        for (identity, reason) in denied {
            self.close_authority(identity, reason)?;
        }
        self.deadline = self.authority.deadline(&mut self.slots);
        Ok(())
    }
    pub(super) fn fence_changed(&mut self) -> Result<(), StreamError> {
        let snapshot = self
            .authority
            .snapshot()
            .map_err(|_| StreamError::AccessDenied)?;
        if snapshot.revision != self.trust_revision
            || self
                .deadline
                .is_some_and(|deadline| snapshot.now_unix_seconds >= deadline)
        {
            self.revalidate(None)?;
        }
        Ok(())
    }
}
