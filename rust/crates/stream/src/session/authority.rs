//! Public trust, credential slots and immutable subscription ceilings.
#[cfg(any(test, feature = "test-support"))]
#[path = "testing.rs"]
pub mod testing;
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Scope, Target},
    trust::{ChangeSource, Changed, SourceClosed},
    verify::{self, Trust, VerifiedCapability, VerifyError},
};
use futures::FutureExt as _;

/// One authenticated, current public trust view and its protected clock.
/// No issuer or workload signing secret belongs in this object.
pub struct TrustSnapshot {
    /// Immutable keyring/revocation view; missing or stale state must fail closed.
    pub trust: Arc<dyn Trust + Send + Sync>,
    /// Protected current UTC Unix seconds: never earlier than any time the
    /// trust owner has returned or published.
    pub now_unix_seconds: u64,
}

/// Runtime-owned public trust feed. Waiting must be push driven, including
/// expiry; callers must never implement this with periodic trust polling.
pub trait AuthorityState: crate::ProviderPlatform {
    /// Captures current authenticated public trust and the protected time.
    fn snapshot(&self) -> Result<TrustSnapshot, VerifyError>;
    /// The owner's trust publications. A registered change resolves only
    /// once [`Self::snapshot`] reads the published trust, and with
    /// [`SourceClosed`] when the owner is lost.
    fn changes(&self) -> &dyn ChangeSource;
    /// Completes at the absolute UTC deadline, independently of record credit.
    /// Registration failure or loss of the clock producer returns `SourceClosed`;
    /// neither may be reported as expiry or left pending.
    fn expires_at(
        &self,
        unix_seconds: u64,
    ) -> crate::BoxProviderFuture<'_, Result<(), SourceClosed>>;
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum AuthorityEvent {
    Trust(Result<(), SourceClosed>),
    Expired,
    ClockLost,
}

/// Authenticated recipient and holder binding for one session host.
/// The runtime supplies public trust; the host never receives a root signer.
pub struct Authority {
    pub(super) state: Arc<dyn AuthorityState>,
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
impl From<StreamError> for Rejection {
    fn from(error: StreamError) -> Self {
        match error {
            StreamError::Unavailable => Self::Unavailable,
            _ => Self::Forbidden,
        }
    }
}
pub(super) type Credential = Result<Arc<VerifiedCapability>, Rejection>;

impl Slots {
    fn remove_expiry(&mut self, slot: u32) {
        if let Some(entry) = self.entries.get(&slot)
            && let Ok(capability) = &entry.credential
        {
            self.expiries
                .remove(&(capability.effective().expires_at_unix_seconds(), slot));
        }
    }

    pub(super) fn custody(&self, slot: u32) -> Option<Arc<dyn CredentialCustody>> {
        self.entries
            .get(&slot)
            .and_then(|entry| entry.custody.clone())
    }

    pub(super) fn can_reserve(&self, slot: u32, maximum: usize) -> Result<(), StreamError> {
        if usize::try_from(slot).map_err(|_| StreamError::Capacity)? >= maximum {
            return Err(StreamError::Capacity);
        }
        if self.entries.contains_key(&slot) {
            return Err(StreamError::AlreadyExists);
        }
        Ok(())
    }

    pub(super) fn reserve(&mut self, slot: u32, owner: wire::SessionChannel) {
        self.entries.insert(
            slot,
            Slot {
                revision: 0,
                bytes: 0,
                credential: Err(Rejection::Unavailable),
                changes: Default::default(),
                custody: None,
                reservation: Some(owner),
            },
        );
    }

    pub(super) fn release(&mut self, slot: u32, owner: wire::SessionChannel) {
        if self
            .entries
            .get(&slot)
            .is_some_and(|entry| entry.reservation == Some(owner))
        {
            self.entries.remove(&slot);
        }
    }

    /// The credential bytes held once `slot` holds `length` of them.
    fn credential_bytes(&self, slot: u32, length: usize) -> Result<usize, StreamError> {
        let previous = self.entries.get(&slot).map_or(0, |entry| entry.bytes);
        self.credential_bytes
            .checked_sub(previous)
            .and_then(|current| current.checked_add(length))
            .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
            .ok_or(StreamError::Capacity)
    }
}

#[derive(Default)]
pub(super) struct Slots {
    entries: BTreeMap<u32, Slot>,
    next_revision: u64,
    credential_bytes: usize,
    expiries: BTreeSet<(u64, u32)>,
}

struct Slot {
    revision: u64,
    bytes: usize,
    credential: Credential,
    changes: std::sync::OnceLock<tokio::sync::watch::Sender<Credential>>,
    custody: Option<Arc<dyn CredentialCustody>>,
    reservation: Option<wire::SessionChannel>,
}

pub(super) struct Binding {
    credential_revision: u64,
    pub(super) slot: u32,
    /// No resource means a revision-only credential-context observation.
    resource: Option<String>,
    requested: wire::SessionFilter,
    filter_digest: [u8; 32],
    resource_authorization: Option<ResourceAuthorization>,
    pub(super) enrollment: Option<u32>,
    custody: Option<Arc<dyn CredentialCustody>>,
    additional: Vec<(&'static str, String)>,
}

impl Binding {
    pub(super) fn custody(&self) -> Option<Arc<dyn CredentialCustody>> {
        self.custody.clone()
    }
    /// Finite one-shot replies (Actors calls and children pages) carry no
    /// record filter and get no credit-free lookahead.
    pub(super) fn is_reply(&self) -> bool {
        self.resource.is_none()
            || self.resource_authorization.as_ref().is_some_and(|value| {
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
    pub(super) fn enroll(
        &self,
        slots: &mut Slots,
        slot: u32,
        owner: wire::SessionChannel,
        enrollment: CredentialEnrollment,
        channels: usize,
    ) -> Result<(), StreamError> {
        if !slots
            .entries
            .get(&slot)
            .is_some_and(|entry| entry.reservation == Some(owner))
        {
            return Err(StreamError::InvalidArgument);
        }
        let snapshot = self.snapshot().map_err(|reason| match reason {
            Rejection::Unavailable => StreamError::Unavailable,
            _ => StreamError::AccessDenied,
        })?;
        let capability = enrollment.capability;
        if !capability.is_sealed() {
            return Err(StreamError::AccessDenied);
        }
        let caller = enrollment
            .caller
            .as_ref()
            .ok_or(StreamError::InvalidResponse)?;
        if capability.effective().principal() != caller.principal() {
            return Err(StreamError::AccessDenied);
        }
        self.check_at(&capability, Target::Context, false, channels, &snapshot)
            .map_err(|reason| match reason {
                Rejection::Unavailable => StreamError::Unavailable,
                _ => StreamError::AccessDenied,
            })?;
        let length = capability.encoded_len();
        let bytes = slots.credential_bytes(slot, length)?;
        let revision = slots
            .next_revision
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        enrollment.custody.check()?;
        let expires = capability.effective().expires_at_unix_seconds();
        slots.entries.insert(
            slot,
            Slot {
                revision,
                bytes: length,
                credential: Ok(Arc::new(capability)),
                changes: Default::default(),
                custody: Some(enrollment.custody),
                reservation: None,
            },
        );
        slots.next_revision = revision;
        slots.credential_bytes = bytes;
        slots.expiries.insert((expires, slot));
        Ok(())
    }

    /// Binds a canonical authenticated audience and presenting account.
    /// Credentials are this SDK's signed capability envelope, verified
    /// against `state`'s current trust at its protected time.
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
            audience,
            holder,
        })
    }
    pub(super) fn snapshot(&self) -> Result<TrustSnapshot, Rejection> {
        self.state.snapshot().map_err(Into::into)
    }
    /// The one place a session credential is verified: `bytes` as a signed
    /// capability envelope under the current trust, registered in `slot`
    /// under a new revision, within the slot and credential byte bounds.
    /// Empty `bytes` clear the slot. Every call, read and follow
    /// authorization is built from the registered capability and rechecked
    /// against the current trust on every change.
    pub(super) fn register(
        &self,
        slots: &mut Slots,
        slot: u32,
        bytes: &[u8],
        maximum: usize,
    ) -> Result<(), StreamError> {
        if usize::try_from(slot).map_err(|_| StreamError::Capacity)? >= maximum {
            return Err(StreamError::Capacity);
        }
        if !bytes.is_empty()
            && slots
                .entries
                .get(&slot)
                .is_some_and(|entry| entry.custody.is_some() || entry.reservation.is_some())
        {
            return Err(StreamError::AlreadyExists);
        }
        let credential_bytes = slots.credential_bytes(slot, bytes.len())?;
        if bytes.is_empty() {
            slots.remove_expiry(slot);
            slots.entries.remove(&slot);
            slots.credential_bytes = credential_bytes;
            return Ok(());
        }
        let credential: Credential = self.snapshot().and_then(|snapshot| {
            verify::verify(bytes, snapshot.trust.as_ref(), snapshot.now_unix_seconds)
                .map(Arc::new)
                .map_err(Into::into)
        });
        let revision = slots
            .next_revision
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        slots.remove_expiry(slot);
        slots.next_revision = revision;
        if let Ok(capability) = &credential {
            slots
                .expiries
                .insert((capability.effective().expires_at_unix_seconds(), slot));
        }
        match slots.entries.entry(slot) {
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                if let Some(changes) = current.changes.get() {
                    drop(changes.send_replace(credential.clone()));
                }
                current.revision = revision;
                current.bytes = bytes.len();
                current.credential = credential;
            }
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(Slot {
                    revision,
                    bytes: bytes.len(),
                    credential,
                    changes: Default::default(),
                    custody: None,
                    reservation: None,
                });
            }
        }
        slots.credential_bytes = credential_bytes;
        Ok(())
    }
    fn authorize(
        &self,
        slots: &Slots,
        binding: &Binding,
        channels: usize,
    ) -> Result<(), Rejection> {
        if let Some(custody) = &binding.custody {
            custody.check().map_err(Rejection::from)?;
        }
        let capability = slots
            .entries
            .get(&binding.slot)
            .ok_or(Rejection::Forbidden)?
            .credential
            .as_ref()
            .map_err(|error| *error)?;
        let action = binding
            .resource_authorization
            .as_ref()
            .map_or("streams/follow", |authorization| authorization.action);
        let scoped = binding.resource_authorization.is_some();
        let target = binding
            .resource
            .as_deref()
            .map_or(Target::Context, |resource| Target::Resource {
                action,
                resource,
            });
        self.check(&**capability, target, scoped, channels)?;
        for (action, resource) in &binding.additional {
            self.check(
                &**capability,
                Target::Resource { action, resource },
                false,
                channels,
            )?;
        }
        Ok(())
    }
    /// Admits `action` on one stream resource for an already admitted credential.
    pub(super) fn permits(
        &self,
        capability: &VerifiedCapability,
        action: &'static str,
        resource: &str,
        channels: usize,
    ) -> Result<(), Rejection> {
        self.check(
            capability,
            Target::Resource { action, resource },
            false,
            channels,
        )
    }
    /// The one admission check for session context and resource actions.
    /// `scoped` follows a product resource through its owner's authorization.
    fn check(
        &self,
        capability: &VerifiedCapability,
        target: Target<'_>,
        scoped: bool,
        channels: usize,
    ) -> Result<(), Rejection> {
        let snapshot = self.snapshot()?;
        self.check_at(capability, target, scoped, channels, &snapshot)
    }

    fn check_at(
        &self,
        capability: &VerifiedCapability,
        target: Target<'_>,
        scoped: bool,
        channels: usize,
        snapshot: &TrustSnapshot,
    ) -> Result<(), Rejection> {
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
            target,
            audience: &self.audience,
            holder: self.holder.as_deref(),
            now_unix_seconds: snapshot.now_unix_seconds,
            keys: &[],
            attributes: &attributes,
            usage: &usage,
            cpu_millis: 0,
            memory_bytes: 0,
        };
        let scope = match (target, scoped) {
            (
                Target::Context
                | Target::Resource {
                    action: "actors/invoke" | "streams/read",
                    ..
                },
                _,
            ) => Scope::Exact,
            (
                Target::Resource {
                    action: "streams/list",
                    ..
                },
                _,
            ) => Scope::Children,
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
            .credential
            .as_ref()
            .map_err(|error| *error)?;
        crate::FollowFilter::authorized(binding.requested.clone(), capability)
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
            credential_revision: slots
                .entries
                .get(&slot)
                .ok_or(Rejection::Forbidden)?
                .revision,
            slot,
            resource: Some(stream_resource(Some(path))),
            requested,
            filter_digest: [0; 32],
            resource_authorization: None,
            enrollment: None,
            custody: slots.custody(slot),
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
        let Slot {
            revision,
            credential,
            ..
        } = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability))
                .with_custody(slots.custody(slot));
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource: Some(resource),
            requested,
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            enrollment: None,
            custody: slots.custody(slot),
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
            .revision;
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
                .credential
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
        let Slot {
            revision,
            credential,
            ..
        } = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let mut authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability))
                .with_custody(slots.custody(slot));
        authorization.action = "actors/invoke";
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource: Some(resource),
            requested: wire::SessionFilter::default(),
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            enrollment: None,
            custody: slots.custody(slot),
            additional: Vec::new(),
        };
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        let changes = slots
            .entries
            .get(&slot)
            .ok_or(Rejection::Forbidden)?
            .changes
            .get_or_init(|| tokio::sync::watch::channel(credential.clone()).0)
            .subscribe();
        let filter_digest = binding.filter_digest;
        Ok((
            binding,
            CallAuthorization(
                authorization,
                changes,
                Arc::clone(capability),
                filter_digest,
                false,
                None,
            ),
        ))
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
        let Slot {
            revision,
            credential,
            ..
        } = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
        let capability = credential.as_ref().map_err(|error| *error)?;
        let resource = stream_resource(parent);
        let mut authorization =
            ResourceAuthorization::new(Arc::clone(self), resource.clone(), Arc::clone(capability))
                .with_custody(slots.custody(slot));
        authorization.action = "streams/list";
        let mut binding = Binding {
            credential_revision: *revision,
            slot,
            resource: Some(resource),
            requested: wire::SessionFilter::default(),
            filter_digest: [0; 32],
            resource_authorization: Some(authorization.clone()),
            enrollment: None,
            custody: slots.custody(slot),
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
            let Slot {
                revision,
                credential,
                ..
            } = slots.entries.get(&slot).ok_or(Rejection::Forbidden)?;
            let capability = credential.as_ref().map_err(|error| *error)?;
            let primary = request
                .paths
                .first()
                .or_else(|| request.retained.first().map(|selected| &selected.path));
            let resource = stream_resource(primary);
            let mut authorization = ResourceAuthorization::new(
                Arc::clone(self),
                resource.clone(),
                Arc::clone(capability),
            )
            .with_custody(slots.custody(slot));
            authorization.action = "streams/read";
            let binding = Binding {
                credential_revision: *revision,
                slot,
                resource: primary.map(|_| resource),
                requested: wire::SessionFilter::default(),
                filter_digest: [0; 32],
                resource_authorization: Some(authorization.clone()),
                enrollment: None,
                custody: slots.custody(slot),
                additional: Vec::new(),
            };
            (binding, authorization, None)
        };
        binding.additional = request
            .paths
            .iter()
            .chain(request.retained.iter().map(|selected| &selected.path))
            // Without a child page, the primary binding already checks path one.
            .skip(usize::from(request.children.is_none()))
            .map(|path| ("streams/read", stream_resource(Some(path))))
            .collect();
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        Ok((binding, authorization, visibility))
    }

    pub(super) fn deadline(&self, slots: &mut Slots) -> Option<u64> {
        let now = self.snapshot().ok()?.now_unix_seconds;
        loop {
            let &(expiry, _) = slots.expiries.first()?;
            if expiry > now {
                return Some(expiry);
            }
            slots.expiries.pop_first();
        }
    }
    /// Registers the next trust change, before the caller checks authority.
    /// `None` while the trust owner is lost: nothing is admitted then, and
    /// the session registers again once the owner is back.
    pub(super) fn register_change(&self) -> Option<Changed> {
        let changes = self.state.changes();
        changes.check_open().ok()?;
        Some(changes.changed())
    }
    /// Waits for the registered trust change, consuming it, or for the
    /// credential expiry at `deadline`. Clock producer loss is terminal and
    /// distinct from trust loss, whose original feed may recover.
    pub(super) async fn changed(
        &self,
        registration: &mut Option<Changed>,
        deadline: Option<u64>,
    ) -> AuthorityEvent {
        let changed = std::pin::pin!(async move {
            let Some(changed) = registration.as_mut() else {
                return future::pending::<Result<(), SourceClosed>>().await;
            };
            let result = changed.await;
            *registration = None;
            result
        });
        match deadline {
            Some(deadline) => {
                match future::select(changed, self.state.expires_at(deadline)).await {
                    future::Either::Left((result, _)) => AuthorityEvent::Trust(result),
                    future::Either::Right((Ok(()), _)) => AuthorityEvent::Expired,
                    future::Either::Right((Err(SourceClosed), _)) => AuthorityEvent::ClockLost,
                }
            }
            None => AuthorityEvent::Trust(changed.await),
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
    /// Rechecks every channel bound to `slot`, or every channel, against
    /// the current trust, after registering the next change so none is missed.
    pub(super) fn revalidate(&mut self, slot: Option<u32>) -> Result<(), StreamError> {
        if self.trust_changed.is_none() {
            self.trust_changed = self.authority.register_change();
        }
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
    /// The trust owner is lost, so no trust can be read: every bound channel
    /// closes as unavailable, and the session registers again.
    fn fence(&mut self) -> Result<(), StreamError> {
        let bound = self
            .channels
            .iter()
            .filter(|(_, channel)| channel.binding.is_some())
            .map(|(id, channel)| wire::SessionChannel {
                channel_id: *id,
                generation: channel.generation,
            })
            .collect::<Vec<_>>();
        for identity in bound {
            self.close_authority(identity, Rejection::Unavailable)?;
        }
        self.trust_changed = self.authority.register_change();
        self.deadline = self.authority.deadline(&mut self.slots);
        Ok(())
    }
    /// Acts on what [`Authority::changed`] waited for.
    pub(super) fn trust_event(&mut self, event: AuthorityEvent) -> Result<(), StreamError> {
        match event {
            AuthorityEvent::Trust(Err(SourceClosed)) => self.fence(),
            AuthorityEvent::Trust(Ok(())) | AuthorityEvent::Expired => self.revalidate(None),
            AuthorityEvent::ClockLost => Err(StreamError::Unavailable),
        }
    }
    /// Applies a trust change or credential expiry that happened since the
    /// session last looked: one look at the registered change, never a wait.
    pub(super) fn fence_changed(&mut self) -> Result<(), StreamError> {
        let Some(changed) = self.trust_changed.as_mut() else {
            // Lost earlier: register and recheck once the owner is back.
            if self.authority.state.changes().check_open().is_ok() {
                return self.revalidate(None);
            }
            return self.expire();
        };
        match changed.now_or_never() {
            Some(event) => {
                self.trust_changed = None;
                self.trust_event(AuthorityEvent::Trust(event))
            }
            None => self.expire(),
        }
    }
    /// Rechecks every channel once the earliest credential expiry has passed.
    fn expire(&mut self) -> Result<(), StreamError> {
        let Some(deadline) = self.deadline else {
            return Ok(());
        };
        let now = self
            .authority
            .snapshot()
            .map_err(|_| StreamError::AccessDenied)?
            .now_unix_seconds;
        if now >= deadline {
            self.revalidate(None)?;
        }
        Ok(())
    }
}
