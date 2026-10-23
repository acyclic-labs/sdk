//! Public trust, credential slots and immutable subscription ceilings.
#[cfg(any(test, feature = "test-support"))]
#[path = "testing.rs"]
pub mod testing;
use super::*;
use acyclic_auth::{
    authority::{PolicyDecision, Request, Target},
    verify::{self, Trust, VerifiedCapability, VerifyError},
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
    state: Arc<dyn AuthorityState>,
    audience: String,
    holder: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Rejection {
    Forbidden,
    Revoked,
    Expired,
    Filter,
}
impl Rejection {
    pub(super) fn code(self) -> &'static str {
        match self {
            Self::Forbidden => "forbidden",
            Self::Revoked => "revoked",
            Self::Expired => "credential_expired",
            Self::Filter => "filter_invalid",
        }
    }
}
impl From<VerifyError> for Rejection {
    fn from(error: VerifyError) -> Self {
        match error {
            VerifyError::Revoked => Self::Revoked,
            VerifyError::Time => Self::Expired,
            _ => Self::Forbidden,
        }
    }
}
type Credential = Result<Arc<VerifiedCapability>, Rejection>;
#[derive(Default)]
pub(super) struct Slots {
    entries: BTreeMap<u32, (u64, usize, Credential)>,
    next_revision: u64,
    credential_bytes: usize,
    expiries: std::collections::BinaryHeap<std::cmp::Reverse<(u64, u64, u32)>>,
}

pub(super) struct Binding {
    children: bool,
    credential_revision: u64,
    pub(super) slot: u32,
    resource: String,
    requested: wire::SessionFilter,
    filter_digest: [u8; 32],
}

fn stream_resource(path: Option<&StreamPath>) -> String {
    let mut resource = String::from("streams:/");
    if let Some(path) = path {
        for (index, component) in path.as_str().split('/').enumerate() {
            if index != 0 {
                resource.push('/');
            }
            resource.push_str(&acyclic_auth::encode_component(component.as_bytes()));
        }
    }
    resource
}
impl Authority {
    /// Binds a canonical authenticated audience and presenting account.
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
        let previous_bytes = slots.entries.get(&slot).map_or(0, |entry| entry.1);
        let credential_bytes = slots
            .credential_bytes
            .checked_sub(previous_bytes)
            .and_then(|current| current.checked_add(bytes.len()))
            .filter(|total| *total <= crate::MAX_COMMAND_BYTES)
            .ok_or(StreamError::Capacity)?;
        if bytes.is_empty() {
            slots.entries.remove(&slot);
            slots.credential_bytes = credential_bytes;
            return Ok(());
        }
        let credential = self.snapshot().and_then(|snapshot| {
            verify::verify(bytes, snapshot.trust.as_ref(), snapshot.now_unix_seconds)
                .map(Arc::new)
                .map_err(Into::into)
        });
        let revision = slots
            .next_revision
            .checked_add(1)
            .ok_or(StreamError::Capacity)?;
        slots.next_revision = revision;
        if let Ok(capability) = &credential {
            slots.expiries.push(std::cmp::Reverse((
                capability.effective().expires_at_unix_seconds(),
                revision,
                slot,
            )));
        }
        slots
            .entries
            .insert(slot, (revision, bytes.len(), credential));
        slots.credential_bytes = credential_bytes;
        if slots.expiries.len() > maximum.saturating_mul(2) {
            slots.expiries = slots
                .entries
                .iter()
                .filter_map(|(slot, (revision, _, credential))| {
                    credential.as_ref().ok().map(|cap| {
                        std::cmp::Reverse((
                            cap.effective().expires_at_unix_seconds(),
                            *revision,
                            *slot,
                        ))
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
            target: Target::Resource {
                action: if binding.children {
                    "streams/list"
                } else {
                    "streams/follow"
                },
                resource: &binding.resource,
            },
            audience: &self.audience,
            holder: self.holder.as_deref(),
            now_unix_seconds: snapshot.now_unix_seconds,
            keys: &[],
            attributes: &attributes,
            usage: &usage,
            cpu_millis: 0,
            memory_bytes: 0,
        };
        if binding.children {
            capability.authorize_children(snapshot.trust.as_ref(), &request, PolicyDecision::Allow)
        } else {
            capability.authorize_follow(snapshot.trust.as_ref(), &request, PolicyDecision::Allow)
        }
        .map_err(Rejection::from)?;
        Ok(())
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
            children: false,
            credential_revision: slots.entries.get(&slot).ok_or(Rejection::Forbidden)?.0,
            slot,
            resource: stream_resource(Some(path)),
            requested,
            filter_digest: [0; 32],
        };
        let filter = self.filter(slots, &binding, channels)?;
        binding.filter_digest = filter.digest();
        Ok((binding, filter))
    }
    pub(super) fn bind_children(
        &self,
        slots: &Slots,
        slot: u32,
        parent: Option<&StreamPath>,
        requested: wire::SessionFilter,
        channels: usize,
    ) -> Result<Binding, Rejection> {
        let mut binding = Binding {
            children: true,
            credential_revision: slots.entries.get(&slot).ok_or(Rejection::Forbidden)?.0,
            slot,
            resource: stream_resource(parent),
            requested,
            filter_digest: [0; 32],
        };
        binding.filter_digest = self.filter(slots, &binding, channels)?.digest();
        Ok(binding)
    }
    pub(super) fn permits_child(
        &self,
        slots: &Slots,
        binding: &mut Binding,
        child: &StreamPath,
        channels: usize,
    ) -> Result<bool, Rejection> {
        self.validate(slots, binding, channels)?;
        let child_binding = Binding {
            children: true,
            credential_revision: binding.credential_revision,
            slot: binding.slot,
            resource: stream_resource(Some(child)),
            requested: binding.requested.clone(),
            filter_digest: binding.filter_digest,
        };
        match self.filter(slots, &child_binding, channels) {
            Ok(filter) => {
                let headers = crate::RecordHeaders::new(vec![child.as_str().into()], vec![])
                    .map_err(|_| Rejection::Filter)?;
                Ok(filter.matches(&headers))
            }
            Err(Rejection::Forbidden) => Ok(false),
            Err(error) => Err(error),
        }
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
        if self.filter(slots, binding, channels)?.digest() != binding.filter_digest {
            return Err(Rejection::Forbidden);
        }
        binding.credential_revision = revision;
        Ok(())
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
