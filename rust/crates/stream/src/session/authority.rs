//! Public trust, credential slots and immutable subscription ceilings.
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
pub(super) struct Slots(BTreeMap<u32, Credential>);

pub(super) struct Binding {
    pub(super) slot: u32,
    resource: String,
    requested: wire::SessionFilter,
    filter_digest: [u8; 32],
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
        if bytes.is_empty() {
            slots.0.remove(&slot);
            return Ok(());
        }
        let credential = self.snapshot().and_then(|snapshot| {
            verify::verify(bytes, snapshot.trust.as_ref(), snapshot.now_unix_seconds)
                .map(Arc::new)
                .map_err(Into::into)
        });
        slots.0.insert(slot, credential);
        Ok(())
    }
    fn filter(
        &self,
        slots: &Slots,
        binding: &Binding,
        channels: usize,
    ) -> Result<crate::FollowFilter, Rejection> {
        let capability = slots
            .0
            .get(&binding.slot)
            .ok_or(Rejection::Forbidden)?
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
        capability
            .authorize_follow(
                snapshot.trust.as_ref(),
                &Request {
                    target: Target::Resource {
                        action: "streams/follow",
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
                },
                PolicyDecision::Allow,
            )
            .map_err(Rejection::from)?;
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
            slot,
            resource: format!("streams:/{}", path.as_str()),
            requested,
            filter_digest: [0; 32],
        };
        let filter = self.filter(slots, &binding, channels)?;
        binding.filter_digest = filter.digest();
        Ok((binding, filter))
    }
    pub(super) fn validate(
        &self,
        slots: &Slots,
        binding: &Binding,
        channels: usize,
    ) -> Result<(), Rejection> {
        if self.filter(slots, binding, channels)?.digest() != binding.filter_digest {
            return Err(Rejection::Forbidden);
        }
        Ok(())
    }
    pub(super) fn deadline(&self, slots: &Slots) -> Option<u64> {
        let now = self.snapshot().ok()?.now_unix_seconds;
        slots
            .0
            .values()
            .filter_map(|credential| credential.as_ref().ok())
            .map(|cap| cap.effective().expires_at_unix_seconds())
            .filter(|expiry| *expiry > now)
            .min()
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
