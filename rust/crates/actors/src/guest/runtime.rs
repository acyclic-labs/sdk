use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Maximum UTF-8 bytes in each admitted identity or opaque host handle.
pub const MAX_GUEST_METADATA_BYTES: usize = 1024;

/// Invalid host metadata representation; never includes credential or handle values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid guest metadata field: {0}")]
pub struct GuestMetadataError(pub &'static str);

fn identity(value: &str, field: &'static str) -> Result<(), GuestMetadataError> {
    if value.len() > MAX_GUEST_METADATA_BYTES {
        return Err(GuestMetadataError(field));
    }
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(GuestMetadataError(field));
    }
    Ok(())
}

fn handle(version: u32, value: &str) -> Result<(), GuestMetadataError> {
    if version == 0 {
        return Err(GuestMetadataError("version"));
    }
    identity(value, "handle")
}

/// Opaque host capability handle, reserved for S1 forwarding/attenuation.
/// This is neither a bearer token nor a second capability-chain representation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", try_from = "RawHandle")]
#[non_exhaustive]
pub struct CallerCapability {
    /// Host handle protocol version; S1 owns the capability chain behind it.
    version: u32,
    /// Invocation-scoped host handle, interpreted only by the host.
    handle: String,
}

impl CallerCapability {
    /// Host handle protocol version.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// Invocation-scoped opaque host handle.
    pub fn handle(&self) -> &str {
        &self.handle
    }
    /// Construct an opaque versioned handle. S1, not this constructor, admits authority.
    pub fn new(version: u32, value: String) -> Result<Self, GuestMetadataError> {
        handle(version, &value)?;
        Ok(Self {
            version,
            handle: value,
        })
    }
}

/// Identity admitted by the host, never inferred from customer request headers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", try_from = "RawCaller")]
#[non_exhaustive]
pub struct AdmittedCaller {
    /// Owning account.
    account_id: String,
    /// Owning organization.
    org_id: String,
    /// Attested principal.
    principal: String,
    /// Credential identity, without credential bytes.
    credential_id: String,
    /// Actor audience admitted by the authority.
    actor_id: String,
    /// Decimal u64 expiry in Unix milliseconds.
    expires_at_ms: String,
    /// Opaque caller capability, absent until S1 admission supplies it.
    capability: Option<CallerCapability>,
}

impl AdmittedCaller {
    /// Owning account.
    pub fn account_id(&self) -> &str {
        &self.account_id
    }
    /// Owning organization.
    pub fn org_id(&self) -> &str {
        &self.org_id
    }
    /// Host-attested principal.
    pub fn principal(&self) -> &str {
        &self.principal
    }
    /// Credential identity, without credential bytes.
    pub fn credential_id(&self) -> &str {
        &self.credential_id
    }
    /// Admitted actor audience.
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }
    /// Canonical positive decimal u64 Unix milliseconds.
    pub fn expires_at_ms(&self) -> &str {
        &self.expires_at_ms
    }
    /// Optional validated caller capability.
    pub fn capability(&self) -> Option<&CallerCapability> {
        self.capability.as_ref()
    }
    /// Construct the host's admitted projection with a nonzero Unix-ms expiry.
    /// Authentication, audience and expiry-at-delivery checks remain host obligations.
    pub fn new(
        account_id: String,
        org_id: String,
        principal: String,
        credential_id: String,
        actor_id: String,
        expires_at_ms: u64,
    ) -> Result<Self, GuestMetadataError> {
        Self::try_from(RawCaller {
            account_id,
            org_id,
            principal,
            credential_id,
            actor_id,
            expires_at_ms: expires_at_ms.to_string(),
            capability: None,
        })
    }

    /// Attach an S1 handle after checking its representation.
    pub fn with_capability(
        mut self,
        capability: CallerCapability,
    ) -> Result<Self, GuestMetadataError> {
        handle(capability.version, &capability.handle)?;
        self.capability = Some(capability);
        Ok(self)
    }
}

/// Versioned runtime-owned extension handle. Feature contracts add operations to
/// the handle; these names never enter the manifest binding namespace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", try_from = "RawHandle")]
#[non_exhaustive]
pub struct RuntimeHandle {
    /// Feature protocol version.
    version: u32,
    /// Scoped runtime handle identity.
    handle: String,
}

impl RuntimeHandle {
    /// Runtime handle protocol version.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// Scoped runtime handle identity.
    pub fn handle(&self) -> &str {
        &self.handle
    }
    /// Construct a runtime extension handle; unknown nonzero versions remain opaque.
    pub fn new(version: u32, value: String) -> Result<Self, GuestMetadataError> {
        handle(version, &value)?;
        Ok(Self {
            version,
            handle: value,
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawHandle {
    version: u32,
    #[serde(deserialize_with = "metadata_string")]
    handle: String,
}
impl TryFrom<RawHandle> for CallerCapability {
    type Error = GuestMetadataError;
    fn try_from(raw: RawHandle) -> Result<Self, Self::Error> {
        Self::new(raw.version, raw.handle)
    }
}
impl TryFrom<RawHandle> for RuntimeHandle {
    type Error = GuestMetadataError;
    fn try_from(raw: RawHandle) -> Result<Self, Self::Error> {
        Self::new(raw.version, raw.handle)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCaller {
    #[serde(deserialize_with = "metadata_string")]
    account_id: String,
    #[serde(deserialize_with = "metadata_string")]
    org_id: String,
    #[serde(deserialize_with = "metadata_string")]
    principal: String,
    #[serde(deserialize_with = "metadata_string")]
    credential_id: String,
    #[serde(deserialize_with = "metadata_string")]
    actor_id: String,
    #[serde(deserialize_with = "expiry_string")]
    expires_at_ms: String,
    capability: Option<CallerCapability>,
}
impl TryFrom<RawCaller> for AdmittedCaller {
    type Error = GuestMetadataError;
    fn try_from(raw: RawCaller) -> Result<Self, Self::Error> {
        for (value, field) in [
            (&raw.account_id, "accountId"),
            (&raw.org_id, "orgId"),
            (&raw.principal, "principal"),
            (&raw.credential_id, "credentialId"),
            (&raw.actor_id, "actorId"),
        ] {
            identity(value, field)?;
        }
        let expiry = raw
            .expires_at_ms
            .parse::<u64>()
            .map_err(|_| GuestMetadataError("expiresAtMs"))?;
        if expiry == 0 || !canonical_expiry(&raw.expires_at_ms) {
            return Err(GuestMetadataError("expiresAtMs"));
        }
        Ok(Self {
            account_id: raw.account_id,
            org_id: raw.org_id,
            principal: raw.principal,
            credential_id: raw.credential_id,
            actor_id: raw.actor_id,
            expires_at_ms: raw.expires_at_ms,
            capability: raw.capability,
        })
    }
}

fn canonical_expiry(value: &str) -> bool {
    !value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn metadata_string<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<String, D::Error> {
    bounded_string(decoder, MAX_GUEST_METADATA_BYTES)
}
fn expiry_string<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<String, D::Error> {
    bounded_string(decoder, 20)
}
fn bounded_string<'de, D: serde::Deserializer<'de>>(
    decoder: D,
    limit: usize,
) -> Result<String, D::Error> {
    struct Bounded(usize);
    impl serde::de::Visitor<'_> for Bounded {
        type Value = String;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "a string of at most {} UTF-8 bytes", self.0)
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<String, E> {
            if value.len() > self.0 {
                return Err(E::custom("metadata string exceeds UTF-8 byte limit"));
            }
            Ok(value.to_owned())
        }
        fn visit_string<E: serde::de::Error>(self, value: String) -> Result<String, E> {
            if value.len() > self.0 {
                return Err(E::custom("metadata string exceeds UTF-8 byte limit"));
            }
            Ok(value)
        }
    }
    decoder.deserialize_string(Bounded(limit))
}

/// Runtime namespace reserved independently of manifest env bindings and secrets.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", default)]
#[non_exhaustive]
pub struct ActorRuntime {
    /// Actor identity; absent for a stateless Worker.
    pub id: Option<String>,
    /// Actor class identity for actor-address and fork operations.
    pub class: Option<String>,
    /// Optional socket Send/Close/Broadcast/SetTags/SetAttachment/Connect authority.
    #[ts(optional)]
    pub sockets: Option<RuntimeHandle>,
    /// Optional runtime-owned auto-response configuration/timestamp authority.
    #[ts(optional)]
    pub auto_response: Option<RuntimeHandle>,
    /// Optional setAlarm/schedule authority; fires are idempotent inbox appends.
    #[ts(optional)]
    pub alarms: Option<RuntimeHandle>,
    /// Optional SQL-over-fs-volume handle; never heap-checkpoint pages.
    #[ts(optional)]
    pub sql: Option<RuntimeHandle>,
    /// Optional host-managed lifetime extension authority.
    #[ts(optional)]
    pub wait_until: Option<RuntimeHandle>,
}
