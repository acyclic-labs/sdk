use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Invalid host metadata representation; never includes credential or handle values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid guest metadata field: {0}")]
pub struct GuestMetadataError(pub &'static str);

fn identity(value: &str, field: &'static str) -> Result<(), GuestMetadataError> {
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
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct CallerCapability {
    /// Host handle protocol version; S1 owns the capability chain behind it.
    pub version: u32,
    /// Invocation-scoped host handle, interpreted only by the host.
    pub handle: String,
}

impl CallerCapability {
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
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct AdmittedCaller {
    /// Owning account.
    pub account_id: String,
    /// Owning organization.
    pub org_id: String,
    /// Attested principal.
    pub principal: String,
    /// Credential identity, without credential bytes.
    pub credential_id: String,
    /// Actor audience admitted by the authority.
    pub actor_id: String,
    /// Decimal u64 expiry in Unix milliseconds.
    pub expires_at_ms: String,
    /// Opaque caller capability, absent until S1 admission supplies it.
    pub capability: Option<CallerCapability>,
}

impl AdmittedCaller {
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
        for (value, field) in [
            (&account_id, "accountId"),
            (&org_id, "orgId"),
            (&principal, "principal"),
            (&credential_id, "credentialId"),
            (&actor_id, "actorId"),
        ] {
            identity(value, field)?;
        }
        if expires_at_ms == 0 {
            return Err(GuestMetadataError("expiresAtMs"));
        }
        Ok(Self {
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
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RuntimeHandle {
    /// Feature protocol version.
    pub version: u32,
    /// Scoped runtime handle identity.
    pub handle: String,
}

impl RuntimeHandle {
    /// Construct a runtime extension handle; unknown nonzero versions remain opaque.
    pub fn new(version: u32, value: String) -> Result<Self, GuestMetadataError> {
        handle(version, &value)?;
        Ok(Self {
            version,
            handle: value,
        })
    }
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
