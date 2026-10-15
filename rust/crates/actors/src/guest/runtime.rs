use serde::{Deserialize, Serialize};
use ts_rs::TS;

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
