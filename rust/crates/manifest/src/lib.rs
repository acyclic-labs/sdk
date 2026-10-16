//! Deployment admission shared by Cloud, native tooling and WASM consumers.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

mod cron;
mod generation;
mod validation;
pub mod wire;
pub use acyclic_actors::domain::{ActorLimits, Binding, SubscriptionSpec};
pub use acyclic_auth::contract::{Limits, ResourceGrant, ResourcePattern};
pub use generation::{environment_typescript, json_schema, typescript};
pub use validation::{ManifestError, lower_actor, validate, validate_work};
use wire::Contract;

/// Maximum untrusted JSON size, before allocation or parsing.
pub const MAX_JSON_BYTES: usize = 1_048_576;
/// Maximum environments in one deployment.
pub const MAX_ENVIRONMENTS: usize = 16;
/// Maximum items in any declaration or selection list.
pub const MAX_ITEMS: usize = 64;
/// Maximum UTF-8 bytes in any string, including imported contracts.
pub const MAX_STRING_BYTES: usize = 2048;
/// Maximum JSON nodes across the complete document.
pub const MAX_NODES: usize = 16384;

/// Versioned deployment document; project and account identity come from the deploy call.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentManifest {
    /// Schema discriminator, exactly one.
    #[schemars(range(min = 1, max = 1))]
    pub schema_version: u32,
    /// Independent DNS-label-named deployment environments.
    pub environments: Vec<Environment>,
}

/// Complete isolated configuration; no environment overlays or implicit selections.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Environment {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Artifact entries available for deployment.
    pub entries: Vec<Entry>,
    /// The Actors contract binding, with capability naming a typed grant below.
    #[ts(type = "Binding[]")]
    pub bindings: Vec<Contract<Binding>>,
    /// Named typed workload grants; account spend entitlements remain host-owned.
    pub grants: Vec<BindingGrant>,
    /// Credential-store references available only through explicit selection.
    pub secrets: Vec<SecretRef>,
    /// UTC schedules delivering idempotent records to subscribed inboxes.
    pub crons: Vec<CronSpec>,
    /// Content-addressed asset bundle declarations.
    pub assets: Vec<AssetSpec>,
    /// Customer routes; host/prefix pairs are unique across all environments.
    pub routes: Vec<RouteSpec>,
}

/// Named S1 grant and optional ceilings used to derive a workload capability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingGrant {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Authority using the shared auth.v1 resource and action contract.
    #[ts(type = "ResourceGrant")]
    pub grant: Contract<ResourceGrant>,
    /// Product-specific execution or capability ceilings.
    #[ts(type = "Limits | null")]
    pub limits: Option<Contract<Limits>>,
}

/// Content-addressed guest entry with explicitly selected environment handles.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Nonzero lowercase SHA-256 of the immutable code artifact.
    pub artifact_sha256: String,
    /// Logical artifact module identifier, never a filesystem path.
    pub module: String,
    /// Explicitly selected binding names.
    pub bindings: Vec<String>,
    /// Credential-store references available only through explicit selection.
    pub secrets: Vec<String>,
    /// Actor instance or stateless Worker configuration.
    pub runtime: EntryRuntime,
    /// Optional scoped HTTPS handle; absence denies outbound HTTPS.
    pub fetch: Option<FetchBinding>,
}

/// Product configuration lowered to the corresponding platform request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum EntryRuntime {
    /// One actor instance per project/environment/entry deploy identity.
    Actor {
        /// Explicit initial placement region.
        #[serde(rename = "homeRegion")]
        home_region: String,
        /// Product-specific execution or capability ceilings.
        #[ts(type = "ActorLimits")]
        limits: Contract<ActorLimits>,
        /// Canonical Actors subscription specs, including starts and placement anchors.
        #[ts(type = "SubscriptionSpec[]")]
        subscriptions: Vec<Contract<SubscriptionSpec>>,
        /// Optional durable alarm destination; enabling timers requires its wake subscription.
        #[serde(default, rename = "timerInbox")]
        timer_inbox: Option<TimerInbox>,
    },
    /// Stateless Worker with explicit request ceilings.
    Worker {
        /// Product-specific execution or capability ceilings.
        limits: WorkerLimits,
    },
}

/// Durable alarm destination, admitted only with a matching wake subscription.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimerInbox {
    /// Selected environment binding name.
    pub binding: String,
    /// Exact canonical Streams resource for durable fires.
    #[ts(type = "ResourcePattern")]
    pub stream: Contract<ResourcePattern>,
}

/// Positive stateless Worker CPU, memory and wall-clock ceilings.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerLimits {
    /// Positive CPU milliseconds per request.
    pub cpu_millis: u32,
    /// Positive memory ceiling in bytes.
    pub memory_bytes: u32,
    /// Positive wall-clock milliseconds per request.
    pub timeout_millis: u32,
}

/// One entry-owned HTTPS handle and its complete destination allow-list.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FetchBinding {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Complete allow-list; an empty list is rejected.
    pub egress: Vec<EgressRule>,
}

/// Exact canonical DNS authority, resolved and pinned by the host before use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressRule {
    /// Lowercase canonical ASCII DNS name, excluding numeric IP forms.
    pub host: String,
    /// The host always uses TLS with certificate validation, on this port.
    pub port: u16,
}

/// Typed credential-store reference, resolved by the host outside the manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretRef {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Only secrets:/ followed by nonempty ASCII identifier path segments.
    pub reference: SecretReference,
}

/// Validated credential-store address; never a credential value or URL.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(try_from = "String", into = "String")]
#[ts(type = "string")]
pub struct SecretReference(String);

impl JsonSchema for SecretReference {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SecretReference".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({"type":"string", "maxLength":256,
            "pattern":"^secrets:/[A-Za-z0-9_-]{1,63}(/[A-Za-z0-9_-]{1,63})*$"})
    }
}

impl SecretReference {
    /// Admit only bounded secrets:/ identifier paths.
    pub fn new(value: String) -> Result<Self, ManifestError> {
        let path = value
            .strip_prefix("secrets:/")
            .ok_or_else(|| ManifestError::invalid("reference", "secret-reference"))?;
        if value.len() > 256
            || path.split('/').any(|segment| {
                segment.is_empty()
                    || segment.len() > 63
                    || !segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
            })
        {
            return Err(ManifestError::invalid("reference", "secret-reference"));
        }
        Ok(Self(value))
    }
    /// Return the validated credential-store address.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for SecretReference {
    type Error = ManifestError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}
impl From<SecretReference> for String {
    fn from(value: SecretReference) -> Self {
        value.0
    }
}

/// UTC numeric five-field cron; restricted day-of-month and weekday use OR.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronSpec {
    /// Declaration name; environment names are DNS labels, env properties are identifiers.
    pub name: String,
    /// Five numeric UTC cron fields, with lists, ranges and positive steps.
    pub schedule: String,
    /// Target entry name in this environment.
    pub entry: String,
    /// Selected Streams binding authorizing append to this inbox.
    pub inbox_binding: String,
    /// Exact streams:/ resource; the target must subscribe to this stream.
    #[ts(type = "ResourcePattern")]
    pub inbox: Contract<ResourcePattern>,
}

/// Immutable asset bundle served through a selected Objects binding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetSpec {
    /// Target entry name in this environment.
    pub entry: String,
    /// Selected environment binding name.
    pub binding: String,
    /// Immutable Objects keys are assets/<bundleSha256>/<path>.
    pub bundle_sha256: String,
    /// Serve the bundle index for missing asset paths when enabled.
    pub spa_fallback: bool,
    /// Explicit cache policy for bundle responses.
    pub cache: AssetCache,
}
/// Browser cache behavior for content-addressed asset paths.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub enum AssetCache {
    /// Cache the immutable bundle path without revalidation.
    Immutable,
    /// Require revalidation for browser responses.
    Revalidate,
}

/// Customer route with canonical segment-boundary prefix matching.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteSpec {
    /// Lowercase canonical ASCII DNS name, excluding numeric IP forms.
    pub host: String,
    /// Canonical absolute prefix; segment-boundary matching, no trailing slash except /.
    pub path_prefix: String,
    /// Target entry name in this environment.
    pub entry: String,
}

/// Version discrimination precedes strict v1 decoding; all inputs are size bounded.
pub fn parse(input: &[u8]) -> Result<DeploymentManifest, ManifestError> {
    if input.len() > MAX_JSON_BYTES {
        return Err(ManifestError::invalid("$", "json-size"));
    }
    let value: serde_json::Value = serde_json::from_slice(input).map_err(ManifestError::json)?;
    if value
        .get("schemaVersion")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        return Err(ManifestError::invalid(
            "$.schemaVersion",
            "unsupported-version",
        ));
    }
    validation::bound_json(&value)?;
    let manifest = serde_json::from_slice(input).map_err(ManifestError::json)?;
    validate(&manifest)?;
    Ok(manifest)
}

/// Node/browser admission export; executes the same parser and validator as Rust.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = validate)]
pub fn validate_json(input: &str) -> Result<(), wasm_bindgen::JsValue> {
    parse(input.as_bytes())
        .map(|_| ())
        .map_err(|error| wasm_bindgen::JsValue::from_str(&error.to_string()))
}
