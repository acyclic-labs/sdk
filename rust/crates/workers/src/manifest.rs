//! The deployment manifest authority consumed by Cloud and SDK authoring tools.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use ts_rs::{Config, TS};

mod validation;
pub use validation::{ManifestError, validate};

/// Version-one deployment manifest. Environment configurations are complete, not overlays.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentManifest {
    /// Exactly 1 for this schema.
    #[schemars(range(min = 1, max = 1))]
    pub schema_version: u32,
    /// Independent named deployment configurations.
    pub environments: Vec<Environment>,
}

/// One isolated environment; secret values are never part of this document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Environment {
    /// Stable environment name.
    pub name: String,
    /// Artifact entry points.
    pub entries: Vec<Entry>,
    /// Declared capabilities available for explicit entry selection.
    pub bindings: Vec<BindingSpec>,
    /// Credential-store references available for explicit entry selection.
    pub secrets: Vec<SecretRef>,
    /// Exact outbound HTTPS destinations; DNS/private-address enforcement is host-owned.
    pub egress: Vec<EgressRule>,
    /// Schedules delivering to actor inbox streams.
    pub crons: Vec<CronSpec>,
    /// Objects-backed static asset declarations.
    pub assets: Vec<AssetSpec>,
    /// HTTP routing declarations.
    pub routes: Vec<RouteSpec>,
}

/// Execution product for an artifact entry point.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub enum EntryKind {
    /// Stateful guest with restore/event/checkpoint hooks.
    Actor,
    /// Stateless default.fetch guest.
    Worker,
}

/// Content-addressed code and its selected environment handles.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Entry {
    /// Unique entry name within the environment.
    pub name: String,
    /// Actor or stateless worker.
    pub kind: EntryKind,
    /// Lowercase hexadecimal SHA-256 of the artifact.
    pub artifact_sha256: String,
    /// Logical module name within the artifact; not a filesystem path.
    pub module: String,
    /// Names of explicitly granted bindings.
    pub bindings: Vec<String>,
    /// Names of explicitly granted credential references.
    pub secrets: Vec<String>,
}

/// Binding handle family. Capabilities remain opaque references to Trust policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase")]
pub enum BindingKind {
    /// Stream namespace handle.
    Streams,
    /// Filesystem volume handle.
    Volume,
    /// Objects bucket handle.
    Objects,
    /// Machines or sandbox compute handle.
    Compute,
    /// Actor client handle, for apps.
    Actors,
    /// Outbound HTTPS handle.
    Fetch,
}

impl BindingKind {
    fn typescript_parameter(self) -> &'static str {
        match self {
            Self::Streams => "Streams",
            Self::Volume => "Volume",
            Self::Objects => "Objects",
            Self::Compute => "Compute",
            Self::Actors => "Actors",
            Self::Fetch => "Fetch",
        }
    }
}

/// A named resource capability; deployment does not embed credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BindingSpec {
    /// Guest env property name.
    pub name: String,
    /// Runtime handle family.
    pub kind: BindingKind,
    /// Opaque platform resource reference.
    pub resource: String,
    /// Opaque Trust capability/policy reference, not a bearer token.
    pub capability: String,
}

/// Reference resolved by the host and sealed to the deployment artifact digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretRef {
    /// Guest env property name.
    pub name: String,
    /// Opaque credential-store reference; never the secret value.
    pub reference: String,
}

/// Exact HTTPS authority allowed for outbound fetch; no wildcard or IP literals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EgressRule {
    /// Canonical lowercase ASCII DNS hostname.
    pub host: String,
    /// Explicit TCP port.
    pub port: u16,
}

/// UTC five-field numeric cron, with lists, ranges and positive steps. Sunday is 0.
/// Day-of-month/day-of-week use OR when both are restricted. A fire appends to the
/// actor inbox, keyed by occurrence timer ID; no Invoke or runtime keepalive wake.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronSpec {
    /// Unique schedule name.
    pub name: String,
    /// Minute/hour/day-of-month/month/day-of-week numeric cron expression.
    pub schedule: String,
    /// Target actor entry name.
    pub entry: String,
    /// Target inbox stream binding selected by that entry.
    pub inbox_binding: String,
    /// Canonical account-relative stream path resolved through that binding.
    pub inbox_stream: String,
}

/// An asset prefix in an explicitly selected Objects binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AssetSpec {
    /// Entry serving the assets.
    pub entry: String,
    /// Objects binding selected by the entry.
    pub binding: String,
    /// Opaque object-key prefix.
    pub prefix: String,
}

/// Hostname and URL path-prefix route to a declared artifact entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteSpec {
    /// Canonical DNS hostname.
    pub host: String,
    /// Absolute URL path prefix, not a filesystem path.
    pub path_prefix: String,
    /// Target entry name.
    pub entry: String,
}

/// Admit untrusted JSON through the same Rust validation used by generation.
pub fn parse(input: &[u8]) -> Result<DeploymentManifest, ManifestError> {
    let manifest = serde_json::from_slice(input).map_err(ManifestError::Json)?;
    validate(&manifest)?;
    Ok(manifest)
}

/// Generate structural JSON Schema from the executable Rust declaration.
/// Cross-reference and cron admission additionally require validate/parse.
pub fn json_schema() -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(&schemars::schema_for!(DeploymentManifest)).map(|text| text + "\n")
}

/// Generate all static manifest types from Rust; no independently authored schema.
pub fn typescript() -> String {
    let config = Config::default();
    let mut output = String::from("// Generated from acyclic_workers::manifest. Do not edit.\n");
    macro_rules! declarations {
        ($($ty:ty),+ $(,)?) => {$(
            output.push_str("export ");
            output.push_str(&<$ty as TS>::decl(&config));
            output.push('\n');
        )+};
    }
    declarations!(
        DeploymentManifest,
        Environment,
        Entry,
        EntryKind,
        BindingSpec,
        BindingKind,
        SecretRef,
        EgressRule,
        CronSpec,
        AssetSpec,
        RouteSpec
    );
    output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

/// Generate a selected entry's env type, parameterized by host-provided handle types.
pub fn environment_typescript(
    manifest: &DeploymentManifest,
    environment: &str,
    entry: &str,
) -> Result<String, ManifestError> {
    validate(manifest)?;
    let environment = manifest
        .environments
        .iter()
        .find(|value| value.name == environment)
        .ok_or_else(|| ManifestError::Invalid("unknown environment".into()))?;
    let entry = environment
        .entries
        .iter()
        .find(|value| value.name == entry)
        .ok_or_else(|| ManifestError::Invalid("unknown entry".into()))?;
    let mut output = String::from(
        "// Generated from the admitted deployment manifest. Do not edit.\nexport type Env<Streams, Volume, Objects, Compute, Actors, Fetch, Secret> = {\n",
    );
    for binding in environment
        .bindings
        .iter()
        .filter(|binding| entry.bindings.contains(&binding.name))
    {
        output.push_str(&format!(
            "  readonly {}: {};\n",
            serde_json::to_string(&binding.name).map_err(ManifestError::Json)?,
            binding.kind.typescript_parameter()
        ));
    }
    for secret in environment
        .secrets
        .iter()
        .filter(|secret| entry.secrets.contains(&secret.name))
    {
        output.push_str(&format!(
            "  readonly {}: Secret;\n",
            serde_json::to_string(&secret.name).map_err(ManifestError::Json)?
        ));
    }
    output.push_str("};\n");
    Ok(output)
}
