//! Common harness identities and operation outcomes.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Maximum UTF-8 byte length for names crossing a component boundary.
pub const COMPONENT_LABEL_MAX_BYTES: usize = crate::conversation::MAX_PORTABLE_COUNT;

/// Exact labels and separators rejected by every component registry.
pub const COMPONENT_LABEL_FORBIDDEN_EXACT: [&str; 2] = [".", ".."];
/// Separators rejected by every component registry.
pub const COMPONENT_LABEL_FORBIDDEN_SEPARATORS: [char; 2] = ['/', '\\'];

/// Capability names the substrate checks; scopes grant them as plain strings.
pub(crate) mod capability {
    use std::fmt::Display;

    pub(crate) const LIFECYCLE_MANAGE: &str = "lifecycle:manage";
    pub(crate) const EVENT_APPEND: &str = "event:append";
    pub(crate) const EXTENSION_MIGRATE: &str = "extension:migrate";
    pub(crate) const EXTENSION_ACTIVATE: &str = "extension:activate";
    pub(crate) const EXTENSION_CONFIGURE: &str = "extension:configure";
    pub(crate) const EFFECT_RUN: &str = "effect:run";
    pub(crate) const EFFECT_PLAN: &str = "effect:plan";
    pub(crate) const FORK_PUBLISH: &str = "fork:publish";
    pub(crate) const PROJECT_MERGE: &str = "project:merge";
    pub(crate) const CONVERSATION_BIND: &str = "conversation:bind";
    pub(crate) const CONVERSATION_APPEND: &str = "conversation:append";
    pub(crate) const CONVERSATION_SELECT_CONTEXT: &str = "conversation:select_context";
    pub(crate) const INTERACTION_OPEN: &str = "interaction:open";
    pub(crate) const INTERACTION_RESOLVE: &str = "interaction:resolve";
    pub(crate) const INTERACTION_ROUTE: &str = "interaction:route";
    pub(crate) const OPERATION_OBSERVE: &str = "operation:observe";
    pub(crate) const OPERATION_CANCEL: &str = "operation:cancel";
    pub(crate) const OPERATION_DECLARE: &str = "operation:declare";
    pub(crate) const MAIL_SEND: &str = "mail:send";
    pub(crate) const MAIL_READ: &str = "mail:read";
    pub(crate) const TIMER_WAIT: &str = "timer:wait";
    pub(crate) const MODEL_GENERATE: &str = "model:generate";
    pub(crate) const CONTEXT_BUILD: &str = "context:build";

    pub(crate) fn effect_provider(provider: impl Display) -> String {
        format!("effect:provider:{provider}")
    }

    pub(crate) fn tool_call(tool: impl Display) -> String {
        format!("tool:call:{tool}")
    }

    pub(crate) fn interaction_respond(id: impl Display) -> String {
        format!("interaction:respond:{id}")
    }
}

/// Successor of a durable revision or sequence, failing instead of wrapping.
pub(crate) fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("revision exhausted".into()))
}

mod json;
pub(crate) use json::{admit_json, lexical_json_diagnostic_bytes};
mod event_json;
pub(crate) use event_json::{
    EventPayloadJsonAdmission, audited_serde_diagnostic_bytes, event_payload_json_admission,
};
mod snapshot_json;
pub(crate) use snapshot_json::{SnapshotJsonAdmission, snapshot_json_admission};
pub use json::{
    JsonBounds, JsonCodecCharge, canonical_json_bytes_bounded, json_bytes_bounded_trusted,
    json_from_slice_bounded_trusted, json_value_bounded_trusted, json_value_decode_owned_bytes,
    sdk_json_diagnostic_bytes,
};

/// One sorted-key JSON encoding for durable identities and Rust/WASM output.
/// The canonical bounded serializer preserves Value integer/float and duplicate
/// key semantics, without an unchecked initial `serde_json::to_value`.
pub(crate) fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    json::canonical_json_bytes_trusted(value)
}

/// Checks compact JSON size without retaining an encoded buffer or cloning a
/// Value tree. Key ordering does not change the size of ordinary typed records.
/// Canonical encoding and identity checks still use `canonical_json_bytes`.
/// This is only an encoded-size check, never owned-tree or serde heap admission.
pub(crate) fn validate_json_byte_bound<T: Serialize>(value: &T, maximum: u64) -> Result<()> {
    json_byte_length(value, maximum).map(|_| ())
}

/// Counts compact JSON input using a bounded writer, without allocating its bytes.
pub(crate) fn json_byte_length<T: Serialize>(value: &T, maximum: u64) -> Result<u64> {
    struct Budget(u64);

    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("JSON exceeds declared byte bound"))?;
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut budget = Budget(maximum);
    serde_json::to_writer(&mut budget, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(maximum - budget.0)
}

/// Complete typed decoding for trusted existing record implementations. This
/// checks finite input structure, not arbitrary custom Deserialize heap usage.
pub(crate) fn json_from_slice<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> serde_json::Result<T> {
    json::admit_json_trusted(bytes, JsonBounds::IDENTITY, &mut |_| Ok(()))
        .map_err(serde::de::Error::custom)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder)?;
    decoder.end()?;
    Ok(value)
}

/// Digest of the exact canonical JSON bytes used at durable identity boundaries.
pub(crate) fn canonical_json_digest<T: Serialize>(value: &T) -> Result<[u8; 32]> {
    Ok(*blake3::hash(&canonical_json_bytes(value)?).as_bytes())
}

/// The same canonical digest, charging its actual original encoder/hash work.
pub(crate) fn canonical_json_digest_with_work<T: Serialize + ?Sized>(
    value: &T,
    maximum_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
    let bytes = crate::store::history::canonical_bytes_with_work(
        value, maximum_bytes, custody.work())?;
    custody.adopt(bytes.capacity() as u64)?;
    if let Some(work) = custody.work().as_deref_mut() {
        work.hashed_bytes(bytes.len() as u64)?;
    }
    Ok(*blake3::hash(&bytes).as_bytes())
}

/// One JSON Schema admission rule shared by host task/tool execution and the
/// WASM facade before untrusted model values reach typed executors.
pub(crate) fn validate_json_schema_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
) -> Result<()> {
    validate_json_schema_value_with_io(schema, value, label, None)
}

/// The ordinary and measured paths use the same validator and shared cache.
/// Measured scope covers SDK-observed JSON data and library calls, not the
/// validator's opaque CPU, graph allocations, allocator nodes, or RSS.
pub(crate) fn validate_json_schema_value_with_io(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let compiled = compile_json_schema_engine(schema, label, io)?;
    #[cfg(feature = "filesystem")]
    let _value_scope = json_schema_scope(value, io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        preparation_step(io)?;
    }
    compiled.validator.validate(value).map_err(|error| {
        json_schema_error(format_args!("{label} failed validation: {error}"), io)
    })
}

#[cfg(feature = "filesystem")]
fn preparation_step(io: &crate::filesystem::PreparationIo) -> Result<()> {
    let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
    io.preflight(work)?;
    io.charge(work)
}

// A conservative, scoped logical data allowance around opaque schema calls.
// Serialization visits actual JSON nodes before the allowance is admitted.
#[cfg(feature = "filesystem")]
fn json_schema_scope<'a>(
    value: &serde_json::Value,
    io: Option<&'a crate::filesystem::PreparationIo>,
) -> Result<Option<crate::filesystem::DecodeReservation<'a>>> {
    let Some(io) = io else { return Ok(None); };
    let before = io.receipt().items_examined;
    let _bytes;
    let bytes = io.encode_json(value, io.remaining()?.bytes_encoded, false)?;
    _bytes = io.guard_owned(bytes.len() as u64)?;
    let items = io.receipt().items_examined.checked_sub(before)
        .ok_or_else(|| Error::Invalid("schema data witness counter underflow".into()))?;
    Ok(Some(json_schema_data_scope(io, bytes.len() as u64, items)?))
}

#[cfg(feature = "filesystem")]
fn json_schema_data_scope(
    io: &crate::filesystem::PreparationIo, wire_bytes: u64, items: u64,
) -> Result<crate::filesystem::DecodeReservation<'_>> {
    let slots = std::mem::size_of::<serde_json::Value>()
        .max(std::mem::size_of::<(String, serde_json::Value)>()) as u64;
    let owned = items.checked_mul(slots).and_then(|size| size.checked_mul(8))
        .and_then(|size| wire_bytes.checked_mul(4)
            .and_then(|strings| size.checked_add(strings)))
        .ok_or_else(|| Error::Invalid("schema data scope overflow".into()))?;
    io.reserve_decode(owned)
}

fn json_schema_error(
    message: std::fmt::Arguments<'_>,
    io: Option<&crate::executor::PreparationIo>,
) -> Error {
    match preparation_format(message, io) {
        Ok(message) => Error::Invalid(message),
        Err(error) => error,
    }
}

#[cfg(not(feature = "filesystem"))]
pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    _io: Option<&crate::executor::PreparationIo>,
) -> Result<String> {
    Ok(message.to_string())
}

#[cfg(feature = "filesystem")]
pub(crate) fn preparation_format(
    message: std::fmt::Arguments<'_>,
    io: Option<&crate::filesystem::PreparationIo>,
) -> Result<String> {
    let Some(io) = io else { return Ok(message.to_string()); };
    struct Diagnostic<'a> {
        io: &'a crate::filesystem::PreparationIo,
        text: String,
        held: u64,
        error: Option<Error>,
    }
    impl std::fmt::Write for Diagnostic<'_> {
        fn write_str(&mut self, text: &str) -> std::fmt::Result {
            let result = (|| -> Result<()> {
                let length = self.text.len().checked_add(text.len())
                    .ok_or_else(|| Error::Invalid("schema diagnostic size overflow".into()))?;
                let work = acyclic_fs::WorkCounters {
                    bytes_copied: text.len() as u64, items_examined: 1,
                    ..Default::default()
                };
                self.io.preflight(work)?;
                if length > self.text.capacity() {
                    let growth = length.checked_sub(self.text.capacity())
                        .ok_or_else(|| Error::Invalid("schema diagnostic capacity underflow".into()))?;
                    self.io.claim(growth as u64)?;
                    self.held += growth as u64;
                    self.io.allocation_attempt(growth as u64)?;
                    self.text.try_reserve_exact(length - self.text.len())
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    let extra = self.text.capacity().saturating_sub(length) as u64;
                    self.io.claim(extra)?;
                    self.held += extra;
                }
                self.text.push_str(text);
                self.io.charge(work)
            })();
            if let Err(error) = result { self.error = Some(error); return Err(std::fmt::Error); }
            Ok(())
        }
    }
    let mut diagnostic = Diagnostic { io, text: String::new(), held: 0, error: None };
    if std::fmt::write(&mut diagnostic, message).is_err() {
        let Diagnostic { text, held, error, .. } = diagnostic;
        drop(text);
        let _ = io.release(held);
        return Err(error.unwrap_or_else(|| Error::Invalid("schema diagnostic formatting failed".into())));
    }
    Ok(diagnostic.text)
}

/// Compiles one JSON Schema; `label` names it in the error. Schemas are
/// immutable contracts re-checked on every tool, task, and state boundary, so
/// validators are memoised by content digest instead of being rebuilt per
/// value. The bounded cache is reset when full; failures are never cached.
pub(crate) fn compile_json_schema(
    schema: &serde_json::Value,
    label: &str,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    compile_json_schema_with_io(schema, label, None)
}

// Retain the schema/diagnostic logical allowance until validation ends, even
// for a cache hit. The Arc/cache and opaque validator graph remain unchanged.
struct CompiledJsonSchema<'a> {
    validator: std::sync::Arc<jsonschema::Validator>,
    #[cfg(feature = "filesystem")]
    _scope: Option<crate::filesystem::DecodeReservation<'a>>,
    #[cfg(not(feature = "filesystem"))]
    _scope: std::marker::PhantomData<&'a ()>,
}

pub(crate) fn compile_json_schema_with_io(
    schema: &serde_json::Value,
    label: &str,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    Ok(compile_json_schema_engine(schema, label, io)?.validator)
}

fn compile_json_schema_engine<'a>(
    schema: &serde_json::Value,
    label: &str,
    io: Option<&'a crate::executor::PreparationIo>,
) -> Result<CompiledJsonSchema<'a>> {
    use std::sync::{Arc, Mutex, PoisonError};
    type Cache = std::collections::BTreeMap<[u8; 32], Arc<jsonschema::Validator>>;
    static CACHE: Mutex<Cache> = Mutex::new(Cache::new());
    #[cfg(feature = "filesystem")]
    let before = io.map(|io| io.receipt().items_examined).unwrap_or(0);
    #[cfg(feature = "filesystem")]
    let _encoded;
    let encoded = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => io.encode_json(schema, io.remaining()?.bytes_encoded, false)?,
        _ => serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?,
    };
    #[cfg(feature = "filesystem")]
    _encoded = io.map(|io| io.guard_owned(encoded.len() as u64)).transpose()?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let work = acyclic_fs::WorkCounters { bytes_hashed: encoded.len() as u64, ..Default::default() };
        io.preflight(work)?;
        io.charge(work)?;
    }
    #[cfg(feature = "filesystem")]
    let scope = io.map(|io| {
        let items = io.receipt().items_examined.checked_sub(before)
            .ok_or_else(|| Error::Invalid("schema data witness counter underflow".into()))?;
        json_schema_data_scope(io, encoded.len() as u64, items)
    }).transpose()?;
    #[cfg(not(feature = "filesystem"))]
    let scope = std::marker::PhantomData;
    let key = *blake3::hash(&encoded).as_bytes();
    let cached = CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
        .cloned();
    if let Some(validator) = cached {
        return Ok(CompiledJsonSchema { validator, _scope: scope });
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { preparation_step(io)?; }
    let validator = Arc::new(
        jsonschema::validator_for(schema)
            .map_err(|error| json_schema_error(format_args!("invalid {label} schema: {error}"), io))?,
    );
    let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
    if cache.len() >= 256 {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&validator));
    Ok(CompiledJsonSchema { validator, _scope: scope })
}

/// Returns whether a component label satisfies the shared Rust spelling rule.
pub fn is_valid_component_label(value: &str) -> bool {
    !(value.is_empty()
        || value.len() as u64 > COMPONENT_LABEL_MAX_BYTES as u64
        || value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        || value.contains(COMPONENT_LABEL_FORBIDDEN_SEPARATORS)
        || COMPONENT_LABEL_FORBIDDEN_EXACT.contains(&value))
}

/// One cross-target spelling rule for pinned component and command names.
pub(crate) fn validate_component_label(value: &str, field: &str) -> Result<()> {
    if !is_valid_component_label(value) {
        return Err(Error::Invalid(format!("{field} is invalid")));
    }
    Ok(())
}

macro_rules! uuid_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a fresh identity.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Creates a deterministic identity from bytes.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            /// Returns canonical identity bytes for signed records.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                *self.0.as_bytes()
            }

            /// Parses a canonical UUID.
            pub fn parse(value: &str) -> Result<Self> {
                Uuid::parse_str(value)
                    .map(Self)
                    .map_err(|error| Error::Invalid(error.to_string()))
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

uuid_id!(AgentId, "Identity of one agent definition.");
uuid_id!(ConversationId, "Identity of one durable conversation.");
uuid_id!(SessionId, "Identity of one live conversation session.");
uuid_id!(TurnId, "Identity of one user-driven turn.");
uuid_id!(TaskId, "Identity of one general task.");
uuid_id!(GroupId, "Identity of one stable task group.");
uuid_id!(
    BatchId,
    "Caller-retained identity of one ordered task batch."
);
uuid_id!(
    OperationId,
    "Identity assigned before an operation is admitted."
);
uuid_id!(EffectId, "Identity of one external effect.");
uuid_id!(
    EffectAttemptId,
    "Identity of one dispatch attempt for an effect."
);
uuid_id!(InteractionId, "Identity of one typed open interaction.");

/// Caller-selected identity used to reconcile uncertain admission.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    /// Creates a bounded non-empty caller retry identity.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(Error::Invalid("idempotency key is invalid".into()));
        }
        Ok(Self(value))
    }

    /// Returns the validated string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One admission result per submitted input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Admission<T> {
    /// Work was accepted.
    Accepted(T),
    /// Work was rejected before execution.
    Rejected {
        /// Stable human-readable rejection reason.
        reason: String,
    },
    /// Existing work must be reconciled before retrying.
    Indeterminate {
        /// Operation whose admission must be reconciled.
        operation_id: OperationId,
    },
}

/// Terminal outcome of admitted work.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum Outcome<T> {
    /// Work completed successfully.
    Succeeded(T),
    /// Work failed after admission.
    Failed {
        /// Stable human-readable failure description.
        message: String,
    },
    /// Work was cancelled.
    Cancelled,
    /// Completion is uncertain.
    Indeterminate {
        /// Operation whose completion must be reconciled.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        operation_id: OperationId,
    },
}

/// Stable wire identity used during compatibility handshakes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(all(feature = "wasm", target_arch = "wasm32"), derive(tsify::Tsify))]
pub struct ProtocolIdentity {
    /// Semantic protocol version.
    pub version: String,
    /// Digest of the canonical descriptor set.
    pub descriptor_digest: String,
}

/// Deterministic capability set.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Capabilities(BTreeSet<String>);

impl Capabilities {
    /// Builds a capability set.
    pub fn new(values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(values.into_iter().map(Into::into).collect())
    }

    /// Returns whether a capability is present.
    #[must_use]
    pub fn contains(&self, value: &str) -> bool {
        self.0.contains(value)
    }

    /// Returns whether this set attenuates an ancestor set.
    #[must_use]
    pub fn is_subset_of(&self, ancestor: &Self) -> bool {
        self.0.is_subset(&ancestor.0)
    }

    /// Iterates in canonical lexical order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.0.iter().try_fold(0_u64, |bytes, capability| {
            bytes.checked_add(std::mem::size_of::<String>() as u64)
                .and_then(|bytes| bytes.checked_add(capability.capacity() as u64))
                .ok_or_else(|| Error::Invalid("capability owned byte count overflows".into()))
        })
    }

    /// Returns the deterministic intersection of two grants.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).cloned().collect())
    }

    /// Removes explicit denials from this grant.
    #[must_use]
    pub fn without(&self, denied: &Self) -> Self {
        Self(self.0.difference(&denied.0).cloned().collect())
    }
}

/// One explicit authority policy layer; grants intersect and denials always win.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPolicy {
    /// Capabilities this layer permits.
    pub grants: Capabilities,
    /// Capabilities this layer explicitly denies.
    pub denies: Capabilities,
}

/// Ordered authority-resolution level from the runtime root to one invocation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(all(feature = "wasm", target_arch = "wasm32"), derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum AuthorityLevel {
    /// Runtime-wide policy.
    Runtime,
    /// Agent policy.
    Agent,
    /// Conversation policy.
    Conversation,
    /// Session policy.
    Session,
    /// Turn policy.
    Turn,
    /// Durable task policy.
    Task,
    /// Exact invocation policy.
    Invocation,
}

/// One named layer in an explicit authority chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyLayer {
    /// Hierarchy position.
    pub level: AuthorityLevel,
    /// Grants and denials contributed by this level.
    pub policy: AuthorityPolicy,
}

/// Resolves runtime → invocation policy layers without ambient authority.
#[must_use]
pub fn resolve_policies(layers: &[AuthorityPolicy]) -> Capabilities {
    let Some(first) = layers.first() else {
        return Capabilities::default();
    };
    let granted = layers
        .iter()
        .skip(1)
        .fold(first.grants.clone(), |current, layer| {
            current.intersect(&layer.grants)
        });
    let denied = Capabilities::new(layers.iter().flat_map(|layer| layer.denies.iter()));
    granted.without(&denied)
}

/// Validates hierarchy order and resolves the exact effective grant.
pub fn resolve_policy_layers(layers: &[PolicyLayer]) -> Result<Capabilities> {
    if layers
        .first()
        .is_none_or(|layer| layer.level != AuthorityLevel::Runtime)
        || layers
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.level >= right.level))
    {
        return Err(Error::Invalid(
            "policy layers must start at runtime and be strictly ordered".into(),
        ));
    }
    Ok(resolve_policies(
        &layers
            .iter()
            .map(|layer| layer.policy.clone())
            .collect::<Vec<_>>(),
    ))
}

/// Terminal non-approval outcomes kept distinct for tool callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InteractionRejection {
    /// A participant explicitly refused the action.
    #[error("declined")]
    Declined,
    /// The requester or responder cancelled it.
    #[error("cancelled")]
    Cancelled,
    /// Its response deadline elapsed.
    #[error("expired")]
    Expired,
    /// Interaction policy denied it.
    #[error("denied")]
    Denied,
}

/// Errors shared by harness surfaces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The requested resource does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// An immutable identity or revision precondition failed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A required capability is unavailable.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// The request is malformed.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The caller lacks authority.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// A bound approval resolved without permission to execute.
    #[error("interaction {0}")]
    InteractionRejected(InteractionRejection),
    /// Durable provider operation failed.
    #[error("storage failure: {0}")]
    Storage(String),
    /// An actual SDK buffer reservation failure, without allocating a diagnostic.
    #[error(transparent)]
    Allocation(#[from] std::collections::TryReserveError),
    /// Durable admission may have committed and must be reconciled.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(OperationId),
    /// An owned native failure, preserving its cause and existing allocation custody.
    ///
    /// The cause is dropped before its custody. Allocation-free cancellation and
    /// work-limit causes need no custody; owned diagnostics transfer their
    /// already-admitted guard rather than allocating while reporting failure.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{cause}")]
    NativeWork {
        /// The actual native failure, without string conversion.
        #[source]
        cause: acyclic_fs::NativeTreeMaterializationError,
        /// Existing custody for any SDK-owned diagnostic payload.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// Failure before the original native accounting state could be allocated.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("native accounting construction failed: {cause}")]
    NativeConstruction {
        /// The actual rejection, without constructing an owned diagnostic.
        #[source]
        cause: acyclic_fs::NativeTreeMaterializationError,
        /// The actual constructor work prefix, held inline without a receipt box.
        work: acyclic_fs::WorkCounters,
    },
    /// A fixed native boundary rejection that needs no diagnostic allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{reason}")]
    NativeBoundary {
        /// The existing wire classification of the rejection.
        code: crate::wire::ErrorCode,
        /// A fixed SDK diagnostic, not a formatted payload.
        reason: &'static str,
    },
    /// An already-owned diagnostic with its original classification and custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{message}")]
    NativeDiagnostic {
        /// The classification of the original diagnostic.
        code: crate::wire::ErrorCode,
        /// The original message, moved without formatting or copying.
        message: String,
        /// Existing custody, released after the diagnostic is dropped.
        custody: acyclic_fs::NativeAllocation,
    },
    /// An actual native decode failure, preserving its source and classification.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeDecode {
        /// Invalid for a request; Storage for a persisted record.
        code: crate::wire::ErrorCode,
        /// The original decoder error.
        #[source]
        source: serde_json::Error,
        /// Existing custody for SDK-owned decoder diagnostics.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual Stream failure in a native operation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeStream {
        /// The original provider error, moved without string conversion.
        #[source]
        source: acyclic_stream::StreamError,
        /// Existing SDK payload custody, not opaque provider allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native supervisor failure.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeSupervisor {
        /// The original join error, including its actual panic or cancellation.
        #[source]
        source: tokio::task::JoinError,
        /// Existing SDK payload custody, not opaque Tokio allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual selected lazy-source failure.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeSource {
        /// The original source error.
        #[source]
        source: acyclic_fs::LazyWorkspaceError,
        /// Existing custody for any SDK-owned diagnostic payload.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native workspace failure, moved without diagnostic conversion.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeWorkspace {
        /// The original workspace cause.
        #[source]
        source: acyclic_fs::WorkspaceError,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A native view failure, including any retained physical cleanup owner.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{cause}")]
    NativeView {
        /// The original lifecycle failure and its actual cleanup ownership.
        #[source]
        cause: acyclic_fs::native_mount::MountLifecycleError,
        /// Existing custody for SDK-owned diagnostic payloads.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A publication failure with its actionable destination and completed work.
    ///
    /// The recursive failure slot is admitted and allocated before publication.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{failure}")]
    NativePublication {
        /// The original failure, written into the preallocated slot.
        #[source]
        failure: Box<crate::filesystem::NativeViewPublicationError>,
        /// Custody for the slot, released after its Box is deallocated.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual SDK workspace publication failure, including its original prefix.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeWorkspacePublication {
        /// The original publication cause and any physical cleanup ownership.
        #[source]
        source: acyclic_fs::NativeWorkspacePublicationError,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native process timeout, without a new boxed IO error.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeTimeout {
        /// The original elapsed timeout.
        #[source]
        source: tokio::time::error::Elapsed,
        /// Existing payload custody, not opaque timer allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
}

impl Error {
    /// Stable wire classification from which every adapter derives its status.
    #[must_use]
    pub fn code(&self) -> crate::wire::ErrorCode {
        use crate::wire::ErrorCode;
        match self {
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Conflict(_) => ErrorCode::Conflict,
            Self::Unsupported(_) => ErrorCode::Unsupported,
            Self::Invalid(_) => ErrorCode::Invalid,
            Self::Unauthorized(_) => ErrorCode::Unauthorized,
            Self::InteractionRejected(InteractionRejection::Declined) => {
                ErrorCode::InteractionDeclined
            }
            Self::InteractionRejected(InteractionRejection::Cancelled) => {
                ErrorCode::InteractionCancelled
            }
            Self::InteractionRejected(InteractionRejection::Expired) => {
                ErrorCode::InteractionExpired
            }
            Self::InteractionRejected(InteractionRejection::Denied) => ErrorCode::InteractionDenied,
            Self::Storage(_) | Self::Allocation(_) => ErrorCode::Storage,
            Self::Indeterminate(_) => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeBoundary { code, .. } | Self::NativeDiagnostic { code, .. }
            | Self::NativeDecode { code, .. } => *code,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeConstruction { .. }
            | Self::NativeStream { .. }
            | Self::NativeSupervisor { .. }
            | Self::NativeSource { .. }
            | Self::NativeWorkspace { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            Self::NativeView { .. } | Self::NativePublication { .. }
            | Self::NativeWorkspacePublication { .. }
            | Self::NativeTimeout { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeWork { cause, .. } => {
                use acyclic_fs::NativeTreeMaterializationError as Native;
                match cause {
                    Native::InvalidLayout
                    | Native::InvalidPreimage
                    | Native::InvalidPath(_)
                    | Native::OverlappingPaths
                    | Native::PayloadTooLarge { .. } => ErrorCode::Invalid,
                    Native::MissingTarget(_) | Native::MissingPreimage(_) => ErrorCode::NotFound,
                    Native::ExternalMutation(_)
                    | Native::UnexpectedBackup(_)
                    | Native::AllocationChanged { .. }
                    | Native::PreimageConflict => ErrorCode::Conflict,
                    Native::UnsupportedRename | Native::UnverifiedSourceMetadata(_) => {
                        ErrorCode::Unsupported
                    }
                    // A native failure alone does not authenticate an uncertain
                    // Harness admission. Only Indeterminate(original_operation)
                    // makes that claim.
                    Native::Io(_)
                    | Native::Cancelled
                    | Native::Work(_)
                    | Native::Allocation(_)
                    | Native::AllocationFailed(_)
                    | Native::IndeterminatePreparation => ErrorCode::Storage,
                }
            }
        }
    }

    /// Stable classification for tracing's `error.kind`; never the message,
    /// which can embed paths.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.code().as_str_name()
    }

    /// Native failures must propagate before additional provider or deadline work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn is_native_failure(&self) -> bool {
        match self {
            Self::Allocation(_)
            | Self::NativeWork { .. }
            | Self::NativeConstruction { .. }
            | Self::NativeBoundary { .. }
            | Self::NativeDiagnostic { .. }
            | Self::NativeDecode { .. }
            | Self::NativeStream { .. }
            | Self::NativeSupervisor { .. }
            | Self::NativeSource { .. }
            | Self::NativeWorkspace { .. } => true,
            #[cfg(feature = "native-execution")]
            Self::NativeView { .. } | Self::NativePublication { .. }
            | Self::NativeWorkspacePublication { .. }
            | Self::NativeTimeout { .. } => true,
            _ => false,
        }
    }
}

/// Moves a native cause without formatting it or manufacturing allocation custody.
/// Measured producers with owned diagnostics transfer their existing guard
/// explicitly through `NativeWork` instead of using this conversion.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl From<acyclic_fs::NativeTreeMaterializationError> for Error {
    fn from(cause: acyclic_fs::NativeTreeMaterializationError) -> Self {
        Self::NativeWork { cause, custody: None }
    }
}

/// Moves a lifecycle failure without dropping any retained physical cleanup owner.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::MountLifecycleError> for Error {
    fn from(cause: acyclic_fs::native_mount::MountLifecycleError) -> Self {
        Self::NativeView { cause, custody: None }
    }
}

/// Wraps the actual mount cause in the existing lifecycle error, without allocation.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::NativeMountError> for Error {
    fn from(cause: acyclic_fs::native_mount::NativeMountError) -> Self {
        Self::NativeView {
            cause: acyclic_fs::native_mount::MountLifecycleError::Native(cause),
            custody: None,
        }
    }
}

/// A Stream failure below the harness is a durable-storage failure.
impl From<acyclic_stream::StreamError> for Error {
    fn from(error: acyclic_stream::StreamError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Harness result type.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_sorts_nested_keys_and_preserves_full_width_integers() -> Result<()> {
        let value = serde_json::json!({
            "z": { "later": 2, "earlier": 1 },
            "a": u64::MAX,
        });
        assert_eq!(
            canonical_json_bytes(&value)?,
            br#"{"a":18446744073709551615,"z":{"earlier":1,"later":2}}"#
        );
        let nested = format!("{}0{}", "[".repeat(129), "]".repeat(129));
        let value: serde_json::Value = json_from_slice(nested.as_bytes())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(canonical_json_bytes(&value)?, nested.as_bytes());
        assert!(json_from_slice::<serde_json::Value>(b"{} {}").is_err());
        Ok(())
    }

    #[test]
    fn canonical_json_bytes_are_pinned_for_every_value_kind() -> Result<()> {
        #[derive(Serialize)]
        struct Declared<'a> {
            zeta: Option<u8>,
            alpha: &'a str,
            #[serde(rename = "\u{e9}")]
            accent: (i64, f64, f64, bool),
            mid: std::collections::BTreeMap<&'a str, Vec<u8>>,
        }
        let value = Declared {
            zeta: None,
            alpha: "quote\" back\\ nl\n tab\t ctl\u{1} \u{2028} \u{1f600}",
            accent: (i64::MIN, 1.5e300, -0.0, false),
            mid: [("b", vec![]), ("a", vec![0, 255])].into(),
        };
        let expected = concat!(
            r#"{"alpha":"quote\" back\\ nl\n tab\t ctl\u0001 "#,
            "\u{2028} \u{1f600}\",",
            r#""mid":{"a":[0,255],"b":[]},"zeta":null,"#,
            "\"\u{e9}\":[-9223372036854775808,1.5e+300,-0.0,false]}",
        );
        assert_eq!(canonical_json_bytes(&value)?, expected.as_bytes());
        validate_json_byte_bound(&value, expected.len() as u64)?;
        assert!(validate_json_byte_bound(&value, expected.len() as u64 - 1).is_err());
        Ok(())
    }

    struct Pairs<K>(Vec<(K, u8)>);

    impl<K: Serialize> Serialize for Pairs<K> {
        fn serialize<S: serde::Serializer>(
            &self,
            serializer: S,
        ) -> std::result::Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let mut map = serializer.serialize_map(Some(self.0.len()))?;
            for (key, value) in &self.0 {
                map.serialize_entry(key, value)?;
            }
            map.end()
        }
    }

    struct Failing;

    impl Serialize for Failing {
        fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("canonical fixture failure"))
        }
    }

    #[test]
    fn canonical_json_pins_raw_key_order_and_duplicate_collapse() -> Result<()> {
        let pairs = Pairs(vec![
            ("\u{1f600}", 5),
            ("\u{e9}", 4),
            ("\\", 3),
            ("\"", 2),
            ("\0", 1),
        ]);
        let expected = concat!(
            r#"{"\u0000":1,"\"":2,"\\":3,"#,
            "\"\u{e9}\":4,\"\u{1f600}\":5}"
        );
        assert_eq!(canonical_json_bytes(&pairs)?, expected.as_bytes());
        assert_eq!(
            canonical_json_bytes(&Pairs(vec![("key", 1), ("key", 2)]))?,
            br#"{"key":2}"#
        );
        assert_eq!(
            canonical_json_bytes(&[serde_json::json!({"z": {"b": 2, "a": 1}})])?,
            br#"[{"z":{"a":1,"b":2}}]"#
        );
        Ok(())
    }

    #[test]
    fn canonical_json_preserves_value_conversion_and_rejects_invalid_values() -> Result<()> {
        assert_eq!(
            canonical_json_bytes(&[f64::NAN, f64::INFINITY, f64::NEG_INFINITY])?,
            b"[null,null,null]"
        );
        assert_eq!(canonical_json_bytes(&0.1_f32)?, b"0.10000000149011612");
        assert_eq!(
            canonical_json_bytes(&Pairs(vec![(1.5_f64, 1)]))?,
            br#"{"1.5":1}"#
        );
        for key in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(matches!(
                canonical_json_bytes(&Pairs(vec![(key, 1)])),
                Err(Error::Invalid(_))
            ));
        }
        assert!(matches!(
            canonical_json_bytes(&Pairs(vec![(vec![1_u8], 1)])),
            Err(Error::Invalid(_))
        ));
        assert!(matches!(canonical_json_bytes(&Failing), Err(Error::Invalid(_))));
        assert!(matches!(canonical_json_bytes(&i128::MIN), Err(Error::Invalid(_))));
        assert!(matches!(canonical_json_bytes(&u128::MAX), Err(Error::Invalid(_))));
        assert_eq!(
            canonical_json_bytes(&(i128::from(i64::MIN), u128::from(u64::MAX)))?,
            b"[-9223372036854775808,18446744073709551615]"
        );
        Ok(())
    }

    #[test]
    fn json_byte_bound_stops_serialization_before_remaining_elements() {
        use serde::ser::SerializeSeq as _;
        use std::cell::Cell;

        struct Observed<'a>(&'a Cell<usize>);
        impl Serialize for Observed<'_> {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                let mut sequence = serializer.serialize_seq(Some(1_000))?;
                for _ in 0..1_000 {
                    self.0.set(self.0.get() + 1);
                    sequence.serialize_element(&"x".repeat(64))?;
                }
                sequence.end()
            }
        }

        let visited = Cell::new(0);
        assert!(matches!(
            validate_json_byte_bound(&Observed(&visited), 32),
            Err(Error::Invalid(message)) if message.contains("declared byte bound")
        ));
        assert_eq!(visited.get(), 1);
    }

    #[test]
    fn policy_grants_intersect_and_any_deny_wins() {
        let resolved = resolve_policies(&[
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write", "shell"]),
                denies: Capabilities::new(["shell"]),
            },
            AuthorityPolicy {
                grants: Capabilities::new(["read", "write"]),
                denies: Capabilities::new(["write"]),
            },
        ]);
        assert!(resolved.contains("read"));
        assert!(!resolved.contains("write"));
        assert!(!resolved.contains("shell"));
    }

    #[test]
    fn named_policy_layers_require_canonical_hierarchy_order() {
        let policy = AuthorityPolicy {
            grants: Capabilities::new(["read"]),
            denies: Capabilities::default(),
        };
        assert!(
            resolve_policy_layers(&[
                PolicyLayer {
                    level: AuthorityLevel::Runtime,
                    policy: policy.clone()
                },
                PolicyLayer {
                    level: AuthorityLevel::Invocation,
                    policy
                },
            ])
            .is_ok()
        );
        assert!(
            resolve_policy_layers(&[PolicyLayer {
                level: AuthorityLevel::Agent,
                policy: AuthorityPolicy::default()
            },])
            .is_err()
        );
    }

    #[test]
    fn component_label_validation_is_byte_bounded_and_unicode_precise() {
        assert!(validate_component_label("alpha", "label").is_ok());
        assert!(validate_component_label(&"é".repeat(127), "label").is_ok());
        assert!(validate_component_label(&"😀".repeat(63), "label").is_ok());
        for value in [
            "",
            ".",
            "..",
            "/",
            "\\",
            "a\n",
            "a\u{00a0}",
            "a\u{2003}",
            "a\u{007f}",
        ] {
            assert!(
                validate_component_label(value, "label").is_err(),
                "{value:?}"
            );
        }
        assert!(validate_component_label(&"a".repeat(255), "label").is_ok());
        assert!(validate_component_label(&"a".repeat(256), "label").is_ok());
        assert!(validate_component_label(&"é".repeat(128), "label").is_ok());
    }
}
