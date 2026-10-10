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

/// One sorted-key JSON encoding for durable identities and Rust/WASM output.
/// Conversion through Value preserves full-width serde integer values while
/// avoiding struct declaration order as an accidental wire contract.
pub(crate) fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut value =
        serde_json::to_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    value.sort_all_objects();
    serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Checks compact JSON size without retaining an encoded buffer or cloning a
/// Value tree. Key ordering does not change the size of ordinary typed records.
/// Canonical encoding and identity checks still use `canonical_json_bytes`.
pub(crate) fn validate_json_byte_bound<T: Serialize>(value: &T, maximum: u64) -> Result<()> {
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

    serde_json::to_writer(Budget(maximum), value).map_err(|error| Error::Invalid(error.to_string()))
}

/// Decodes a complete JSON value without an unrelated nesting policy ceiling.
pub(crate) fn json_from_slice<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> serde_json::Result<T> {
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

/// One JSON Schema admission rule shared by host task/tool execution and the
/// WASM facade before untrusted model values reach typed executors.
pub(crate) fn validate_json_schema_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    label: &str,
) -> Result<()> {
    compile_json_schema(schema, label)?
        .validate(value)
        .map_err(|error| Error::Invalid(format!("{label} failed validation: {error}")))
}

/// Compiles one JSON Schema; `label` names it in the error. Schemas are
/// immutable contracts re-checked on every tool, task, and state boundary, so
/// validators are memoised by content digest instead of being rebuilt per
/// value. The bounded cache is reset when full; failures are never cached.
pub(crate) fn compile_json_schema(
    schema: &serde_json::Value,
    label: &str,
) -> Result<std::sync::Arc<jsonschema::Validator>> {
    use std::sync::{Arc, Mutex, PoisonError};
    type Cache = std::collections::BTreeMap<[u8; 32], Arc<jsonschema::Validator>>;
    static CACHE: Mutex<Cache> = Mutex::new(Cache::new());
    let key = *blake3::hash(
        &serde_json::to_vec(schema).map_err(|error| Error::Invalid(error.to_string()))?,
    )
    .as_bytes();
    let cached = CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&key)
        .cloned();
    if let Some(validator) = cached {
        return Ok(validator);
    }
    let validator = Arc::new(
        jsonschema::validator_for(schema)
            .map_err(|error| Error::Invalid(format!("invalid {label} schema: {error}")))?,
    );
    let mut cache = CACHE.lock().unwrap_or_else(PoisonError::into_inner);
    if cache.len() >= 256 {
        cache.clear();
    }
    cache.insert(key, Arc::clone(&validator));
    Ok(validator)
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
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
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
    /// Durable admission may have committed and must be reconciled.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(OperationId),
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
            Self::Storage(_) => ErrorCode::Storage,
            Self::Indeterminate(_) => ErrorCode::Indeterminate,
        }
    }

    /// Stable classification for tracing's `error.kind`; never the message,
    /// which can embed paths.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.code().as_str_name()
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
    fn canonical_json_preserves_value_conversion_and_invalid_messages() -> Result<()> {
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
            assert_eq!(
                canonical_json_bytes(&Pairs(vec![(key, 1)])),
                Err(Error::Invalid(
                    "float key must be finite (got NaN or +/-inf)".into()
                ))
            );
        }
        assert_eq!(
            canonical_json_bytes(&Pairs(vec![(vec![1_u8], 1)])),
            Err(Error::Invalid("key must be a string".into()))
        );
        assert_eq!(
            canonical_json_bytes(&Failing),
            Err(Error::Invalid("canonical fixture failure".into()))
        );
        assert_eq!(
            canonical_json_bytes(&i128::MIN),
            Err(Error::Invalid("number out of range".into()))
        );
        assert_eq!(
            canonical_json_bytes(&u128::MAX),
            Err(Error::Invalid("number out of range".into()))
        );
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
