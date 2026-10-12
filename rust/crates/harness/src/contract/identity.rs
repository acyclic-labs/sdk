use super::*;

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

