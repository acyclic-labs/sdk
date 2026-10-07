//! Rust-owned nominal wrappers for Stream values crossing the UniFFI facade.
//!
//! The wrappers preserve the existing wire carriers (`String` and `Vec<u8>`)
//! while delegating admission to the canonical `acyclic_stream` constructors.
//! Generated language bindings may expose these as nominal values without
//! copying path, width, or retry-key predicates into each language.

use acyclic_stream::{CommitId as RustCommitId, IdempotencyKey as RustIdempotencyKey};
use acyclic_stream::{StreamError, StreamPath as RustStreamPath};

/// A semantically valid Stream path carried as UTF-8 text at the boundary.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamPath(RustStreamPath);

impl StreamPath {
    pub fn new(value: String) -> Result<Self, StreamError> {
        RustStreamPath::new(value).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<String> for StreamPath {
    type Error = StreamError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StreamPath> for String {
    fn from(value: StreamPath) -> Self {
        value.as_str().to_owned()
    }
}

uniffi::custom_type!(StreamPath, String, {
    lower: |value| value.as_str().to_owned(),
    try_lift: |value| Ok(StreamPath::try_from(value)?),
});

/// A fixed-width committed-envelope identity.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitId(RustCommitId);

impl CommitId {
    pub fn new(value: Vec<u8>) -> Result<Self, StreamError> {
        RustCommitId::try_from_bytes(value).map(Self)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for CommitId {
    type Error = StreamError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<CommitId> for Vec<u8> {
    fn from(value: CommitId) -> Self {
        value.as_bytes().to_vec()
    }
}

uniffi::custom_type!(CommitId, Vec<u8>, {
    lower: |value| value.as_bytes().to_vec(),
    try_lift: |value| Ok(CommitId::try_from(value)?),
});

/// A bounded, non-empty caller retry identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(RustIdempotencyKey);

impl IdempotencyKey {
    pub fn new(value: Vec<u8>) -> Result<Self, StreamError> {
        RustIdempotencyKey::new(value).map(Self)
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for IdempotencyKey {
    type Error = StreamError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<IdempotencyKey> for Vec<u8> {
    fn from(value: IdempotencyKey) -> Self {
        value.as_bytes().to_vec()
    }
}

uniffi::custom_type!(IdempotencyKey, Vec<u8>, {
    lower: |value| value.as_bytes().to_vec(),
    try_lift: |value| Ok(IdempotencyKey::try_from(value)?),
});

/// Carrier kinds exported by the Rust producer metadata.
///
/// This is deliberately structural metadata. It describes the existing wire
/// carrier and lets each maintained generator derive its own safe-copy hint;
/// it is not a second validation implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NominalCarrier {
    String,
    Bytes,
    FixedBytes32,
    U64,
    Bool,
}

/// One Rust-owned nominal declaration consumed by binding generators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NominalMetadata {
    pub rust_name: &'static str,
    pub carrier: NominalCarrier,
}

/// Canonical nominal declarations for the Actors UniFFI producer.
///
/// The generated TOML config is a projection of this table. Binding-specific
/// validator names are derived mechanically from each Rust type name, so the
/// TOML file is not an independently maintained registry.
pub const NOMINAL_METADATA: &[NominalMetadata] = &[
    NominalMetadata {
        rust_name: "ActorId",
        carrier: NominalCarrier::String,
    },
    NominalMetadata {
        rust_name: "CodeSha256",
        carrier: NominalCarrier::FixedBytes32,
    },
    NominalMetadata {
        rust_name: "PositiveU64",
        carrier: NominalCarrier::U64,
    },
    NominalMetadata {
        rust_name: "CurrentHeadMarker",
        carrier: NominalCarrier::Bool,
    },
    NominalMetadata {
        rust_name: "StreamPath",
        carrier: NominalCarrier::String,
    },
    NominalMetadata {
        rust_name: "CommitId",
        carrier: NominalCarrier::FixedBytes32,
    },
    NominalMetadata {
        rust_name: "IdempotencyKey",
        carrier: NominalCarrier::Bytes,
    },
];

fn snake_case(value: &str) -> String {
    value
        .chars()
        .enumerate()
        .flat_map(|(index, character)| {
            let mut output = String::new();
            if character.is_uppercase() && index != 0 {
                output.push('_');
            }
            output.extend(character.to_lowercase());
            output.chars().collect::<Vec<_>>()
        })
        .collect()
}

fn camel_case(value: &str) -> String {
    value
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_default()
        })
        .collect()
}

fn toml_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn carrier_safe_value(carrier: NominalCarrier) -> Option<&'static str> {
    match carrier {
        NominalCarrier::FixedBytes32 => Some("fixed-bytes-32"),
        NominalCarrier::Bytes => Some("bytes"),
        NominalCarrier::String | NominalCarrier::U64 | NominalCarrier::Bool => None,
    }
}

/// Renders the binding config directly from [`NOMINAL_METADATA`].
pub fn nominal_metadata_toml() -> String {
    let mut output = String::from(
        "# GENERATED FILE: derive with `cargo run -p acyclic-actors-uniffi --bin export-nominal-metadata --`\n",
    );
    output.push_str("# Rust source: src/nominal.rs::NOMINAL_METADATA\n\n");
    output.push_str("[bindings.kotlin]\ngenerate_immutable_records = true\n\n");

    for metadata in NOMINAL_METADATA {
        let snake = snake_case(metadata.rust_name);
        let camel = camel_case(&snake);
        output.push_str(&format!(
            "[bindings.kotlin.custom_types.{}]\n",
            metadata.rust_name
        ));
        output.push_str("nominal = true\n");
        output.push_str(&format!(
            "validator = {}\n",
            toml_quote(&format!("validate{camel}"))
        ));
        output.push_str(&format!(
            "imports = [{}]\n\n",
            toml_quote(&format!("uniffi.acyclic_actors_uniffi.validate{camel}"))
        ));
    }

    for metadata in NOMINAL_METADATA {
        let snake = snake_case(metadata.rust_name);
        output.push_str(&format!(
            "[bindings.python.custom_types.{}]\n",
            metadata.rust_name
        ));
        output.push_str("nominal = true\n");
        output.push_str(&format!(
            "validator = {}\n\n",
            toml_quote(&format!("acyclic_actors_uniffi.validate_{snake}"))
        ));
    }

    output.push_str("[bindings.dart]\nreadonly_collections = true\n\n");
    for metadata in NOMINAL_METADATA {
        let snake = snake_case(metadata.rust_name);
        output.push_str(&format!(
            "[bindings.dart.custom_types.{}]\n",
            metadata.rust_name
        ));
        output.push_str("nominal = true\n");
        if let Some(safe_value) = carrier_safe_value(metadata.carrier) {
            output.push_str(&format!("safe_value = {}\n", toml_quote(safe_value)));
        }
        output.push_str(&format!(
            "validator = {}\n",
            toml_quote(&format!("validate_{snake}"))
        ));
        output.push_str("imports = [\"acyclic_actors_uniffi.dart\"]\n\n");
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_path_uses_canonical_constructor() {
        assert!(StreamPath::new("tenant/records".to_owned()).is_ok());
        assert!(StreamPath::new("/tenant/records".to_owned()).is_err());
    }

    #[test]
    fn commit_id_preserves_exact_width() {
        assert!(CommitId::new(vec![0x42; 32]).is_ok());
        assert!(CommitId::new(vec![0x42; 31]).is_err());
    }

    #[test]
    fn idempotency_key_rejects_empty_values() {
        assert!(IdempotencyKey::new(vec![0x42]).is_ok());
        assert!(IdempotencyKey::new(Vec::new()).is_err());
    }

    #[test]
    fn metadata_projection_is_derived_from_the_rust_table() {
        assert_eq!(NOMINAL_METADATA.len(), 7);
        let config = nominal_metadata_toml();
        assert!(config.starts_with("# GENERATED FILE:"));
        assert!(config.contains("custom_types.CurrentHeadMarker"));
        assert!(config.contains("validate_current_head_marker"));
        assert!(config.contains("safe_value = \"fixed-bytes-32\""));
        assert!(!config.contains("\\\\n"));
    }
}
