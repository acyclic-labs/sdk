//! Portable guest checkpoint and refusal data.
use crate::valid_environment_name;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// Artifact-defined codec identity. Versions identify byte formats, not code builds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CodecIdentity {
    /// Stable artifact codec name.
    pub name: String,
    /// Artifact-defined format version.
    pub version: String,
}

/// Opaque artifact-owned bytes, tagged with their encoder identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Checkpoint {
    /// Encoder identity.
    pub codec: CodecIdentity,
    /// Encoded logical state. No runtime heap format is implied.
    #[ts(type = "Uint8Array")]
    pub bytes: Bytes,
}

/// Authoring and admission failures.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS, thiserror::Error)]
#[serde(tag = "type", content = "detail", rename_all = "camelCase")]
pub enum GuestError {
    /// A host compositor observed a changed external guard before submission.
    /// No part of its candidate committed; the original delivery is revalidated.
    #[error("composed transaction precondition changed")]
    PreconditionChanged,
    /// Host does not support the artifact-declared ABI.
    #[error("unsupported guest ABI: {0}")]
    UnsupportedAbi(u32),
    /// A recognized guest message carries an absent, old or unknown format version.
    #[error("unsupported guest message format")]
    UnsupportedFormat,
    /// Nested synchronous mutations violate the documented no-nesting lock order.
    #[error("nested guest state mutation")]
    NestedMutation,
    /// Retry keys require a delivery ID and an effect name.
    #[error("missing delivery or effect identity")]
    MissingIdentity,
    /// The candidate does not admit the retained codec.
    #[error("incompatible checkpoint codec")]
    IncompatibleCodec,
    /// Artifact decoder rejected the retained bytes.
    #[error("checkpoint decode failed: {0}")]
    Decode(String),
    /// Artifact encoder failed.
    #[error("checkpoint encode failed: {0}")]
    Encode(String),
    /// Guest handler failed definitively; the host must discard its heap.
    #[error("guest handler failed: {0}")]
    Handler(String),
    /// External work is undecided. Retain its delivery without a failure
    /// transition that could acknowledge or replace the original operation.
    #[error("guest handler pending: {0}")]
    Pending(String),
    /// A capability call named a request whose handler invocation ended:
    /// it returned, timed out, was cancelled or lost its placement epoch.
    #[error("host request is revoked")]
    Revoked,
    /// A manifest binding or host endpoint the handler required failed or is
    /// absent. Definitive for this delivery; the name is never prose.
    #[error("guest dependency {dependency} is unavailable")]
    DependencyUnavailable {
        /// Manifest binding or host endpoint name, in the environment name grammar.
        #[serde(deserialize_with = "dependency_name")]
        dependency: String,
    },
    /// Original local cause, independent of the guest's wire classification.
    #[serde(untagged, skip_deserializing)]
    #[ts(skip)]
    #[error("{}", .0.classification())]
    Caused(
        #[source]
        #[serde(serialize_with = "serialize_guest_cause")]
        acyclic_auth::failure::Failure<Self>,
    ),
}

fn dependency_name<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let name = String::deserialize(deserializer)?;
    if valid_environment_name(&name) {
        Ok(name)
    } else {
        Err(serde::de::Error::custom(
            "guest dependency is not a binding name",
        ))
    }
}

fn serialize_guest_cause<S: serde::Serializer>(
    failure: &acyclic_auth::failure::Failure<GuestError>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    failure.classification().serialize(serializer)
}

impl GuestError {
    /// A required binding or host endpoint failed; `None` unless `dependency`
    /// is a manifest binding or endpoint name, so it can never carry prose.
    #[must_use]
    pub fn dependency_unavailable(dependency: &str) -> Option<Self> {
        valid_environment_name(dependency).then(|| Self::DependencyUnavailable {
            dependency: dependency.to_owned(),
        })
    }

    /// Stable, path-free snake_case name of the variant; a retained local
    /// cause reports its classification's name. Exhaustive, so each new
    /// variant is named where it is declared.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Caused(failure) => failure.classification().kind(),
            Self::PreconditionChanged => "precondition_changed",
            Self::UnsupportedAbi(_) => "unsupported_abi",
            Self::UnsupportedFormat => "unsupported_format",
            Self::NestedMutation => "nested_mutation",
            Self::MissingIdentity => "missing_identity",
            Self::IncompatibleCodec => "incompatible_codec",
            Self::Decode(_) => "decode",
            Self::Encode(_) => "encode",
            Self::Handler(_) => "handler",
            Self::Pending(_) => "pending",
            Self::Revoked => "revoked",
            Self::DependencyUnavailable { .. } => "dependency_unavailable",
        }
    }

    /// Keep the original local failure without changing the wire refusal.
    #[must_use]
    pub fn with_source(self, source: impl acyclic_auth::failure::Source) -> Self {
        Self::Caused(acyclic_auth::failure::Failure::new(self, source))
    }

    /// The original guest refusal, independently of local diagnostic custody.
    #[must_use]
    pub fn category(&self) -> &Self {
        match self {
            Self::Caused(failure) => failure.classification().category(),
            category => category,
        }
    }

    /// Whether the same retained input may become decidable on another attempt.
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self {
            Self::Caused(failure) => failure.classification().retryable(),
            Self::Pending(_) | Self::PreconditionChanged => true,
            Self::UnsupportedAbi(_)
            | Self::UnsupportedFormat
            | Self::NestedMutation
            | Self::MissingIdentity
            | Self::IncompatibleCodec
            | Self::Decode(_)
            | Self::Encode(_)
            | Self::Handler(_)
            | Self::Revoked
            | Self::DependencyUnavailable { .. } => false,
        }
    }

    /// Adds operation context without changing pending versus definitive failure.
    /// Structured guest refusals retain their original identity.
    #[must_use]
    pub fn with_context(self, context: &str) -> Self {
        match self {
            Self::Caused(failure) => {
                Self::Caused(failure.map_classification(|category| category.with_context(context)))
            }
            Self::Pending(message) => Self::Pending(format!("{context}: {message}")),
            Self::Handler(message) => Self::Handler(format!("{context}: {message}")),
            other => other,
        }
    }
}

/// Artifact codec hooks for exactly one checkpoint byte format.
#[derive(TS)]
#[ts(bound = "State: TS")]
pub struct CheckpointCodec<State> {
    /// Identity written on the next checkpoint.
    pub identity: CodecIdentity,
    /// Artifact encoder.
    #[ts(type = "(state: State) => Uint8Array")]
    pub encode: fn(&State) -> Result<Bytes, GuestError>,
    /// Artifact decoder, called only after its exact codec identity is admitted.
    #[ts(type = "(checkpoint: Checkpoint) => State")]
    pub decode: fn(&Checkpoint) -> Result<State, GuestError>,
}
