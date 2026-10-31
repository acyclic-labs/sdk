//! Secrets contract. Protected values exist only in put requests; the API has
//! no read operation. Authorization is the shared typed resource grant over
//! `secrets:/<scope>[/<name>]` with the catalog actions `secrets/put`,
//! `secrets/list` and `secrets/delete`.
use super::*;
use serde::{Deserialize, Serialize};

/// Declared visibility, or a deletion receipt.
#[acyclic_contract_derive::enumeration(error = Error, unknown = Error::UnknownEnum)]
#[derive(Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum SecretKind {
    /// Never admitted.
    Unspecified = 0,
    /// List may return the value.
    Plain = 1,
    /// List returns metadata only.
    Protected = 2,
    /// Only returned by delete.
    Deleted = 3,
}

/// Create or replace; values must never be included in diagnostics.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_put, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretPutRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
    /// Exact variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Plain or protected; deleted is not a write kind.
    #[wire(tag = 3, enumeration)]
    pub kind: SecretKind,
    /// Write-only for protected rows.
    #[wire(tag = 4, string)]
    pub value: String,
    /// Generation observed before the mutation, including zero for an empty scope.
    #[wire(tag = 5, uint64)]
    pub expected_generation: u64,
}

/// Position in one generation-fenced listing. A page continues only the
/// listing of its own generation; any mutation in between ends that listing.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_cursor, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretListCursor {
    /// Scope generation of the listing being continued.
    #[wire(tag = 1, uint64)]
    pub generation: u64,
    /// Last name already returned; the next page starts strictly after it.
    #[wire(tag = 2, string)]
    pub after: String,
}

/// List one page of one scope, in ascending byte order of name. There is
/// deliberately no account selector or reveal flag.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_list, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretListRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
    /// Null starts a listing at the current generation.
    #[wire(tag = 2, message)]
    #[serde(default)]
    pub cursor: Option<SecretListCursor>,
    /// Chunking bound for this page. Zero lets the provider choose. It never
    /// bounds the listing: every row is reachable through `nextCursor`.
    #[wire(tag = 3, uint32)]
    #[serde(default)]
    pub page_size: u32,
}

/// Generation-fenced deletion.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, post = crate::secrets::validate_delete, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretDeleteRequest {
    /// Exact owner-local scope.
    #[wire(tag = 1, string)]
    pub scope: String,
    /// Exact variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Generation observed before the mutation.
    #[wire(tag = 3, uint64)]
    pub expected_generation: u64,
}

/// Mutation metadata only. It has no value field, even for plain puts.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct SecretMutationReply {
    /// Scope generation after this mutation.
    #[wire(tag = 1, uint64)]
    pub generation: u64,
    /// Mutated variable name.
    #[wire(tag = 2, string)]
    pub name: String,
    /// Immutable version identity: the scope generation of this mutation.
    #[wire(tag = 3, uint64)]
    pub version: u64,
    /// Declared put kind, or deleted.
    #[wire(tag = 4, enumeration)]
    pub kind: SecretKind,
}

/// One declared plain row.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct PlainSecretVariable {
    /// Variable name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Scope generation that wrote this value.
    #[wire(tag = 2, uint64)]
    pub version: u64,
    /// The only response type that can contain a value.
    #[wire(tag = 3, string)]
    pub value: String,
}

/// Protected metadata cannot represent a value.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct ProtectedSecretVariable {
    /// Variable name.
    #[wire(tag = 1, string)]
    pub name: String,
    /// Scope generation that wrote this value.
    #[wire(tag = 2, uint64)]
    pub version: u64,
}

/// Discriminated visibility; protected and plain use different payload types.
#[acyclic_contract_derive::oneof(error = Error, file = AuthFile)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SecretVariableKind {
    /// Explicit plain value.
    #[wire(tag = 1, message)]
    Plain(PlainSecretVariable),
    /// Protected metadata only.
    #[wire(tag = 2, message)]
    Protected(ProtectedSecretVariable),
}

/// List row. JSON is the tagged visibility payload, protobuf a required oneof.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, TS)]
#[ts(type = "SecretVariableKind")]
pub struct SecretVariable {
    /// Exactly one visibility payload.
    #[wire(oneof = "1, 2")]
    pub kind: SecretVariableKind,
}

impl SecretVariable {
    /// Name and version, whichever the visibility.
    pub fn identity(&self) -> (&str, u64) {
        match &self.kind {
            SecretVariableKind::Plain(row) => (&row.name, row.version),
            SecretVariableKind::Protected(row) => (&row.name, row.version),
        }
    }
}

/// One page of one generation; no get, reveal or export operation exists.
#[acyclic_contract_derive::message(error = Error, file = AuthFile, redacted)]
#[derive(Clone, PartialEq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SecretListReply {
    /// Generation of the listed scope; equal to the request cursor's.
    #[wire(tag = 1, uint64)]
    pub generation: u64,
    /// Plain values and protected metadata, in ascending byte order of name.
    #[wire(tag = 2, message)]
    pub variables: Vec<SecretVariable>,
    /// Null when the listing is complete; otherwise the next page's cursor.
    #[wire(tag = 3, message)]
    pub next_cursor: Option<SecretListCursor>,
}

macro_rules! redacted_debug {
    ($($ty:ty),* $(,)?) => {$(
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(concat!(stringify!($ty), "([redacted])"))
            }
        }
    )*};
}
redacted_debug!(
    SecretPutRequest,
    SecretListCursor,
    SecretListRequest,
    SecretDeleteRequest,
    SecretMutationReply,
    PlainSecretVariable,
    ProtectedSecretVariable,
    SecretVariable,
    SecretListReply
);

impl Serialize for SecretVariable {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.kind.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SecretVariable {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        SecretVariableKind::deserialize(deserializer).map(|kind| Self { kind })
    }
}
impl zeroize::Zeroize for SecretPutRequest {
    fn zeroize(&mut self) {
        zeroize::Zeroize::zeroize(&mut self.value);
    }
}
