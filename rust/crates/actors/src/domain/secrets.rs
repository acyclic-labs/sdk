use super::*;
// The derive names a message field's wire type by appending `Proto`.
use acyclic_auth::secrets::{SecretRef, SecretRefProto};

/// One environment property bound to the canonical [`SecretRef`]. Never secret
/// bytes: the host resolves the reference's exact version when the guest uses it.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SecretSelection::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SecretSelection.ts")]
#[ts(rename_all = "camelCase")]
pub struct SecretSelection {
    #[wire(tag = 1)]
    name: String,
    // Tag 2 carried the retired unversioned address; it is never reused.
    #[wire(tag = 3, message, import = "harness/v2/resources.proto")]
    #[ts(type = "{ scope: string; name: string; version: bigint }")]
    reference: SecretRef,
}

impl SecretSelection {
    /// Binds `name` to `reference` without resolving or revealing its value.
    pub fn new(name: String, reference: SecretRef) -> Result<Self, DomainError> {
        let selection = Self { name, reference };
        selection.validate_from_proto()?;
        Ok(selection)
    }

    /// The explicitly selected environment property.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The canonical reference, with its immutable version.
    pub fn reference(&self) -> &SecretRef {
        &self.reference
    }

    fn validate_from_proto(&self) -> Result<(), DomainError> {
        if !valid_environment_name(&self.name) {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
    }
}

impl From<acyclic_auth::secrets::Error> for DomainError {
    fn from(_: acyclic_auth::secrets::Error) -> Self {
        Self::Contract(crate::ContractError::InvalidArgument)
    }
}

/// Canonical environment property grammar for bindings and secret selections.
pub fn valid_environment_name(value: &str) -> bool {
    value.len() <= 63
        && value
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
