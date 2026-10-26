use super::*;

/// A selected environment name and credential-store address. Never secret bytes.
#[acyclic_contract_derive::message(error = DomainError, file = ActorsFile, post = SecretSelection::validate_from_proto)]
#[derive(Clone, Debug, Eq, PartialEq, TS)]
#[ts(export_to = "actors/SecretSelection.ts")]
#[ts(rename_all = "camelCase")]
pub struct SecretSelection {
    #[wire(tag = 1)]
    name: String,
    #[wire(tag = 2)]
    reference: String,
}

impl SecretSelection {
    /// Validates a reference selection without resolving or revealing its value.
    pub fn new(name: String, reference: String) -> Result<Self, DomainError> {
        let selection = Self { name, reference };
        selection.validate_from_proto()?;
        Ok(selection)
    }

    /// The explicitly selected environment property.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The host-resolved address, without versions or unwrap credentials.
    pub fn reference(&self) -> &str {
        &self.reference
    }

    /// Canonical bounded reference grammar shared with manifest admission.
    pub fn valid_reference(value: &str) -> bool {
        value.len() <= 256
            && value.strip_prefix("secrets:/").is_some_and(|path| {
                path.split('/').all(|segment| {
                    !segment.is_empty()
                        && segment.len() <= 63
                        && segment
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                })
            })
    }

    fn validate_from_proto(&self) -> Result<(), DomainError> {
        if !valid_environment_name(&self.name) || !Self::valid_reference(&self.reference) {
            return Err(DomainError::Contract(crate::ContractError::InvalidArgument));
        }
        Ok(())
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
