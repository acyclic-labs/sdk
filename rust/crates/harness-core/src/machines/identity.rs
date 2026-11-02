use super::*;

pub(super) fn parse_uuid<T>(
    bytes: &[u8],
    parse: impl FnOnce([u8; 16]) -> std::result::Result<T, acyclic_machines::IdentityError>,
) -> Result<T> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Invalid("Machines reference key must be 16 bytes".into()))?;
    parse(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn machines_key(key: &IdempotencyKey) -> Result<MachinesKey> {
    key.validate()?;
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = *digest
        .as_bytes()
        .first_chunk::<16>()
        .ok_or_else(|| Error::Invalid("key digest".into()))?;
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    MachinesKey::from_bytes(bytes).map_err(|error| Error::Invalid(error.to_string()))
}

/// Keeps the provider cause; only an uncertain mutation with a known operation becomes a receipt.
pub(super) fn map_error(error: ProviderError, operation: Option<OperationId>) -> Error {
    match (error, operation) {
        (ProviderError::OperationIndeterminate(operation), _) => {
            Error::Indeterminate(OperationId::from_bytes(operation.as_bytes()))
        }
        (
            ProviderError::Indeterminate(_)
            | ProviderError::Unavailable
            | ProviderError::CommandIndeterminate(_)
            | ProviderError::FileIndeterminate(_)
            | ProviderError::DeadlineSourceLost {
                machine: Some(_), ..
            },
            Some(operation),
        ) => Error::Indeterminate(operation),
        (cause, _) => Error::Machines(cause),
    }
}
