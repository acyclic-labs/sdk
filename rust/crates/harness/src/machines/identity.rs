use super::*;

pub(super) fn parse_uuid<T>(
    bytes: &[u8],
    parse: impl FnOnce(&str) -> std::result::Result<T, acyclic_machines::IdentityError>,
) -> Result<T> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Invalid("Machines reference key must be 16 bytes".into()))?;
    parse(&uuid::Uuid::from_bytes(bytes).to_string())
        .map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn machines_key(key: &IdempotencyKey) -> Result<MachinesKey> {
    IdempotencyKey::new(key.0.clone())?;
    let mut bytes = *blake3::hash(key.as_str().as_bytes()).as_bytes();
    if bytes[..16] == [0; 16] {
        bytes[0] = 1;
    }
    MachinesKey::parse(
        &uuid::Uuid::from_bytes(
            bytes[..16]
                .try_into()
                .map_err(|_| Error::Invalid("key digest".into()))?,
        )
        .to_string(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))
}

pub(super) fn map_error(error: ProviderError, operation: Option<OperationId>) -> Error {
    match error {
        ProviderError::NotFound(value) => Error::NotFound(value),
        ProviderError::Conflict(value) => Error::Conflict(value),
        ProviderError::Unsupported(value) => Error::Unsupported(value),
        ProviderError::Invalid(value) | ProviderError::Rejected(value) => Error::Invalid(value),
        ProviderError::Indeterminate(_) | ProviderError::Unavailable => operation.map_or_else(
            || Error::Storage("Machines outcome is unknown".into()),
            Error::Indeterminate,
        ),
        ProviderError::OperationIndeterminate(operation) => {
            Error::Indeterminate(OperationId::from_bytes(operation.as_bytes()))
        }
        ProviderError::Failed => Error::Storage("Machines operation failed".into()),
        ProviderError::Cancelled => Error::Storage("Machines operation was cancelled".into()),
    }
}
