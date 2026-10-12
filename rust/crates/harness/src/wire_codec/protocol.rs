use super::*;

pub(crate) fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: EVENT_WIRE_VERSION.into(),
        descriptor_digest: blake3::hash(crate::FILE_DESCRIPTOR_SET)
            .to_hex()
            .to_string(),
    }
}

/// Requires the exact `expected` protocol identity; `missing` classifies its absence.
pub(crate) fn validate_protocol(
    protocol: Option<&wire::ProtocolIdentity>,
    expected: &wire::ProtocolIdentity,
    missing: Fault,
) -> Result<()> {
    match protocol {
        None => Err(missing("protocol identity is missing".into())),
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::Unsupported("protocol identity mismatch".into())),
    }
}
