use super::*;

pub(crate) fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: EVENT_WIRE_VERSION.into(),
        descriptor_digest: blake3::hash(crate::FILE_DESCRIPTOR_SET)
            .to_hex()
            .to_string(),
    }
}

pub(crate) fn validate_protocol(
    protocol: Option<&wire::ProtocolIdentity>,
    expected: (&str, &str),
    missing: wire::ErrorCode,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let actual = protocol.ok_or_else(|| boundary(work, missing, "protocol identity is missing"))?;
    if actual.version != expected.0 || actual.descriptor_digest != expected.1 {
        return Err(boundary(
            work,
            wire::ErrorCode::Unsupported,
            "protocol identity mismatch",
        ));
    }
    Ok(())
}
