use super::*;
pub(crate) fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: EVENT_WIRE_VERSION.into(),
        descriptor_digest: env!("HARNESS_DESCRIPTOR_DIGEST").into(),
    }
}
pub(crate) fn validate_protocol(
    protocol: Option<&wire::ProtocolIdentity>,
    expected: (&str, &str),
    missing: wire::ErrorCode,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<()> {
    let actual = protocol.ok_or_else(|| boundary(work, missing, "protocol identity is missing"))?;
    if (actual.version.as_str(), actual.descriptor_digest.as_str()) != expected {
        return Err(boundary(
            work,
            wire::ErrorCode::Unsupported,
            "protocol identity mismatch",
        ));
    }
    Ok(())
}
