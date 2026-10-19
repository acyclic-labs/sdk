use super::*;
pub(crate) const COMPILED_PROTOCOL: (&str, &str) =
    (EVENT_WIRE_VERSION, crate::build_metadata::DESCRIPTOR_DIGEST);
pub(crate) fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: COMPILED_PROTOCOL.0.into(),
        descriptor_digest: COMPILED_PROTOCOL.1.into(),
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
