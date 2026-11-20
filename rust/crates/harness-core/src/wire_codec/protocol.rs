use super::*;
#[doc(hidden)]
pub fn validate_protocol(
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
