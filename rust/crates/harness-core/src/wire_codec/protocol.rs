use super::*;
#[doc(hidden)]
pub const COMPILED_PROTOCOL: (&str, &str) =
    (PROTOCOL_VERSION, crate::build_metadata::DESCRIPTOR_DIGEST);
#[doc(hidden)]
pub fn protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: COMPILED_PROTOCOL.0.into(),
        descriptor_digest: COMPILED_PROTOCOL.1.into(),
    }
}
#[doc(hidden)]
pub fn validate_protocol(
    protocol: Option<&wire::ProtocolIdentity>,
    expected: (&str, &str),
    missing: wire::ErrorCode,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<()> {
    acyclic_protocol::validate_identity(
        protocol.map(|actual| (actual.version.as_str(), actual.descriptor_digest.as_str())),
        expected,
    )
    .map_err(|error| match error {
        acyclic_protocol::Error::MissingIdentity => {
            boundary(work, missing, "protocol identity is missing")
        }
        _ => boundary(
            work,
            wire::ErrorCode::Unsupported,
            "protocol identity mismatch",
        ),
    })
}
