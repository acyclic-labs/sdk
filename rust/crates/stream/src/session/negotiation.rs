use super::{StreamError, VERSION, wire};
use wire::protocol::v1::{
    Capability, CapabilitySet, HandshakeRequest, HandshakeResponse, ProtocolIdentity,
};

fn identity() -> &'static ProtocolIdentity {
    static IDENTITY: std::sync::OnceLock<ProtocolIdentity> = std::sync::OnceLock::new();
    IDENTITY.get_or_init(|| ProtocolIdentity {
        version: VERSION.to_string(),
        descriptor_digest: blake3::hash(crate::FILE_DESCRIPTOR_SET)
            .to_hex()
            .to_string(),
    })
}

fn capabilities() -> CapabilitySet {
    CapabilitySet {
        capabilities: vec![Capability {
            name: "stream.session".into(),
            version: VERSION.to_string(),
        }],
    }
}

/// Bind a session's first request to the compiled schema and session contract.
pub fn protocol_handshake() -> HandshakeRequest {
    HandshakeRequest {
        protocol: Some(identity().clone()),
        required: Some(capabilities()),
    }
}

/// Advertise the compiled schema and supported session contract at admission.
pub fn protocol_welcome() -> HandshakeResponse {
    HandshakeResponse {
        protocol: Some(identity().clone()),
        supported: Some(capabilities()),
    }
}

pub(super) fn admit(request: Option<&HandshakeRequest>) -> Result<(), StreamError> {
    let request = request.ok_or(StreamError::Unsupported)?;
    let supported = capabilities();
    negotiate(
        request.protocol.as_ref(),
        request.required.as_ref(),
        &supported,
    )
}

pub(super) fn confirm(response: Option<&HandshakeResponse>) -> Result<(), StreamError> {
    let response = response.ok_or(StreamError::Unsupported)?;
    let supported = response
        .supported
        .as_ref()
        .ok_or(StreamError::Unsupported)?;
    negotiate(response.protocol.as_ref(), Some(&capabilities()), supported)
}

fn negotiate(
    actual: Option<&ProtocolIdentity>,
    required: Option<&CapabilitySet>,
    supported: &CapabilitySet,
) -> Result<(), StreamError> {
    let expected = identity();
    acyclic_protocol::negotiate(
        actual.map(|actual| (actual.version.as_str(), actual.descriptor_digest.as_str())),
        (&expected.version, &expected.descriptor_digest),
        required
            .into_iter()
            .flat_map(|required| &required.capabilities)
            .map(|capability| (capability.name.as_str(), capability.version.as_str())),
        supported
            .capabilities
            .iter()
            .map(|capability| (capability.name.as_str(), capability.version.as_str())),
    )
    .map_err(|_| StreamError::Unsupported)
}
