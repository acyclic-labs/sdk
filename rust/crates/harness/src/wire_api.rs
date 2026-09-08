//! Transport-neutral server port implemented identically by every wire adapter.

use crate::{
    Error, Result, wire,
    wire_codec::{protocol_identity, validate_protocol},
};
use futures::{future::BoxFuture, stream::BoxStream};
use std::collections::BTreeMap;

/// Complete application-facing wire API. Adapters own framing, never semantics.
pub trait HarnessWireApi: Send + Sync + 'static {
    /// Negotiates the exact protocol and required capabilities before other calls.
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> BoxFuture<'a, Result<wire::HandshakeResponse>>;

    /// Admits one identity-bound command.
    fn submit<'a>(
        &'a self,
        command: wire::CommandEnvelope,
    ) -> BoxFuture<'a, Result<wire::Admission>>;

    /// Replays validated gapless deliveries and then follows live events.
    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> BoxFuture<'a, Result<BoxStream<'static, Result<wire::Delivery>>>>;
}

/// Returns the protocol identity compiled into this crate.
#[must_use]
pub fn current_protocol() -> wire::ProtocolIdentity {
    protocol_identity()
}

/// Validates a handshake and constructs the canonical response.
pub fn negotiate(
    request: &wire::HandshakeRequest,
    supported: &wire::CapabilitySet,
) -> Result<wire::HandshakeResponse> {
    let expected = current_protocol();
    let actual = request
        .protocol
        .as_ref()
        .ok_or_else(|| Error::Invalid("handshake protocol is missing".into()))?;
    if actual != &expected {
        return Err(Error::Unsupported("protocol identity mismatch".into()));
    }
    let available = supported
        .capabilities
        .iter()
        .map(|capability| ((capability.name.as_str(), capability.version.as_str()), ()))
        .collect::<BTreeMap<_, _>>();
    for required in request
        .required
        .as_ref()
        .map_or(&[][..], |set| set.capabilities.as_slice())
    {
        if required.name.is_empty()
            || required.version.is_empty()
            || !available.contains_key(&(required.name.as_str(), required.version.as_str()))
        {
            return Err(Error::Unsupported(format!(
                "unsupported capability {}@{}",
                required.name, required.version
            )));
        }
    }
    Ok(wire::HandshakeResponse {
        protocol: Some(expected),
        supported: Some(supported.clone()),
    })
}

/// Rejects malformed or non-identity-preserving admission results.
pub fn validate_admission(
    command: &wire::CommandEnvelope,
    admission: &wire::Admission,
) -> Result<()> {
    let expected = command
        .operation
        .as_ref()
        .ok_or_else(|| Error::Invalid("command operation identity is missing".into()))?;
    let actual = admission
        .operation
        .as_ref()
        .ok_or_else(|| Error::Conflict("admission operation identity is missing".into()))?;
    if expected.operation_id.is_empty() || expected.idempotency_key.is_empty() || actual != expected
    {
        return Err(Error::Conflict("admission identity mismatch".into()));
    }
    Ok(())
}

/// Requires every stateless command to carry the exact negotiated wire identity.
pub fn validate_command_protocol(command: &wire::CommandEnvelope) -> Result<()> {
    validate_protocol(command.protocol.as_ref())
}

/// Requires every replay request to carry the exact negotiated wire identity.
pub fn validate_resume_protocol(request: &wire::ResumeRequest) -> Result<()> {
    validate_protocol(request.protocol.as_ref())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiation_is_exact_and_capability_checked() -> Result<()> {
        let supported = wire::CapabilitySet {
            capabilities: vec![wire::Capability {
                name: "replay".into(),
                version: "1".into(),
            }],
        };
        let request = wire::HandshakeRequest {
            protocol: Some(current_protocol()),
            required: Some(supported.clone()),
        };
        assert_eq!(negotiate(&request, &supported)?.supported, Some(supported));
        let mut incompatible = request;
        incompatible
            .protocol
            .as_mut()
            .ok_or_else(|| Error::Invalid("protocol".into()))?
            .version = "2".into();
        assert!(matches!(
            negotiate(&incompatible, &wire::CapabilitySet::default()),
            Err(Error::Unsupported(_))
        ));
        Ok(())
    }

    #[test]
    fn stateless_calls_require_the_exact_protocol() {
        let mut command = wire::CommandEnvelope::default();
        let mut resume = wire::ResumeRequest::default();
        assert!(validate_command_protocol(&command).is_err());
        assert!(validate_resume_protocol(&resume).is_err());
        command.protocol = Some(current_protocol());
        resume.protocol = Some(current_protocol());
        assert!(validate_command_protocol(&command).is_ok());
        assert!(validate_resume_protocol(&resume).is_ok());
    }
}
