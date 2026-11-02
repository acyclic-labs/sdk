//! Compiler-linked wire projection; no transport, caller or storage authority.
use super::*;
use crate::contract::{
    IssuanceDecisionProto, IssuanceRefusal, IssuanceRejectedProto, IssuanceReplyProto,
    IssuanceUnknownProto,
};

pub(super) fn version(mut bytes: &[u8]) -> Result<(), VerifyError> {
    // Admit the leading version before nested claims allocate. Canonical
    // re-encoding below also rejects duplicate, unknown and reordered fields.
    bytes = bytes.strip_prefix(&[8]).ok_or(VerifyError::Version(0))?;
    let value = prost::encoding::decode_varint(&mut bytes).map_err(|_| VerifyError::Malformed)?;
    let value = u32::try_from(value).map_err(|_| VerifyError::Malformed)?;
    if value == 1 {
        Ok(())
    } else {
        Err(VerifyError::Version(value))
    }
}

impl Reply {
    /// Consume the public reply into its canonical v1 envelope. The original
    /// serving transaction must retain this result before disclosure.
    pub fn into_wire(self) -> Vec<u8> {
        let decision = match self.decision {
            Decision::Accepted(certificates) => IssuanceDecisionProto::Accepted(certificates),
            Decision::Refused(reason) => IssuanceDecisionProto::Refused(IssuanceRejectedProto {
                reason: match reason {
                    Refusal::Denied => IssuanceRefusal::Denied,
                    Refusal::Conflict => IssuanceRefusal::Conflict,
                    Refusal::IssuerChanged => IssuanceRefusal::IssuerChanged,
                } as i32,
            }),
            Decision::Unknown => IssuanceDecisionProto::Unknown(IssuanceUnknownProto {}),
        };
        IssuanceReplyProto {
            version: 1,
            request_digest: self.request_digest.to_vec(),
            decision: Some(decision),
        }
        .encode_to_vec()
    }

    /// Admit only the public wire shape. The SAME Client still checks exact
    /// claims, actual signatures, current trust and original Context at use.
    ///
    /// # Errors
    /// Rejects unsupported versions, unknown decisions and noncanonical bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, VerifyError> {
        version(bytes)?;
        let reply = IssuanceReplyProto::decode(bytes).map_err(|_| VerifyError::Malformed)?;
        if reply.encode_to_vec() != bytes {
            return Err(VerifyError::Malformed);
        }
        let decision = match reply.decision.ok_or(VerifyError::Malformed)? {
            IssuanceDecisionProto::Accepted(certificates) => Decision::Accepted(certificates),
            IssuanceDecisionProto::Refused(reason) => {
                Decision::Refused(match IssuanceRefusal::try_from(reason.reason)? {
                    IssuanceRefusal::Denied => Refusal::Denied,
                    IssuanceRefusal::Conflict => Refusal::Conflict,
                    IssuanceRefusal::IssuerChanged => Refusal::IssuerChanged,
                    IssuanceRefusal::Unspecified => return Err(VerifyError::Malformed),
                })
            }
            IssuanceDecisionProto::Unknown(_) => Decision::Unknown,
        };
        Ok(Self {
            request_digest: reply
                .request_digest
                .try_into()
                .map_err(|_| VerifyError::Malformed)?,
            decision,
        })
    }
}
