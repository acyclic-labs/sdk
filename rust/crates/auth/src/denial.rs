//! Final authorization verdicts shared by product and transport projections.

/// Why an authenticated operation was refused. Every variant is final for the
/// supplied credential and request. Loss of trust, a source revision, a clock or
/// a serving member is availability failure, never a denial cause.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DenialCause {
    /// The effective credential does not cover the requested action or resource.
    #[error("not_covered")]
    NotCovered,
    /// An authenticated key, epoch or capability ancestor was revoked.
    #[error("revoked")]
    Revoked,
    /// The credential is outside its authenticated validity interval.
    #[error("expired")]
    Expired,
    /// The presenting account does not match the credential's holder binding.
    #[error("wrong_account")]
    WrongAccount,
    /// The recipient is outside the credential's audience.
    #[error("wrong_audience")]
    WrongAudience,
    /// The owning resource's current policy denied the request.
    #[error("owner_policy")]
    OwnerPolicy,
    /// Credential or request authority cannot be admitted canonically.
    #[error("malformed")]
    Malformed,
    /// The authenticated caller lacks the required serving admission.
    #[error("caller_not_admitted")]
    CallerNotAdmitted,
    /// The credential's authenticated usage ceiling does not cover the request.
    #[error("quota_denied")]
    QuotaDenied,
}

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl From<DenialCause> for tonic::Status {
    fn from(cause: DenialCause) -> Self {
        match cause {
            DenialCause::Expired
            | DenialCause::Revoked
            | DenialCause::Malformed
            | DenialCause::WrongAccount
            | DenialCause::WrongAudience => Self::unauthenticated(cause.to_string()),
            DenialCause::NotCovered
            | DenialCause::OwnerPolicy
            | DenialCause::CallerNotAdmitted
            | DenialCause::QuotaDenied => Self::permission_denied(cause.to_string()),
        }
    }
}

impl crate::verify::VerifyError {
    /// The final denial, if this error is a verdict on the supplied authority.
    /// A refused trust record fences its trust view; it is not a verdict on an
    /// otherwise unrelated credential. Availability errors retain no denial.
    #[must_use]
    pub fn denial_cause(self) -> Option<DenialCause> {
        use crate::verify::VerifyError;
        match self {
            VerifyError::Trust | VerifyError::Unavailable | VerifyError::Refused => None,
            VerifyError::Revoked => Some(DenialCause::Revoked),
            VerifyError::Time => Some(DenialCause::Expired),
            VerifyError::Malformed
            | VerifyError::Version(_)
            | VerifyError::Signature
            | VerifyError::RepeatedKey => Some(DenialCause::Malformed),
            VerifyError::Sealed => Some(DenialCause::NotCovered),
            VerifyError::Denied(cause) => Some(cause),
        }
    }
}
