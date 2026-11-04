//! The shared Objects category, attribution and status conversion.
use crate::wire;
use prost::Message as _;

/// Request metadata carrying the shared minimum revision.
pub const MINIMUM_REVISION: &str = "acyclic-min-revision-bin";
/// Response metadata carrying the shared serving revision.
pub const REVISION: &str = "acyclic-revision-bin";
pub(crate) const OWNED: &[&str] = &[REVISION, MINIMUM_REVISION];

/// Semantic service failure or the original local operation stop.
///
/// Its `source` is the local stop or, for a remote failure, the server's
/// [`Cause`](acyclic_auth::refusal::Cause).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
    /// Original local stop, when the caller's operation budget ended. Neither
    /// cancellation nor expiry proves that a remote mutation did not commit.
    pub stopped: Option<acyclic_auth::operation::Stopped>,
    /// What the server said beyond `code`: its cause, and the member's
    /// original status when that said more than `code`'s canonical status.
    /// A diagnostic only; recovery follows `code`.
    pub refusal: Option<acyclic_auth::refusal::Refusal>,
}
impl Error {
    /// Decodes an owned member status through the same canonical detail and
    /// category conversion used by native transports. Original diagnostic
    /// bytes remain owned by the refusal, including unknown fields.
    #[must_use]
    pub fn from_status_parts(parts: acyclic_auth::refusal::Parts) -> Self {
        let mut error = Self::decode_status(parts.code, &parts.details);
        let detail = wire::ErrorDetail::from(&error).encode_to_vec();
        let canonical = error.canonical_status(&detail);
        let cause = error
            .refusal
            .take()
            .and_then(acyclic_auth::refusal::Refusal::into_cause);
        error.refusal = acyclic_auth::refusal::Refusal::capture_owned(parts, canonical, cause);
        error
    }

    /// Encodes the member's original status, or the canonical status carrying
    /// this failure's semantic code and attribution.
    #[must_use]
    pub fn status_parts(&self) -> acyclic_auth::refusal::Parts {
        let detail = wire::ErrorDetail::from(self).encode_to_vec();
        acyclic_auth::refusal::Refusal::encode(
            self.refusal.as_ref(),
            self.canonical_status(&detail),
        )
    }

    pub(crate) fn decode_status(code: i32, details: &[u8]) -> Self {
        wire::ErrorDetail::decode(details)
            .ok()
            .and_then(Self::from_detail)
            .unwrap_or_else(|| {
                match code {
                    3 => wire::ErrorCode::InvalidArgument,
                    5 => wire::ErrorCode::NotFound,
                    7 | 16 => wire::ErrorCode::AccessDenied,
                    8 => wire::ErrorCode::QuotaExceeded,
                    11 => wire::ErrorCode::RangeNotSatisfiable,
                    12 => wire::ErrorCode::Unsupported,
                    15 => wire::ErrorCode::Corrupt,
                    4 => wire::ErrorCode::DeadlineElapsed,
                    _ => wire::ErrorCode::Unavailable,
                }
                .into()
            })
    }

    pub(crate) fn canonical_status<'a>(
        &self,
        details: &'a [u8],
    ) -> acyclic_auth::refusal::Canonical<'a> {
        let code = match self.code {
            wire::ErrorCode::InvalidArgument
            | wire::ErrorCode::DigestMismatch
            | wire::ErrorCode::Unspecified => 3,
            wire::ErrorCode::NotFound => 5,
            wire::ErrorCode::MissingReference
            | wire::ErrorCode::Conflict
            | wire::ErrorCode::Suspended
            | wire::ErrorCode::RevisionForeign => 9,
            wire::ErrorCode::DeadlineElapsed => 4,
            wire::ErrorCode::QuotaExceeded => 8,
            wire::ErrorCode::Unsupported => 12,
            wire::ErrorCode::AccessDenied => 7,
            wire::ErrorCode::RangeNotSatisfiable => 11,
            wire::ErrorCode::Corrupt => 15,
            wire::ErrorCode::Unavailable | wire::ErrorCode::OutcomeUnknown => 14,
        };
        acyclic_auth::refusal::Canonical {
            code,
            message: "objects operation failed",
            details,
            owned: OWNED,
        }
    }

    /// A failure of `code` the server attributes to `cause`.
    #[must_use]
    pub fn caused(code: wire::ErrorCode, cause: acyclic_auth::refusal::Cause) -> Self {
        Self {
            code,
            stopped: None,
            refusal: Some(acyclic_auth::refusal::Refusal::caused(cause)),
        }
    }
    /// The failure a transport detail reports, unless it names no canonical
    /// semantic code.
    #[must_use]
    pub fn from_detail(detail: wire::ErrorDetail) -> Option<Self> {
        let code = wire::ErrorCode::try_from(detail.code).ok()?;
        (code != wire::ErrorCode::Unspecified).then(|| Self {
            code,
            stopped: None,
            refusal: acyclic_auth::refusal::Cause::reported(
                detail.component,
                detail.reason,
                detail.request_id,
            )
            .map(acyclic_auth::refusal::Refusal::caused),
        })
    }
    /// The server's attribution, when it reported one.
    #[must_use]
    pub fn cause(&self) -> Option<&acyclic_auth::refusal::Cause> {
        self.refusal.as_ref()?.cause()
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Objects operation failed ({:?})", self.code)?;
        match &self.refusal {
            Some(refusal) => write!(formatter, "{refusal}"),
            None => Ok(()),
        }
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.stopped {
            Some(stopped) => Some(stopped),
            None => self
                .cause()
                .map(|cause| cause as &(dyn std::error::Error + 'static)),
        }
    }
}
impl From<wire::ErrorCode> for Error {
    fn from(code: wire::ErrorCode) -> Self {
        Self {
            code,
            stopped: None,
            refusal: None,
        }
    }
}
impl From<acyclic_auth::operation::Stopped> for Error {
    fn from(stopped: acyclic_auth::operation::Stopped) -> Self {
        Self {
            code: match stopped {
                acyclic_auth::operation::Stopped::Deadline => wire::ErrorCode::DeadlineElapsed,
                acyclic_auth::operation::Stopped::Cancelled => wire::ErrorCode::OutcomeUnknown,
            },
            stopped: Some(stopped),
            refusal: None,
        }
    }
}
/// The one detail every transport carries for a failure: its code and cause.
impl From<Error> for wire::ErrorDetail {
    fn from(error: Error) -> Self {
        error_detail(
            error.code,
            error
                .refusal
                .and_then(acyclic_auth::refusal::Refusal::into_cause),
        )
    }
}

impl From<&Error> for wire::ErrorDetail {
    fn from(error: &Error) -> Self {
        error_detail(error.code, error.cause().cloned())
    }
}

fn error_detail(
    code: wire::ErrorCode,
    cause: Option<acyclic_auth::refusal::Cause>,
) -> wire::ErrorDetail {
    let cause = cause.unwrap_or_default();
    wire::ErrorDetail {
        code: code as i32,
        request_id: cause.request_id,
        component: cause.component,
        reason: cause.reason,
    }
}
