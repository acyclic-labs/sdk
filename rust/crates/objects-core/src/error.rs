//! The shared Objects category, attribution and status conversion.
use crate::wire;
use acyclic_auth::refusal::{Canonical, Cause, Refusal};
use prost::Message as _;

/// Request metadata carrying the shared minimum revision.
pub const MINIMUM_REVISION: &str = "acyclic-min-revision-bin";
/// Response metadata carrying the shared serving revision.
pub const REVISION: &str = "acyclic-revision-bin";
/// The request metadata keys the server owns and a client must not send.
pub const OWNED: &[&str] = &[REVISION, MINIMUM_REVISION];

/// The category a temporarily unavailable provider reports.
pub fn unavailable() -> Error {
    wire::ErrorCode::Unavailable.into()
}

/// The category a malformed or inadmissible request reports.
pub fn invalid() -> Error {
    wire::ErrorCode::InvalidArgument.into()
}

/// An authenticated range was larger than the caller's payload limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("read requires {observed} bytes, exceeding the requested limit {maximum}")]
pub struct ReadLimitExceeded {
    /// Exact size of the selected range.
    pub observed: u64,
    /// Maximum accepted payload size for the request.
    pub maximum: u64,
}

/// Semantic service failure or the original local operation stop.
///
/// Its `source` is the local stop or, for a remote failure, the server's
/// [`Cause`](Cause).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    /// Public semantic category, suitable for programmatic handling.
    pub code: wire::ErrorCode,
    /// Original local failure, retained independently of the public category.
    pub source: Option<acyclic_auth::failure::Failure<Error>>,
    /// Original local stop, when the caller's operation budget ended. Neither
    /// cancellation nor expiry proves that a remote mutation did not commit.
    pub stopped: Option<acyclic_auth::operation::Stopped>,
    /// What the server said beyond `code`: its cause, and the member's
    /// original status when that said more than `code`'s canonical status.
    /// A diagnostic only; recovery follows `code`.
    pub refusal: Option<Refusal>,
}
impl Error {
    /// Retain a concrete local cause without changing the semantic category or
    /// the refusal encoded on the wire. Clones share the original cause owner.
    #[must_use]
    pub fn with_source(self, source: impl acyclic_auth::failure::Source) -> Self {
        let mut failure = self.clone();
        failure.source = Some(acyclic_auth::failure::Failure::new(self, source));
        failure
    }

    /// Decodes an owned member status through the same canonical detail and
    /// category conversion used by native transports. Original diagnostic
    /// bytes remain owned by the refusal, including unknown fields.
    #[must_use]
    pub fn from_status_parts(parts: acyclic_auth::refusal::Parts) -> Self {
        Self::decode_status(parts.code, &parts.details)
            .with_status_refusal(|canonical, cause| Refusal::capture_owned(parts, canonical, cause))
    }

    /// Captures the canonical refusal this status carries, if any.
    pub fn with_status_refusal(
        mut self,
        capture: impl FnOnce(Canonical<'_>, Option<Cause>) -> Option<Refusal>,
    ) -> Self {
        let detail = wire::ErrorDetail::from(&self).encode_to_vec();
        let canonical = self.canonical_status(&detail);
        let cause = self.refusal.take().and_then(Refusal::into_cause);
        self.refusal = capture(canonical, cause);
        self
    }

    /// Encodes the member's original status, or the canonical status carrying
    /// this failure's semantic code and attribution.
    #[must_use]
    pub fn status_parts(&self) -> acyclic_auth::refusal::Parts {
        let detail = wire::ErrorDetail::from(self).encode_to_vec();
        Refusal::encode_embedded(self.refusal.as_ref(), self.canonical_status(&detail))
    }

    /// Classifies an HTTP failure when the response carries no usable detail.
    #[must_use]
    pub fn from_http_status(status: u16) -> Self {
        match status {
            400 => wire::ErrorCode::InvalidArgument,
            401 | 403 => wire::ErrorCode::AccessDenied,
            404 => wire::ErrorCode::NotFound,
            409 => wire::ErrorCode::Conflict,
            413 | 429 => wire::ErrorCode::QuotaExceeded,
            416 => wire::ErrorCode::RangeNotSatisfiable,
            501 => wire::ErrorCode::Unsupported,
            _ => wire::ErrorCode::Unavailable,
        }
        .into()
    }

    /// Classifies a gRPC status when no usable typed detail is present.
    #[must_use]
    pub fn from_grpc_status_code(code: i32) -> Self {
        match code {
            3 => wire::ErrorCode::InvalidArgument,
            4 => wire::ErrorCode::DeadlineElapsed,
            5 => wire::ErrorCode::NotFound,
            7 | 16 => wire::ErrorCode::AccessDenied,
            8 => wire::ErrorCode::QuotaExceeded,
            11 => wire::ErrorCode::RangeNotSatisfiable,
            12 => wire::ErrorCode::Unsupported,
            15 => wire::ErrorCode::Corrupt,
            _ => wire::ErrorCode::Unavailable,
        }
        .into()
    }

    /// Decodes one gRPC status and its canonical detail bytes.
    pub fn decode_status(code: i32, details: &[u8]) -> Self {
        wire::ErrorDetail::decode(details)
            .ok()
            .and_then(Self::from_detail)
            .unwrap_or_else(|| Self::from_grpc_status_code(code))
    }

    /// Canonical gRPC status code, independent of retained refusal diagnostics.
    #[must_use]
    pub fn status_code(&self) -> i32 {
        match self.code {
            wire::ErrorCode::InvalidArgument
            | wire::ErrorCode::DigestMismatch
            | wire::ErrorCode::Unspecified => 3,
            wire::ErrorCode::NotFound => 5,
            wire::ErrorCode::MissingReference
            | wire::ErrorCode::Conflict
            | wire::ErrorCode::Suspended
            | wire::ErrorCode::RevisionForeign
            | wire::ErrorCode::TerminalFence => 9,
            wire::ErrorCode::DeadlineElapsed => 4,
            wire::ErrorCode::QuotaExceeded => 8,
            wire::ErrorCode::Unsupported => 12,
            wire::ErrorCode::AccessDenied => 7,
            wire::ErrorCode::RangeNotSatisfiable => 11,
            wire::ErrorCode::Corrupt => 15,
            wire::ErrorCode::Unavailable | wire::ErrorCode::OutcomeUnknown => 14,
        }
    }

    pub(crate) fn canonical_status<'a>(&self, details: &'a [u8]) -> Canonical<'a> {
        Canonical {
            code: self.status_code(),
            message: "objects operation failed",
            details,
            owned: OWNED,
            cause_is_embedded: true,
        }
    }

    /// A failure of `code` the server attributes to `cause`.
    #[must_use]
    pub fn caused(code: wire::ErrorCode, cause: Cause) -> Self {
        Self {
            code,
            source: None,
            stopped: None,
            refusal: Some(Refusal::caused(cause)),
        }
    }
    /// The failure a transport detail reports, unless it names no canonical
    /// semantic code.
    #[must_use]
    pub fn from_detail(detail: wire::ErrorDetail) -> Option<Self> {
        let code = wire::ErrorCode::try_from(detail.code).ok()?;
        (code != wire::ErrorCode::Unspecified).then(|| Self {
            code,
            source: None,
            stopped: None,
            refusal: Cause::reported(detail.component, detail.reason, detail.request_id)
                .map(Refusal::caused),
        })
    }
    /// The server's attribution, when it reported one.
    #[must_use]
    pub fn cause(&self) -> Option<&Cause> {
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
        if let Some(source) = &self.source {
            return std::error::Error::source(source);
        }
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
            source: None,
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
            source: None,
            stopped: Some(stopped),
            refusal: None,
        }
    }
}
/// The one detail every transport carries for a failure: its code and cause.
impl From<Error> for wire::ErrorDetail {
    fn from(error: Error) -> Self {
        error_detail(error.code, error.refusal.and_then(Refusal::into_cause))
    }
}

impl From<&Error> for wire::ErrorDetail {
    fn from(error: &Error) -> Self {
        error_detail(error.code, error.cause().cloned())
    }
}

fn error_detail(code: wire::ErrorCode, cause: Option<Cause>) -> wire::ErrorDetail {
    let cause = cause.unwrap_or_default();
    wire::ErrorDetail {
        code: code as i32,
        request_id: cause.request_id,
        component: cause.component,
        reason: cause.reason,
    }
}

/// The guest-runnable TypeScript form of [`Error::from_grpc_status_code`].
/// It is one self-contained function, so a host may inline it verbatim.
#[must_use]
pub fn grpc_status_code_typescript() -> String {
    let fallback = Error::from_grpc_status_code(-1).code as i32;
    let cases: String = (0..=16)
        .map(|status| (status, Error::from_grpc_status_code(status).code as i32))
        .filter(|&(_, code)| code != fallback)
        .map(|(status, code)| format!("    case {status}: return {code};\n"))
        .collect();
    format!(
        "// Generated from acyclic_objects::Error::from_grpc_status_code; do not edit.\n\n\
         /** Classifies a gRPC status without a usable typed detail as its Objects error code. */\n\
         export function objectsGrpcErrorCode(status: number): number {{\n  switch (status) {{\n\
         {cases}    default: return {fallback};\n  }}\n}}\n"
    )
}

#[cfg(test)]
mod typescript_tests {
    use super::{Error, grpc_status_code_typescript};

    /// The rendered guest function answers exactly as the Rust classifier for
    /// every status: each case line is read back, and every other status
    /// takes the rendered default.
    #[test]
    fn rendered_grpc_classification_equals_the_rust_classifier() {
        let rendered = grpc_status_code_typescript();
        let case = |status: i32| {
            rendered
                .lines()
                .find_map(|line| line.trim().strip_prefix(&format!("case {status}: return ")))
                .or_else(|| {
                    rendered
                        .lines()
                        .find_map(|line| line.trim().strip_prefix("default: return "))
                })
                .and_then(|code| code.strip_suffix(';'))
                .and_then(|code| code.parse::<i32>().ok())
        };
        for status in (-1..=17).chain([i32::MIN, i32::MAX]) {
            assert_eq!(
                case(status),
                Some(Error::from_grpc_status_code(status).code as i32),
                "status {status}"
            );
        }
    }
}
