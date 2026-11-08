/// Stable public failures. CAS and absence conflicts are values instead.
/// With `serde`, failures use their canonical [`Self::code`] string; unknown
/// codes are refused rather than replaced with another cause.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StreamError {
    /// Peer violated the response contract; retrying the connection cannot repair it.
    #[error("invalid stream response")]
    InvalidResponse,
    /// Invalid path syntax.
    #[error("invalid stream path")]
    InvalidPath,
    /// Invalid operation shape.
    #[error("invalid stream argument")]
    InvalidArgument,
    /// Count or byte ceiling exceeded.
    #[error("stream limit exceeded")]
    LimitExceeded,
    /// Path does not exist.
    #[error("stream not found")]
    NotFound,
    /// Destination already exists.
    #[error("stream already exists")]
    AlreadyExists,
    /// Requested source prefix or conditional publication cut is not retained.
    #[error("stream prefix not retained")]
    PrefixNotRetained,
    /// Requested sequence is beyond the current tail.
    #[error("stream sequence out of range")]
    OutOfRange,
    /// A hierarchy changed between paginated reads; restart from the first page.
    #[error("stream hierarchy changed during pagination")]
    HierarchyChanged,
    /// Retry identity was reused with different arguments.
    #[error("idempotency mismatch")]
    IdempotencyMismatch,
    /// Provider's bounded retained state is exhausted.
    #[error("stream capacity exhausted")]
    Capacity,
    /// Access denied.
    #[error("stream access denied")]
    AccessDenied,
    /// Required authority is unavailable.
    #[error("stream unavailable")]
    Unavailable,
    /// A member's original status beyond its canonical category. Recovery
    /// follows [`Self::category`]; diagnostics never grant retry permission.
    #[error("stream member refused{0}")]
    Refused(acyclic_auth::refusal::Refusal),
    /// A provider-evaluated commit deadline elapsed before linearization.
    #[error("stream commit deadline elapsed")]
    DeadlineElapsed,
    /// The provider cannot supply a required semantic capability.
    #[error("stream capability unsupported")]
    Unsupported,
    /// A minimum revision names another authority scope or incarnation.
    #[error("stream revision belongs to another authority or incarnation")]
    RevisionForeign,
}

#[cfg(feature = "serde")]
impl serde::Serialize for StreamError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.code())
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for StreamError {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FailureCode;
        impl serde::de::Visitor<'_> for FailureCode {
            type Value = StreamError;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a canonical stream failure code")
            }

            fn visit_str<E: serde::de::Error>(self, code: &str) -> Result<Self::Value, E> {
                StreamError::from_code(code).ok_or_else(|| E::custom("unknown stream failure code"))
            }
        }
        deserializer.deserialize_str(FailureCode)
    }
}

impl StreamError {
    /// Every failure, in canonical code order.
    pub const ALL: [Self; 16] = [
        Self::InvalidResponse,
        Self::InvalidPath,
        Self::InvalidArgument,
        Self::LimitExceeded,
        Self::NotFound,
        Self::AlreadyExists,
        Self::PrefixNotRetained,
        Self::OutOfRange,
        Self::IdempotencyMismatch,
        Self::Capacity,
        Self::AccessDenied,
        Self::Unavailable,
        Self::HierarchyChanged,
        Self::DeadlineElapsed,
        Self::Unsupported,
        Self::RevisionForeign,
    ];

    /// Whether the provider left the outcome undecided: it was unreachable or
    /// its deadline elapsed, so the same request may yet succeed unchanged.
    #[must_use]
    pub fn uncertain(&self) -> bool {
        matches!(self.category(), Self::Unavailable | Self::DeadlineElapsed)
    }

    /// The typed failure independently of a member's retained diagnostics.
    #[must_use]
    pub fn category(&self) -> Self {
        match self {
            Self::Refused(refusal) => refusal.original().map_or(Self::Unavailable, |parts| {
                Self::from_grpc_status(parts.code, &parts.message)
            }),
            error => error.clone(),
        }
    }

    pub(crate) fn from_grpc_status(code: i32, message: &str) -> Self {
        match (code, message) {
            (15, _) => Self::InvalidResponse,
            (4, _) => Self::DeadlineElapsed,
            (3, "invalid_path") => Self::InvalidPath,
            (3, "limit_exceeded") => Self::LimitExceeded,
            (3, _) => Self::InvalidArgument,
            (5, _) => Self::NotFound,
            (6, _) => Self::AlreadyExists,
            (11, _) => Self::OutOfRange,
            (7 | 16, _) => Self::AccessDenied,
            (8, _) => Self::Capacity,
            (9, "hierarchy_changed") => Self::HierarchyChanged,
            (9, "idempotency_mismatch") => Self::IdempotencyMismatch,
            (9, "prefix_not_retained") => Self::PrefixNotRetained,
            (9, "deadline_elapsed") => Self::DeadlineElapsed,
            (9, "revision_foreign") => Self::RevisionForeign,
            (12, _) => Self::Unsupported,
            _ => Self::Unavailable,
        }
    }

    /// Stable `snake_case` code shared by the HTTP, gRPC, and WASM transports.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidResponse => "invalid_response",
            Self::InvalidPath => "invalid_path",
            Self::InvalidArgument => "invalid_argument",
            Self::LimitExceeded => "limit_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::PrefixNotRetained => "prefix_not_retained",
            Self::OutOfRange => "out_of_range",
            Self::IdempotencyMismatch => "idempotency_mismatch",
            Self::Capacity => "capacity",
            Self::AccessDenied => "access_denied",
            Self::Unavailable => "unavailable",
            Self::Refused(_) => self.category().code(),
            Self::HierarchyChanged => "hierarchy_changed",
            Self::DeadlineElapsed => "deadline_elapsed",
            Self::Unsupported => "unsupported",
            Self::RevisionForeign => "revision_foreign",
        }
    }

    /// Parses a code produced by [`Self::code`].
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|error| error.code() == code)
    }

    /// Decodes a Session channel's close code: one of [`Self::code`], an
    /// authority rejection, or a gRPC canonical status name mapped as the unary
    /// status mapping maps that status. A refusal such as
    /// `unimplemented` stays a refusal; only an unknown code is unavailability.
    #[must_use]
    pub fn from_session_close(code: &str) -> Self {
        Self::from_code(code).unwrap_or(match code {
            "permission_denied" | "unauthenticated" | "forbidden" | "revoked"
            | "credential_expired" => Self::AccessDenied,
            "filter_invalid" => Self::InvalidArgument,
            "resource_exhausted" => Self::Capacity,
            "unimplemented" => Self::Unsupported,
            _ => Self::Unavailable,
        })
    }
}
