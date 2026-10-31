/// Stable public failures. CAS and absence conflicts are values instead.
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
    pub const fn uncertain(&self) -> bool {
        matches!(self, Self::Unavailable | Self::DeadlineElapsed)
    }

    /// Stable `snake_case` code shared by the HTTP, gRPC, and WASM transports.
    #[must_use]
    pub const fn code(&self) -> &'static str {
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
