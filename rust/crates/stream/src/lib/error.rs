/// Stable public failures. CAS and absence conflicts are values instead.
/// With `serde`, failures use their canonical [`Self::code`] string; unknown
/// codes are refused rather than replaced with another cause.
/// With `grpc`, servers must use `tonic::Status::from(error)` (owned or borrowed)
/// for the canonical mapping, which preserves retained member refusal diagnostics.
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
    Refused(#[source] StreamRefusal),
    /// Original local source, retained independently of its public classification.
    #[error("{0}")]
    Caused(#[source] acyclic_auth::failure::Failure<Self>),
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

/// A retained diagnostic paired with the category the protocol declared.
/// Category is independent of the diagnostic's original status message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamRefusal {
    category: &'static str,
    diagnostic: acyclic_auth::refusal::Refusal,
}

impl std::ops::Deref for StreamRefusal {
    type Target = acyclic_auth::refusal::Refusal;

    fn deref(&self) -> &Self::Target {
        &self.diagnostic
    }
}

impl std::fmt::Display for StreamRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.diagnostic.fmt(formatter)
    }
}

impl std::error::Error for StreamRefusal {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        std::error::Error::source(&self.diagnostic)
    }
}

impl<T> From<std::sync::PoisonError<T>> for StreamError {
    fn from(error: std::sync::PoisonError<T>) -> Self {
        drop(error);
        Self::unavailable(acyclic_auth::refusal::Cause::new("stream.lock", "poisoned"))
    }
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

/// Status message of [`StreamError::not_before`].
const NOT_BEFORE: &str = "not_before";

impl StreamError {
    /// Preserve the original concrete error without rendering or changing its category.
    #[must_use]
    pub fn with_source(self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Caused(acyclic_auth::failure::Failure::new(self, source))
    }
    /// Attach original diagnostics without changing this failure's category.
    #[must_use]
    pub fn with_refusal(self, diagnostic: acyclic_auth::refusal::Refusal) -> Self {
        Self::Refused(StreamRefusal {
            category: self.code(),
            diagnostic,
        })
    }
    /// Unavailability carrying the producer failure's rendered diagnostic.
    /// The canonical status/refusal codec retains that message, not the
    /// borrowed producer's concrete Rust error type. Its standard error source
    /// exposes the retained refusal status. This never grants permission to
    /// retry an application failure.
    #[must_use]
    pub fn unavailable_with_cause(cause: &(dyn std::error::Error + 'static)) -> Self {
        acyclic_auth::refusal::Refusal::capture(
            14,
            &cause.to_string(),
            b"",
            &::http::HeaderMap::new(),
            acyclic_auth::refusal::Canonical {
                code: 14,
                message: "stream unavailable",
                details: b"",
                owned: &[],
                cause_is_embedded: false,
            },
            None,
        )
        .map_or(Self::Unavailable, |refusal| {
            Self::Unavailable.with_refusal(refusal)
        })
    }

    /// Retain an already boxed native cause with the same clone and category
    /// guarantees as [`Self::with_source`]. The original concrete source remains
    /// downcastable; it is shared as an Arc, not wrapped in another error.
    #[must_use]
    pub fn with_boxed_source(
        self,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        Self::Caused(acyclic_auth::failure::Failure::from_boxed(self, source))
    }

    /// Unavailability with its original typed attribution. Recovery still follows
    /// the unavailable category; attribution is diagnostic, not retry permission.
    #[must_use]
    pub fn unavailable(cause: acyclic_auth::refusal::Cause) -> Self {
        Self::Unavailable.with_refusal(acyclic_auth::refusal::Refusal::caused(cause))
    }

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

    /// A definitive `OutOfRange` refusal: the authority's commit-time rule
    /// had not yet been reached at `authority_unix_millis`, which it carries.
    #[must_use]
    pub fn not_before(authority_unix_millis: u64) -> Self {
        acyclic_auth::refusal::Refusal::capture(
            11,
            NOT_BEFORE,
            &authority_unix_millis.to_be_bytes(),
            &::http::HeaderMap::new(),
            acyclic_auth::refusal::Canonical {
                code: 11,
                message: "stream sequence out of range",
                details: b"",
                owned: &[],
                cause_is_embedded: false,
            },
            None,
        )
        .map_or(Self::OutOfRange, |refusal| {
            Self::OutOfRange.with_refusal(refusal)
        })
    }

    pub(crate) fn member_refusal(&self) -> Option<&StreamRefusal> {
        let mut error = self;
        loop {
            match error {
                Self::Caused(failure) => error = failure.classification(),
                Self::Refused(refusal) => return Some(refusal),
                _ => return None,
            }
        }
    }
    /// The authority time a [`Self::not_before`] refusal carries.
    #[must_use]
    pub fn not_before_authority(&self) -> Option<u64> {
        let refusal = self.member_refusal()?;
        let parts = refusal.original()?;
        (parts.code == 11 && parts.message == NOT_BEFORE)
            .then(|| {
                parts
                    .details
                    .as_ref()
                    .try_into()
                    .ok()
                    .map(u64::from_be_bytes)
            })
            .flatten()
    }

    /// The typed failure independently of a member's retained diagnostics.
    #[must_use]
    pub fn category(&self) -> Self {
        match self {
            Self::Caused(failure) => failure.classification().category(),
            Self::Refused(refusal) => Self::from_code(refusal.category)
                .expect("retained refusal category comes from StreamError::code"),
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
            Self::Refused(refusal) => refusal.category,
            Self::Caused(failure) => failure.classification().code(),
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
