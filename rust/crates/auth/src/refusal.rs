//! Original remote refusal diagnostics, shared across product transports.
//!
//! A reason reports why a member refused an unavailable operation. It is not
//! authorization, an ownership hint, proof of non-execution or retry permission.

/// Response metadata key supplied by the member and relayed unchanged.
pub const REASON_HEADER: &str = "acyclic-unserved-reason";

/// A refusal's original wire reason. Future values remain verbatim.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum RefusalReason {
    /// The response did not supply a reason; no cause may be inferred.
    #[default]
    Unspecified,
    /// Original `not-leader` refusal.
    NotLeader,
    /// Original `no-leader` refusal.
    NoLeader,
    /// Original `leadership-moved` refusal.
    LeadershipMoved,
    /// Original `read-barrier` refusal.
    ReadBarrier,
    /// Original `no-writer` refusal.
    NoWriter,
    /// Original `owner-draining` refusal.
    OwnerDraining,
    /// Original `keyring-unbound` refusal.
    KeyringUnbound,
    /// Original `authorization-unavailable` refusal.
    AuthorizationUnavailable,
    /// Original `account-not-ready` refusal.
    AccountNotReady,
    /// Original `account-quarantined` refusal.
    AccountQuarantined,
    /// Original `heap-unavailable` refusal.
    HeapUnavailable,
    /// Original `placement-unavailable` refusal.
    PlacementUnavailable,
    /// Original `streams-unavailable` refusal.
    StreamsUnavailable,
    /// Original `objects-unavailable` refusal.
    ObjectsUnavailable,
    /// Original `executor-unavailable` refusal.
    ExecutorUnavailable,
    /// A future reason, retained without normalization or truncation.
    Unknown(std::sync::Arc<[u8]>),
}

impl RefusalReason {
    /// Decode the original response metadata once. Known values allocate
    /// nothing; an unknown value owns exactly its original bytes. Work is linear
    /// in this one header's length, independent of requests or members.
    #[must_use]
    pub fn from_header(value: Option<&[u8]>) -> Self {
        match value {
            None => Self::Unspecified,
            Some(b"not-leader") => Self::NotLeader,
            Some(b"no-leader") => Self::NoLeader,
            Some(b"leadership-moved") => Self::LeadershipMoved,
            Some(b"read-barrier") => Self::ReadBarrier,
            Some(b"no-writer") => Self::NoWriter,
            Some(b"owner-draining") => Self::OwnerDraining,
            Some(b"keyring-unbound") => Self::KeyringUnbound,
            Some(b"authorization-unavailable") => Self::AuthorizationUnavailable,
            Some(b"account-not-ready") => Self::AccountNotReady,
            Some(b"account-quarantined") => Self::AccountQuarantined,
            Some(b"heap-unavailable") => Self::HeapUnavailable,
            Some(b"placement-unavailable") => Self::PlacementUnavailable,
            Some(b"streams-unavailable") => Self::StreamsUnavailable,
            Some(b"objects-unavailable") => Self::ObjectsUnavailable,
            Some(b"executor-unavailable") => Self::ExecutorUnavailable,
            Some(value) => Self::Unknown(std::sync::Arc::from(value)),
        }
    }

    /// The original response value, or absence when none was supplied.
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Unspecified => None,
            Self::NotLeader => Some(b"not-leader"),
            Self::NoLeader => Some(b"no-leader"),
            Self::LeadershipMoved => Some(b"leadership-moved"),
            Self::ReadBarrier => Some(b"read-barrier"),
            Self::NoWriter => Some(b"no-writer"),
            Self::OwnerDraining => Some(b"owner-draining"),
            Self::KeyringUnbound => Some(b"keyring-unbound"),
            Self::AuthorizationUnavailable => Some(b"authorization-unavailable"),
            Self::AccountNotReady => Some(b"account-not-ready"),
            Self::AccountQuarantined => Some(b"account-quarantined"),
            Self::HeapUnavailable => Some(b"heap-unavailable"),
            Self::PlacementUnavailable => Some(b"placement-unavailable"),
            Self::StreamsUnavailable => Some(b"streams-unavailable"),
            Self::ObjectsUnavailable => Some(b"objects-unavailable"),
            Self::ExecutorUnavailable => Some(b"executor-unavailable"),
            Self::Unknown(value) => Some(value),
        }
    }
    /// The original UTF-8 value, when supplied and valid. Non-text metadata
    /// remains available verbatim through `as_bytes`, never confused with absence.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(self.as_bytes()?).ok()
    }

    /// Foreign bindings use raw bytes only when a value cannot be represented as text.
    #[must_use]
    pub fn non_text_bytes(&self) -> Option<&[u8]> {
        self.as_bytes().filter(|_| self.as_str().is_none())
    }
}

impl std::fmt::Display for RefusalReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.as_str(), self.as_bytes()) {
            (Some(value), _) => f.write_str(value),
            (None, Some(bytes)) => write!(f, "non-UTF-8 refusal {bytes:?}"),
            (None, None) => f.write_str("unspecified"),
        }
    }
}
