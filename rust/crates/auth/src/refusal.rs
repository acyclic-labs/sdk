//! Original remote refusal diagnostics, shared across product transports.
//!
//! A reason reports why a member refused an unavailable operation. It is not
//! authorization, an ownership hint, proof of non-execution or retry permission.

/// Response metadata key supplied by the member and relayed unchanged.
pub const REASON_HEADER: &str = "acyclic-unserved-reason";

// The variant, wire spelling and diagnostic name have one declaration. The
// public bindings generator consumes ALL rather than maintaining another list.
macro_rules! refusal_reasons {
    ($($variant:ident => $wire:literal),+ $(,)?) => {
        /// A refusal's original wire reason. Future values remain verbatim.
        #[derive(Clone, Debug, Default, PartialEq, Eq)]
        pub enum RefusalReason {
            /// The response did not supply a reason; no cause may be inferred.
            #[default]
            Unspecified,
            $(#[doc = concat!("Original `", stringify!($variant), "` refusal.")]
            $variant,)+
            /// A future reason, retained without normalization or truncation.
            Unknown(std::sync::Arc<[u8]>),
        }

        impl RefusalReason {
            /// Every recognized reason, excluding absent and future values.
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Decode the original response metadata once. Known values allocate
            /// nothing; an unknown value owns exactly its original bytes. Work is
            /// linear in this header's length, independent of requests or members.
            #[must_use]
            pub fn from_header(value: Option<&[u8]>) -> Self {
                match value {
                    None => Self::Unspecified,
                    $(Some($wire) => Self::$variant,)+
                    Some(value) => Self::Unknown(std::sync::Arc::from(value)),
                }
            }

            /// The original response value, or absence when none was supplied.
            #[must_use]
            pub fn as_bytes(&self) -> Option<&[u8]> {
                match self {
                    Self::Unspecified => None,
                    $(Self::$variant => Some($wire),)+
                    Self::Unknown(value) => Some(value),
                }
            }

            /// The stable diagnostic name of a recognized reason. Unknown and
            /// absent values have no inferred class or recovery permission.
            #[must_use]
            pub const fn class(&self) -> Option<&'static str> {
                match self {
                    $(Self::$variant => Some(stringify!($variant)),)+
                    Self::Unspecified | Self::Unknown(_) => None,
                }
            }
        }
    };
}

refusal_reasons! {
    NotLeader => b"not-leader",
    NoLeader => b"no-leader",
    LeadershipMoved => b"leadership-moved",
    ReadBarrier => b"read-barrier",
    NoWriter => b"no-writer",
    OwnerDraining => b"owner-draining",
    KeyringUnbound => b"keyring-unbound",
    AuthorizationUnavailable => b"authorization-unavailable",
    AccountNotReady => b"account-not-ready",
    AccountQuarantined => b"account-quarantined",
    HeapUnavailable => b"heap-unavailable",
    PlacementUnavailable => b"placement-unavailable",
    StreamsUnavailable => b"streams-unavailable",
    ObjectsUnavailable => b"objects-unavailable",
    ExecutorUnavailable => b"executor-unavailable",
}

impl RefusalReason {
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
