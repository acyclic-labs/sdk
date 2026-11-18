//! Stable filesystem digests and UUID identities.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Canonical 256-bit digest.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Digest([u8; 32]);

impl Digest {
    /// Digest used only for an empty/genesis predecessor.
    pub const ZERO: Self = Self([0; 32]);

    /// Constructs a digest from canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows canonical bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns canonical bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Digest")
            .field(&hex::encode(self.0))
            .finish()
    }
}

/// Declares a time-ordered filesystem identity with a canonical UUID encoding.
#[macro_export]
macro_rules! uuid_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a fresh time-ordered identity.
            #[must_use]
            pub fn new() -> Self {
                Self($crate::identity::fresh_time_uuid())
            }

            /// Constructs an identity from its canonical 16 bytes.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(Uuid::from_bytes(bytes))
            }

            /// Returns the canonical bytes.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                self.0.into_bytes()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

uuid_identity!(
    AuthorityId,
    "Stable identity of one independently fenced authority stream."
);
uuid_identity!(
    OperationId,
    "Stable idempotency identity for one authority operation."
);
uuid_identity!(
    VolumeId,
    "Stable identity of one independently versioned volume."
);
uuid_identity!(
    CheckoutId,
    "Process-independent identity of one volume checkout."
);
uuid_identity!(MountId, "Stable identity of one mounted-volume binding.");
uuid_identity!(
    WatchId,
    "Process-local identity of one native watcher handle."
);
uuid_identity!(
    FileId,
    "Stable path-independent identity of one file record."
);

/// Creates one time-ordered identity from the active clock and entropy source.
pub fn fresh_time_uuid() -> uuid::Uuid {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if let Some(millis) = acyclic_native_runtime::testing::unix_millis()
            && acyclic_native_runtime::testing::fill_seeded(&mut bytes)
        {
            bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
            bytes[6] = (bytes[6] & 0x0f) | 0x70;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            return uuid::Uuid::from_bytes(bytes);
        }
    }
    uuid::Uuid::now_v7()
}
