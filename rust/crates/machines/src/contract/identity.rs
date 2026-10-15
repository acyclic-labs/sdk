use super::*;

macro_rules! uuid_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        pub struct $name(pub(crate) Uuid);

        impl $name {
            /// Generates a fresh identity.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Parses a retained non-nil UUID.
            pub fn parse(value: &str) -> Result<Self, IdentityError> {
                let value = Uuid::parse_str(value).map_err(|_| IdentityError)?;
                if value.is_nil() {
                    return Err(IdentityError);
                }
                Ok(Self(value))
            }

            /// Derives a deterministic, never-nil identity from a domain-separated SHA-256 digest: its first 16 bytes stamped as a version-5 UUID.
            pub fn from_digest(digest: [u8; 32]) -> Self {
                let mut bytes = [0_u8; 16];
                for (byte, value) in bytes.iter_mut().zip(digest) {
                    *byte = value;
                }
                Self(uuid::Builder::from_sha1_bytes(bytes).into_uuid())
            }

            /// Returns the canonical 16-byte UUID representation.
            pub fn as_bytes(self) -> [u8; 16] {
                *self.0.as_bytes()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

/// Invalid public identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("identity must be a non-nil UUID")]
pub struct IdentityError;

uuid_identity!(
    IdempotencyKey,
    "Caller-retained identity for exactly one mutation intent."
);
uuid_identity!(OperationId, "Address of one admitted operation.");
uuid_identity!(MachineId, "Stable logical machine identity.");
uuid_identity!(CheckpointId, "Stable immutable checkpoint identity.");
