use super::*;

fn admit_uuid(value: Uuid) -> Result<Uuid, IdentityError> {
    if value.is_nil() {
        return Err(IdentityError);
    }
    Ok(value)
}

fn deserialize_uuid<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    admit_uuid(Uuid::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

macro_rules! uuid_identity {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        pub struct $name(#[serde(deserialize_with = "deserialize_uuid")] pub(crate) Uuid);

        impl $name {
            /// Generates a fresh identity.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Parses a retained non-nil UUID.
            pub fn parse(value: &str) -> Result<Self, IdentityError> {
                let value = Uuid::parse_str(value).map_err(|_| IdentityError)?;
                admit_uuid(value).map(Self)
            }

            /// Admits the original 16-byte UUID without formatting or allocation.
            pub fn from_bytes(bytes: [u8; 16]) -> Result<Self, IdentityError> {
                admit_uuid(Uuid::from_bytes(bytes)).map(Self)
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

impl MachineId {
    /// Global pair retaining this machine's exact 16-byte UUID wire representation.
    pub fn identity(
        self,
        account: &[u8],
    ) -> Result<crate::addressing::ResourceIdentity, crate::addressing::IdentityError> {
        crate::addressing::ResourceIdentity::new(account, &self.as_bytes())
    }
}

#[cfg(test)]
mod resource_identity_tests {
    #[test]
    fn addressing_retains_machine_wire_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let id = super::MachineId::parse("12345678-1234-4234-9234-123456789abc")?;
        let identity = id.identity(b"account")?;
        assert_eq!(identity.id().as_bytes(), id.as_bytes());
        assert_eq!(identity.account().as_bytes(), b"account");
        let (account, retained) =
            crate::addressing::ResourceIdentity::decode(&identity.encode())?.into_parts();
        let restored = super::MachineId::from_bytes(retained.as_bytes().try_into()?)?;
        assert_eq!(account.into_bytes(), b"account");
        assert_eq!(restored, id);
        assert_ne!(retained.as_bytes(), id.to_string().as_bytes());
        assert!(super::MachineId::from_bytes([0; 16]).is_err());
        let wire = serde_json::to_string(&id)?;
        assert_eq!(serde_json::from_str::<super::MachineId>(&wire)?, id);
        let nil = serde_json::to_string(&uuid::Uuid::nil())?;
        assert!(serde_json::from_str::<super::MachineId>(&nil).is_err());
        assert!(serde_json::from_str::<super::CheckpointId>(&nil).is_err());
        assert!(serde_json::from_str::<super::OperationId>(&nil).is_err());
        assert!(serde_json::from_str::<super::IdempotencyKey>(&nil).is_err());
        Ok(())
    }
}
