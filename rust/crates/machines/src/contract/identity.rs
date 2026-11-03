use super::*;

pub(crate) fn admit_uuid(value: Uuid) -> Result<Uuid, IdentityError> {
    if value.is_nil() {
        return Err(IdentityError);
    }
    Ok(value)
}

fn deserialize_uuid<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Uuid, D::Error> {
    admit_uuid(Uuid::deserialize(deserializer)?).map_err(serde::de::Error::custom)
}

fn uuid_from_digest(digest: [u8; 32]) -> Uuid {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid::Builder::from_sha1_bytes(bytes).into_uuid()
}

/// Normalizes a Machines identity, preserving non-nil UUIDs in canonical form.
/// Other nonempty values use the revision-one SHA-256 domain and caller kind.
/// Empty values return an identity error.
pub fn normalize_identity(kind: &str, value: &str) -> Result<String, IdentityError> {
    if let Ok(parsed) = Uuid::parse_str(value)
        && !parsed.is_nil()
    {
        return Ok(parsed.to_string());
    }
    if value.is_empty() {
        return Err(IdentityError);
    }
    let mut digest = Sha256::new();
    digest.update(b"acyclic-machines-identity-v1\0");
    digest.update(kind.as_bytes());
    digest.update([0]);
    digest.update(value.as_bytes());
    Ok(uuid_from_digest(digest.finalize().into()).to_string())
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
                Self(uuid_from_digest(digest))
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

/// Shared oracle for the public byte, text and serde identity admissions.
#[cfg(any(test, feature = "test-support"))]
pub mod identity_codec_fuzz {
    use super::*;

    /// UUID syntax is owned by the UUID library; each public identity must add
    /// exactly the non-nil constraint and preserve all original bits.
    pub fn check(bytes: &[u8]) {
        #[cfg(all(feature = "grpc", feature = "test-support"))]
        crate::grpc::codec_fuzz::check_identities(bytes);
        let expected_bytes = <[u8; 16]>::try_from(bytes)
            .ok()
            .filter(|value| value.iter().any(|byte| *byte != 0));
        let expected_text = std::str::from_utf8(bytes).ok().and_then(|text| {
            Uuid::parse_str(text)
                .ok()
                .filter(|value| !value.is_nil())
                .map(|value| *value.as_bytes())
        });
        let expected_json = serde_json::from_slice::<Uuid>(bytes)
            .ok()
            .filter(|value| !value.is_nil())
            .map(|value| *value.as_bytes());
        macro_rules! check {
            ($($identity:ident),+ $(,)?) => { $(
                let admitted = <[u8; 16]>::try_from(bytes)
                    .ok()
                    .and_then(|raw| $identity::from_bytes(raw).ok());
                assert_eq!(admitted.map($identity::as_bytes), expected_bytes,
                    "public identity byte admission must retain non-nil bits");
                assert_eq!(std::str::from_utf8(bytes).ok()
                    .and_then(|text| $identity::parse(text).ok())
                    .map($identity::as_bytes), expected_text,
                    "public identity text admission must retain non-nil bits");
                assert_eq!(serde_json::from_slice::<$identity>(bytes).ok()
                    .map($identity::as_bytes), expected_json,
                    "public identity serde admission must retain non-nil bits");
                if let Some(id) = admitted {
                    assert_eq!($identity::parse(&id.to_string()), Ok(id));
                    assert_eq!(serde_json::from_slice::<$identity>(
                        &serde_json::to_vec(&id).unwrap()).unwrap(), id);
                }
            )+ };
        }
        check!(MachineId, CheckpointId, OperationId, IdempotencyKey);
    }

    /// Capture the original public identity writers and the UUID library's nil
    /// writer as a refusal input. The four identity types share these formats.
    pub fn seeds() -> Vec<(&'static str, Vec<u8>)> {
        let id = MachineId::from_digest([1; 32]);
        let nil = Uuid::nil();
        vec![
            ("machine-bytes", id.as_bytes().to_vec()),
            ("machine-text", id.to_string().into_bytes()),
            ("machine-json", serde_json::to_vec(&id).unwrap()),
            ("machine-nil-bytes", nil.as_bytes().to_vec()),
            ("machine-nil-text", nil.to_string().into_bytes()),
            ("machine-nil-json", serde_json::to_vec(&nil).unwrap()),
        ]
    }
}

#[cfg(test)]
mod resource_identity_tests {
    use proptest::prelude::*;

    #[test]
    fn public_identity_admission_matches_uuid_syntax_and_non_nil_bits() {
        use super::identity_codec_fuzz::check;
        use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};
        for (_, bytes) in super::identity_codec_fuzz::seeds() {
            check(&bytes);
        }
        let strategy = (
            proptest::collection::vec(any::<u8>(), 0..128),
            any::<[u8; 16]>(),
        );
        TestRunner::new_with_rng(
            Config::with_cases(64),
            TestRng::deterministic_rng(RngAlgorithm::ChaCha),
        )
        .run(&strategy, |(bytes, raw)| {
            check(&bytes);
            check(&raw);
            let original = uuid::Uuid::from_bytes(raw);
            check(original.to_string().as_bytes());
            check(&serde_json::to_vec(&original).unwrap());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn normalization_preserves_revision_one_bytes_and_domains()
    -> Result<(), Box<dyn std::error::Error>> {
        for (kind, value, expected) in [
            (
                "machine",
                "machine-label",
                "f505efb2-cd64-5ea0-9974-9ba4f3406d41",
            ),
            (
                "idempotency",
                "caller-key",
                "17349194-b4fc-57e4-8d9b-9b394f7ef694",
            ),
            (
                "checkpoint",
                "checkpoint-label",
                "35d49ad2-6173-566b-899b-3b9e7ca6dd8f",
            ),
            (
                "operation",
                "operation-label",
                "e244134d-f2b2-5427-966f-fed03a393176",
            ),
            (
                "machine",
                "00000000-0000-0000-0000-000000000000",
                "1e2580b5-4e09-54f6-8a53-c545d68d48f0",
            ),
            ("machine", "λ/東京", "f7b55a84-f196-5927-b8d1-b58c87631e9c"),
            (
                "operation",
                "machine-label",
                "ce6b2acf-3009-5161-80df-3e82cd99b3ea",
            ),
            (
                "machine",
                "{01020304-0506-4708-890A-0B0C0D0E0F10}",
                "01020304-0506-4708-890a-0b0c0d0e0f10",
            ),
        ] {
            assert_eq!(super::normalize_identity(kind, value)?, expected);
        }
        assert!(super::normalize_identity("machine", "").is_err());
        Ok(())
    }

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
        Ok(())
    }
}
