use super::*;

/// Validated nonzero immutable image digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "[u8; 32]", into = "[u8; 32]")]
pub struct ImageDigest([u8; 32]);

impl ImageDigest {
    /// Constructs a checked SHA-256 digest.
    pub fn new(value: [u8; 32]) -> Result<Self, ProviderError> {
        if value == [0; 32] {
            return Err(ProviderError::Invalid("image digest cannot be zero".into()));
        }
        Ok(Self(value))
    }

    /// Returns the exact digest bytes.
    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl TryFrom<[u8; 32]> for ImageDigest {
    type Error = ProviderError;

    fn try_from(value: [u8; 32]) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ImageDigest> for [u8; 32] {
    fn from(value: ImageDigest) -> Self {
        value.0
    }
}

/// Canonical immutable OCI digest separator.
pub const MANAGED_OCI_REFERENCE_SEPARATOR: &str = "@sha256:";
/// Canonical hexadecimal character count for a SHA-256 digest.
pub const MANAGED_OCI_DIGEST_HEX_LENGTH: usize = 64;
/// Hexadecimal characters accepted by the canonical OCI parser.
pub const MANAGED_OCI_HEX_DIGITS: &str = "0123456789abcdefABCDEF";
/// Character used to identify the all-zero digest rejected by the parser.
pub const MANAGED_OCI_ZERO_DIGIT: char = '0';

/// Immutable machine image selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Image {
    /// Digest parsed from an immutable OCI reference.
    ManagedOci(ImageDigest),
    /// Customer image content digest.
    Custom(ImageDigest),
    /// Existing immutable checkpoint.
    Checkpoint(CheckpointId),
}

impl Image {
    /// Creates a managed OCI image reference after checking immutable digest syntax.
    pub fn oci(reference: impl Into<String>) -> Result<Self, ProviderError> {
        let reference = reference.into();
        let Some((name, digest)) = reference.rsplit_once(MANAGED_OCI_REFERENCE_SEPARATOR) else {
            return Err(ProviderError::Invalid(
                "OCI image must contain @sha256:<digest>".into(),
            ));
        };
        if name.is_empty() || digest.len() != MANAGED_OCI_DIGEST_HEX_LENGTH {
            return Err(ProviderError::Invalid("OCI image digest is invalid".into()));
        }
        let bytes = hex::decode(digest)
            .map_err(|_| ProviderError::Invalid("OCI image digest is invalid".into()))?;
        let digest: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ProviderError::Invalid("OCI image digest is invalid".into()))?;
        ImageDigest::new(digest).map(Self::ManagedOci)
    }

    /// Creates a managed image from an already resolved digest.
    pub fn managed(digest: [u8; 32]) -> Result<Self, ProviderError> {
        ImageDigest::new(digest).map(Self::ManagedOci)
    }

    /// Creates a custom image from its content digest.
    pub fn custom(digest: [u8; 32]) -> Result<Self, ProviderError> {
        ImageDigest::new(digest).map(Self::Custom)
    }
}
