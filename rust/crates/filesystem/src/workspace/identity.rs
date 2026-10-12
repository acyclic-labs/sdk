use super::*;

const WORKSPACE_ID_DOMAIN: &[u8] = b"acyclic-fs-workspace-id-v1\0";

/// Stable opaque identity of one named workspace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    /// Restores a stable workspace identity from canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Derives the stable identity under supplied work limits.
    pub(crate) fn derive<W: crate::facade::FsWork>(
        namespace: [u8; 16], name: &WorkspaceName, work: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let performed = crate::WorkCounters {
                bytes_hashed: (WORKSPACE_ID_DOMAIN.len() + 16 + 8 + name.as_str().len()) as u64,
                bytes_copied: 16, ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(performed)?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(WORKSPACE_ID_DOMAIN);
            hasher.update(&namespace);
            hasher.update(
                &u64::try_from(name.as_str().len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            hasher.update(name.as_str().as_bytes());
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            accounting.record_completed(performed)?;
            Ok(Self(bytes))
        })();
        work.finish(result, &accounting)
    }

    /// Returns the stable opaque bytes for persistence or transport.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Returns the volume identity represented by this workspace identity.
    pub const fn volume_id(self) -> VolumeId {
        VolumeId::from_bytes(self.0)
    }
}

/// Stable retry identity for one customer-visible mutation, fork, or join.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey([u8; 16]);

impl IdempotencyKey {
    /// Creates a fresh time-ordered retry identity.
    #[must_use]
    pub fn new() -> Self {
        Self(OperationId::new().into_bytes())
    }

    /// Restores an exact key after an ambiguous outcome.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the stable wire bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn operation_id(self) -> OperationId {
        OperationId::from_bytes(self.0)
    }
}

impl Default for IdempotencyKey {
    fn default() -> Self {
        Self::new()
    }
}

