use super::*;

/// Canonical immutable customer workspace name.
#[derive(Clone)]
pub struct WorkspaceName(Arc<(String, KernelOwnedAllocation)>);

impl WorkspaceName {
    /// Canonicalizes a name under the supplied work limits.
    pub fn new<W: crate::facade::FsWork>(
        name: impl AsRef<str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = Self::with_accounting(name.as_ref(), &mut accounting);
        work.finish(result, &accounting)
    }

    pub(crate) fn with_accounting(
        name: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        accounting.check()?;
        let (mut pending, mut maximum_pending, mut maximum_chunk) = (0_u64, 0_u64, 0_u64);
        for character in name.chars() {
            accounting.charge(item_work(1))?;
            let mut chunk = 0_u64;
            let mut failure = None;
            unicode_normalization::char::decompose_canonical(character, |scalar| {
                if failure.is_some() {
                    return;
                }
                failure = (|| -> Result<(), WorkspaceError> {
                    accounting.charge(item_work(1))?;
                    chunk = chunk.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                    if unicode_normalization::char::canonical_combining_class(scalar) == 0 {
                        pending = 0;
                    } else {
                        pending = pending.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                        maximum_pending = maximum_pending.max(pending);
                    }
                    Ok(())
                })()
                .err();
            });
            if let Some(error) = failure {
                return Err(error);
            }
            maximum_chunk = maximum_chunk.max(chunk);
        }
        // NFC's two TinyVec buffers and combining-mark sort scratch are admitted before iteration.
        let buffer = |count: u64, width: usize| -> Result<u64, WorkspaceError> {
            if count <= 4 {
                return Ok(0);
            }
            count
                .checked_next_power_of_two()
                .and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_mul(width as u64))
                .ok_or(crate::WorkError::Overflow.into())
        };
        let scratch = buffer(
            maximum_pending
                .checked_add(maximum_chunk)
                .ok_or(crate::WorkError::Overflow)?,
            std::mem::size_of::<(u8, char)>(),
        )?
        .checked_add(buffer(maximum_pending, std::mem::size_of::<char>())?)
        .and_then(|n| {
            maximum_pending
                .checked_mul(std::mem::size_of::<(u8, char)>() as u64)
                .and_then(|sort| n.checked_add(sort))
        })
        .ok_or(crate::WorkError::Overflow)?;
        let live = match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
        };
        accounting.admit(crate::WorkCounters {
            peak_allocation_bytes: live
                .checked_add(scratch)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let mut output = [0_u8; MAXIMUM_WORKSPACE_NAME_BYTES];
        let mut length = 0;
        let mut normalized = name.nfc();
        loop {
            accounting.charge(item_work(1))?;
            let Some(character) = normalized.next() else {
                break;
            };
            let end = length + character.len_utf8();
            if end > output.len() {
                return Err(WorkspaceNameError::TooLong.into());
            }
            let copied = copy_work(character.len_utf8() as u64);
            accounting.admit(copied)?;
            character.encode_utf8(&mut output[length..end]);
            length = end;
            accounting.record_completed(copied)?;
        }
        drop(normalized);
        accounting.charge(item_work(length as u64))?;
        let canonical = std::str::from_utf8(&output[..length]).map_err(WorkspaceError::engine)?;
        if canonical.is_empty() {
            return Err(WorkspaceNameError::Empty.into());
        }
        if canonical == "." || canonical == ".." {
            return Err(WorkspaceNameError::Reserved.into());
        }
        for character in canonical.chars() {
            accounting.charge(item_work(1))?;
            if character.is_control() || matches!(character, '/' | '\\') {
                return Err(WorkspaceNameError::InvalidCharacter.into());
            }
        }
        let copied = copy_work(length as u64);
        accounting.admit(copied)?;
        let mut owned = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
        owned.value.extend_from_slice(canonical.as_bytes());
        accounting.record_completed(copied)?;
        let name = String::from_utf8(owned.value).map_err(WorkspaceError::engine)?;
        let control = crate::kernel::allocation::bytes_owner_control_bytes::<(
            String,
            KernelOwnedAllocation,
        )>()?;
        owned
            .allocation
            .absorb(&mut accounting.claim_owned(control, 1)?)?;
        Ok(Self(Arc::new((name, owned.allocation))))
    }

    /// Returns the canonical customer spelling.
    pub fn as_str(&self) -> &str {
        &self.0.0
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.as_str().len() as u64
    }
}

impl PartialEq for WorkspaceName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for WorkspaceName {}
impl PartialOrd for WorkspaceName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorkspaceName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl std::hash::Hash for WorkspaceName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl fmt::Debug for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("WorkspaceName")
            .field(&self.as_str())
            .finish()
    }
}
impl fmt::Display for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable opaque identity of one named workspace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    /// Restores a stable workspace identity from canonical bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Derives the stable identity for a canonical name in one deployment namespace. The namespace is deliberately internal to the selected `Fs` deployment.
    pub(crate) fn derive<W: crate::facade::FsWork>(
        namespace: [u8; 16],
        name: &WorkspaceName,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let performed = crate::WorkCounters {
                bytes_hashed: (WORKSPACE_ID_DOMAIN.len() + 16 + 8 + name.as_str().len()) as u64,
                bytes_copied: 16,
                ..crate::WorkCounters::UNCHARGED
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
    pub fn new() -> Self {
        Self(OperationId::new().into_bytes())
    }

    /// Restores an exact key after an ambiguous outcome.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the stable wire bytes.
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
