use super::*;

const MAXIMUM_WORKSPACE_NAME_BYTES: usize = 255;

/// Canonical immutable customer workspace name.
#[derive(Clone)]
pub struct WorkspaceName(Arc<(String, KernelOwnedAllocation)>);

impl WorkspaceName {
    /// Canonicalizes a name under the supplied work limits.
    pub fn new<W: crate::facade::FsWork>(
        name: impl AsRef<str>, work: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = Self::with_accounting(name.as_ref(), &mut accounting);
        work.finish(result, &accounting)
    }

    pub(super) fn with_accounting(name: &str, accounting: &mut KernelAccounting<'_>) -> Result<Self, WorkspaceError> {
        accounting.check()?;
        let (mut pending, mut maximum_pending, mut maximum_chunk) = (0_u64, 0_u64, 0_u64);
        for character in name.chars() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let mut chunk = 0_u64;
            let mut failure = None;
            unicode_normalization::char::decompose_canonical(character, |scalar| {
                if failure.is_some() { return; }
                failure = (|| -> Result<(), WorkspaceError> {
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    chunk = chunk.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                    if unicode_normalization::char::canonical_combining_class(scalar) == 0 {
                        pending = 0;
                    } else {
                        pending = pending.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                        maximum_pending = maximum_pending.max(pending);
                    }
                    Ok(())
                })().err();
            });
            if let Some(error) = failure { return Err(error); }
            maximum_chunk = maximum_chunk.max(chunk);
        }
        // NFC's two TinyVec buffers and combining-mark sort scratch are admitted before iteration.
        let buffer = |count: u64, width: usize| -> Result<u64, WorkspaceError> {
            if count <= 4 { return Ok(0); }
            count.checked_next_power_of_two().and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_mul(width as u64)).ok_or(crate::WorkError::Overflow.into())
        };
        let scratch = buffer(maximum_pending.checked_add(maximum_chunk).ok_or(crate::WorkError::Overflow)?, std::mem::size_of::<(u8, char)>())?
            .checked_add(buffer(maximum_pending, std::mem::size_of::<char>())?)
            .and_then(|n| maximum_pending.checked_mul(std::mem::size_of::<(u8, char)>() as u64).and_then(|sort| n.checked_add(sort)))
            .ok_or(crate::WorkError::Overflow)?;
        let live = match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
        };
        accounting.admit(crate::WorkCounters {
            peak_allocation_bytes: live.checked_add(scratch).ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let mut output = [0_u8; MAXIMUM_WORKSPACE_NAME_BYTES];
        let mut length = 0;
        let mut normalized = name.nfc();
        loop {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let Some(character) = normalized.next() else { break };
            let end = length + character.len_utf8();
            if end > output.len() { return Err(WorkspaceNameError::TooLong.into()); }
            let copied = crate::WorkCounters { bytes_copied: character.len_utf8() as u64, ..crate::WorkCounters::UNCHARGED };
            accounting.admit(copied)?;
            character.encode_utf8(&mut output[length..end]);
            length = end;
            accounting.record_completed(copied)?;
        }
        drop(normalized);
        accounting.charge(crate::WorkCounters { items_examined: length as u64, ..crate::WorkCounters::UNCHARGED })?;
        let canonical = std::str::from_utf8(&output[..length]).map_err(WorkspaceError::engine)?;
        if canonical.is_empty() { return Err(WorkspaceNameError::Empty.into()); }
        if canonical == "." || canonical == ".." { return Err(WorkspaceNameError::Reserved.into()); }
        for character in canonical.chars() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if character.is_control() || matches!(character, '/' | '\\') {
                return Err(WorkspaceNameError::InvalidCharacter.into());
            }
        }
        let copied = crate::WorkCounters { bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED };
        accounting.admit(copied)?;
        let mut owned = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
        owned.value.extend_from_slice(canonical.as_bytes());
        accounting.record_completed(copied)?;
        let name = String::from_utf8(owned.value).map_err(WorkspaceError::engine)?;
        let control = crate::kernel::allocation::bytes_owner_control_bytes::<(String, KernelOwnedAllocation)>()?;
        owned.allocation.absorb(&mut accounting.claim_owned(control, 1)?)?;
        Ok(Self(Arc::new((name, owned.allocation))))
    }

    /// Returns the canonical customer spelling.
    #[must_use]
    pub fn as_str(&self) -> &str { &self.0.0 }

    pub(crate) fn retained_bytes(&self) -> u64 { self.as_str().len() as u64 }
}

impl PartialEq for WorkspaceName {
    fn eq(&self, other: &Self) -> bool { self.as_str() == other.as_str() }
}
impl Eq for WorkspaceName {}
impl PartialOrd for WorkspaceName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for WorkspaceName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.as_str().cmp(other.as_str()) }
}
impl std::hash::Hash for WorkspaceName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { std::hash::Hash::hash(self.as_str(), state); }
}
impl fmt::Debug for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("WorkspaceName").field(&self.as_str()).finish()
    }
}
impl fmt::Display for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str(self.as_str()) }
}

/// Canonical workspace-name validation failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceNameError {
    /// Names must contain at least one scalar.
    #[error("workspace name is empty")]
    Empty,
    /// Dot names are reserved for namespace traversal prevention.
    #[error("workspace name is reserved")]
    Reserved,
    /// Canonical UTF-8 exceeds the stable bound.
    #[error("workspace name exceeds 255 UTF-8 bytes")]
    TooLong,
    /// Names cannot contain separators or control characters.
    #[error("workspace name contains a path separator or control character")]
    InvalidCharacter,
    /// Canonical name storage could not be allocated.
    #[error("workspace name allocation failed")]
    Allocation,
}
