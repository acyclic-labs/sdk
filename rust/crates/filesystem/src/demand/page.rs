use super::*;
use std::sync::Arc;

/// A bounded directory page awaiting consumer publication.
#[derive(Debug)]
pub struct SourceDirectoryPage {
    /// Entries in native enumeration order.
    pub entries: SourceDirectoryEntries,
    /// Continuation usable after publication.
    pub next: Option<SourceCursor>,
    /// Observed directory version.
    pub version: SourceVersion,
    /// Pending producer position.
    pub publication: Option<Box<SourceDirectoryPublication>>,
}
impl SourceDirectoryPage {
    /// Copies one bounded borrowed page onto the caller's original ledger.
    /// Names and entry backing are funded before their allocations occur.
    pub fn copied<'a>(
        entries: impl Iterator<Item = (&'a crate::kernel::LogicalName, SourceNode)> + Clone,
        next: Option<SourceCursor>,
        version: SourceVersion,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        Self::copied_with(entries, next, version, accounting, |_, node, _| Ok(node))
    }

    /// Copies a bounded page, transforming each node exactly once on its
    /// original accounting context before publishing the retained entries.
    pub fn copied_with<'a>(
        entries: impl Iterator<Item = (&'a crate::kernel::LogicalName, SourceNode)> + Clone,
        next: Option<SourceCursor>,
        version: SourceVersion,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
        mut transform: impl FnMut(
            &crate::kernel::LogicalName,
            SourceNode,
            &mut crate::kernel::KernelAccounting<'_>,
        ) -> Result<SourceNode, DemandError>,
    ) -> Result<Self, DemandError> {
        let mut owned = crate::kernel::Owned::allocate(entries.clone().count(), accounting)?;
        for (name, node) in entries {
            let node = transform(name, node, accounting)?;
            let cancel = accounting.cancellation();
            let (name, mut allocation) = name.clone_with_accounting(accounting, cancel)?;
            owned.bytes += name.logical_owned_bytes();
            owned.allocation.absorb(&mut allocation)?;
            owned.push(SourceDirectoryEntry { name, node }, accounting)?;
        }
        Self::stateless(owned, next, version, accounting)
    }

    /// Constructs a page without a producer transaction.
    pub fn stateless(
        mut entries: crate::kernel::Owned<Vec<SourceDirectoryEntry>>,
        next: Option<SourceCursor>,
        version: SourceVersion,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<Self, DemandError> {
        let bytes = std::mem::size_of::<crate::kernel::Owned<Vec<SourceDirectoryEntry>>>() as u64
            + 2 * std::mem::size_of::<usize>() as u64;
        let mut header = accounting
            .claim_owned(bytes, 1)
            .map_err(DemandError::from)?;
        entries
            .allocation
            .absorb(&mut header)
            .map_err(DemandError::from)?;
        entries.bytes += bytes;
        let end = entries.value.len();
        Ok(Self {
            entries: SourceDirectoryEntries {
                backing: Arc::new(entries),
                range: 0..end,
            },
            next,
            version,
            publication: None,
        })
    }
    /// Retained page storage and continuation bytes.
    pub fn retained_bytes(&self) -> u64 {
        self.entries.backing.bytes
    }
}

/// An immutable slice shared with the producer's retry position.
pub struct SourceDirectoryEntries {
    pub(super) backing: Arc<crate::kernel::Owned<Vec<SourceDirectoryEntry>>>,
    pub(super) range: std::ops::Range<usize>,
}
impl std::ops::Deref for SourceDirectoryEntries {
    type Target = [SourceDirectoryEntry];
    fn deref(&self) -> &Self::Target {
        let (through_end, _) = self.backing.value.split_at(self.range.end);
        let (_, entries) = through_end.split_at(self.range.start);
        entries
    }
}
impl<'a> IntoIterator for &'a SourceDirectoryEntries {
    type Item = &'a SourceDirectoryEntry;
    type IntoIter = std::slice::Iter<'a, SourceDirectoryEntry>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl std::fmt::Debug for SourceDirectoryEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
pub use super::native::SourceDirectoryPublication;
/// No native producer exists in this configuration.
#[cfg(not(all(feature = "native-watch", not(target_arch = "wasm32"))))]
#[derive(Debug)]
pub enum SourceDirectoryPublication {}
#[cfg(not(all(feature = "native-watch", not(target_arch = "wasm32"))))]
impl SourceDirectoryPublication {
    /// Commits the producer position.
    pub fn accept(
        &mut self,
        _: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        match *self {}
    }
    /// Restores the producer position.
    pub fn abort(
        &mut self,
        _: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        match *self {}
    }
}
