use super::*;

/// An unpublished directory page in source order.
#[derive(Debug)]
pub struct SourceDirectoryPage {
    /// Entries retained by this page.
    pub entries: SourceDirectoryEntries,
    /// Continuation usable after publication.
    pub next: Option<SourceCursor>,
    /// The directory version.
    pub version: SourceVersion,
    /// Producer transaction for this page.
    pub publication: Option<Box<dyn SourceDirectoryPublication>>,
}

impl SourceDirectoryPage {
    /// Constructs a stateless page from caller-owned entries.
    pub fn stateless(
        entries: Vec<SourceDirectoryEntry>,
        next: Option<SourceCursor>,
        version: SourceVersion,
    ) -> Self {
        Self {
            entries: SourceDirectoryEntries {
                storage: SourceEntryStorage::Owned(entries),
                selection: None,
            },
            next,
            version,
            publication: None,
        }
    }
    /// Returns retained page and producer bytes.
    pub fn retained_bytes(&self) -> u64 {
        self.entries.retained_bytes()
            + self.next.as_ref().map_or(0, SourceCursor::retained_bytes)
            + self
                .publication
                .as_ref()
                .map_or(0, |producer| producer.retained_bytes())
    }
}

/// A page's ordered view of owned or replayed entries.
#[derive(Debug)]
pub struct SourceDirectoryEntries {
    storage: SourceEntryStorage,
    // The selector drops before its original accounting claim.
    selection: Option<(Vec<usize>, SourcePayloadOwnership)>,
}

#[derive(Debug)]
enum SourceEntryStorage {
    Owned(Vec<SourceDirectoryEntry>),
    Shared {
        backing: std::sync::Arc<SourceDirectoryBacking>,
        range: std::ops::Range<usize>,
    },
}

#[derive(Debug)]
pub(in crate::demand) struct SourceDirectoryBacking {
    pub(in crate::demand) entries: Vec<SourceDirectoryEntry>,
    pub(in crate::demand) ownership: SourcePayloadOwnership,
}

impl SourceDirectoryBacking {
    pub(in crate::demand) fn retained_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64 + SourceDirectoryEntries::buffer_bytes(&self.entries)
    }
}

impl SourceDirectoryEntries {
    fn buffer_bytes(entries: &Vec<SourceDirectoryEntry>) -> u64 {
        (entries.capacity() * std::mem::size_of::<SourceDirectoryEntry>()) as u64
            + entries
                .iter()
                .map(|entry| entry.name().retained_bytes() as u64)
                .sum::<u64>()
    }
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub(in crate::demand) fn shared(
        backing: std::sync::Arc<SourceDirectoryBacking>,
        range: std::ops::Range<usize>,
    ) -> Self {
        Self {
            storage: SourceEntryStorage::Shared { backing, range },
            selection: None,
        }
    }
    pub(in crate::demand) fn set_selection(
        &mut self,
        indices: Vec<usize>,
        ownership: SourcePayloadOwnership,
    ) {
        self.selection = Some((indices, ownership));
    }
    pub(in crate::demand) fn physical_index(&self, index: usize) -> Option<usize> {
        if let Some((indices, _)) = &self.selection {
            return indices.get(index).copied();
        }
        match &self.storage {
            SourceEntryStorage::Owned(entries) => (index < entries.len()).then_some(index),
            SourceEntryStorage::Shared { range, .. } => {
                (index < range.len()).then(|| range.start + index)
            }
        }
    }
    /// Returns the selected entry count.
    pub fn len(&self) -> usize {
        self.selection.as_ref().map_or_else(
            || match &self.storage {
                SourceEntryStorage::Owned(entries) => entries.len(),
                SourceEntryStorage::Shared { range, .. } => range.len(),
            },
            |(indices, _)| indices.len(),
        )
    }
    /// Returns whether the page has no selected entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Borrows an entry by its position in the selected page.
    pub fn get(&self, index: usize) -> Option<&SourceDirectoryEntry> {
        let index = self.physical_index(index)?;
        match &self.storage {
            SourceEntryStorage::Owned(entries) => entries.get(index),
            SourceEntryStorage::Shared { backing, .. } => backing.entries.get(index),
        }
    }
    /// Iterates selected entries in source order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SourceDirectoryEntry> {
        (0..self.len()).map(|index| &self[index])
    }
    /// Returns the bytes retained by the backing and selector.
    pub fn retained_bytes(&self) -> u64 {
        let bytes = match &self.storage {
            SourceEntryStorage::Owned(entries) => Self::buffer_bytes(entries),
            SourceEntryStorage::Shared { backing, .. } => backing.retained_bytes(),
        };
        bytes
            + self.selection.as_ref().map_or(0, |(indices, _)| {
                (indices.capacity() * std::mem::size_of::<usize>()) as u64
            })
    }
}

impl std::ops::Index<usize> for SourceDirectoryEntries {
    type Output = SourceDirectoryEntry;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("bounded source page index")
    }
}
