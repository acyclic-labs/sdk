use super::*;

/// Admission barrier for every version-pinned file in a conversation message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum PrivateDirectoryEntryKind {
    /// A named regular file; resolve its current immutable ref by path.
    File,
    /// A named child directory.
    Directory,
}

/// One lazily discovered name, without eager byte or ref transfer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct PrivateDirectoryEntry {
    /// One normalized child name relative to the listed directory.
    pub name: String,
    /// Whether the name resolves to a regular file or another directory.
    pub kind: PrivateDirectoryEntryKind,
}

/// A bounded page from one exact owner-private generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct PrivateDirectoryPage {
    /// Exact immutable generation from which this page was read.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
    )]
    pub generation: GenerationRef,
    /// Ordered entries returned for the requested directory segment.
    pub entries: Vec<PrivateDirectoryEntry>,
    /// Whether another page may be requested with the returned cursor.
    pub has_more: bool,
}

/// The original publication fence paired with a directory's first page.
/// This identifies a source; it grants no authority to follow that resource.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct DirectoryChanges {
    /// Exact filesystem resource for this selected directory.
    pub resource: String,
    /// Original exclusive publication position.
    pub next: u64,
    /// Original preceding commit, retained without substituting a later head.
    pub preceding: [u8; 32],
}

/// Maximum number of entries admitted in one private-directory page.
pub const MAX_PRIVATE_DIRECTORY_PAGE: usize = u32::MAX as usize;

impl PrivateDirectoryPage {
    /// The one conversion from filesystem names into the public Unicode page.
    #[cfg(feature = "filesystem")]
    pub(crate) fn from_filesystem(
        generation: GenerationRef,
        page: acyclic_fs::DirectoryPage,
    ) -> Result<Self> {
        let mut cancellation = None;
        let entries = page
            .entries
            .into_iter()
            .map(|entry| {
                let name = entry
                    .name
                    .unicode_text(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        cancellation.get_or_insert_with(acyclic_fs::CancellationToken::new),
                    )
                    .map_err(|_| {
                        Error::Unsupported("private directory name is not Unicode".into())
                    })?
                    .value;
                let kind = match entry.kind {
                    acyclic_fs::kernel::FileKind::Regular => PrivateDirectoryEntryKind::File,
                    acyclic_fs::kernel::FileKind::Directory => PrivateDirectoryEntryKind::Directory,
                    _ => {
                        return Err(Error::Unsupported(
                            "private directory contains an unsupported entry kind".into(),
                        ));
                    }
                };
                Ok(PrivateDirectoryEntry { name, kind })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            generation,
            entries,
            has_more: page.has_more,
        })
    }

    /// Rejects malformed, duplicate, or out-of-order names independently of the concrete Filesystem page implementation.
    pub fn validate(&self) -> Result<()> {
        self.generation.validate(&mut None)?;
        if self.entries.len() as u64 > MAX_PRIVATE_DIRECTORY_PAGE as u64 {
            return Err(Error::Invalid(
                "private directory page exceeds protocol limit".into(),
            ));
        }
        let mut previous: Option<&[u8]> = None;
        for entry in &self.entries {
            validate_content_path(&entry.name, &mut None)?;
            if entry.name.contains('/')
                || previous.is_some_and(|name| entry.name.as_bytes() <= name)
            {
                return Err(Error::Invalid(
                    "private directory page names are not ordered".into(),
                ));
            }
            previous = Some(entry.name.as_bytes());
        }
        Ok(())
    }
}
