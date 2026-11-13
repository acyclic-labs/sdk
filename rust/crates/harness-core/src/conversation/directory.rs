use super::*;

/// One immutable directory selection on the caller's original reader.
#[derive(Clone, Copy)]
pub struct DirectoryRead<'a> {
    /// Authenticated namespace volume.
    pub volume: &'a VolumeRef,
    /// The original readable subtree.
    pub granted_prefix: &'a str,
    /// Volume-relative directory; empty selects the root.
    pub path: &'a str,
    /// Pinned generation, or current head for the first page.
    pub generation: Option<&'a GenerationRef>,
    /// Exclusive child name from the previous page.
    pub after: Option<&'a str>,
    /// Page chunk size.
    pub maximum_entries: u32,
}

/// A discovered name's kind at its pinned namespace generation.
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

/// Maximum number of entries admitted in one private-directory page.
pub const MAX_PRIVATE_DIRECTORY_PAGE: usize = u32::MAX as usize;

impl PrivateDirectoryPage {
    /// Rejects malformed, duplicate, or out-of-order names independently of the concrete Filesystem page implementation.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_io(None)
    }

    pub(crate) fn validate_with_io(&self, io: Option<&ContentIo>) -> Result<()> {
        crate::executor::with_history_io(io, |history| {
            self.generation.validate(history)?;
            if self.entries.len() as u64 > MAX_PRIVATE_DIRECTORY_PAGE as u64
                || (self.has_more && self.entries.is_empty())
            {
                return Err(crate::store::guarded_boundary(
                    history,
                    crate::wire::ErrorCode::Invalid,
                    "private directory page exceeds its bound or cannot advance",
                ));
            }
            let mut previous: Option<&[u8]> = None;
            for entry in &self.entries {
                validate_content_path(&entry.name, history)?;
                if entry.name.contains('/')
                    || previous.is_some_and(|name| entry.name.as_bytes() <= name)
                {
                    return Err(crate::store::guarded_boundary(
                        history,
                        crate::wire::ErrorCode::Invalid,
                        "private directory page names are not ordered",
                    ));
                }
                previous = Some(entry.name.as_bytes());
            }
            Ok(())
        })
    }
}
