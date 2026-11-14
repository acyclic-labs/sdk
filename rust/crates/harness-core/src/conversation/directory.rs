use super::*;

/// A selected agent's directory namespace, never a caller-supplied volume grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum DirectoryTarget {
    /// The agent's authored UI and interaction files.
    Private {},
    /// Its current primary workspace, independent of its display name.
    Primary {},
    /// One retained workspace slot, including its declared subtree boundary.
    Workspace {
        /// Stable entry identity, preserved across workspace renames.
        #[cfg_attr(feature = "wasm", tsify(type = "string"))]
        id: crate::contract::EntryId,
    },
}

/// One bounded directory selection under an agent's original authority.
pub struct DirectoryQuery<'a> {
    /// The selected namespace in the agent's committed workspace table.
    pub target: DirectoryTarget,
    /// Directory relative to the selected workspace's declared root.
    pub path: &'a str,
    /// Immutable generation for all continuation pages.
    pub generation: Option<&'a GenerationRef>,
    /// Wait for the selected volume's original publication after this cut.
    pub after_generation: Option<&'a GenerationRef>,
    /// Opaque continuation at this directory's immutable selection.
    pub after: Option<&'a [u8]>,
    /// Row credit; continuation splits work without a directory-size ceiling.
    pub maximum_entries: u32,
}

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
    /// Opaque provider continuation from the previous page.
    pub after: Option<&'a [u8]>,
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
    /// Symbolic link; the listing does not follow its target.
    SymbolicLink,
    /// Named pipe.
    Fifo,
    /// Socket node.
    Socket,
    /// Character device.
    CharacterDevice,
    /// Block device.
    BlockDevice,
    /// Native reparse point.
    ReparsePoint,
    /// Mounted namespace boundary.
    MountBoundary,
    /// Source kind that cannot be projected losslessly.
    Unsupported,
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
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, crate::HeapLayout)]
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
    /// Opaque continuation at the same directory and generation.
    pub next: Option<Vec<u8>>,
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
                || self.next.as_ref().is_some_and(Vec::is_empty)
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

/// Borrowed wire projection over the same directory record.
#[doc(hidden)]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateDirectoryPageView<'a> {
    pub generation: &'a GenerationRef,
    pub entries: &'a [PrivateDirectoryEntry],
    pub next: Option<&'a [u8]>,
}

impl Serialize for PrivateDirectoryPage {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        PrivateDirectoryPageView {
            generation: &self.generation,
            entries: &self.entries,
            next: self.next.as_deref(),
        }
        .serialize(serializer)
    }
}
