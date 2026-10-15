use super::*;

/// One caller-authored filesystem operation compiled into a single sparse,
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthoredMutation {
    /// Rebinds an existing non-directory object to a stable source identity.
    Reidentify {
        /// Existing namespace path.
        path: NamespacePath,
        /// Stable path-independent identity to retain.
        file_id: FileId,
    },
    /// Creates one regular file with exact initial bytes and metadata.
    CreateFile {
        /// New namespace path.
        path: NamespacePath,
        /// Complete initial bytes.
        bytes: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one regular file from already staged authenticated content.
    CreateFileFromContent {
        /// New namespace path.
        path: NamespacePath,
        /// Authenticated streamed content.
        content: StagedContent,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
        /// Optional stable source identity; the engine rejects an existing ID.
        file_id: Option<FileId>,
    },
    /// Creates one empty directory.
    CreateDirectory {
        /// New namespace path.
        path: NamespacePath,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one symbolic link with opaque target bytes.
    CreateSymbolicLink {
        /// New namespace path.
        path: NamespacePath,
        /// Exact uninterpreted target bytes.
        target: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one payload-free FIFO, socket, or mount boundary.
    CreateEmptySpecial {
        /// New namespace path.
        path: NamespacePath,
        /// Exact payload-free kind.
        kind: FileKind,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one POSIX character or block device.
    CreateDevice {
        /// New namespace path.
        path: NamespacePath,
        /// Character or block device kind.
        kind: FileKind,
        /// Native major identity.
        major: u32,
        /// Native minor identity.
        minor: u32,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Creates one opaque Windows reparse point.
    CreateReparsePoint {
        /// New namespace path.
        path: NamespacePath,
        /// Opaque reparse payload.
        payload: acyclic_stream::OwnedBytes,
        /// Exact cross-profile metadata.
        metadata: FileMetadata,
    },
    /// Removes one namespace binding.
    Remove {
        /// Existing namespace path.
        path: NamespacePath,
        /// Optional exact path-independent identity precondition.
        expected_file_id: Option<FileId>,
    },
    /// Atomically moves one binding within the volume.
    Rename {
        /// Existing source path.
        source: NamespacePath,
        /// Destination path.
        destination: NamespacePath,
        /// Whether an existing destination may be replaced.
        replace: bool,
    },
    /// Creates a same-volume hard link.
    HardLink {
        /// Existing source path.
        source: NamespacePath,
        /// New destination path.
        destination: NamespacePath,
    },
    /// Replaces one regular-file range.
    Write {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Exact replacement bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Replaces one range from already staged authenticated content.
    WriteFromContent {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Inclusive logical offset.
        offset: u64,
        /// Authenticated streamed content.
        content: StagedContent,
    },
    /// Replaces complete cross-profile metadata.
    SetMetadata {
        /// Existing path.
        path: NamespacePath,
        /// Exact replacement metadata.
        metadata: FileMetadata,
    },
    /// Changes logical regular-file length.
    Resize {
        /// Existing regular-file path.
        path: NamespacePath,
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Punches a hole or installs physically allocated zeros.
    ZeroRange {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Exact logical range.
        range: ByteRange,
        /// Preserve physical allocation when true.
        allocated: bool,
        /// Permit logical file growth to the range end.
        extend: bool,
    },
    /// Allocates holes without replacing existing content.
    Preallocate {
        /// Existing regular-file path.
        path: NamespacePath,
        /// Exact allocation range.
        range: ByteRange,
        /// Preserve logical file length.
        keep_size: bool,
    },
    /// Clones an immutable logical range without reading content bytes.
    CloneRange(FileCloneRequest),
}

impl AuthoredMutation {
    /// Exact owned namespace-path buffer bytes transferred with this mutation.
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        match self {
            Self::Rename {
                source,
                destination,
                ..
            }
            | Self::HardLink {
                source,
                destination,
            } => source.retained_bytes() + destination.retained_bytes(),
            Self::CloneRange(request) => {
                request.source.retained_bytes() + request.destination.retained_bytes()
            }
            Self::Reidentify { path, .. }
            | Self::CreateFile { path, .. }
            | Self::CreateFileFromContent { path, .. }
            | Self::CreateDirectory { path, .. }
            | Self::CreateSymbolicLink { path, .. }
            | Self::CreateEmptySpecial { path, .. }
            | Self::CreateDevice { path, .. }
            | Self::CreateReparsePoint { path, .. }
            | Self::Remove { path, .. }
            | Self::Write { path, .. }
            | Self::WriteFromContent { path, .. }
            | Self::SetMetadata { path, .. }
            | Self::Resize { path, .. }
            | Self::ZeroRange { path, .. }
            | Self::Preallocate { path, .. } => path.retained_bytes(),
        }
    }
}
