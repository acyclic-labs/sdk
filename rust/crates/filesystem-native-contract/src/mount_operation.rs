//! Operation values shared by native projection adapters.

/// Concrete namespace mechanism selected for this binary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeMountKind {
    /// Linux kernel FUSE through `/dev/fuse`.
    LinuxFuse,
    /// Self-contained macOS loopback-NFS session.
    MacOsNfs,
    /// Windows Projected File System provider.
    WindowsProjFs,
}

/// Opaque evidence of the exact external content one lookup observed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MountContentPin(pub [u8; 32]);

/// Native sparse-file seek target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountSeekTarget {
    /// First allocated-zero or content byte.
    Data,
    /// First unallocated hole byte, including logical end-of-file.
    Hole,
}

/// Exact sparse-allocation operation requested by a native mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountRangeAllocation {
    /// Deallocates the range without changing logical size.
    PunchHole,
    /// Replaces the range with allocated canonical zeros.
    ZeroRange {
        /// Permit logical growth to the range end.
        extend: bool,
    },
    /// Allocates only holes while preserving all existing content.
    Preallocate {
        /// Preserve logical file length.
        keep_size: bool,
    },
}

/// Content rights requested by a native open that also returns metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountOpenAccess {
    /// Names and metadata only.
    Metadata,
    /// Content reads.
    Read,
    /// Content writes or appends; each mutation retains its distinct check.
    Write,
    /// Both content reads and writes.
    ReadWrite,
}

/// Native named-attribute write precondition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MountAttributeWriteMode {
    /// Insert or replace.
    Upsert,
    /// Require absence.
    Create,
    /// Require presence.
    Replace,
}
