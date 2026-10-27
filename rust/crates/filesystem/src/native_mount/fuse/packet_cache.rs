//! Cache behavior is selected by a physical transport, never by a guest claim.
//!
//! Direct replies are necessary for an uninvalidation-capable frontend, but
//! do not themselves qualify private mmap or source revocation semantics.

use super::{InitFlags, REQUESTED_CAPABILITIES};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum ProjectionCache {
    Notifications,
    Direct,
}

impl ProjectionCache {
    pub(super) fn capabilities(self) -> InitFlags {
        match self {
            Self::Notifications => REQUESTED_CAPABILITIES,
            // No automatic readdirplus, symlink caching, writeback, mmap,
            // directory caching, or opening without an admitted callback.
            Self::Direct => InitFlags::FUSE_PARALLEL_DIROPS,
        }
    }
}

pub(super) struct ProjectionConfig {
    pub(super) writable: bool,
    pub(super) owner: (u32, u32),
    pub(super) inode_base: u64,
    pub(super) cache: ProjectionCache,
}
