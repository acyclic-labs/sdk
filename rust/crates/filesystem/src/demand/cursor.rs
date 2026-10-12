use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub(super) source: SourceReference,
    pub(super) directory: NamespacePath,
    pub(super) directory_version: SourceVersion,
    pub(super) token: u64,
}

impl SourceCursor {
    pub(crate) fn retained_bytes(&self) -> u64 {
        self.directory.retained_bytes()
    }
}
