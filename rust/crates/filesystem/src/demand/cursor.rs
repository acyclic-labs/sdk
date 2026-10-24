use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub(crate) source: SourceReference,
    pub(crate) token: u64,
    pub(crate) resume: Option<SourceDirectoryResume>,
}

/// A native directory cookie is usable only with its unchanged metadata token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SourceDirectoryResume {
    pub(crate) version: SourceVersion,
    pub(crate) position: u64,
}
