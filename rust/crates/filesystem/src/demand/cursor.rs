use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub(super) source: SourceReference,
    pub(super) token: u64,
}
