use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceCursor {
    pub(crate) source: SourceReference,
    pub(crate) token: u64,
}
