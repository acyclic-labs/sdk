use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone)]
pub struct SourceCursor {
    pub(crate) source: SourceReference,
    pub(crate) token: u64,
    /// The native enumeration this continuation resumes. Erased: a cursor is a
    /// value consumers hold across awaits, and its auto-trait proof must not
    /// walk the native source's owner graph in every downstream crate.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) native: Option<crate::kernel::allocation::retained::ErasedRetainedArc>,
}

impl SourceCursor {
    pub(crate) fn detached(source: SourceReference, token: u64) -> Self {
        Self {
            source,
            token,
            #[cfg(not(target_arch = "wasm32"))]
            native: None,
        }
    }

    /// A pure view's next position. `Computed` binds this value to the source,
    /// cut and directory before publishing it to a caller.
    pub fn position(token: u64) -> Self {
        Self::detached(
            SourceReference {
                identity: [0; 16],
                epoch: 0,
            },
            token,
        )
    }

    /// Position from a continuation already checked by `Computed`.
    pub fn offset(&self) -> u64 {
        self.token
    }
}

impl std::fmt::Debug for SourceCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceCursor")
            .field("source", &self.source)
            .field("token", &self.token)
            .finish()
    }
}

impl PartialEq for SourceCursor {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.token == other.token
    }
}

impl Eq for SourceCursor {}
