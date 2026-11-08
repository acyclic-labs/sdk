use super::*;

/// Cursor bound to one directory and source epoch.
#[derive(Clone)]
pub struct SourceCursor {
    pub(crate) source: SourceReference,
    pub(crate) token: u64,
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub(super) native:
        Option<crate::kernel::allocation::retained::RetainedArc<native::DirectoryCursor>>,
}

impl SourceCursor {
    pub(crate) fn detached(source: SourceReference, token: u64) -> Self {
        Self {
            source,
            token,
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            native: None,
        }
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
