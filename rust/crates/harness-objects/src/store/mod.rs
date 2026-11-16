use super::*;
mod binding;
mod content;
mod publication;
mod ranges;
pub(in crate) use content::storage;

/// One authenticated account-blob binding for a content volume. A file's
/// version is its content blob digest; an artifact names its record blob,
/// which binds the descriptor and display name to that digest.
pub struct ObjectContentStore {
    objects: Arc<dyn Blobs>,
    pub(in crate) volume: VolumeRef,
    pub(in crate) verifier: AuthorityVerifier,
    pub(in crate) scope: Scope,
    maximum_bytes: u64,
    writer: Option<ContentGrant>,
}
