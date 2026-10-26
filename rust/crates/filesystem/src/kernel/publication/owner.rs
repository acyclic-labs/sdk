//! Concrete store custody at the publication boundary.

/// Authenticated publication cache owned by the same composition as its stores.
#[derive(Default)]
pub struct PublicationCache {
    pub(crate) inner: std::sync::Mutex<crate::kernel::closure::CachedProofs>,
}

/// Owns the storage composition used to prepare and publish a generation.
///
/// Clones must retain the same stores and their lifecycle ownership. In
/// particular, independently reopening stores does not retain a local root's
/// lifecycle lease. Publication keeps this owner through its final store use.
pub trait PublicationOwner: Clone + acyclic_stream::ProviderPlatform + 'static {
    /// Authority that orders the volume's publications.
    type Authority: crate::AsyncAuthorityStore;
    /// Immutable objects authenticated by those publications.
    type Objects: crate::AsyncObjectStore;

    /// Borrows this composition's actual authority.
    fn authority(&self) -> &Self::Authority;
    /// Borrows this composition's actual immutable-object store.
    fn objects(&self) -> &Self::Objects;

    /// Shares authenticated work with both owned preparation and receipt delivery.
    fn publication_cache(&self) -> Option<&PublicationCache> {
        None
    }
}
