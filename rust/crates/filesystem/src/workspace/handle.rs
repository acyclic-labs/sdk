use super::*;

/// One named mutable filesystem head.
pub struct Workspace<A, O> {
    pub(crate) name: WorkspaceName,
    pub(crate) id: WorkspaceId,
    pub(crate) volume: Volume<A, O>,
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub(crate) source: Option<crate::Source<A, O>>,
}

impl<A, O> Clone for Workspace<A, O> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            id: self.id,
            volume: self.volume.clone(),
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: self.source.clone(),
        }
    }
}

impl<A, O> Workspace<A, O> {
    /// Stable opaque workspace identity.
    #[must_use]
    pub const fn id(&self) -> WorkspaceId {
        self.id
    }

    pub(crate) const fn authority_id(&self) -> crate::AuthorityId {
        crate::kernel::volume_authority_id(self.id.volume_id())
    }

    /// Immutable canonical workspace name.
    #[must_use]
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }

    /// Exact immutable filesystem profile selected when this workspace was created.
    #[must_use]
    pub const fn profile(&self) -> crate::model::FilesystemProfile {
        self.volume.config.profile
    }

    /// Exact immutable bounds selected for this workspace volume.
    #[must_use]
    pub const fn limits(&self) -> crate::model::VolumeLimits {
        self.volume.config.limits
    }
    /// Authenticated immutable volume configuration.
    #[must_use]
    pub const fn volume_config(&self) -> crate::model::VolumeConfig { self.volume.config }
}
