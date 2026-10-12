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
    /// Authenticated immutable volume configuration.
    pub const fn volume_config(&self) -> crate::model::VolumeConfig {
        self.volume.config
    }

    /// Stable opaque workspace identity.
    pub const fn id(&self) -> WorkspaceId {
        self.id
    }

    pub(crate) const fn authority_id(&self) -> crate::AuthorityId {
        crate::kernel::volume_authority_id(self.id.volume_id())
    }

    /// Immutable canonical workspace name.
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }

    /// Exact immutable filesystem profile selected when this workspace was created.
    pub const fn profile(&self) -> crate::model::FilesystemProfile {
        self.volume.config.profile
    }

    /// Exact immutable bounds selected for this workspace volume.
    pub const fn limits(&self) -> crate::model::VolumeLimits {
        self.volume.config.limits
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns the attached native source.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub fn source(&self) -> Option<&crate::Source<A, O>> {
        self.source.as_ref()
    }

    /// Captures the attached source and returns its exact generation.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub async fn seal(&self) -> Result<Generation<A, O>, crate::SourceError> {
        match &self.source {
            Some(source) => source.seal().await,
            None => self
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| crate::SourceError::Engine(failure.error.to_string())),
        }
    }
}
