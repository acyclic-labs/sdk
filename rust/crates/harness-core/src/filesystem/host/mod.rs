use super::*;

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
pub struct FilesystemHost<A, O> {
    pub(in crate::filesystem) filesystem: Fs<A, O>,
    pub(in crate::filesystem) provider: ProviderRef,
    workspace_binding: Option<(WorkspaceRef, Workspace<A, O>)>,
    #[cfg(test)]
    publication_pause: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    native_skills: Arc<
        crate::sync::SyncMutex<
            std::collections::HashMap<String, Arc<crate::context::native_skills::NativeSkills>>,
        >,
    >,
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    project_family: Option<(acyclic_fs::WorkspaceId, acyclic_fs::LocalCoreStateStore)>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemHost<A, O> {
    /// Binds a deployment and funds its shared descriptor on the optional ledger.
    pub fn new(
        filesystem: Fs<A, O>,
        provider: &ProviderRef,
        io: Option<&PreparationIo>,
    ) -> Result<Arc<Self>> {
        crate::executor::with_history_io(io, |work| provider.validate(work))?;
        if provider.family() != "filesystem" {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "Filesystem adapter requires the filesystem provider family",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let provider = match io {
            Some(io) => provider.clone_with_io(io)?,
            None => provider.clone(),
        };
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
            io.allocation_attempt(std::mem::size_of::<Self>() as u64)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let _native_custody = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| io.take_native_custody(io.owned_since(before)?))
            .transpose()?;
        Ok(Arc::new(Self {
            filesystem,
            provider,
            workspace_binding: None,
            #[cfg(test)]
            publication_pause: None,
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            native_skills: Arc::new(crate::sync::SyncMutex::new(std::collections::HashMap::new())),
            #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
            project_family: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
        }))
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn for_workspace(
        &self,
        volume: &VolumeRef,
        workspace: Workspace<A, O>,
    ) -> Result<Arc<Self>> {
        if self._native_custody.is_some() || volume.provider() != &self.provider {
            return Err(Error::Unauthorized(
                "workspace binding requires ordinary project authority".into(),
            ));
        }
        Ok(Arc::new(Self {
            filesystem: self.filesystem.clone(),
            provider: self.provider.clone(),
            workspace_binding: Some((
                workspace_ref(self.provider.clone(), &volume.storage_name(&mut None)?)?,
                workspace,
            )),
            #[cfg(test)]
            publication_pause: self.publication_pause.clone(),
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            native_skills: self.native_skills.clone(),
            #[cfg(feature = "filesystem-local")]
            project_family: self.project_family.clone(),
            _native_custody: None,
        }))
    }
}

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl<A, O> FilesystemHost<A, O> {
    #[cfg(feature = "native-execution")]
    #[doc(hidden)]
    pub fn with_native_skills(
        self: Arc<Self>,
        mut sources: Arc<crate::context::native_skills::NativeSkills>,
    ) -> Result<Arc<Self>> {
        let mut registry = self
            .native_skills
            .lock()
            .map_err(|_| Error::Storage("skill registry lock is poisoned".into()))?;
        let key = sources.volume().storage_name(&mut None)?;
        if let Some(previous) = registry.get(&key) {
            Arc::get_mut(&mut sources)
                .ok_or_else(|| Error::Conflict("bind skills before sharing their catalog".into()))?
                .retain_sources_from(previous);
        }
        registry.insert(key, sources);
        drop(registry);
        Ok(self)
    }

    #[cfg(feature = "native-execution")]
    #[doc(hidden)]
    pub fn skill_sources(
        &self,
        volume: &VolumeRef,
    ) -> Result<Option<Arc<crate::context::native_skills::NativeSkills>>> {
        Ok(self
            .native_skills
            .lock()
            .map_err(|_| Error::Storage("skill registry lock is poisoned".into()))?
            .get(&volume.storage_name(&mut None)?)
            .cloned())
    }

    #[cfg(feature = "native-execution")]
    #[doc(hidden)]
    pub fn with_project_family(
        mut self: Arc<Self>,
        root: acyclic_fs::WorkspaceId,
        store: acyclic_fs::LocalCoreStateStore,
    ) -> Result<Arc<Self>> {
        Arc::get_mut(&mut self)
            .ok_or_else(|| Error::Conflict("bind the project before sharing its host".into()))?
            .project_family = Some((root, store));
        Ok(self)
    }
}

mod content;
mod publication;
pub(in crate::filesystem) use publication::PublicationLookup;
mod directory;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod family;
mod keys;
mod ranges;
mod reads;
pub(in crate::filesystem) use directory::DirectoryListing;
mod import;
mod volume_access;
pub use import::VolumeImportPlan;
mod volumes;
mod workspaces;
pub(crate) use keys::map_error;
pub(in crate::filesystem) use keys::{
    content_identity_path_io, content_metadata_path_io, project_join_key,
};
pub use keys::{filesystem_key, workspace_ref};
pub(crate) use reads::file_publication;
pub(in crate::filesystem) use reads::read_generation;

use crate::executor::stock_boundary;
#[cfg(not(target_arch = "wasm32"))]
pub use volumes::{
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};

pub use directory::DirectoryOptions;
