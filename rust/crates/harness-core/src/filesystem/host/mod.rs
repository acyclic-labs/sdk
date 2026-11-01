use super::*;

use crate::conversation::{PublicationDomain, PublicationWitness};

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
pub struct FilesystemHost<A: 'static, O: 'static> {
    pub(in crate::filesystem) filesystem: Fs<A, O>,
    pub(in crate::filesystem) provider: ProviderRef,
    publication_domain: Arc<PublicationDomain>,
    workspace_binding: Option<(WorkspaceRef, Workspace<A, O>)>,
    #[cfg(test)]
    publication_pause:
        Option<Arc<dyn Fn() -> acyclic_stream::BoxProviderFuture<'static, ()> + Send + Sync>>,
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
    project_family: Option<Arc<dyn family::ProjectFamily<A, O>>>,
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
        crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let provider = crate::core::copy_provider(provider, &mut quota)?;
            if quota.work().is_some() {
                quota.retain(std::mem::size_of::<Self>() as u64)?;
                quota
                    .observer()?
                    .allocation_attempt(std::mem::size_of::<Self>() as u64)?;
            }
            let publication_domain = PublicationDomain::new(quota.work())?;
            let _native_custody = quota.into_custody()?;
            Ok(Arc::new(Self {
                filesystem,
                provider,
                publication_domain,
                workspace_binding: None,
                #[cfg(test)]
                publication_pause: None,
                #[cfg(all(
                    feature = "filesystem-local",
                    feature = "native-execution",
                    not(target_arch = "wasm32")
                ))]
                native_skills: Arc::new(crate::sync::SyncMutex::new(
                    std::collections::HashMap::new(),
                )),
                project_family: None,
                #[cfg(not(target_arch = "wasm32"))]
                _native_custody,
            }))
        })
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
            publication_domain: Arc::clone(&self.publication_domain),
            workspace_binding: Some((
                workspace_ref(self.provider.clone(), &volume.storage_name(&mut None)?)?,
                workspace,
            )),
            #[cfg(test)]
            publication_pause: self.publication_pause.clone(),
            #[cfg(all(feature = "filesystem-local", feature = "native-execution"))]
            native_skills: self.native_skills.clone(),
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
}

mod content;
mod publication;
pub(in crate::filesystem) use publication::{PublicationLookup, PublicationPurpose};
mod content_selection;
pub(super) mod authentication;
