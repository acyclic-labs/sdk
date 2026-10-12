use super::*;

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
pub struct FilesystemHost<A, O> {
    pub(in crate::filesystem) filesystem: Fs<A, O>,
    pub(in crate::filesystem) provider: ProviderRef,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A, O> FilesystemHost<A, O> {
    /// Binds one Filesystem deployment and immutable provider identity.
    pub fn new(filesystem: Fs<A, O>, provider: ProviderRef) -> Result<Self> {
        provider.validate(&mut None)?;
        if provider.family() != "filesystem" {
            return Err(Error::Invalid(
                "Filesystem adapter requires the filesystem provider family".into(),
            ));
        }
        Ok(Self {
            filesystem,
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: None,
        })
    }

    /// Copies only this host's provider descriptor on the original resource.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        filesystem: Fs<A, O>,
        provider: &ProviderRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        provider.validate(&mut Some(&mut crate::store::native_work_io(work)?))?;
        if provider.family() != "filesystem" {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Filesystem adapter requires the filesystem provider family",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let header = work
            .claim(
                std::mem::size_of::<Self>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)?;
        io.absorb_native_custody(header)?;
        let provider = provider.clone_with_io(&io)?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(Self {
            filesystem,
            provider,
            native_custody: Some(custody),
        })
    }

    /// Shares an already funded logical host Self with one actual SDK Arc.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            .native_custody
            .as_ref()
            .is_some_and(|guard| guard.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Filesystem host belongs to another original native resource",
            });
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(Error::from)?;
        Ok(Arc::new(self))
    }
}

mod content;
mod keys;
mod reads;
mod volumes;
mod workspaces;
pub(crate) use keys::map_error;
pub use keys::workspace_ref;
pub(in crate::filesystem) use keys::{
    content_identity_path_io, content_metadata_path_io, filesystem_key, project_join_key,
};
pub(in crate::filesystem) use reads::read_generation_with_io;
