use super::*;

/// Adapter over any embedded, local, or distributed Filesystem provider pair.
pub struct FilesystemHost<A, O> {
    pub(in crate::filesystem) filesystem: Fs<A, O>,
    pub(in crate::filesystem) provider: ProviderRef,
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
            .map(|io| {
                io.take_native_custody(
                    io.owned_bytes()?
                        .checked_sub(before)
                        .ok_or_else(|| io.invalid("Filesystem host custody underflows"))?,
                )
            })
            .transpose()?;
        Ok(Arc::new(Self {
            filesystem,
            provider,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody,
        }))
    }
}

mod content;
mod keys;
mod reads;
mod volumes;
mod workspaces;
pub(crate) use keys::map_error;
pub(in crate::filesystem) use keys::{
    content_identity_path_io, content_metadata_path_io, project_join_key,
};
pub use keys::{filesystem_key, workspace_ref};
pub(in crate::filesystem) use reads::read_generation;

use crate::executor::stock_boundary;
#[cfg(not(target_arch = "wasm32"))]
pub use volumes::{
    bind_original_root_conversation_with_native_work, native_root_output_binding_operation,
};
