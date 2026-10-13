//! Created content through the configured signing authority.
use super::*;
use crate::core::AuthorityIssuer;
use crate::resource_preparation::{RootBootstrapContentRequest, RootBootstrapContentRole};
use acyclic_fs::{CancellationToken, WorkCounters};

/// Authenticates the protected allocations owned by this Filesystem provider.
pub struct FilesystemResourceCreationProvider<A, O> {
    pub(super) host: Arc<FilesystemHost<A, O>>,
    pub(super) issuer: AuthorityIssuer,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) roots: [std::sync::OnceLock<Arc<FilesystemRootBootstrapAllocation<A, O>>>; 2],
    #[cfg(not(target_arch = "wasm32"))]
    native: Option<acyclic_fs::NativeAllocation>,
}
impl<A, O> FilesystemResourceCreationProvider<A, O> {
    /// Copies the configured signing identity on the optional original ledger.
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        issuer: &AuthorityIssuer,
        #[cfg(not(target_arch = "wasm32"))] work: Option<&acyclic_fs::NativeMaterializationWork>,
    ) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        let io = work.map(PreparationIo::from_native_work).transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io = None::<PreparationIo>;
        if let Some(io) = &io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        }
        let issuer = crate::executor::with_history_io(io.as_ref(), |work| issuer.copy(work))?;
        #[cfg(not(target_arch = "wasm32"))]
        let native = io
            .map(|io| {
                let bytes = io.owned_bytes()?;
                io.into_native_custody(bytes)
            })
            .transpose()?;
        Ok(Self {
            host,
            issuer,
            #[cfg(not(target_arch = "wasm32"))]
            roots: std::array::from_fn(|_| std::sync::OnceLock::new()),
            #[cfg(not(target_arch = "wasm32"))]
            native,
        })
    }
    /// Shares the paid creator on its original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(
        self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !self
            .native
            .as_ref()
            .is_some_and(|allocation| allocation.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "creator belongs to another original ledger",
            });
        }
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..Default::default()
        })?;
        Ok(Arc::new(self))
    }
    /// Binds this creator's original protected Root allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bind_root_bootstrap_allocation_with_native_work(
        &self,
        proof: Arc<FilesystemRootBootstrapAllocation<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let io = PreparationIo::from_native_work(work)?;
        let configured = self.issuer.verifier(&mut Some(&mut io.history_io()))?;
        if !Arc::ptr_eq(&proof.host, &self.host)
            || proof.creator != configured
            || !proof.matches_native_work(work)
        {
            return Err(crate::executor::stock_boundary(
                Some(&io),
                crate::wire::ErrorCode::Unauthorized,
                "Root allocation differs from its configured creator",
            ));
        }
        let index = match proof.request.role {
            RootBootstrapContentRole::Project => 0,
            RootBootstrapContentRole::Scratch => 1,
        };
        if let Some(other) = self.roots[1 - index].get() {
            let binding = proof.witness.binding();
            let original = other.witness.binding();
            if original.original_task() != binding.original_task()
                || original.agent() != binding.agent()
                || other.owner() != proof.owner()
                || other.admission_digest != proof.admission_digest
            {
                return Err(crate::executor::stock_boundary(
                    Some(&io),
                    crate::wire::ErrorCode::Unauthorized,
                    "Root allocations belong to different original admissions",
                ));
            }
        }
        if let Err(proof) = self.roots[index].set(proof) {
            if !Arc::ptr_eq(
                self.roots[index].get().expect("installed Root allocation"),
                &proof,
            ) {
                return Err(crate::executor::stock_boundary(
                    Some(&io),
                    crate::wire::ErrorCode::Conflict,
                    "configured original Root allocation changed",
                ));
            }
        }
        Ok(())
    }
}
