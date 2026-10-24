//! Bound native checkout publication, including original allocation custody.
use super::NativeDirectory;
use crate::{Error, Result};
use acyclic_fs::{
    Generation, LocalAuthorityBackend, LocalCoreStateStore, LocalObjectBackend,
    MaterializationJournal, MaterializeOptions, NativeAllocation, NativeMaterializationWork,
    NativeRootIdentity, NativeWorkspacePublication, OperationId,
};
use std::{
    future::Future,
    path::{Path, PathBuf},
};

type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;

/// One native checkout and its same-filesystem operation staging directory.
pub struct CheckoutPublication {
    root: PathBuf,
    directory: PathBuf,
    identity: NativeRootIdentity,
    operation: OperationId,
    _custody: NativeAllocation,
}
impl CheckoutPublication {
    /// Retains paths on the caller's original native ledger.
    pub fn new(
        root: &Path,
        identity: NativeRootIdentity,
        operation: OperationId,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native checkout root must be absolute",
            });
        }
        let mut root = work.path(root, None)?;
        let mut directory =
            NativeDirectory(&root.0).publication_directory_with_native_work(operation, work)?;
        root.1.absorb(&mut directory.1)?;
        Ok(Self {
            root: root.0,
            directory: directory.0,
            identity,
            operation,
            _custody: root.1,
        })
    }
    /// Physical checkout bound into publication and approval.
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// Operation staging directory bound into publication and approval.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    fn request<'a>(
        &'a self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        options: &'a MaterializeOptions,
        work: &'a NativeMaterializationWork,
    ) -> NativeWorkspacePublication<'a> {
        NativeWorkspacePublication {
            root: &self.root,
            source_root_identity: self.identity,
            operation_directory: &self.directory,
            operation_id: self.operation,
            from: from.id(),
            to: to.id(),
            excluded_names: &[".git"],
            options,
            budget: work.budget(),
            cancellation: work.cancellation(),
        }
    }
    /// Prepares a native generation transition without publishing it.
    pub async fn prepare(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
    ) -> Result<MaterializationJournal> {
        let mut request = self.request(from, to, options, work);
        request.budget = work.remaining()?;
        let prepared = acyclic_fs::materializer::prepare_native_generation_transition(
            from, to, state, request,
        )
        .await;
        match prepared {
            Ok(receipt) => {
                work.spent(receipt.work)?;
                Ok(receipt.value)
            }
            Err((source, spent)) => {
                work.spent(spent)?;
                Err(source.into())
            }
        }
    }
    /// Publishes with the caller's guard and original native work ledger.
    pub async fn publish<F, Fut>(
        &self,
        from: &LocalGeneration,
        to: &LocalGeneration,
        state: &LocalCoreStateStore,
        options: &MaterializeOptions,
        work: &NativeMaterializationWork,
        guard: F,
    ) -> Result<MaterializationJournal>
    where
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = std::result::Result<(), acyclic_fs::NativeWorkspacePublicationError>>
            + Send,
    {
        acyclic_fs::publish_native_generation_transition(
            from,
            to,
            state,
            self.request(from, to, options, work),
            work.clone(),
            guard,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|(source, _)| source.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkout_paths_retain_the_original_ledger_on_success_and_refusal() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let identity = NativeRootIdentity::from_bytes([0; 16]);
        let root = std::env::temp_dir().join("checkout-parent/checkout");
        let publication = CheckoutPublication::new(&root, identity, OperationId::new(), &work)?;
        assert_eq!(publication.root(), root.as_path());
        assert!(
            publication
                .directory()
                .starts_with(std::env::temp_dir().join("checkout-parent/.acyclic-m"))
        );
        assert!(work.live_allocation_bytes() > baseline);
        drop(publication);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let refused = CheckoutPublication::new(Path::new(""), identity, OperationId::new(), &work);
        assert!(matches!(
            refused,
            Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                ..
            })
        ));
        drop(refused);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
