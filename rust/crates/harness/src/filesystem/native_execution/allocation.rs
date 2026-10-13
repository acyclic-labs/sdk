use super::*;
use acyclic_fs::{MaterializationBackend, NativeTreeMaterializationBackend};

/// Host-witnessed root and staging allocations.
pub struct NativePreparationRoot {
    /// Original namespace path.
    pub root: PathBuf,
    /// Same-volume staging allocation outside the namespace.
    pub operation_directory: PathBuf,
    /// Original physical root.
    pub expected_identity: acyclic_fs::NativeRootIdentity,
    /// Original staging and backup allocations.
    pub expected_preparation_identity: acyclic_fs::MaterializationPreparationIdentity,
}

impl NativePreparationRoot {
    /// Observes existing empty allocations on the original finite ledger.
    pub fn witness<'a>(
        root: &'a Path,
        operation_directory: &'a Path,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Self, NativeAllocation)>> {
        if !root.is_absolute()
            || !operation_directory.is_absolute()
            || !work.remaining()?.has_finite_allowances()
        {
            return Err(acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into());
        }
        crate::NativeFutureBundle::new(work, || async move {
            let backend = open(root, operation_directory, work).await?;
            backend.require_initial_empty()?;
            let expected_identity = acyclic_fs::NativeRootIdentity::from_bytes(
                backend
                    .allocation_identity()
                    .ok_or(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)?,
            );
            let expected_preparation_identity = backend
                .preparation_identity()
                .ok_or(acyclic_fs::NativeTreeMaterializationError::InvalidLayout)?;
            let (root, mut custody) = work.path(backend.root_path(), None)?;
            let (operation_directory, mut operation_custody) =
                work.path(backend.operation_directory_path(), None)?;
            custody.absorb(&mut operation_custody)?;
            Ok((
                Self {
                    root,
                    operation_directory,
                    expected_identity,
                    expected_preparation_identity,
                },
                custody,
            ))
        })
    }

    pub(super) async fn backend(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<NativeTreeMaterializationBackend>> {
        let backend = open(&self.root, &self.operation_directory, work).await?;
        if backend.allocation_identity() != Some(self.expected_identity.to_bytes())
            || backend.preparation_identity().as_ref() != Some(&self.expected_preparation_identity)
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Conflict,
                reason: "native allocation was physically replaced",
            });
        }
        Ok(backend)
    }
}

async fn open(
    root: &Path,
    operation_directory: &Path,
    work: &NativeMaterializationWork,
) -> Result<Arc<NativeTreeMaterializationBackend>> {
    let (root, mut custody) = work.path(root, None)?;
    let (operation_directory, mut operation_custody) = work.path(operation_directory, None)?;
    custody.absorb(&mut operation_custody)?;
    let original = work.clone();
    acyclic_native_runtime::run_blocking_io(
        move || {
            NativeTreeMaterializationBackend::new(
                root,
                operation_directory,
                custody,
                original,
            )
        },
        Some((work.runtime_meter(), false)),
    )
    .map_err(|error| Error::from(work.runtime_error(error)))?
    .await
    .map_err(|error| Error::from(work.runtime_error(error)))?
    .map_err(Error::from)
}
