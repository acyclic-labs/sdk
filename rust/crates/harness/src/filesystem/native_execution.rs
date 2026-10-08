//! Optional native command composition over existing task, effect and workspace owners.

use super::{
    FilesystemHost, NativeNamespaceKind, NativeViewManifest, NativeViewOptions,
    NativeVolumeMapping, workspace_ref,
};
use crate::{
    Error, Result,
    conversation::{ContentGrant, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    durable_host::TaskJournalOwner,
    resources::GenerationRef,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, NativeWorkingSet, PublicationPermit};
use acyclic_stream::StreamProvider;
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};

/// Host-only selection. Credentials and publication permits never enter a command.
pub struct NativeVolumeBinding<A, O> {
    /// Exact provider capability used for this volume; a consumer router may
    /// compose heterogeneous stores under the existing provider traits.
    pub host: Arc<FilesystemHost<A, O>>,
    /// Owner-bound selected volume.
    pub volume: VolumeRef,
    /// Exact immutable input generation.
    pub generation: GenerationRef,
    /// Relative native path below the prepared view root.
    pub path: String,
    /// Whether captured edits may publish to this destination.
    pub writable: bool,
    /// Existing signed authority verifier.
    pub verifier: AuthorityVerifier,
    /// Original read authority, and write authority for a destination.
    pub scope: Scope,
    /// Existing Filesystem operation permit for destination publication.
    pub publication: PublicationPermit,
}

struct PreparedVolume<A, O> {
    view: NativeWorkingSet<A, O>,
    writable: bool,
    publication: PublicationPermit,
}

/// Provider-owned materialized namespace, with the canonical SDK capture owner
/// for every independently authorized volume. It carries no second workspace engine.
pub struct NativeVolumeView<A, O> {
    manifest: NativeViewManifest,
    volumes: Vec<PreparedVolume<A, O>>,
}

impl<A, O> NativeVolumeView<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Prepares exact generations after both host and retained task grants pass.
    /// Read-only generations may precede the current head. Destinations must
    /// still have their admitted head and publish through existing conflict checks.
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered authority-validation and exact-volume preparation transaction"
    )]
    pub async fn prepare<P: StreamProvider>(
        owner: &TaskJournalOwner<P>,
        options: NativeViewOptions,
        bindings: Vec<NativeVolumeBinding<A, O>>,
    ) -> Result<Self> {
        owner.verify(false).await?;
        if options.maximum_volumes == 0
            || bindings.is_empty()
            || bindings.len() as u64 > u64::from(options.maximum_volumes)
            || !options.root.is_absolute()
        {
            return Err(Error::Invalid(
                "invalid native view root or volume allowance".into(),
            ));
        }
        if !options.work_per_volume.has_finite_allowances() {
            return Err(Error::Invalid(
                "native view work allowances must be finite".into(),
            ));
        }
        let metadata = std::fs::symlink_metadata(&options.root).map_err(native_io)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || std::fs::read_dir(&options.root)
                .map_err(native_io)?
                .next()
                .is_some()
        {
            return Err(Error::Invalid(
                "native view root must be an empty real directory".into(),
            ));
        }
        let mut options = options;
        options.root = std::fs::canonicalize(&options.root).map_err(native_io)?;
        let mut paths: Vec<PathBuf> = Vec::new();
        // Validate the complete authority/path selection before any materialization.
        for binding in &bindings {
            let path = Path::new(&binding.path);
            if path.as_os_str().is_empty()
                || path
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
                || paths
                    .iter()
                    .any(|existing| path.starts_with(existing) || existing.starts_with(path))
            {
                return Err(Error::Invalid(
                    "native volume paths overlap or escape their root".into(),
                ));
            }
            owner.require_volume_grant(&binding.volume, VolumeOperation::Read)?;
            ContentGrant::verify(
                &binding.verifier,
                &binding.scope,
                &binding.volume,
                VolumeOperation::Read,
            )?;
            if binding.writable {
                owner.require_volume_grant(&binding.volume, VolumeOperation::Write)?;
                ContentGrant::verify(
                    &binding.verifier,
                    &binding.scope,
                    &binding.volume,
                    VolumeOperation::Write,
                )?;
            }
            binding.host.validate_provider(binding.volume.provider())?;
            binding
                .host
                .validate_provider(binding.generation.as_resource().provider())?;
            paths.push(path.to_path_buf());
        }
        let mut volumes = Vec::new();
        let mut mappings = Vec::new();
        let mut native_paths: Vec<PathBuf> = Vec::new();
        let mut native_roots = Vec::new();
        for binding in bindings {
            owner.verify(false).await?;
            let workspace_ref = workspace_ref(
                binding.host.provider.clone(),
                &binding.volume.storage_name()?,
            )?;
            let workspace = binding.host.open(&workspace_ref).await?;
            let generation = binding
                .host
                .generation(&workspace, &binding.generation)
                .await?;
            let path = options.root.join(&binding.path);
            std::fs::create_dir_all(&path).map_err(native_io)?;
            let identity = acyclic_fs::capture_root_identity(&path)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let path = std::fs::canonicalize(path).map_err(native_io)?;
            if !path.starts_with(&options.root)
                || native_roots.contains(&identity)
                || native_paths
                    .iter()
                    .any(|existing| path.starts_with(existing) || existing.starts_with(&path))
            {
                return Err(Error::Invalid(
                    "native volume directories physically overlap or escape their root".into(),
                ));
            }
            native_paths.push(path.clone());
            native_roots.push(identity);
            let mount = if binding.writable {
                acyclic_fs::MountOptions::read_write()
                    .publication(acyclic_fs::MountPublication::Manual)
            } else {
                acyclic_fs::MountOptions::read_only()
            };
            let view = generation
                .prepare_native_working_set(
                    &mount,
                    &acyclic_fs::MaterializeOptions::native(&path),
                    options.work_per_volume,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            mappings.push(NativeVolumeMapping {
                volume: binding.volume,
                generation: binding.generation,
                path,
                writable: binding.writable,
                authority_revision: crate::contract::canonical_json_digest(&(
                    binding.verifier.audience(),
                    binding.scope,
                ))?,
            });
            volumes.push(PreparedVolume {
                view,
                writable: binding.writable,
                publication: binding.publication,
            });
        }
        owner.verify(false).await?;
        Ok(Self {
            manifest: NativeViewManifest {
                kind: NativeNamespaceKind::MaterializedDirectories,
                options,
                volumes: mappings,
            },
            volumes,
        })
    }

    /// Exact request/approval manifest. Changing it requires new admission.
    #[must_use]
    pub const fn manifest(&self) -> &NativeViewManifest {
        &self.manifest
    }

    /// Rechecks the admitted views without changing their selected generations.
    pub async fn validate_for_dispatch(&self) -> Result<()> {
        for volume in &self.volumes {
            volume
                .view
                .validate_for_presentation()
                .await
                .map_err(|error| Error::Conflict(error.to_string()))?;
            volume
                .view
                .verify_unchanged()
                .await
                .map_err(|error| Error::Conflict(error.to_string()))?;
        }
        Ok(())
    }

    /// Publishes only authorized destinations after source integrity passes.
    /// Every volume uses its existing permit/conflict lifecycle independently.
    pub async fn publish<P: StreamProvider>(&self, owner: &TaskJournalOwner<P>) -> Result<()> {
        for volume in &self.volumes {
            if !volume.writable {
                volume
                    .view
                    .verify_unchanged()
                    .await
                    .map_err(|error| Error::Conflict(error.to_string()))?;
            }
        }
        for volume in &self.volumes {
            if volume.writable {
                owner.verify(false).await?;
                volume
                    .view
                    .sync_with_permit(volume.publication)
                    .await
                    .map_err(|error| Error::Conflict(error.to_string()))?;
            }
        }
        Ok(())
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned I/O error callback used by map_err"
)]
fn native_io(error: std::io::Error) -> Error {
    Error::Storage(error.to_string())
}
