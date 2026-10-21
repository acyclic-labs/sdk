//! Native checkout publication shared by facade and multi-root merges.
use super::NativeDirectory;
use crate::{Error, Result};
use acyclic_fs::{
    CancellationToken, Generation, GenerationId, LocalAuthorityBackend, LocalCoreStateStore,
    LocalFs, LocalObjectBackend, MaterializeOptions, MultiRootMaterializer, NativeRootIdentity,
    OperationId, WorkBudget, Workspace, WorkspaceId, WorkspaceRootId,
};
use std::{collections::BTreeMap, path::PathBuf};

/// Configured physical checkout receiving workspace publications.
#[derive(Clone)]
pub struct CheckoutRoot {
    /// Workspace whose generations are projected into this checkout.
    pub workspace_id: WorkspaceId,
    /// Canonical physical checkout root.
    pub path: PathBuf,
    /// Host-observed root identity.
    pub source_root_identity: NativeRootIdentity,
}

/// Multi-root materialization through the same native publication path as git.
#[derive(Clone)]
pub struct WorkspaceRootMaterializer {
    /// Durable native publication journals.
    pub state: LocalCoreStateStore,
    /// Physical roots; child workspaces publish through their existing mounts.
    pub physical_roots: BTreeMap<WorkspaceRootId, CheckoutRoot>,
}

type LocalWorkspace = Workspace<LocalAuthorityBackend, LocalObjectBackend>;
type LocalGeneration = Generation<LocalAuthorityBackend, LocalObjectBackend>;

impl MultiRootMaterializer<LocalAuthorityBackend, LocalObjectBackend>
    for WorkspaceRootMaterializer
{
    type Error = Error;
    async fn materialize(
        &self,
        operation: OperationId,
        root: WorkspaceRootId,
        workspace: &LocalWorkspace,
        from: GenerationId,
        to: GenerationId,
    ) -> Result<()> {
        let physical = self
            .physical_roots
            .get(&root)
            .ok_or_else(|| Error::Unauthorized("publication root is not configured".into()))?;
        if workspace.id() != physical.workspace_id || from == to {
            return Ok(());
        }
        let cancellation = CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let from = workspace.generation(from, &work, &cancellation).await?;
        let to = workspace.generation(to, &work, &cancellation).await?;
        publish_checkout(
            &self.state,
            &physical.path,
            physical.source_root_identity,
            operation,
            &from,
            &to,
            work,
        )
        .await
    }
    async fn finalize(&self, operation: OperationId, root: WorkspaceRootId) -> Result<()> {
        let physical = self
            .physical_roots
            .get(&root)
            .ok_or_else(|| Error::Unauthorized("publication root is not configured".into()))?;
        finish_checkout(&self.state, &physical.path, operation).await
    }
}

pub(crate) async fn publish_checkout(
    state: &LocalCoreStateStore,
    root: &std::path::Path,
    identity: NativeRootIdentity,
    operation: OperationId,
    from: &LocalGeneration,
    to: &LocalGeneration,
    work: acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if from.id() == to.id() {
        return Ok(());
    }
    let publication = super::CheckoutPublication::new(root, identity, operation, &work)?;
    let options = MaterializeOptions {
        destination: publication.directory().join("target"),
        expected_root_identity: None,
        maximum_directory_entries: 4096,
        maximum_extent_spans: 65536,
        transfer_bytes: 8 * 1024 * 1024,
    };
    publication
        .publish(from, to, state, &options, &work, || {
            std::future::ready(Ok(()))
        })
        .await?;
    Ok(())
}

pub(crate) async fn finish_checkout(
    state: &LocalCoreStateStore,
    root: &std::path::Path,
    operation: OperationId,
) -> Result<()> {
    state.remove_materialization_async(operation).await?;
    let directory = NativeDirectory(root).publication_directory(operation)?;
    if !directory
        .try_exists()
        .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))?
    {
        return Ok(());
    }
    acyclic_native_runtime::remove_child_directory(
        directory
            .parent()
            .ok_or_else(|| Error::Invalid("native staging directory has no parent".into()))?,
        &directory,
    )
    .map_err(|cause| Error::from(acyclic_fs::NativeTreeMaterializationError::Io(cause)))
}

/// Local SDK multi-root publisher with native checkout materialization.
pub type WorkspacePublicationPublisher = acyclic_fs::MaterializingWorkspaceMultiRootPublisher<
    LocalAuthorityBackend,
    LocalObjectBackend,
    (LocalFs, acyclic_fs::WorkspaceGraph<LocalCoreStateStore>),
    WorkspaceRootMaterializer,
>;
/// Local SDK multi-root coordinator using workspace lineage authorization.
pub type WorkspacePublicationCoordinator = acyclic_fs::MultiRootPublicationCoordinator<
    LocalCoreStateStore,
    WorkspacePublicationPublisher,
    acyclic_fs::LineageMultiRootPublicationAuthorizer<LocalCoreStateStore>,
>;

/// Binds configured checkouts and destination merge drivers once per coordinator.
pub async fn workspace_publication_coordinator<'a>(
    filesystem: LocalFs,
    state: LocalCoreStateStore,
    physical_roots: BTreeMap<WorkspaceRootId, CheckoutRoot>,
    views: impl IntoIterator<
        Item = (
            WorkspaceRootId,
            &'a super::workspace_policy::LocalLazyWorkspace,
        ),
    >,
) -> Result<WorkspacePublicationCoordinator> {
    let mut publisher = WorkspacePublicationPublisher::new(
        (filesystem, acyclic_fs::WorkspaceGraph::new(state.clone())),
        WorkspaceRootMaterializer {
            state: state.clone(),
            physical_roots,
        },
    );
    for (root, view) in views {
        publisher = publisher.with_root_merge_drivers(
            root,
            super::workspace_policy::merge_drivers_for(view).await?,
        );
    }
    Ok(WorkspacePublicationCoordinator::new(
        state.clone(),
        publisher,
        acyclic_fs::LineageMultiRootPublicationAuthorizer::new(state),
    ))
}
