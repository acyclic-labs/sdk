//! Shared real filesystem fixture for durable task consumers.
use super::*;
use crate::conversation::VolumeOperation;
use crate::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    FilesystemResourceCreationProvider, FilesystemTaskRuntime,
};
use crate::resource_preparation::{ResourcePreparationRequest, TaskResourcePreparation};
use crate::resources::ProviderRef;
#[cfg(not(target_arch = "wasm32"))]
use crate::runtime::AgentHarness;
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};

pub(crate) type MemoryFilesystem = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

pub(crate) struct CreationFixture {
    pub(crate) host: Arc<CoordinatorTaskHost<MemoryStream>>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) harness: Arc<AgentHarness>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) runtime:
        FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>,
    pub(crate) filesystem: Arc<MemoryFilesystem>,
    pub(crate) stream: Arc<MemoryStream>,
    pub(crate) filesystem_provider: ProviderRef,
    pub(crate) root: TaskId,
    pub(crate) root_lease: crate::distributed::WorkLease,
    pub(crate) worker: crate::distributed::Worker,
    task: ComponentIdentity,
    machine: MachineIdentity,
}

pub(crate) struct PreparedFilesystemChild {
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) private: VolumeRef,
    pub(crate) project: VolumeRef,
    pub(crate) seed: crate::fork::ForkSeed,
    pub(crate) report: crate::fork::ForkReport,
    pub(crate) parent_scope: Scope,
    pub(crate) parent_verifier: AuthorityVerifier,
}

mod fixture;
mod fork;
#[cfg(all(not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod task_files;
#[cfg(all(not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod task_fork;

pub(crate) use super::fixture::test_registries;
