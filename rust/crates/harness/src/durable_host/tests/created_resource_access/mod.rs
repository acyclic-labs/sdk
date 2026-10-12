//! Shared real filesystem fixture for durable task consumers.
use super::*;
use crate::conversation::VolumeOperation;
use crate::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    FilesystemResourceCreationProvider, FilesystemSchedulerPayloadStore, FilesystemTaskRuntime,
    WorkspaceMutation, workspace_ref,
};
use crate::resource_preparation::{ResourcePreparationRequest, TaskResourcePreparation};
use crate::resources::{GenerationRef, ProviderRef, WorkspaceRef};
use crate::runtime::{AgentHarness, Bindings, TaskStateProvider};
use crate::workflow::{MachineStatus, MachineTransition, ResumableMachine};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};

pub(crate) type MemoryFilesystem = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;
pub(crate) type NativeCustomerRuntime =
    FilesystemTaskRuntime<MemoryStream, MemoryAuthorityBackend, MemoryObjectBackend>;

pub(crate) struct OriginalRootFilesystemBirth {
    pub(crate) volume: VolumeRef,
    pub(crate) operation: acyclic_fs::OperationId,
    pub(crate) config: acyclic_fs::VolumeConfig,
    pub(crate) initial_generation: acyclic_fs::GenerationId,
    pub(crate) selected_generation: acyclic_fs::GenerationId,
}

pub(crate) struct CreationFixture {
    pub(crate) host: Arc<CoordinatorTaskHost<MemoryStream>>,
    pub(crate) harness: Arc<AgentHarness>,
    pub(crate) filesystem: Arc<MemoryFilesystem>,
    pub(crate) stream: Arc<MemoryStream>,
    pub(crate) filesystem_provider: ProviderRef,
    pub(crate) filesystem_creator:
        Arc<FilesystemResourceCreationProvider<MemoryAuthorityBackend, MemoryObjectBackend>>,
    pub(crate) root_scope: RuntimeScope,
    pub(crate) root: TaskId,
    pub(crate) root_lease: crate::distributed::WorkLease,
    pub(crate) worker: crate::distributed::Worker,
    pub(crate) original_root_births: Option<[OriginalRootFilesystemBirth; 2]>,
    task: ComponentIdentity,
    machine: MachineIdentity,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCustomerFixture {
    pub(crate) creation: CreationFixture,
    pub(crate) runtime: NativeCustomerRuntime,
}

pub(crate) struct PreparedFilesystemChild {
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) private: VolumeRef,
    pub(crate) project: VolumeRef,
    pub(crate) seed: crate::fork::ForkSeed,
    pub(crate) report: crate::fork::ForkReport,
    pub(crate) parent: crate::core::Reducer,
    pub(crate) parent_scope: Scope,
    pub(crate) parent_verifier: AuthorityVerifier,
}

mod fixture;
mod fork;
mod machine;
mod protocol;
#[cfg(not(target_arch = "wasm32"))]
mod roots;
mod source;

mod mail;
