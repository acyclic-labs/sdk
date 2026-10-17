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
use crate::workflow::ResumableMachine;
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

pub(crate) fn test_registries(
    machine: Arc<dyn ResumableMachine>,
    input: Value,
    output: Value,
    _native_io: Option<&crate::filesystem::PreparationIo>,
) -> Result<(
    ComponentIdentity,
    MachineIdentity,
    crate::runtime::TaskRegistry,
    MachineRegistry,
)> {
    let identity = machine.identity().clone();
    let definition = {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) =
            _native_io.and_then(crate::filesystem::PreparationIo::original_native_work)
        {
            let (input, mut custody) = crate::tool::clone_value(&input, None, work)?;
            let (output, mut output_custody) = crate::tool::clone_value(&output, None, work)?;
            custody.absorb(&mut output_custody)?;
            crate::runtime::TaskDefinition::<Value, Value>::resumable_with_native_work(
                machine.clone(),
                input,
                output,
                custody,
                work,
            )?
        } else {
            crate::runtime::TaskDefinition::<Value, Value>::resumable(
                machine.clone(),
                input,
                output,
            )?
        }
        #[cfg(target_arch = "wasm32")]
        crate::runtime::TaskDefinition::<Value, Value>::resumable(machine.clone(), input, output)?
    };
    let task = definition.identity().clone();
    let mut tasks = crate::runtime::TaskRegistry::default();
    tasks.register(definition)?;
    let mut machines = MachineRegistry::default();
    machines.register(machine, &mut None)?;
    Ok((task, identity, tasks, machines))
}
