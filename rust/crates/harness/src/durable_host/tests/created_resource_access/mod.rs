//! Canonical resource controls use real Stream/Filesystem allocation and IO.

use super::*;
use crate::conversation::VolumeOperation;
use crate::filesystem::{
    FilesystemContentVerifier, FilesystemForkPreparer, FilesystemHost,
    FilesystemResourceCreationProvider, FilesystemSchedulerPayloadStore, WorkspaceMutation,
    workspace_ref,
};
use crate::resource_preparation::{
    CreatedResourceOperation, CreatedResourceReceipt, ResourceCreationOrigin,
    ResourceCreationProofRef, ResourceCreationProvider, ResourcePreparationRequest,
    TaskResourcePreparation, created_resource_capability,
};
use crate::resources::{GenerationRef, ResourceKind, ResourceRef, WorkspaceRef};
use crate::runtime::TaskStateProvider;
use crate::runtime::{AgentHarness, Bindings};
use acyclic_fs::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};

pub(crate) type MemoryFilesystem = FilesystemHost<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(not(target_arch = "wasm32"))]
fn metadata_control_budget() -> acyclic_fs::WorkBudget {
    acyclic_fs::WorkBudget {
        authority_records_read: 512,
        authority_records_appended: 128,
        authority_bytes_read: 1_048_576,
        authority_bytes_written: 1_048_576,
        object_probes: 512,
        backend_read_operations: 512,
        backend_write_operations: 128,
        durability_operations: 128,
        page_reads: 512,
        page_writes: 128,
        object_bytes_read: 1_048_576,
        object_bytes_written: 1_048_576,
        bytes_hashed: 4_194_304,
        bytes_copied: 4_194_304,
        bytes_encoded: 4_194_304,
        source_bytes_read: 1_048_576,
        source_path_components: 128,
        source_entries_visited: 512,
        output_bytes: 1_048_576,
        items_examined: 1024,
        items_returned: 512,
        allocation_operations: 4096,
        peak_allocation_bytes: 4_194_304,
        materializations: 128,
    }
}

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
    pub(crate) stream: std::sync::Arc<MemoryStream>,
    pub(crate) filesystem_provider: ProviderRef,
    pub(crate) filesystem_creator:
        Arc<FilesystemResourceCreationProvider<MemoryAuthorityBackend, MemoryObjectBackend>>,
    creator: Arc<SandboxCreator>,
    pub(crate) root_scope: RuntimeScope,
    task: crate::registry::ComponentIdentity,
    machine: MachineIdentity,
    schema: Value,
    pub(crate) root: TaskId,
    pub(crate) root_lease: crate::distributed::WorkLease,
    pub(crate) worker: crate::distributed::Worker,
    pub(crate) original_root_births: Option<[OriginalRootFilesystemBirth; 2]>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativeCustomerRuntime = crate::filesystem::FilesystemTaskRuntime<
    MemoryStream,
    MemoryAuthorityBackend,
    MemoryObjectBackend,
>;

/// One configured real customer, shared by native transport and stock controls.
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
    pub(crate) parent_scope: crate::core::Scope,
    pub(crate) parent_verifier: crate::core::AuthorityVerifier,
}

#[cfg(not(target_arch = "wasm32"))]
mod creator_native;
mod recursive_lifecycle;

mod mailbox_parcels;
mod source_authorization;

mod content;
mod creator;
mod custom;
mod fixture;
mod machine;
mod native_lineage;
mod native_root;
mod native_startup;
use creator::SandboxCreator;
use machine::CreatedControlMachine;
