//! Native Pi commands composed with the original task, effect and filesystem owners.

use crate::{
    LocalAuthorityBackend, LocalLazyWorkspace, LocalObjectBackend, LocalSingleMount,
    PiScratchAllocation, PiTaskBinding, ServiceResources, pi_caller,
    pi_fork::{self, PiForkCoordinator, PiForkOperations, PreparedPiFork},
    pi_native::{self, PiNativeLaunchIntent, PiNativeRoot},
    pi_owner::PiOwnerConfig,
    pi_runtime::PiTaskRuntime,
    pi_task::{NATIVE_LAUNCH_TASK_COMMAND_KIND, PiTaskExecution, PiTaskInput},
};
use acyclic_fs::NativeAllocation;
use acyclic_fs::native_mount::NativeMountSession;
use acyclic_fs::{
    CancellationToken, Digest, GenerationId, LazySnapshotRef, MeasuredResult,
    NativeMaterializationWork, OperationReceipt, PublicationPermit, WorkBudget, WorkCounters,
};
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::store::HistoryIo;
use acyclic_harness::{
    Error, JsonValueSchema, OperationId, Outcome, Result, TaskId,
    conversation::VolumeOperation,
    core::{EffectStatus, SchemaRegistry},
    distributed::{WorkPull, Worker},
    durable_host::{TaskJournalOwner, task_interaction_id},
    effect_host::{ConversationEffectHost, TaskEffectPlan},
    effects::EffectRegistry,
    filesystem::{
        BorrowedNativeVolumeBinding, FilesystemExecutionJournal, FilesystemHost,
        NativeProcessAuthority, NativeProcessProvider, NativePublicationCleanup,
        NativeRecordedPreparation, NativeVolumeNamespace, NativeVolumeView,
        RecursiveSourceSnapshot, TaskCommandHost, TaskCommandProgress, TaskSourceInspection,
        TaskWorkerAttempt, workspace_ref,
    },
    fork::ForkReport,
    resources::GenerationRef,
    runtime::{ContentBindings, DurableTaskHost, TaskAdmissionRecord, TaskContext},
    scheduler::{LeaseFence, ResourceRequest},
    store::{HistoryReadLimits, HistoryReader},
    workflow::WorkflowCommand,
};
use acyclic_stream::{BoxProviderFuture, LocalStream};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

pub(crate) type PiNativeFilesystem = FilesystemHost<LocalAuthorityBackend, LocalObjectBackend>;
pub(crate) type PiNativeLiveView = NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>;

mod authority;
mod execution;
mod lifecycle;
mod preparation;
mod sources;
mod state;

use sources::PiNativeColdLocator;
pub(crate) use sources::PiNativeLiveSource;
use state::*;

pub(crate) struct PiNativeCommandHost {
    runtime: Arc<PiTaskRuntime>,
    filesystem: Arc<PiNativeFilesystem>,
    stream: std::sync::Arc<LocalStream>,
    resources: ServiceResources,
    pub(crate) protected: Arc<PiOwnerConfig>,
    entries: RwLock<BTreeMap<TaskId, Arc<NativeEntry>>>,
    // Original factory metadata stays funded while this reusable host escapes.
    _factory_custody: NativeAllocation,
}

impl PiNativeCommandHost {
    pub(crate) fn new(
        runtime: Arc<PiTaskRuntime>,
        filesystem: Arc<PiNativeFilesystem>,
        stream: std::sync::Arc<LocalStream>,
        resources: &ServiceResources,
        protected: PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        runtime.verify_owner_with_native_work(&protected, work)?;
        protected.native_runtime()?.validate(work)?;
        let protected = protected.into_native_shared(work)?;
        let mut resources =
            crate::pi_root_provider::service_resources_with_native_work(resources, work)?;
        let custody = work.claim(
            std::mem::size_of::<Self>() as u64,
            u64::from((std::mem::size_of::<Self>() as u64) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut host = Self {
            runtime,
            filesystem,
            stream,
            resources: resources.0,
            protected,
            entries: RwLock::new(BTreeMap::new()),
            _factory_custody: custody,
        };
        host._factory_custody.absorb(&mut resources.1)?;
        Ok(Arc::new(host))
    }
}

fn validate_child_binding(actual: &PiTaskBinding, admitted: &PiTaskBinding) -> Result<()> {
    if actual.native_session_id != admitted.native_session_id
        || actual.task_id != admitted.task_id
        || actual.agent_id != admitted.agent_id
        || actual.project_volumes != admitted.project_volumes
        || actual.scratch.volume != admitted.scratch.volume
        || actual.results.as_ref().map(|result| &result.volume)
            != admitted.results.as_ref().map(|result| &result.volume)
    {
        return Err(unauthorized(
            "Pi native binding differs from its original admitted volume selection",
        ));
    }
    Ok(())
}

fn generation(file: &acyclic_harness::conversation::FileRef) -> Result<GenerationRef> {
    let mut digest = [0; 32];
    hex::decode_to_slice(file.version(), &mut digest)
        .map_err(|_| Error::Invalid("Pi session lacks its exact immutable generation".into()))?;
    GenerationRef::new(file.volume().provider().clone(), digest.into(), None)
}

fn history_limits(context: &TaskContext) -> Result<HistoryReadLimits> {
    let limits = HistoryReadLimits {
        maximum_events: context.scope().limits().context_messages,
        maximum_bytes: context
            .scope()
            .limits()
            .file_bytes
            .min(crate::MAXIMUM_ADAPTER_STATE_BYTES),
    };
    if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
        return Err(unauthorized(
            "Pi native effect history has no original finite read allowance",
        ));
    }
    Ok(limits)
}

fn view_volume_share(budget: WorkBudget, volumes: u64) -> Result<WorkBudget> {
    if volumes == 0 || !budget.has_finite_allowances() {
        return Err(Error::Invalid(
            "Pi native namespace requires an original finite nonempty allowance".into(),
        ));
    }
    macro_rules! share { ($($field:ident),* $(,)?) => { Ok(WorkBudget {
        $($field: budget.$field / volumes,)*
    }) }; }
    share!(
        authority_records_read,
        authority_records_appended,
        authority_bytes_read,
        authority_bytes_written,
        object_probes,
        backend_read_operations,
        backend_write_operations,
        durability_operations,
        page_reads,
        page_writes,
        object_bytes_read,
        object_bytes_written,
        bytes_hashed,
        bytes_copied,
        bytes_encoded,
        source_bytes_read,
        source_path_components,
        source_entries_visited,
        output_bytes,
        items_examined,
        items_returned,
        allocation_operations,
        peak_allocation_bytes,
        materializations
    )
}

fn accept<T>(
    result: MeasuredResult<OperationReceipt<T>, Error>,
    meter: &NativeMaterializationWork,
) -> Result<T> {
    match result {
        Ok(receipt) => {
            meter.spent(receipt.work)?;
            remaining(meter)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            meter.spent(*failure.work)?;
            Err(failure.error)
        }
    }
}

fn remaining(meter: &NativeMaterializationWork) -> Result<WorkBudget> {
    meter.remaining().map_err(acyclic_harness::Error::from)
}
fn native_construction(
    (cause, work): (acyclic_fs::NativeTreeMaterializationError, WorkCounters),
) -> Error {
    Error::NativeConstruction { cause, work }
}
fn unauthorized(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}
