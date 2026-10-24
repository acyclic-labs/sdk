//! Recursive Pi admission over the original task and provider journals.

use crate::{
    LocalAuthorityBackend, LocalLazyWorkspace, LocalObjectBackend, PiScratchAllocation,
    PiScratchCreation, PiTaskBinding, ServiceResources,
    pi_native::PiNativeLaunchIntent,
    pi_native_host::PiNativeFilesystem as Filesystem,
    pi_owner::PiOwnerConfig,
    pi_runtime::PiTaskRuntime as Runtime,
    pi_task::{NATIVE_LAUNCH_TASK_COMMAND_KIND, PiTaskExecution, PiTaskInput},
};
use acyclic_fs::{
    CancellationToken, Digest, GenerationId, LazySnapshotRef, NativeMaterializationWork,
};
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::{
    AgentId, Capabilities, Error, OperationId, Outcome, Result, TaskId,
    conversation::{FileRef, VolumeRef},
    core::{AggregateKind, Authority, Reducer, SchemaRegistry, Snapshot},
    distributed::{WorkPull, Worker},
    durable_host::TaskJournalOwner,
    filesystem::{FORK_PREPARE_TASK_COMMAND_KIND, ForkPreparationLimits},
    fork::{ForkReport, ForkRequest},
    resource_preparation::ResourcePreparationRequest,
    runtime::{TaskAdmissionRecord, task_id_for_admission},
    scheduler::{LeaseFence, ResourceRequest},
    source_authorization::{
        ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider,
    },
    store::{HistoryIo, HistoryReadLimits, StreamAggregate},
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

/// Constructs the SDK snapshot from its selected volume and native cut.
pub(crate) fn project_snapshot(
    project: &VolumeRef,
    frozen: LazySnapshotRef,
    maximum: u64,
    work: &NativeMaterializationWork,
) -> Result<(ProjectSourceSnapshot, acyclic_fs::NativeAllocation)> {
    let slot = work.claim(
        std::mem::size_of::<ProjectSourceSnapshot>() as u64,
        0,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let native = crate::pi_root_provider::guarded_pi_value(&frozen, maximum, work)?;
    let generation = acyclic_harness::resources::GenerationRef::new(
        project.provider(),
        frozen.authored_generation.digest().as_bytes(),
        None,
        Some(work),
    )?;
    let mut io = acyclic_harness::store::HistoryWork::new(
        acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
    );
    let project = project.copy(&mut Some(&mut io))?;
    let mut owned = (
        ProjectSourceSnapshot {
            project,
            generation: generation.0,
            native_snapshot: Some(native.0),
        },
        slot,
        [
            native.1,
            generation
                .1
                .ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?,
            io.into_custody()?,
        ],
    );
    for allocation in &mut owned.2 {
        owned.1.absorb(allocation)?;
    }
    Ok((owned.0, owned.1))
}

const SDK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/sdk-operation/v1\0";
const FORK_OPERATION_DOMAIN: &[u8] = b"acyclic.pi/fork-operation/v1\0";

pub(crate) fn original_operation(
    root: TaskId,
    caller: TaskId,
    sdk_tool_call_id: &str,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    operation_id(
        SDK_OPERATION_DOMAIN,
        &[root.into_bytes(), caller.into_bytes()],
        sdk_tool_call_id,
        work,
    )
}

pub(crate) fn derived(
    original: OperationId,
    role: &str,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    operation_id(FORK_OPERATION_DOMAIN, &[original.into_bytes()], role, work)
}

fn operation_id(
    domain: &[u8],
    identities: &[[u8; 16]],
    label: &str,
    work: &NativeMaterializationWork,
) -> Result<OperationId> {
    let bytes = (identities.len() as u64)
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(domain.len() as u64))
        .and_then(|bytes| bytes.checked_add(8))
        .and_then(|bytes| bytes.checked_add(label.len() as u64))
        .ok_or(Error::NativeWork {
            cause: acyclic_fs::WorkError::Overflow.into(),
            custody: None,
        })?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_hashed: bytes,
        ..acyclic_fs::WorkCounters::default()
    })?;
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    for identity in identities {
        hash.update(identity);
    }
    hash.update(&(label.len() as u64).to_le_bytes());
    hash.update(label.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(id))
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout,
)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkOperations {
    pub(crate) child: OperationId,
    pub(crate) preparation: OperationId,
    pub(crate) fork: OperationId,
    pub(crate) native: OperationId,
    pub(crate) agent: AgentId,
}
impl PiForkOperations {
    pub(crate) fn new(original: OperationId, work: &NativeMaterializationWork) -> Result<Self> {
        Ok(Self {
            child: derived(original, "child-task", work)?,
            preparation: derived(original, "preparation-task", work)?,
            fork: derived(original, "fork", work)?,
            native: derived(original, "native-effect", work)?,
            agent: AgentId::from_bytes(derived(original, "child-agent", work)?.into_bytes()),
        })
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetainedSpawnCommit {
    pub(crate) sdk_operation: String,
    pub(crate) alias: String,
    pub(crate) prompt: String,
    pub(crate) caller: TaskId,
    pub(crate) source_task: TaskId,
    pub(crate) child: PiTaskBinding,
    pub(crate) launch: PiNativeLaunchIntent,
}

#[derive(Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkPlan {
    pub(crate) original: OperationId,
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) preparation: TaskAdmissionRecord,
    pub(crate) request: ResourcePreparationRequest,
    pub(crate) frozen: LazySnapshotRef,
    #[serde(skip)]
    _input_allocations: Option<acyclic_fs::NativeAllocation>,
    /// The digest of the publication this plan was read back from; a plan
    /// still being built has none.
    #[serde(skip)]
    published: Option<blake3::Hash>,
}

impl PartialEq for PiForkPlan {
    fn eq(&self, other: &Self) -> bool {
        self.original == other.original
            && self.commit == other.commit
            && self.preparation == other.preparation
            && self.request == other.request
            && self.frozen == other.frozen
    }
}

impl PiForkPlan {
    fn history_limits(&self) -> HistoryReadLimits {
        HistoryReadLimits {
            maximum_events: u32::try_from(self.request.maximum_work).unwrap_or(u32::MAX),
            maximum_bytes: self.request.maximum_bytes,
        }
    }
}

pub(crate) const FORK_PLAN_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
        + crate::pi_native::PI_NATIVE_LAUNCH_DIAGNOSTIC_LABEL_BYTES
        + 251;

pub(crate) struct PreparedPiFork {
    pub(crate) binding: PiTaskBinding,
    pub(crate) report: ForkReport,
    pub(crate) admission: TaskAdmissionRecord,
    pub(crate) frozen_source: LazySnapshotRef,
    pub(crate) native_lazy: LazySnapshotRef,
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) _metadata_custody: acyclic_fs::NativeAllocation,
}

impl PreparedPiFork {
    pub(crate) fn into_native_shared(mut self) -> Result<Arc<Self>> {
        let mut slot = {
            let claim_bytes =
                (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64;
            self._metadata_custody.work().claim(
                claim_bytes,
                u64::from(claim_bytes != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )
        }?;
        self._metadata_custody.absorb(&mut slot)?;
        Ok(Arc::new(self))
    }
}

pub(crate) struct PiForkCoordinator<'a> {
    runtime: Arc<Runtime>,
    filesystem: Arc<Filesystem>,
    stream: std::sync::Arc<LocalStream>,
    resources: &'a ServiceResources,
    owner: Arc<PiOwnerConfig>,
    work: NativeMaterializationWork,
}
impl<'a> PiForkCoordinator<'a> {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        filesystem: Arc<Filesystem>,
        stream: std::sync::Arc<LocalStream>,
        resources: &'a ServiceResources,
        owner: Arc<PiOwnerConfig>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !work.budget().has_finite_allowances() {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi fork requires finite original work allowances",
            });
        }
        runtime.verify_owner_with_native_work(&owner, work)?;
        Ok(Self {
            runtime,
            filesystem,
            stream,
            resources,
            owner,
            work: work.clone(),
        })
    }
}

/// `RootService`'s explicit same-session selected-PROJECT policy.
struct PiProjectSourcePolicy<'a> {
    coordinator: &'a PiForkCoordinator<'a>,
    source: Arc<LocalLazyWorkspace>,
    frozen: LazySnapshotRef,
    native_snapshot: &'a serde_json::Value,
    caller_context: &'a acyclic_harness::runtime::TaskContext,
    caller_owner: &'a Arc<TaskJournalOwner<LocalStream>>,
}

#[cfg(test)]
#[path = "../pi_fork_tests.rs"]
pub(crate) mod tests;

fn task_id_text_matches(task: TaskId, expected: &str) -> bool {
    uuid::Uuid::from_bytes(task.into_bytes())
        .hyphenated()
        .encode_lower(&mut [0; 36])
        == expected
}

mod allocation;
mod authority;
mod inputs;
mod lifecycle;
mod preparation;
mod source_policy;
mod validated;
mod validation;

pub(crate) use validated::{ValidatedForkPlans, ValidatedPlanKey};
