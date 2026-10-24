//! Native Pi fork controls over the one retained catalog and task journal.
use crate::{
    LocalAuthorityBackend, LocalLazyWorkspace, LocalObjectBackend, PendingSpawn,
    PendingSpawnLifecycle, PluginTaskBinding,
    pi_fork::{
        PiForkCoordinator, PiForkOperations, PiForkPlan, RetainedSpawnCommit, derived,
        original_operation,
    },
    pi_host::{denied, invalid, ready},
    pi_root_provider::PiRootHandle,
    pi_task::{PiTaskExecution, PiTaskInput},
};
use acyclic_fs::{LazySnapshotRef, NativeMaterializationWork};
use acyclic_harness::{
    Error, Result, TaskId,
    durable_host::TaskJournalOwner,
    filesystem::{ForkPreparationLimits, PreparationIo, TaskCommandProgress},
    runtime::TaskContext,
    scheduler::LeaseFence,
    store::HistoryWork,
    workflow::WorkflowCommand,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

pub(crate) const PI_FORK_COMMAND_KIND: &str = "acyclic.pi.fork";
pub(crate) const PI_OBSERVE_SOURCE_COMMAND_KIND: &str = "acyclic.pi.observe-source";

#[derive(Clone, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiForkCommand {
    pub(crate) commit: RetainedSpawnCommit,
    pub(crate) root_source: Option<LazySnapshotRef>,
}

#[derive(Clone, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiObserveSourceCommand {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
}

#[derive(Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservedSource {
    pub(crate) caller: TaskId,
    pub(crate) sdk_operation: String,
    pub(crate) root_key: String,
    pub(crate) snapshot: LazySnapshotRef,
}

pub(crate) const OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES + 240;

#[derive(Serialize)]
struct PiPreparedReply<'a> {
    task: TaskId,
    binding: &'a crate::PiTaskBinding,
    state: &'static str,
}

#[derive(Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootSource<S = String> {
    pub(crate) snapshot: LazySnapshotRef,
    pub(crate) root_key: S,
}

const FORK_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_fork::FORK_PLAN_DIAGNOSTIC_LABEL_BYTES + 42;
const OBSERVE_COMMAND_DIAGNOSTIC_LABEL_BYTES: u64 = OBSERVED_SOURCE_DIAGNOSTIC_LABEL_BYTES + 26;
const PENDING_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES + 444;

impl crate::pi_host::PiControlHost<'_> {
    /// The actor durably saves these existing Preparing records before another pass.
    pub(crate) fn pending_to_save(
        &self,
    ) -> Result<Option<(PendingSpawn, acyclic_fs::NativeAllocation)>> {
        Ok(self
            .pending
            .lock()
            .map_err(|_| invalid("Pi pending control lock poisoned"))?
            .take())
    }

    pub(crate) fn coordinator(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<PiForkCoordinator<'_>> {
        PiForkCoordinator::new(
            self.runtime.clone(),
            self.filesystem.clone(),
            self.stream.clone(),
            self.resources,
            self.protected.clone(),
            work,
        )
    }

    /// The caller's task context bound to its own output: the root's provider-prepared
    /// root output, or a child's created private scratch.
    pub(crate) async fn context(
        &self,
        shared: Arc<TaskContext>,
        fence: &LeaseFence,
        work: &NativeMaterializationWork,
    ) -> Result<(
        Arc<TaskContext>,
        Arc<TaskJournalOwner<LocalStream>>,
        PiTaskInput,
        acyclic_fs::NativeAllocation,
    )> {
        let caller = self.caller(shared.as_ref(), fence, work).await?;
        let (owner, input, _) = &caller;
        let task = owner.task_id();
        let shared = if task == self.protected.policy().root_task {
            let PiTaskExecution::Host { root, .. } = &input.execution else {
                return Err(denied("Pi original root input substituted a child"));
            };
            let preparation = crate::pi_root_provider::load_original_preparation_with_native_work(
                &self.protected,
                &self.filesystem,
                &input.binding,
                root,
                shared,
                work,
            )
            .await?;
            crate::pi_root_provider::bind_root_output_context(
                owner,
                self.protected.clone(),
                self.resources,
                self.filesystem.clone(),
                self.stream.clone(),
                preparation,
                work,
            )
            .await?
        } else {
            shared
                .with_output_volume(&input.binding.scratch.volume)
                .await?
        };
        Ok((shared, caller.0, caller.1, caller.2))
    }

    pub(super) fn decode<
        T: serde::de::DeserializeOwned + acyclic_harness::HeapLayout,
        S: Serialize,
    >(
        &self,
        value: &S,
        label_bytes: u64,
        work: &NativeMaterializationWork,
    ) -> Result<(T, acyclic_fs::NativeAllocation)> {
        let maximum = self.protected.policy().maximum_payload_bytes;
        let bytes = crate::pi_root_provider::canonical_pi_value(value, maximum, work)?;
        let (value, custody) = crate::pi_root_provider::guarded_pi_decode(
            &bytes.0,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            label_bytes,
            work,
        )?;
        Ok((value, custody))
    }
}

pub(crate) fn native_lazy_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    match source {
        acyclic_fs::LazyWorkspaceError::NativeWork(cause) => cause.into(),
        source => source.into(),
    }
}

mod execution;
mod observation;
mod reservation;
mod selection;
mod snapshot;
pub(crate) use snapshot::freeze_child_source;
