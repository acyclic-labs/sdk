//! Recursive forks over the existing task and provider journals.
use crate::filesystem::{FORK_PREPARE_TASK_COMMAND_KIND, FilesystemForkPreparer};
use crate::{
    AgentId, Capabilities, Error, OperationId, Outcome, Result, TaskId,
    conversation::{Limits, VolumeOperation, VolumeRef},
    fork::{ForkReport, ForkRequest, ResourceRevision},
    resource_preparation::ResourcePreparationRequest,
    resources::GenerationRef,
    runtime::{TaskAdmissionRecord, TaskDefinition},
    workflow::{
        MachineIdentity, MachineStatus, MachineTransition, ResumableMachine, WorkflowCommand,
    },
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
mod machine;
mod preparation;
mod source;
pub use machine::ForkPreparationMachine;
pub use preparation::fork_preparation_request;
pub(in crate::filesystem) use source::fork_request;
pub use source::{recursive_fork_source, restore_recursive_fork_source};
/// Portable fork arguments.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct ForkToolArguments {
    /// Chosen new child agent identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub id: AgentId,
    /// Exact initial child prompt.
    pub msg: String,
    /// Explicit workspace source task; absence selects the current calling task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg_attr(feature = "wasm", tsify(type = "string | undefined"))]
    pub source: Option<TaskId>,
}

impl ForkToolArguments {
    /// Resolves the default without performing IO or granting source authority.
    pub fn source_task(
        &self,
        caller: TaskId,
        limits: Limits,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<TaskId> {
        limits.validate(work)?;
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::executor::json::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        }
        if self.msg.len() as u64 > limits.file_bytes {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work
                .as_deref()
                .is_some_and(|observer| observer.original_native_work().is_some())
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "fork message exceeds admitted file bytes",
                });
            }
            return Err(Error::Invalid(
                "fork message exceeds admitted file bytes".into(),
            ));
        }
        Ok(self.source.unwrap_or(caller))
    }
}

/// Pinned caller-owned workspace selection. References alone confer no rights.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecursiveSourceSnapshot {
    /// Actual current caller, independent of its agent and effect identities.
    pub task: TaskId,
    /// Caller-controlled project and exact selected generation.
    pub project: VolumeRef,
    /// Exact generation, never inferred from a native source or lazy mount.
    pub project_generation: GenerationRef,
    /// Caller scratch; children start empty but retain this real lineage.
    pub private_volume: VolumeRef,
    /// Exact scratch generation selected before allocation.
    pub private_generation: GenerationRef,
}

/// Explicit child allocation choices and inherited-history bounds.
#[derive(Clone, Debug)]
pub struct RecursiveForkConfig {
    /// New child conversation identity.
    pub child: crate::core::Authority,
    /// New isolated child project identity.
    pub child_project: VolumeRef,
    /// New child-owned scratch identity.
    pub child_private: VolumeRef,
    /// Optional separate child-owned unmounted output namespace.
    pub child_result_volume: Option<VolumeRef>,
    /// Admitted caller input/file bounds, separate from retained history count.
    pub limits: Limits,
    /// Maximum logical messages inherited, independently of model projections.
    pub inherited_messages: u64,
    /// Maximum child-owned inherited file and reference descriptor bytes.
    pub inherited_bytes: u64,
    /// Maximum immutable references retained by this capture.
    pub inherited_references: u32,
    /// Finite authenticated archive work and bytes.
    pub history_limits: crate::store::HistoryReadLimits,
}

/// Actual selected-provider binding and canonical immutable request.
pub struct RetainedSourceBinding<A, O> {
    /// Exact current caller snapshot; never a root binding reused for descendants.
    pub preparer: Arc<FilesystemForkPreparer<A, O>>,
    /// Original request to retain before allocation/publication.
    pub request: ForkRequest,
}

/// Explicit finite preparation allowances, charged by the existing provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForkPreparationLimits {
    /// Maximum created resource identities.
    pub resources: u32,
    /// Canonical provider work allowance.
    pub work: u64,
    /// Complete canonical request and provider IO allowance.
    pub bytes: u64,
}
