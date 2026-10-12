//! First-party original-root allocation through the stock task effect lifecycle.

use super::{
    LocalGeneration, LocalLazyWorkspace, PiScratchCreation, RootCreation, ServiceResources,
    SharedPhysicalRoot, SharedRootAdmission, SharedRootRegistry,
    pi_owner::PiOwnerConfig,
    pi_root::PiRootIntent,
    pi_root_allocation::PiRootAllocation,
    pi_root_inputs::{
        PiRootPreparation, PiRootRequest, ROOT_ALLOCATION_EFFECT, ROOT_ALLOCATION_PROVIDER,
        ROOT_INPUT_DIRECTORY,
    },
};
use acyclic_fs::demand::{DemandSource, SourceReference};
use acyclic_fs::{
    CancellationToken, LazyWorkspaceStore, Lifecycle, NativeAllocation, NativeMaterializationWork,
    NativeTreeMaterializationError, VolumeConfig, WorkBudget, WorkCounters, WorkError,
};
use acyclic_harness::store::native_work_io;
use acyclic_harness::wire::ErrorCode;
use acyclic_harness::{
    EffectAttemptId, EffectId, Error, IdempotencyKey, JsonBounds, JsonCodecCharge, OperationId,
    Result, TaskId, canonical_json_bytes_bounded,
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::{EffectGuarantee, EffectState, EffectStatus},
    durable_host::TaskJournalOwner,
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    json_bytes_bounded, json_from_slice_bounded, json_value_bounded,
    resources::GenerationRef,
    runtime::TaskContext,
    store::{HistoryReadLimits, HistoryReader},
};
use acyclic_harness::{
    JsonValueSchema, json_typed_diagnostic_owned_bytes, json_value_decode_owned_bytes,
    sdk_json_diagnostic_bytes,
};
use acyclic_stream::{BoxProviderFuture, LocalStream};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
use tokio::sync::Mutex;

pub(crate) const OPERATOR_INPUT_DIRECTORY: &str = "pi-operator-inputs";

use crate::pi_native_host::PiNativeFilesystem as Host;
mod allocation;
mod codec;
mod effects;
mod inputs;
mod preparation;
mod sources;

pub(crate) use codec::{
    canonical_pi_value, digest_pi_value, guarded_pi_bytes, guarded_pi_decode, guarded_pi_value,
    native_buffer_codec_charge, pi_serde_diagnostic_labels_bytes, read_pi_input_with_native_work,
};
use inputs::{
    NativeInputCoordinates, observe_pi_input_at, publish_pi_input_with_native_coordinates,
    read_root_artifact_with_native_work,
};
pub(crate) use inputs::{
    load_original_preparation_with_native_work, observe_pi_input,
    observe_pi_root_input_with_native_work, stage_pi_input, stage_pi_operator_input,
    stage_pi_root_input_with_native_work,
};
pub(crate) use preparation::bind_root_output_context_with_native_work;
pub(crate) use sources::{
    guard_root_preparation_source_with_native_work, prove_root_project_source_with_native_work,
};

/// Inline failure prefix: constructor denial never allocates a counter carrier.

/// Actual handles returned by dispatch, never reconstructed from route IDs.
pub(crate) struct PiRootHandle {
    pub(crate) workspace: Arc<LocalLazyWorkspace>,
    pub(crate) physical: Arc<SharedPhysicalRoot>,
    _custody: NativeAllocation,
}

impl PiRootHandle {
    /// Cold restoration transfers actual copied-key custody and admits the compiled entry payload before its caller grows the root index.
    pub(crate) fn from_observed_with_native_work(
        workspace: LocalLazyWorkspace,
        physical: Arc<SharedPhysicalRoot>,
        key: (String, NativeAllocation),
        work: &NativeMaterializationWork,
    ) -> Result<(String, Self)> {
        let mut key = (key.0, key.1);
        if !key.1.work().shares_ledger(work) {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "root index key custody differs from original source work",
            });
        }
        let mut entry = work.claim(
            (std::mem::size_of::<(String, Self)>()
                + std::mem::size_of::<LocalLazyWorkspace>()
                + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        key.1.absorb(&mut entry)?;
        Ok((
            key.0,
            Self {
                workspace: Arc::new(workspace),
                physical,
                _custody: key.1,
            },
        ))
    }
}

// Original Root request/allocation/attachment and their closed SDK leaves.
pub(crate) const ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES: u64 =
    acyclic_harness::preparation_serde_diagnostic_label_bytes()
        + pi_serde_diagnostic_labels_bytes(&[
            "PiRootRequest",
            "binding",
            "root",
            "PiTaskBinding",
            "native_session_id",
            "task_id",
            "agent_id",
            "project_volumes",
            "scratch",
            "results",
            "PiScratchBinding",
            "volume",
            "allocation",
            "PiScratchAllocation",
            "state",
            "creation",
            "preparing",
            "created",
            "PiScratchCreation",
            "workspace_id",
            "generation",
            "PiRootIntent",
            "kind",
            "scratch_only",
            "project",
            "context_id",
            "root_id",
            "paths",
            "PiRootAllocation",
            "roots",
            "RootCreation",
            "repository_workspace_id",
            "source_identity",
            "source_epoch",
            "native_root_identity",
            "task",
            "command",
            "effect",
            "attempt",
            "digest",
            "provider",
            "effect_kind",
            "native_path",
            "source",
            "native_identity",
            "budget",
            "SourceReference",
            "identity",
            "epoch",
            "TaskEffectPlan",
            "guarantee",
            "request",
            "result_schema",
            "exactly_once",
            "at_least_once",
            "at_most_once",
            "ProviderRefWire",
            "namespace",
            "family",
            "version",
            "ResourceRefWire",
            "key",
            "workspace",
            "generation",
            "artifact",
            "sandbox",
            "checkpoint",
            "stream",
            "context",
            "run",
            "VolumeRefWire",
            "id",
            "class",
            "owner",
            "agent_private",
            "session_shared",
            "agent",
            "session",
            "FileRefWire",
            "path",
            "descriptor",
            "display_name",
            "FileDescriptorWire",
            "sha256",
            "byte_length",
            "media_type",
            "SHA-256 digest must contain exactly 32 bytes",
            "file byte length exceeds cross-language precision",
            "media type is invalid",
            "WorkCounters",
            "authorityRecordsRead",
            "authorityRecordsAppended",
            "authorityBytesRead",
            "authorityBytesWritten",
            "objectProbes",
            "backendReadOperations",
            "backendWriteOperations",
            "durabilityOperations",
            "pageReads",
            "pageWrites",
            "objectBytesRead",
            "objectBytesWritten",
            "bytesHashed",
            "bytesCopied",
            "bytesEncoded",
            "sourceBytesRead",
            "sourcePathComponents",
            "sourceEntriesVisited",
            "outputBytes",
            "itemsExamined",
            "itemsReturned",
            "allocationOperations",
            "peakAllocationBytes",
            "materializations",
        ]);

/// Finite allowance for native attachment plus first-party metadata processing.
pub(crate) fn allocation_budget() -> WorkBudget {
    WorkBudget {
        items_examined: 131_072,
        items_returned: 65_536,
        allocation_operations: 65_536,
        ..super::native_root_source::budget()
    }
}

fn same_allocation(left: &PiRootAllocation, right: &PiRootAllocation) -> bool {
    left.scratch == right.scratch
        && left.roots.keys().eq(right.roots.keys())
        && left.roots.iter().all(|(key, root)| {
            let other = &right.roots[key];
            root.repository_workspace_id == other.repository_workspace_id
                && root.source_identity == other.source_identity
                && root.source_epoch == other.source_epoch
                && root.native_root_identity == other.native_root_identity
        })
}

struct ProviderState {
    dispatched: bool,
    handles: BTreeMap<String, PiRootHandle>,
}

pub(crate) struct PiRootProvider {
    owner: Arc<PiOwnerConfig>,
    fs: crate::LocalFs,
    store: crate::LocalCoreStateStore,
    shared_roots: SharedRootRegistry,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    pub(crate) preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<LocalStream>>>,
    original: NativeMaterializationWork,
    state: Mutex<ProviderState>,
    creation_cancelled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fault: std::sync::atomic::AtomicU8,
    _custody: NativeAllocation,
}

pub(crate) fn service_resources_with_native_work(
    resources: &ServiceResources,
    work: &NativeMaterializationWork,
) -> Result<(ServiceResources, NativeAllocation)> {
    let data = work.copy_path(&resources.data)?;
    let mut binary = work.display(&resources.binary_identity)?;
    let mut instance = work.display(&resources.instance_id)?;
    let mut owned = (
        ServiceResources {
            data: data.0,
            fs: resources.fs.clone(),
            store: resources.store.clone(),
            shared_roots: resources.shared_roots.clone(),
            binary_identity: binary.0,
            instance_id: instance.0,
            private_state_root: resources.private_state_root.clone(),
        },
        data.1,
    );
    owned.1.absorb(&mut binary.1)?;
    owned.1.absorb(&mut instance.1)?;
    Ok((owned.0, owned.1))
}

#[allow(
    clippy::too_many_arguments,
    reason = "original protected owner and issued context remain explicit"
)]
pub(crate) fn root_provider(
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    preparation: PiRootPreparation,
    journal_owner: Option<Arc<TaskJournalOwner<LocalStream>>>,
    original: NativeMaterializationWork,
) -> Result<Arc<PiRootProvider>> {
    original.admit(WorkCounters::UNCHARGED)?;
    if !original.budget().has_finite_allowances() {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Invalid,
            reason: "root allocation requires finite original work allowances",
        });
    }
    let request = &preparation.request;
    if request.binding.task_id != owner.policy().root_task
        || request.binding.agent_id != owner.policy().agent
        || journal_owner.as_ref().is_some_and(|current| {
            current.task_id() != request.binding.task_id
                || preparation.context.durable_task_id() != Some(request.binding.task_id)
        })
        || !matches!(
            request.binding.scratch.allocation,
            super::PiScratchAllocation::Preparing
        )
        || preparation.plan.provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan.effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan.guarantee != EffectGuarantee::AtMostOnce
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "root allocator differs from its original admission and issued context",
        });
    }
    let authority = owner.authority(&original)?;
    for grant in ["effect:run", "conversation:bind"] {
        if !authority.scope.grants().contains(grant) {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "original root allocation scope lacks a required capability",
            });
        }
    }
    let provider_grant =
        original.display(&format_args!("effect:provider:{ROOT_ALLOCATION_PROVIDER}"))?;
    if !authority.scope.grants().contains(&provider_grant.0) {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "original root allocation provider grant is absent",
        });
    }
    for volume in request
        .binding
        .project_volumes
        .values()
        .chain(std::iter::once(&request.binding.scratch.volume))
    {
        let grant = {
            let mut io = native_work_io(&original)?;
            let value = volume.capability(VolumeOperation::Read, &mut Some(&mut io))?;
            (value, io.into_custody())
        };
        if volume.provider() != owner.policy().journal.provider()
            || !authority.scope.grants().contains(&grant.0)
        {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "original root volume provider or read grant differs",
            });
        }
    }
    let grant = {
        let mut io = native_work_io(&original)?;
        let value = request
            .binding
            .scratch
            .volume
            .capability(VolumeOperation::Write, &mut Some(&mut io))?;
        (value, io.into_custody())
    };
    if !authority.scope.grants().contains(&grant.0) {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "original root scratch write grant is absent",
        });
    }
    drop(grant);
    drop(authority);
    let custody = original.claim(
        std::mem::size_of::<PiRootProvider>() as u64,
        0,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    original.charge(WorkCounters {
        allocation_operations: 1,
        ..WorkCounters::UNCHARGED
    })?;
    Ok(Arc::new(PiRootProvider {
        owner,
        fs: resources.fs.clone(),
        store: resources.store.clone(),
        shared_roots: resources.shared_roots.clone(),
        filesystem,
        stream,
        preparation,
        journal_owner,
        original,
        state: Mutex::new(ProviderState {
            dispatched: false,
            handles: BTreeMap::new(),
        }),
        creation_cancelled: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        fault: std::sync::atomic::AtomicU8::new(0),
        _custody: custody,
    }))
}
#[cfg(test)]
#[path = "../pi_root_provider_tests.rs"]
mod tests;
