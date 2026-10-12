//! Browser bindings over the ordinary task runtime and its real providers.
use crate::runtime::TaskSpawner as _;

use super::{
    WasmLimitsInput, WasmReducer, WasmTaskRunLimitsInput, from_js, js_error, key_bytes,
    wasm_limits, wasm_run_limits,
};
use crate::runtime::TaskStateProvider;
use crate::{
    Admission, Error, OperationId, Result, TaskId,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::Scope,
    distributed::{SchedulerPayloadStore, WorkLease, WorkPull, Worker},
    filesystem::{
        FilesystemHost, FilesystemSchedulerPayloadStore, FilesystemTaskRuntime, StockTurnMachine,
        TaskWakeCursor, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    model::{Model, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskDefinition, TaskRegistry},
    scheduler::SessionLimits,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
    workflow::{MachineIdentity, MachineRegistry, MachineTransition, ResumableMachine},
};
use acyclic_fs::{
    EmbeddedCapabilities, Fs,
    browser::{IndexedDbAuthorityStore, IndexedDbObjectStore},
};
use acyclic_stream::{BrowserStream, BrowserStreamLimits, MemoryLimits};
use js_sys::{Function, Promise};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tsify::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

mod callbacks;
mod default_tools;
mod headless;
use callbacks::{HostMachine, HostModel, HostTool};
mod registry;

mod opening;
mod operations;

type BrowserRuntime =
    FilesystemTaskRuntime<BrowserStream, IndexedDbAuthorityStore, IndexedDbObjectStore>;
type BrowserFilesystem = FilesystemHost<IndexedDbAuthorityStore, IndexedDbObjectStore>;

fn to_js<T: serde::Serialize>(value: &T) -> std::result::Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_large_number_types_as_bigints(true)
                .serialize_missing_as_null(true),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

// Project only the admitted state FileRef.
fn normalize_work_file(js: &JsValue, work: &WasmBrowserWork) -> std::result::Result<(), JsValue> {
    let (WasmBrowserWork::Unresolved { lease, .. }
    | WasmBrowserWork::Yielded { lease }
    | WasmBrowserWork::Reconciling { lease }) = work
    else {
        return Ok(());
    };
    normalize_lease_file(
        &js_sys::Reflect::get(js, &JsValue::from_str("lease"))?,
        lease,
    )
}

fn normalize_lease_file(js: &JsValue, lease: &WorkLease) -> std::result::Result<(), JsValue> {
    let field = |value: &JsValue, key| js_sys::Reflect::get(value, &JsValue::from_str(key));
    let operation = field(js, "operation")?;
    let state = field(&operation, "state")?;
    let descriptor = field(&state, "descriptor")?;
    super::set_js_field(
        &descriptor,
        "byte_length",
        &super::exact_js_number(lease.operation.state.descriptor().byte_length())?,
    )
}

fn work_to_js(work: &WasmBrowserWork) -> std::result::Result<JsValue, JsValue> {
    let js = to_js(work)?;
    normalize_work_file(&js, work)?;
    Ok(js)
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WasmBrowserAdmission {
    Accepted { task_id: String },
    Rejected { reason: String },
    Indeterminate { operation_id: String },
}

#[derive(Serialize, Tsify)]
struct WasmBrowserWorkError {
    code: i32,
    message: String,
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[tsify(large_number_types_as_bigints)]
enum WasmBrowserRecoveredWork {
    Idle,
    Claimed {
        lease: WorkLease,
    },
    Unresolved {
        lease: WorkLease,
        error: WasmBrowserWorkError,
    },
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[tsify(large_number_types_as_bigints)]
enum WasmBrowserWork {
    Unresolved {
        lease: WorkLease,
        error: WasmBrowserWorkError,
    },
    Suspended {
        task_id: String,
        revision: u64,
    },
    Completed {
        task_id: String,
    },
    Yielded {
        lease: WorkLease,
    },
    Reconciling {
        lease: WorkLease,
    },
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserWake {
    #[tsify(type = "TaskWakeCursor | null")]
    cursor: Option<TaskWakeCursor>,
    through_revision: u64,
    events_read: u32,
    tasks_polled: u32,
    woken: Vec<String>,
}

#[derive(Serialize, Tsify)]
struct WasmBrowserTick {
    wake: WasmBrowserWake,
    #[tsify(type = "WasmBrowserWork | null")]
    work: Option<WasmBrowserWork>,
}

#[derive(Serialize, Tsify)]
struct WasmBrowserLeaseFence {
    reservation_id: String,
    placement: String,
}

#[derive(Serialize, Tsify)]
struct WasmBrowserTaskObservation {
    task_id: String,
    operation_id: String,
    #[tsify(type = "WasmTaskRunLimitsWire")]
    run_limits: crate::runtime::TaskRunLimits,
    cancellation_requested: bool,
    #[tsify(type = "WasmBrowserLeaseFence | null")]
    reservation: Option<WasmBrowserLeaseFence>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserStreamLimits {
    commands: u64,
    journal_bytes: u64,
    paths: u32,
    path_bytes: u32,
    records: u32,
    payload_bytes: u32,
    commits: u32,
    idempotency_results: u32,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserTaskOptions {
    filesystem_database: String,
    stream_database: String,
    maximum_object_bytes: u64,
    stream_limits: WasmBrowserStreamLimits,
    #[tsify(type = "WasmProviderRefWire")]
    filesystem_provider: ProviderRef,
    #[tsify(type = "WasmVolumeRefWire")]
    volume: VolumeRef,
    limits: WasmLimitsInput,
    run_limits: WasmTaskRunLimitsInput,
    #[tsify(
        type = "{ active_tasks: bigint; total_tasks: bigint; depth: number; model_steps: bigint }"
    )]
    session_limits: SessionLimits,
    concurrency: u32,
    maximum_payload_bytes: u64,
    #[serde(default)]
    #[tsify(type = "WasmRecursiveToolsConfig | null")]
    recursive_tools: Option<default_tools::BrowserToolsConfig>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
struct WasmMachineDefinition {
    name: String,
    version: String,
    digest: Vec<u8>,
    #[tsify(type = "WasmModelJsonSchema")]
    state_schema: Value,
    #[tsify(type = "WasmModelJsonSchema")]
    input_schema: Value,
    #[tsify(type = "WasmModelJsonSchema")]
    output_schema: Value,
    requirements: Vec<String>,
}

/// The existing typed registries, sealed into each opened runtime.
#[wasm_bindgen]
#[derive(Default)]
pub struct WasmTaskRegistry {
    tasks: TaskRegistry,
    machines: MachineRegistry,
    tools: ToolRegistry,
}

/// Thin event-loop handle; scheduling, ownership, effects and recovery stay in `FilesystemTaskRuntime`.
#[wasm_bindgen]
pub struct WasmTaskRuntime {
    runtime: Arc<BrowserRuntime>,
    stream: std::sync::Arc<BrowserStream>,
    filesystem: Arc<BrowserFilesystem>,
    volume: VolumeRef,
    scope: Scope,
    verifier: crate::core::AuthorityVerifier,
    maximum_payload_bytes: u64,
    model: Option<(Model, Arc<HostModel>, crate::context::CompactionPolicy)>,
}

impl WasmTaskRuntime {
    fn commands(
        &self,
    ) -> crate::Result<
        crate::filesystem::FilesystemTaskCommands<
            '_,
            BrowserStream,
            IndexedDbAuthorityStore,
            IndexedDbObjectStore,
        >,
    > {
        let commands = self.runtime.commands();
        match &self.model {
            Some((model, provider, compaction)) => commands.with_model(
                model,
                provider.clone(),
                &ContextPipeline::new([]),
                compaction,
                None,
            ),
            None => Ok(commands),
        }
    }
}

fn work_value(work: TaskWorkerAttempt) -> WasmBrowserWork {
    match work {
        TaskWorkerAttempt::Unresolved { lease, error } => WasmBrowserWork::Unresolved {
            lease,
            error: WasmBrowserWorkError {
                code: error.code() as i32,
                message: error.to_string(),
            },
        },
        TaskWorkerAttempt::Progress(progress) => match progress {
            TaskWorkerOutcome::Suspended { task, revision } => WasmBrowserWork::Suspended {
                task_id: task.to_string(),
                revision,
            },
            TaskWorkerOutcome::Completed { task } => WasmBrowserWork::Completed {
                task_id: task.to_string(),
            },
            TaskWorkerOutcome::Yielded { lease } => WasmBrowserWork::Yielded { lease },
            TaskWorkerOutcome::Reconciling { lease } => WasmBrowserWork::Reconciling { lease },
        },
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod accounting_tests;
