//! Browser bindings over the ordinary task runtime and its real providers.

use super::{WasmReducer, from_js, js_error, js_json_value, key_bytes};
use crate::{
    Admission, Error, OperationId, Result, TaskId,
    conversation::{ContentGrant, FileRef, Limits, VolumeOperation, VolumeRef},
    core::Scope,
    distributed::{SchedulerPayloadStore, WorkLease, WorkPull, Worker},
    filesystem::{
        FilesystemContentPublisher, FilesystemHost, FilesystemTaskRuntime, StockTurnMachine,
        TaskWakeCursor, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    model::Model,
    resources::ProviderRef,
    runtime::{RuntimeScope, TaskRunLimits, TaskStateProvider},
    scheduler::SessionLimits,
};
use acyclic_fs::{
    EmbeddedCapabilities, Fs,
    browser::{IndexedDbAuthorityStore, IndexedDbObjectStore},
};
use acyclic_stream::{BrowserStream, BrowserStreamLimits, MemoryLimits, SystemUnixMillisClock};
use js_sys::Function;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tsify::Tsify;
use wasm_bindgen::prelude::*;

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

// Project only the admitted state FileRef. Entrypoint schemas and generic
// machine/model values must retain every literal, including descriptor-shaped
// user objects, rather than undergoing a recursive structural rewrite.
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
struct WasmBrowserSessionLimits {
    active_tasks: u64,
    total_tasks: u64,
    depth: u32,
    model_steps: u64,
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
    #[tsify(type = "WasmLimitsInput")]
    limits: Limits,
    #[tsify(type = "WasmTaskRunLimitsInput")]
    run_limits: TaskRunLimits,
    session_limits: WasmBrowserSessionLimits,
    concurrency: u32,
    maximum_payload_bytes: u64,
}

#[path = "task_runtime/registry.rs"]
mod registry;
use registry::HostModel;
pub use registry::WasmTaskRegistry;

/// Thin event-loop handle; scheduling, ownership, effects and recovery stay in
/// `FilesystemTaskRuntime`. No worker starts when this handle is opened.
#[wasm_bindgen]
pub struct WasmTaskRuntime {
    runtime: BrowserRuntime,
    filesystem: Arc<BrowserFilesystem>,
    volume: VolumeRef,
    scope: Scope,
    verifier: crate::core::AuthorityVerifier,
    maximum_payload_bytes: u64,
    model: Option<(Model, Arc<HostModel>, crate::context::CompactionPolicy)>,
}

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Opens existing providers with caller-supplied limits and signed authority.
    /// Volume creation and task admission remain separate explicit operations.
    pub async fn open(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserTaskOptions")] options: JsValue,
        owner: &WasmReducer,
        registry: &WasmTaskRegistry,
        #[wasm_bindgen(unchecked_param_type = "Scope")] signed_scope: JsValue,
    ) -> std::result::Result<WasmTaskRuntime, JsValue> {
        let options: WasmBrowserTaskOptions = from_js(options)?;
        let scope: Scope = from_js(signed_scope)?;
        let verifier = owner.issuer.verifier(&mut None).map_err(js_error)?;
        ContentGrant::verify(
            &verifier,
            &scope,
            &options.volume,
            VolumeOperation::Write,
            options.maximum_payload_bytes,
            &mut None,
        )
        .map_err(js_error)?;
        let root = RuntimeScope::new(scope.capabilities().clone(), options.limits)
            .map_err(js_error)?
            .with_run_limits(options.run_limits, &mut None)
            .map_err(js_error)?;
        let authority = IndexedDbAuthorityStore::open(
            &options.filesystem_database,
            options.maximum_object_bytes,
        )
        .await
        .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let objects =
            IndexedDbObjectStore::open(&options.filesystem_database, options.maximum_object_bytes)
                .await
                .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let filesystem = FilesystemHost::new(
            Fs::new(authority, objects, EmbeddedCapabilities { durable: true }),
            &options.filesystem_provider,
            None,
        )
        .map_err(js_error)?;
        let limits = options.stream_limits;
        let stream = Arc::new(
            BrowserStream::open(
                &options.stream_database,
                BrowserStreamLimits {
                    commands: limits.commands,
                    journal_bytes: limits.journal_bytes,
                    memory: MemoryLimits {
                        paths: limits.paths as usize,
                        path_bytes: limits.path_bytes as usize,
                        records: limits.records as usize,
                        payload_bytes: limits.payload_bytes as usize,
                        commits: limits.commits as usize,
                        idempotency_results: limits.idempotency_results as usize,
                    },
                },
            )
            .await
            .map_err(|error| js_error(Error::Storage(error.to_string())))?,
        );
        let limits = options.session_limits;
        let runtime = FilesystemTaskRuntime::open(
            stream,
            filesystem.clone(),
            &options.volume,
            // Browser stock execution has no native task context, so its
            // summaries stay in the journal.
            None,
            &verifier,
            &scope,
            &root,
            &registry.tasks,
            &registry.machines,
            &registry.tools,
            SessionLimits {
                active_tasks: limits.active_tasks,
                total_tasks: limits.total_tasks,
                depth: limits.depth,
                model_steps: limits.model_steps,
            },
            options.concurrency as usize,
            options.maximum_payload_bytes,
            Arc::new(SystemUnixMillisClock),
            None,
            &[],
            &crate::durable_tool::ResumableToolRegistry::default(),
            None,
        )
        .await
        .map_err(js_error)?;
        Ok(Self {
            runtime,
            filesystem,
            volume: options.volume,
            scope,
            verifier,
            maximum_payload_bytes: options.maximum_payload_bytes,
            model: None,
        })
    }

    /// Composes an explicit provider with the stock model command executor.
    /// Generate receives canonical request bytes, separate dispatch identity and
    /// an `AbortSignal`, and returns an async iterator;
    /// reconcile receives the exact retained attempt and never redispatches it.
    /// Optional capacity/counting callbacks are synchronous, effect-free provider
    /// operations. Counting receives exact canonical bytes and their Rust digest;
    /// Rust validates the response binding and ordered message dimensions.
    #[wasm_bindgen(js_name = configureModel)]
    pub fn configure_model(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Model")] model: JsValue,
        #[wasm_bindgen(
            unchecked_param_type = "(request: Uint8Array, dispatch: ModelDispatch, signal: AbortSignal) => AsyncIterator<ModelEvent>"
        )]
        generate: Function,
        #[wasm_bindgen(
            unchecked_param_type = "(attempt: ModelDispatch & { observed: ModelEvent[] }) => ModelEvent[] | null | Promise<ModelEvent[] | null>"
        )]
        reconcile: Function,
        #[wasm_bindgen(
            unchecked_param_type = "((model: Model) => ModelContextCapacity) | undefined"
        )]
        capacity: Option<Function>,
        #[wasm_bindgen(
            unchecked_param_type = "((request: Uint8Array, request_digest: number[]) => ModelTokenCount | Promise<ModelTokenCount>) | undefined"
        )]
        count_tokens: Option<Function>,
        #[wasm_bindgen(unchecked_param_type = "CompactionPolicy")] compaction: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let model: Model = from_js(model)?;
        model.validate().map_err(js_error)?;
        let compaction = from_js(compaction)?;
        self.model = Some((
            model,
            Arc::new(HostModel {
                generate,
                reconcile,
                capacity,
                count_tokens,
            }),
            compaction,
        ));
        Ok(())
    }

    /// Creates only the explicitly authorized private journal volume.
    #[wasm_bindgen(js_name = initializeVolume)]
    pub async fn initialize_volume(&self) -> std::result::Result<(), JsValue> {
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
            self.maximum_payload_bytes,
            &mut None,
        )
        .map_err(js_error)?;
        self.filesystem
            .create_volume(&self.volume)
            .await
            .map_err(js_error)?;
        Ok(())
    }

    /// Stages an exact immutable command/input artifact through the ordinary store.
    #[wasm_bindgen(unchecked_return_type = "WasmFileRefWire")]
    pub async fn stage(
        &self,
        operation: String,
        key: String,
        bytes: Vec<u8>,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let store = FilesystemContentPublisher::new(
            self.filesystem.clone(),
            &self.volume,
            &self.verifier,
            &self.scope,
            self.maximum_payload_bytes,
            None,
        )
        .map_err(js_error)?;
        let file: FileRef = store
            .stage(operation, &key, &bytes, None)
            .map_err(js_error)?
            .await
            .map_err(js_error)?;
        super::to_js_admitted(&file)
    }

    #[wasm_bindgen(unchecked_return_type = "WasmBrowserAdmission")]
    pub async fn admit(
        &self,
        operation: String,
        name: String,
        version: String,
        #[wasm_bindgen(unchecked_param_type = "WasmModelJsonValue")] input: JsValue,
        parent: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let parent = parent
            .map(|id| TaskId::parse(&id))
            .transpose()
            .map_err(js_error)?;
        let definition = self
            .runtime
            .harness()
            .task::<Value, Value>(&format!("{name}@{version}"))
            .map_err(js_error)?;
        let admission = self
            .runtime
            .harness()
            .admit(operation, &definition, js_json_value(&input)?, parent)
            .await
            .map_err(js_error)?;
        let admission = match admission {
            Admission::Accepted(task) => WasmBrowserAdmission::Accepted {
                task_id: task.identity(),
            },
            Admission::Rejected { reason } => WasmBrowserAdmission::Rejected { reason },
            Admission::Indeterminate { operation_id } => WasmBrowserAdmission::Indeterminate {
                operation_id: operation_id.to_string(),
            },
        };
        to_js(&admission)
    }

    /// Runs only this original task through the ordinary fenced worker path.
    #[wasm_bindgen(js_name = runOperation, unchecked_return_type = "WasmBrowserWork | null")]
    pub async fn run_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Worker")] worker: JsValue,
        task: String,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let worker: Worker = from_js(worker)?;
        let task = TaskId::parse(&task).map_err(js_error)?;
        match self
            .runtime
            .run_operation(
                &worker,
                task,
                &self.commands().map_err(js_error)?,
                maximum_transitions,
                None,
            )
            .await
            .map_err(js_error)?
        {
            Some(work) => work_to_js(&work_value(work)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Reads the original reservation without claiming or releasing work.
    #[wasm_bindgen(js_name = recoverWork, unchecked_return_type = "WasmBrowserRecoveredWork")]
    pub async fn recover_work(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        let work = match self
            .runtime
            .task_host()
            .recover_work(task, &mut None)
            .await
            .map_err(js_error)?
        {
            WorkPull::Idle => WasmBrowserRecoveredWork::Idle,
            WorkPull::Claimed(lease) => WasmBrowserRecoveredWork::Claimed { lease },
            WorkPull::Unresolved { lease, error } => WasmBrowserRecoveredWork::Unresolved {
                lease,
                error: WasmBrowserWorkError {
                    code: error.code() as i32,
                    message: error.to_string(),
                },
            },
        };
        let js = to_js(&work)?;
        if let WasmBrowserRecoveredWork::Claimed { lease }
        | WasmBrowserRecoveredWork::Unresolved { lease, .. } = &work
        {
            normalize_lease_file(
                &js_sys::Reflect::get(&js, &JsValue::from_str("lease"))?,
                lease,
            )?;
        }
        Ok(js)
    }

    /// Executes one caller-bounded tick using the production command resolver.
    /// Missing tool/model routes stay unavailable; no fabricated default swarm.
    #[wasm_bindgen(js_name = workerTick, unchecked_return_type = "WasmBrowserTick")]
    pub async fn worker_tick(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Worker")] worker: JsValue,
        #[wasm_bindgen(unchecked_param_type = "TaskWakeCursor | null")] cursor: JsValue,
        maximum_events: u32,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let worker: Worker = from_js(worker)?;
        let cursor: Option<TaskWakeCursor> = from_js(cursor)?;
        let tick = self
            .runtime
            .worker_tick(
                &worker,
                cursor,
                &self.commands().map_err(js_error)?,
                maximum_events,
                maximum_transitions,
            )
            .await
            .map_err(js_error)?;
        let value = WasmBrowserTick {
            wake: WasmBrowserWake {
                cursor: tick.wake.cursor,
                through_revision: tick.wake.through_revision,
                events_read: tick.wake.events_read,
                tasks_polled: tick.wake.tasks_polled,
                woken: tick
                    .wake
                    .woken
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect(),
            },
            work: tick.work.map(work_value),
        };
        let js = to_js(&value)?;
        if let Some(work) = &value.work {
            let js_work = js_sys::Reflect::get(&js, &JsValue::from_str("work"))?;
            normalize_work_file(&js_work, work)?;
        }
        Ok(js)
    }

    /// Resumes only the exact retained lease; another tick never substitutes it.
    #[wasm_bindgen(unchecked_return_type = "WasmBrowserWork")]
    pub async fn resume(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease: WorkLease = from_js(lease)?;
        work_to_js(&work_value(
            self.runtime
                .resume_task(
                    lease,
                    &self.commands().map_err(js_error)?,
                    maximum_transitions,
                    None,
                )
                .await,
        ))
    }

    /// Observes only; absence does not dispatch or replay an uncertain effect.
    #[wasm_bindgen(unchecked_return_type = "Outcome<WasmModelJsonValue> | null")]
    pub async fn outcome(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        to_js(
            &self
                .runtime
                .task_host()
                .outcome(task)
                .await
                .map_err(js_error)?,
        )
    }

    /// Publishes durable cancellation without claiming terminal completion.
    pub async fn cancel(&self, task: String) -> std::result::Result<(), JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        self.runtime
            .task_host()
            .cancel(task)
            .await
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = reconcileAdmission, unchecked_return_type = "[string, ComponentIdentity] | null")]
    pub async fn reconcile_admission(
        &self,
        operation: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        to_js(
            &self
                .runtime
                .harness()
                .reconcile_admission(operation)
                .await
                .map_err(js_error)?,
        )
    }

    /// Returns only after receiver-owned durable bytes are verified. A lost
    /// acknowledgment permits exact redelivery; it grants no effect retry.
    pub async fn send(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        recipient: String,
        message: String,
        #[wasm_bindgen(unchecked_param_type = "WasmFileRefWire")] payload: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let lease: WorkLease = from_js(lease)?;
        let sender = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let recipient = TaskId::parse(&recipient).map_err(js_error)?;
        let message = OperationId::parse(&message).map_err(js_error)?;
        self.runtime
            .task_host()
            .send_mail(
                sender,
                &crate::scheduler::LeaseFence::from(&lease.reservation),
                recipient,
                message,
                from_js(payload)?,
                None,
            )
            .await
            .map(|_| ())
            .map_err(js_error)
    }

    /// A page observation is not a retained model-consumption acknowledgment.
    #[wasm_bindgen(unchecked_return_type = "InboxItem[]")]
    pub async fn inbox(
        &self,
        task: String,
        after: u64,
        maximum: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        super::to_js_admitted(
            &self
                .runtime
                .task_host()
                .inbox(task, after, maximum as usize)
                .await
                .map_err(js_error)?,
        )
    }
}

impl WasmTaskRuntime {
    fn commands(
        &self,
    ) -> Result<
        crate::filesystem::FilesystemTaskCommands<
            '_,
            BrowserStream,
            IndexedDbAuthorityStore,
            IndexedDbObjectStore,
        >,
    > {
        let commands = self.runtime.commands();
        match &self.model {
            Some((model, provider, compaction)) => {
                commands.with_model(model, provider.clone(), compaction, None)
            }
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
#[path = "task_runtime/accounting_tests.rs"]
mod accounting_tests;
