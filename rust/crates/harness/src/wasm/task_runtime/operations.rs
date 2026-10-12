use super::*;

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Stages an exact immutable command/input artifact through the ordinary store.
    #[wasm_bindgen(unchecked_return_type = "WasmFileRefWire")]
    pub async fn stage(
        &self,
        operation: String,
        key: String,
        bytes: Vec<u8>,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let store = FilesystemSchedulerPayloadStore::new(
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
        super::super::to_js_admitted(&file)
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
            .admit(operation, &definition, from_js::<Value>(input)?, parent)
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

    /// Reconciles original admitted effects only.
    #[wasm_bindgen(unchecked_return_type = "WasmBrowserWork")]
    pub async fn reconcile(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_commands: u32,
        maximum_wait_millis: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease: WorkLease = from_js(lease)?;
        work_to_js(&work_value(
            self.runtime
                .reconcile_task(
                    lease,
                    &self.commands().map_err(js_error)?,
                    maximum_commands,
                    maximum_wait_millis,
                    None,
                )
                .await,
        ))
    }

    /// Reads the exact original admission deadline and current owner fence.
    #[wasm_bindgen(js_name = observeTask, unchecked_return_type = "WasmBrowserTaskObservation")]
    pub async fn observe_task(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        let admission = self
            .runtime
            .task_host()
            .observe_admission(task)
            .await
            .map_err(js_error)?;
        let state = self
            .runtime
            .task_host()
            .observe_task(task)
            .await
            .map_err(js_error)?;
        to_js(&WasmBrowserTaskObservation {
            task_id: task.to_string(),
            operation_id: admission.operation_id.to_string(),
            run_limits: admission.run_limits,
            cancellation_requested: state.cancellation_requested,
            reservation: state.reservation.map(|reservation| WasmBrowserLeaseFence {
                reservation_id: reservation.id,
                placement: reservation.placement,
            }),
        })
    }

    /// Reads canonical admission data; raw references remain non-bearer metadata.
    #[wasm_bindgen(js_name = observeAdmission, unchecked_return_type = "WasmTaskAdmissionWire")]
    pub async fn observe_admission(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        let admission = self
            .runtime
            .task_host()
            .observe_admission(task)
            .await
            .map_err(js_error)?;
        super::super::to_js_admitted(&admission.canonical_value())
    }

    /// Admits one canonical record under the exact current parent lease.
    #[wasm_bindgen(js_name = admitRecord, unchecked_return_type = "WasmBrowserAdmission")]
    pub async fn admit_record(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WasmTaskAdmissionWire")] admission: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserLeaseFence")] parent_fence: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let admission = crate::runtime::TaskAdmissionRecord::from_canonical_value(
            from_js::<Value>(admission)?,
            &mut None,
        )
        .map_err(js_error)?
        .0;
        let fence: crate::scheduler::LeaseFence = from_js(parent_fence)?;
        let admission = match self
            .runtime
            .task_host()
            .admit_owned(admission, fence)
            .await
            .map_err(js_error)?
        {
            Admission::Accepted(task) => WasmBrowserAdmission::Accepted {
                task_id: task.to_string(),
            },
            Admission::Rejected { reason } => WasmBrowserAdmission::Rejected { reason },
            Admission::Indeterminate { operation_id } => WasmBrowserAdmission::Indeterminate {
                operation_id: operation_id.to_string(),
            },
        };
        to_js(&admission)
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
            .cancel(task, None)
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

    /// Returns only after receiver-owned durable bytes are verified.
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
        super::super::to_js_admitted(
            &self
                .runtime
                .task_host()
                .inbox(task, after, maximum as usize)
                .await
                .map_err(js_error)?,
        )
    }
}
