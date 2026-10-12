use super::*;

impl AgentHarness {
    /// Admits a process-local task after validating its registration and input.

    pub async fn spawn<I, O>(
        self: &Arc<Self>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        self.spawn_in_group(
            &self.live,
            self.scope.clone(),
            Vec::new(),
            definition,
            input,
            None,
        )
        .await
    }

    pub(super) async fn spawn_in_group<I, O>(
        self: &Arc<Self>,
        group: &TaskGroup,
        scope: RuntimeScope,
        policies: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        operation_id: Option<OperationId>,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        if self.execution.is_some() {
            return Err(Error::Unsupported(
                "live task closures cannot cross an execution provider; register a resumable task and admit it with a stable operation ID".into(),
            ));
        }
        self.spawn_local_in_group(group, scope, policies, definition, input, operation_id)
            .await
    }

    pub(crate) async fn spawn_local_with_operation<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        self.spawn_local_in_group(
            &self.live,
            self.scope.clone(),
            Vec::new(),
            definition,
            input,
            Some(operation_id),
        )
        .await
    }

    pub(super) async fn spawn_local_in_group<I, O>(
        self: &Arc<Self>,
        group: &TaskGroup,
        scope: RuntimeScope,
        policies: Vec<(ComponentIdentity, Arc<dyn ToolPolicy>)>,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        operation_id: Option<OperationId>,
    ) -> Result<RuntimeTask<O>>
    where
        I: Serialize + Send + 'static,
        O: Serialize + DeserializeOwned + Send + 'static,
    {
        let registered = self.tasks.get_version::<I, O>(
            &definition.identity.name,
            &definition.identity.version,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )?;
        if !Arc::ptr_eq(&registered, definition) {
            return Err(Error::Conflict(
                "task definition is not the registered version".into(),
            ));
        }
        let TaskImplementation::Live(handler) = &definition.implementation else {
            return Err(Error::Unsupported(
                "resumable task needs stable durable admission".into(),
            ));
        };
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        crate::executor::validate_json_schema_value(
            &definition.input_schema,
            &serde_json::to_value(&input).map_err(|error| Error::Invalid(error.to_string()))?,
            "task input",
            &mut None,
        )?;
        let handler = Arc::clone(handler);
        let output_schema = definition.output_schema.clone();
        let runtime = Arc::clone(self);
        let context_group = group.clone();
        let descendants = context_group.child(scope.concurrency_bound(self.concurrency), None)?;
        let (identity, receive_identity) = tokio::sync::oneshot::channel();
        let admitted = group
            .spawn(operation_id.unwrap_or_default(), async move {
                let task_id = receive_identity
                    .await
                    .map_err(|_| Error::Conflict("task identity was not admitted".into()))?;
                scope.run_limits().check_deadline()?;
                let deadline = scope.run_limits().deadline_epoch_ms;
                let run = handler(
                    TaskContext {
                        harness: runtime,
                        task_id,
                        durable_task: None,
                        descendants,
                        scope,
                        policy_overrides: policies,
                        owned_interactions: None,
                        owned_mailbox: None,
                        output_content: None,
                        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
                        native_root_output: std::sync::OnceLock::new(),
                        model_steps: Arc::new((AtomicUsize::new(0), None)),
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        native_metadata_source: None,
                        #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
                        working_view: std::sync::OnceLock::new(),
                        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                        native_custody: None,
                    },
                    input,
                );
                let output = match deadline {
                    Some(deadline) => crate::live::timeout_until(deadline, run).await??,
                    None => run.await?,
                };
                drop(extension_leases);
                crate::executor::validate_json_schema_value(
                    &output_schema,
                    &serde_json::to_value(&output)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                    "task output",
                    &mut None,
                )?;
                Ok(output)
            })
            .await;
        let handle = match admitted {
            Admission::Accepted(handle) => handle,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        let _ = identity.send(*handle.id());
        Ok(RuntimeTask::Live(handle))
    }

    /// Admits a pinned resumable task through a host; live futures are rejected.

    pub async fn admit<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        parent: Option<TaskId>,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        self.admit_scoped(operation_id, definition, input, parent, self.scope.clone())
            .await
    }
}
