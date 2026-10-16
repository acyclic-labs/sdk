use super::*;

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) type NativeCustody = acyclic_fs::NativeAllocation;

#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(crate) type NativeCustody = ();

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum IssuedContextPurpose {
    Read,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Dispatch,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Reconciliation,
}

impl AgentHarness {
    pub(super) async fn admit_scoped<I, O>(
        self: &Arc<Self>,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: I,
        parent: Option<TaskId>,
        scope: RuntimeScope,
    ) -> Result<Admission<RuntimeTask<O>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        self.assert_durable_policy_bindings(None)?;
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
        if parent.is_some() {
            require_task_spawn(&scope, definition)?;
        }
        let TaskImplementation::Resumable(machine) = &definition.implementation else {
            return Ok(Admission::Rejected {
                reason: "live tasks are local-only".into(),
            });
        };
        let Some(host) = &self.state else {
            return Ok(Admission::Rejected {
                reason: "durable task state is not bound".into(),
            });
        };
        let spawner = self
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task spawner is not bound".into()))?;
        let value =
            serde_json::to_value(input).map_err(|error| Error::Invalid(error.to_string()))?;
        crate::executor::validate_json_schema_value(
            &definition.input_schema,
            &value,
            "task input",
            &mut None,
        )?;
        let mut expected = TaskAdmissionRecord::from_parts(
            operation_id,
            &definition.identity.name,
            &definition.identity.version,
            value,
            definition.input_schema.clone(),
            definition.output_schema.clone(),
            &definition.requirements,
            &machine.identity().digest,
            parent,
            scope.grants().clone(),
            scope.limits(),
            scope.run_limits(),
            self.policy_identity.clone(),
            scope.extensions().cloned(),
            None,
        )?
        .with_resource_proofs(scope.resource_proofs().to_vec())?;
        if let Some(admission) = self
            .reconcile_scoped_admission(operation_id, definition, &expected, &scope, host, spawner)
            .await?
        {
            return Ok(admission);
        }
        if let Some(execution) = &self.execution {
            let placement = execution.qualify(&expected).await?;
            placement.validate(&mut None)?;
            if placement.provider != execution.identity() {
                return Err(Error::Conflict(
                    "execution qualifier returned another provider".into(),
                ));
            }
            expected.execution = Some(placement);
        }
        let extension_leases = self.retain_task_extensions(definition, &scope)?;
        let admitted = if expected.resource_proofs.is_empty() {
            spawner.admit(expected.clone()).await?
        } else {
            let parent = parent.ok_or_else(|| {
                Error::Unauthorized("created child admission has no parent".into())
            })?;
            let fence = scope.current_creation_parent_fence(parent).await?;
            spawner.admit_owned(expected.clone(), fence).await?
        };
        Ok(match admitted {
            Admission::Accepted(task_id) => {
                self.verify_spawner_admission(task_id, &expected).await?;
                Admission::Accepted(RuntimeTask::Durable {
                    task_id,
                    host: Arc::clone(host),
                    output_schema: definition.output_schema.clone(),
                    extensions: extension_leases,
                })
            }
            Admission::Rejected { reason } => Admission::Rejected { reason },
            Admission::Indeterminate { operation_id } => Admission::Indeterminate { operation_id },
        })
    }

    /// Reconstructs a pinned task context on the supplied IO.
    pub async fn durable_context(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Arc<TaskContext>> {
        self.open_durable_context(task, operation, io, IssuedContextPurpose::Read, None)
            .await
    }

    pub(crate) async fn open_durable_context(
        self: &Arc<Self>,
        task: TaskId,
        operation: OperationId,
        io: Option<&crate::executor::PreparationIo>,
        issued_purpose: IssuedContextPurpose,
        owned_mailbox: Option<(Arc<dyn OwnedTaskMailbox>, RuntimeScope, NativeCustody)>,
    ) -> Result<Arc<TaskContext>> {
        let (owned_mailbox, native_scope) = match owned_mailbox {
            Some((mailbox, scope, custody)) => (Some(mailbox), Some((scope, custody))),
            None => (None, None),
        };
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut native_scope = native_scope;
        let native_work = task_original_work(io)?;
        let host = self.state.as_ref().ok_or_else(|| {
            crate::conversation::content_error(
                io,
                crate::wire::ErrorCode::Unsupported,
                "durable task state is not bound",
            )
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let counter_bytes = (std::mem::size_of::<(AtomicUsize, Option<NativeCustody>)>()
            + 2 * std::mem::size_of::<usize>()) as u64;
        let (scope, custody) = match native_work {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let extra_self = (std::mem::size_of::<TaskContext>()
                    - std::mem::size_of::<RuntimeScope>()) as u64;
                let mut custody = work.claim(
                    extra_self + counter_bytes,
                    1,
                    acyclic_fs::WorkCounters::default(),
                )?;
                let mut admitted = match native_scope.take() {
                    Some(scope) => scope,
                    None => {
                        host.resume_scope_with_native_work(task, operation, work)?
                            .await?
                    }
                };
                custody.absorb(&mut admitted.1)?;
                self.scope.validate_resumed_scope(&admitted.0, io)?;
                admitted.0.extension_runtime = self.scope.extension_runtime.clone();
                admitted.0.extension_schemas = self.scope.extension_schemas.clone();
                admitted.0.extensions_sealed = true;
                if let Some(mailbox) = &owned_mailbox {
                    mailbox.validate_scope(&admitted.0, io)?;
                }
                (admitted.0, Some(custody))
            }
            _ => (
                self.scope
                    .resume_sealed(host.resume_scope(task, operation).await?, operation)?,
                None::<NativeCustody>,
            ),
        };
        drop(native_scope);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut custody = custody;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = (issued_purpose, custody);
        let bound = scope.concurrency_bound(self.concurrency);
        let descendants = self.live.child(bound, native_work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let model_custody = custody
            .as_mut()
            .map(|custody| custody.split_off(counter_bytes))
            .transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let model_custody = None::<NativeCustody>;
        let context = TaskContext {
            harness: Arc::clone(self),
            task_id: operation,
            durable_task: Some(task),
            descendants,
            scope,
            policy_overrides: Vec::new(),
            owned_interactions: None,
            owned_mailbox,
            output_content: None,
            model_steps: Arc::new((AtomicUsize::new(0), model_custody)),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_purpose: native_work.map(|_| issued_purpose),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: custody,
        };
        context.into_shared()
    }
}

impl TaskContext {
    pub(crate) fn preparation_io(&self) -> Result<Option<crate::executor::PreparationIo>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.is_some() || self.model_steps.1.is_some() {
            return crate::executor::PreparationIo::from_native_work(self.native_work()?).map(Some);
        }
        Ok(None)
    }
}
