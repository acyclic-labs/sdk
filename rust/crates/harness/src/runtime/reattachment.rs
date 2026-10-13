use super::*;

impl AgentHarness {
    /// Looks up a lost durable admission acknowledgement without redispatching its input or trusting a caller-selected task definition.
    pub async fn reconcile_admission(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<(TaskId, ComponentIdentity)>> {
        self.assert_durable_policy_bindings(None)?;
        let spawner = self
            .spawner
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task spawner is not bound".into()))?;
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let Some((task_id, admission)) = spawner.reconcile_admission(operation_id).await? else {
            return Ok(None);
        };
        if admission.operation_id != operation_id {
            return Err(Error::Conflict(
                "spawner reconciled another operation".into(),
            ));
        }
        self.validate_observed_admission(&admission, None).await?;
        let identity = admission.task.clone();
        let registered = self
            .tasks
            .find(&identity.name, &identity.version, &mut None)?;
        if *registered.identity() != identity || registered.machine().is_none() {
            return Err(Error::Conflict(
                "reconciled task implementation changed".into(),
            ));
        }
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == admission => {
                self.validate_observed_admission(&observed, None).await?
            }
            Ok(_) => {
                return Err(Error::Conflict(
                    "spawner and state host admission bindings differ".into(),
                ));
            }
            Err(Error::NotFound(_)) => return Err(Error::Indeterminate(operation_id)),
            Err(error) => return Err(error),
        }
        Ok(Some((task_id, identity)))
    }

    /// Reattaches only to an admission pinned to this registered resumable definition.
    pub async fn attach<I: 'static, O: DeserializeOwned + Send + 'static>(
        &self,
        task_id: TaskId,
        definition: &Arc<TaskDefinition<I, O>>,
    ) -> Result<RuntimeTask<O>> {
        let registered = self.tasks.get_version::<I, O>(
            &definition.identity.name,
            &definition.identity.version,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            None,
        )?;
        if !Arc::ptr_eq(&registered, definition)
            || !matches!(&definition.implementation, TaskImplementation::Resumable(_))
        {
            return Err(Error::Conflict(
                "task definition is not the registered resumable version".into(),
            ));
        }
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let observed = host.observe_admission(task_id).await?;
        self.validate_observed_admission(&observed, None).await?;
        if observed.task != definition.identity
            || observed.output_schema != definition.output_schema
        {
            return Err(Error::Conflict(
                "durable task differs from the pinned definition".into(),
            ));
        }
        let extensions = self.retain_task_extensions_existing(definition, &self.scope)?;
        Ok(RuntimeTask::Durable {
            task_id,
            host: Arc::clone(host),
            output_schema: definition.output_schema.clone(),
            extensions: Some(extensions),
        })
    }

    /// Opens an admitted registered task from its retained input and workflow.
    pub async fn open_task<I: 'static, O: 'static>(
        self: &Arc<Self>,
        task_id: TaskId,
        definition: &Arc<TaskDefinition<I, O>>,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableTaskSession> {
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let retained = state.observe_admission(task_id).await?;
        let context = self.durable_context(task_id, retained.operation_id).await?;
        definition.open(Arc::new(context), &fence, journal)?.await
    }

    /// Builds the stock child request from the existing registered metadata.
    #[cfg(feature = "filesystem")]
    pub fn registered_child_admission(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
        input: Value,
        parent: TaskId,
        scope: &RuntimeScope,
    ) -> Result<TaskAdmissionRecord> {
        self.assert_durable_policy_bindings(None)?;
        self.tasks.registered_admission(
            operation,
            name,
            version,
            input,
            Some(parent),
            None,
            scope,
            &mut None,
        )
    }

    /// Builds a registered descendant request using the original guarded input.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn registered_child_admission_with_native_work(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
        input: (Value, acyclic_fs::NativeAllocation),
        parent: TaskId,
        scope: &RuntimeScope,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        if !input.1.work().shares_allocation_context(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "child input belongs to another original metadata ledger",
            });
        }
        self.assert_durable_policy_bindings(Some(work))?;
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        crate::store::history::HistoryIo::absorb_native_custody(&mut io, input.1)?;
        let record = self.tasks.registered_admission(
            operation,
            name,
            version,
            input.0,
            Some(parent),
            None,
            scope,
            &mut Some(&mut io),
        )?;
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(record.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native admission metadata overflows",
            })?;
        let custody = crate::store::history::HistoryIo::take_native_custody(&mut io, bytes)?;
        Ok((record, custody))
    }

    /// Builds a registered root admission from an already guarded native input.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn registered_root_admission_with_native_work<I, O>(
        &self,
        operation: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        input: (Value, acyclic_fs::NativeAllocation),
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskAdmissionRecord, acyclic_fs::NativeAllocation)> {
        work.charge(Default::default())
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        if !input.1.work().shares_allocation_context(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "root input belongs to another original resource ledger",
            });
        }
        self.assert_durable_policy_bindings(Some(work))?;
        if self.execution.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native root admission requires the configured local durable host",
            });
        }
        let mut io = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(work)?,
        );
        use crate::store::history::HistoryIo as _;
        let (value, custody) = input;
        io.absorb_native_custody(custody)?;
        let identity = definition.identity();
        let mut record = self.tasks.registered_admission(
            operation,
            &identity.name,
            &identity.version,
            value,
            None,
            Some(identity.digest),
            &self.scope,
            &mut Some(&mut io),
        )?;
        record.policy = match self.policy_identity.as_ref() {
            None => None,
            Some(identity) => Some(ComponentIdentity {
                name: io.clone_text(&identity.name)?,
                version: io.clone_text(&identity.version)?,
                digest: identity.digest,
            }),
        };
        record.validate(&mut Some(&mut io))?;
        let bytes = (std::mem::size_of::<TaskAdmissionRecord>() as u64)
            .checked_add(record.logical_owned_bytes()?)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native admission metadata overflows",
            })?;
        let custody = io.take_native_custody(bytes)?;
        Ok((record, custody))
    }

    /// Reopens the exact registered task definition under its current context.
    pub async fn open_registered_task(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] context: Option<
            Arc<TaskContext>,
        >,
    ) -> Result<ResumableTaskSession> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(context) = context {
            let work = context.native_work()?;
            if !Arc::ptr_eq(&context.harness, self)
                || !context.original_task_binding_matches(task_id, fence)
                || journal.task_binding() != Some((task_id, fence))
            {
                return Err(task_workflow_error(
                    Some(work),
                    crate::wire::ErrorCode::Unauthorized,
                    "native workflow context or journal owner differs",
                ));
            }
            if !journal.supports_native_work() {
                return Err(task_workflow_error(
                    Some(work),
                    crate::wire::ErrorCode::Unsupported,
                    "workflow journal has no native callbacks",
                ));
            }
            let identity = journal.original_machine_identity().ok_or_else(|| {
                task_workflow_error(
                    Some(work),
                    crate::wire::ErrorCode::Unsupported,
                    "native workflow journal has no original admitted identity",
                )
            })?;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let definition = crate::executor::with_history_io(Some(&io), |observer| {
                let definition = self
                    .tasks
                    .find(&identity.name, &identity.version, observer)?;
                if definition
                    .machine()
                    .map(|machine| machine.identity_with_native_work(work))
                    .transpose()?
                    != Some(identity)
                {
                    return Err(task_workflow_error(
                        Some(work),
                        crate::wire::ErrorCode::Conflict,
                        "original native task definition changed",
                    ));
                }
                Ok(Arc::clone(definition))
            })?;
            return definition.open_workflow(context, fence, journal)?.await;
        }
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let admission = state.observe_admission(task_id).await?;
        let definition = Arc::clone(self.tasks.find(
            &admission.task.name,
            &admission.task.version,
            &mut None,
        )?);
        let context = Arc::new(
            self.durable_context(task_id, admission.operation_id)
                .await?,
        );
        definition.open_workflow(context, fence, journal)?.await
    }

    /// Discovers direct durable children from their owner, not from fork or conversation history.
    pub async fn children(
        &self,
        parent: TaskId,
        expected_revision: Option<u64>,
        after_slot: Option<String>,
        maximum: usize,
    ) -> Result<TaskChildrenPage> {
        self.assert_durable_policy_bindings(None)?;
        validate_children_request(parent, after_slot.as_deref(), maximum)?;
        let state = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state is not bound".into()))?;
        let page = state
            .children(parent, expected_revision, after_slot.clone(), maximum)
            .await?;
        validate_children_page(&page, expected_revision, after_slot.as_deref(), maximum)?;
        Ok(page)
    }
}
