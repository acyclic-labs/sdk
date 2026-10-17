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
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == admission => {
                self.validate_observed_admission(&observed, None).await?;
            }
            Ok(_) => {
                return Err(Error::Conflict(
                    "spawner and state host admission bindings differ".into(),
                ));
            }
            Err(Error::NotFound(_)) => return Err(Error::Indeterminate(operation_id)),
            Err(error) => return Err(error),
        }
        Ok(Some((task_id, admission.task)))
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
        if observed.task != definition.identity {
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

    /// Builds a registered admission using the caller's IO and optional parent scope.
    #[cfg(feature = "filesystem")]
    #[allow(
        clippy::too_many_arguments,
        reason = "The admission boundary keeps the task identity, payload and authority explicit."
    )]
    pub fn registered_admission(
        &self,
        operation: OperationId,
        name: &str,
        version: &str,
        input: Value,
        parent: Option<(TaskId, &RuntimeScope)>,
        expected_digest: Option<[u8; 32]>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(TaskAdmissionRecord, u64)> {
        let native = task_original_work(io)?;
        if native.is_some() && parent.is_none() {
            crate::model::charge_items(io, 0)?;
            if expected_digest.is_none() {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "root admission requires its pinned task digest",
                ));
            }
            if self.execution.is_some() {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "native root admission requires the configured local durable host",
                ));
            }
        }
        self.assert_durable_policy_bindings(native)?;
        crate::executor::with_history_io(io, |work| {
            let mut record = self.tasks.registered_admission(
                operation,
                name,
                version,
                input,
                parent.map(|(task, _)| task),
                expected_digest,
                parent.map_or(&self.scope, |(_, scope)| scope),
                work,
            )?;
            if parent.is_none() {
                record.policy = self
                    .policy_identity
                    .as_ref()
                    .map(|identity| {
                        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                        let copied = task_copy_component(identity, &mut quota)?;
                        quota.transfer();
                        Ok::<_, Error>(copied)
                    })
                    .transpose()?;
            }
            let bytes = if native.is_some() {
                (std::mem::size_of::<TaskAdmissionRecord>() as u64)
                    .checked_add(record.logical_owned_bytes()?)
                    .ok_or_else(|| {
                        crate::executor::stock_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "native admission metadata overflows",
                        )
                    })?
            } else {
                0
            };
            Ok((record, bytes))
        })
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
            if context.native_custody.is_none() {
                if !Arc::ptr_eq(&context.harness, self)
                    || !context.original_task_binding_matches(task_id, fence)
                    || journal.task_binding() != Some((task_id, fence))
                {
                    return Err(Error::Unauthorized(
                        "workflow context or journal owner differs".into(),
                    ));
                }
                context.owned_task_binding(None)?;
                let admission = context.owned_mailbox.as_ref().unwrap().admission().0;
                let definition = Arc::clone(self.tasks.find(
                    &admission.task.name,
                    &admission.task.version,
                    &mut None,
                )?);
                return definition.open_workflow(context, fence, journal)?.await;
            }
            let work = context.native_work()?;
            let error = |code, reason| Error::NativeBoundary { code, reason };
            if !Arc::ptr_eq(&context.harness, self)
                || !context.original_task_binding_matches(task_id, fence)
                || journal.task_binding() != Some((task_id, fence))
            {
                return Err(error(
                    crate::wire::ErrorCode::Unauthorized,
                    "native workflow context or journal owner differs",
                ));
            }
            let identity = journal.original_machine_identity().ok_or_else(|| {
                error(
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
                    return Err(error(
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
        let context = self
            .durable_context(task_id, admission.operation_id, None)
            .await?;
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
