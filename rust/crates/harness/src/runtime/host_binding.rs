use super::*;

impl AgentHarness {
    pub(super) async fn validate_observed_admission(
        &self,
        observed: &TaskAdmissionRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let native = task_original_work(io)?;
        crate::executor::with_history_io(io, |work| {
            observed.validate(work)?;
            self.tasks.validate_durable_admission(
                &observed.task,
                &observed.machine,
                Some(&observed.input_schema),
                &observed.output_schema,
                // The record already validated input; the registry checks exact schema identity.
                None,
                work,
            )?;
            if observed.resource_proofs.is_empty() {
                if !self
                    .scope
                    .allows_authority(&observed.grants, &observed.limits)
                {
                    return Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "runtime scope cannot widen ancestor authority",
                    ));
                }
                self.scope
                    .validate_run_limit_attenuation(&observed.run_limits, work)?;
            }
            Ok(())
        })?;
        if !observed.resource_proofs.is_empty() {
            self.validate_created_scope(observed, io).await?;
        }
        if observed.policy != self.policy_identity
            || observed.extensions.as_ref() != self.scope.extensions()
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "retained admission changed owner bindings",
            ));
        }
        let matches = match (&self.execution, observed.execution.as_ref()) {
            (Some(provider), Some(placement)) => match native {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Some(work) => provider.identity_with_native_work(work)? == &placement.provider,
                _ => placement.provider == provider.identity(),
            },
            (None, None) => true,
            _ => false,
        };
        if !matches {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "retained execution route differs from runtime",
            ));
        }
        Ok(())
    }

    async fn validate_created_scope(
        &self,
        observed: &TaskAdmissionRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let native = task_original_work(io)?;
        let state = self.state.as_ref().ok_or_else(|| {
            crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "resource-derived admission state is not bound",
            )
        })?;
        let resumed = match native {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(work) => {
                let (scope, custody) = state
                    .resume_scope_with_native_work(observed.task_id(), observed.operation_id, work)?
                    .await?;
                if !custody.work().shares_allocation_context(work) {
                    return Err(crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Unauthorized,
                        "resumed scope differs from its original metadata ledger",
                    ));
                }
                (scope, Some(custody))
            }
            _ => (
                state
                    .resume_scope(observed.task_id(), observed.operation_id)
                    .await?,
                None::<NativeCustody>,
            ),
        };
        let digest =
            crate::executor::with_history_io(io, |work| observed.canonical_digest_with_work(work))?;
        if resumed
            .0
            .created_access
            .as_ref()
            .is_none_or(|access| access.admission_digest != digest)
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "resource scope seal differs from observed admission",
            ));
        }
        match native {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Some(_) => self.scope.validate_resumed_scope(&resumed.0, io)?,
            _ => {
                self.scope.resume_sealed(resumed.0, observed.operation_id)?;
            }
        }
        Ok(())
    }

    pub(super) async fn reconcile_scoped_admission<I, O>(
        &self,
        operation_id: OperationId,
        definition: &Arc<TaskDefinition<I, O>>,
        expected: &TaskAdmissionRecord,
        scope: &RuntimeScope,
        host: &Arc<dyn TaskStateProvider>,
        spawner: &Arc<dyn TaskSpawner>,
    ) -> Result<Option<Admission<RuntimeTask<O>>>>
    where
        I: Serialize + Send + 'static,
        O: DeserializeOwned + Send + 'static,
    {
        let retained_admission = match spawner.reconcile_admission(operation_id).await {
            Ok(retained) => retained,
            Err(Error::Unsupported(_)) => None,
            Err(error) => return Err(error),
        };
        let Some((task_id, retained)) = retained_admission else {
            return Ok(None);
        };
        if retained.operation_id != operation_id {
            return Err(Error::Conflict(
                "spawner reconciled another operation".into(),
            ));
        }
        let observed = match host.observe_admission(task_id).await {
            Ok(observed) => observed,
            Err(Error::NotFound(_)) => {
                return Ok(Some(Admission::Indeterminate { operation_id }));
            }
            Err(error) => return Err(error),
        };
        let mut base = observed.clone();
        base.execution = None;
        if base != *expected || observed != retained {
            return Err(Error::Conflict(
                "operation belongs to another admission request".into(),
            ));
        }
        self.validate_observed_admission(&observed, None).await?;
        let extension_leases = self.retain_task_extensions_existing(definition, scope)?;
        Ok(Some(Admission::Accepted(RuntimeTask::Durable {
            task_id,
            host: Arc::clone(host),
            output_schema: definition.output_schema.clone(),
            extensions: Some(extension_leases),
        })))
    }

    pub(super) fn assert_durable_policy_bindings(&self, work: Option<&NativeWork>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native_io = work
            .map(crate::executor::PreparationIo::from_native_work)
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = native_io.as_ref();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters::default())?;
        }
        macro_rules! identity {
            ($provider:expr, $ordinary:ident, $native:ident) => {
                match work {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Some(work) => $provider.$native(work)?.map(std::borrow::Cow::Borrowed),
                    _ => $provider.$ordinary().map(std::borrow::Cow::Owned),
                }
            };
        }
        for state in self
            .host
            .as_ref()
            .map(|host| host.as_ref() as &dyn TaskStateProvider)
            .into_iter()
            .chain(self.state.as_deref())
        {
            if identity!(state, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref()
            {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "durable provider binding changed",
                ));
            }
        }
        if let Some(spawner) = &self.spawner
            && identity!(spawner, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref()
        {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "durable provider binding changed",
            ));
        }
        if let Some(execution) = &self.execution {
            let current: std::borrow::Cow<'_, ComponentIdentity> = match work {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Some(work) => {
                    std::borrow::Cow::Borrowed(execution.identity_with_native_work(work)?)
                }
                _ => std::borrow::Cow::Owned(execution.identity()),
            };
            let matches_state = match &self.state {
                Some(state) => {
                    identity!(
                        state,
                        execution_identity,
                        execution_identity_with_native_work
                    )
                    .as_deref()
                        == Some(current.as_ref())
                }
                None => false,
            };
            let matches_spawner = matches_state
                && match &self.spawner {
                    Some(spawner) => {
                        identity!(
                            spawner,
                            execution_identity,
                            execution_identity_with_native_work
                        )
                        .as_deref()
                            == Some(current.as_ref())
                    }
                    None => false,
                };
            if !matches_spawner {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "durable provider binding changed",
                ));
            }
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn original_host_binding_matches(
        &self,
        host: &dyn DurableTaskHost,
        policy: Option<&dyn ToolPolicy>,
    ) -> bool {
        self.host
            .as_ref()
            .is_some_and(|bound| std::ptr::addr_eq(bound.as_ref(), host))
            && match (self.policy.as_deref(), policy) {
                (Some(bound), Some(policy)) => std::ptr::addr_eq(bound, policy),
                (None, None) => true,
                _ => false,
            }
    }

    pub(super) async fn verify_spawner_admission(
        &self,
        task_id: TaskId,
        expected: &TaskAdmissionRecord,
    ) -> Result<()> {
        self.assert_durable_policy_bindings(None)?;
        let host = self
            .state
            .as_ref()
            .ok_or_else(|| Error::Unsupported("durable task state host is not bound".into()))?;
        match host.observe_admission(task_id).await {
            Ok(observed) if observed == *expected => {
                self.validate_observed_admission(&observed, None).await
            }
            Ok(_) => Err(Error::Conflict(
                "spawner and state host admission bindings differ".into(),
            )),
            Err(Error::NotFound(_)) => Err(Error::Indeterminate(expected.operation_id)),
            Err(error) => Err(error),
        }
    }
}

impl AgentHarness {
    pub(super) fn retain_task_extensions<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<Option<Vec<crate::extension::ExtensionLease>>> {
        let mut requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .peekable();
        if requirements.peek().is_none() {
            return Ok(None);
        }
        let runtime =
            self.scope.extension_runtime.as_ref().ok_or_else(|| {
                Error::Unsupported("task requires a native extension runtime".into())
            })?;
        runtime.validate_admission(scope.extensions(), &mut None)?;
        Ok(Some(runtime.retain(requirements, false)?))
    }

    pub(super) fn retain_task_extensions_existing<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<Vec<crate::extension::ExtensionLease>> {
        let mut requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .peekable();
        if requirements.peek().is_none() {
            return Ok(Vec::new());
        }
        let runtime = self.scope.extension_runtime.as_ref().ok_or_else(|| {
            Error::Unsupported("retained task requires a native extension runtime".into())
        })?;
        runtime.validate_admission(scope.extensions(), &mut None)?;
        runtime.retain(requirements, true)
    }
}
