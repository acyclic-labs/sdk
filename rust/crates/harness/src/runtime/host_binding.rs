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
                Some(&observed.input),
                work,
            )?;
            if observed.resource_proofs.is_empty() {
                if !self
                    .scope
                    .allows_authority(&observed.grants, &observed.limits)
                {
                    return Err(task_workflow_error(
                        native,
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
            let state = self.state.as_ref().ok_or_else(|| {
                task_workflow_error(
                    native,
                    crate::wire::ErrorCode::Unsupported,
                    "resource-derived admission state is not bound",
                )
            })?;
            let resumed = {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(work) = native {
                    let resumed = state
                        .resume_scope_with_native_work(
                            observed.task_id(),
                            observed.operation_id,
                            work,
                        )?
                        .await?;
                    if !resumed.1.work().shares_allocation_context(work) {
                        return Err(task_workflow_error(
                            native,
                            crate::wire::ErrorCode::Unauthorized,
                            "resumed scope differs from its original metadata ledger",
                        ));
                    }
                    (resumed.0, Some(resumed.1))
                } else {
                    (
                        state
                            .resume_scope(observed.task_id(), observed.operation_id)
                            .await?,
                        None,
                    )
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                (
                    state
                        .resume_scope(observed.task_id(), observed.operation_id)
                        .await?,
                    None::<NativeCustody>,
                )
            };
            let digest = crate::executor::with_history_io(io, |work| {
                observed.canonical_digest_with_work(work)
            })?;
            if resumed
                .0
                .created_access
                .as_ref()
                .is_none_or(|access| access.admission_digest != digest)
            {
                return Err(task_workflow_error(
                    native,
                    crate::wire::ErrorCode::Unauthorized,
                    "resource scope seal differs from observed admission",
                ));
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = native {
                self.validate_native_resumed_scope(&resumed.0, observed.operation_id, work)?;
            } else {
                self.scope.resume_sealed(resumed.0, observed.operation_id)?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            self.scope.resume_sealed(resumed.0, observed.operation_id)?;
        }
        if observed.policy != self.policy_identity
            || observed.extensions.as_ref() != self.scope.extensions()
        {
            return Err(task_workflow_error(
                native,
                crate::wire::ErrorCode::Conflict,
                "retained admission changed owner bindings",
            ));
        }
        match (&self.execution, observed.execution.as_ref()) {
            (Some(provider), Some(placement)) => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(work) = native {
                    if provider.identity_with_native_work(work)? == &placement.provider {
                        return Ok(());
                    }
                    return Err(task_workflow_error(
                        native,
                        crate::wire::ErrorCode::Conflict,
                        "retained execution route differs from runtime",
                    ));
                }
                if placement.provider != provider.identity() {
                    return Err(task_workflow_error(
                        native,
                        crate::wire::ErrorCode::Conflict,
                        "retained execution route differs from runtime",
                    ));
                }
                placement.validate(&mut None)?;
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(task_workflow_error(
                native,
                crate::wire::ErrorCode::Conflict,
                "retained execution route differs from runtime",
            )),
        }
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
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters::default())
                .map_err(|cause| Error::NativeWork {
                    cause,
                    custody: None,
                })?;
        }
        macro_rules! identity {
            ($provider:expr, $ordinary:ident, $native:ident) => {{
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    if let Some(work) = work {
                        $provider.$native(work)?.map(std::borrow::Cow::Borrowed)
                    } else {
                        $provider.$ordinary().map(std::borrow::Cow::Owned)
                    }
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                {
                    $provider.$ordinary().map(std::borrow::Cow::Owned)
                }
            }};
        }
        let mut mismatch = false;
        if let Some(host) = &self.host {
            let host: &dyn TaskStateProvider = host.as_ref();
            mismatch = identity!(host, policy_identity, policy_identity_with_native_work)
                .as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(state) = &self.state {
            mismatch = identity!(state, policy_identity, policy_identity_with_native_work)
                .as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(spawner) = &self.spawner {
            mismatch = identity!(spawner, policy_identity, policy_identity_with_native_work)
                .as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(execution) = &self.execution {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let current = match work {
                Some(work) => {
                    std::borrow::Cow::Borrowed(execution.identity_with_native_work(work)?)
                }
                None => std::borrow::Cow::Owned(execution.identity()),
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let current: std::borrow::Cow<'_, ComponentIdentity> =
                std::borrow::Cow::Owned(execution.identity());
            mismatch = match &self.state {
                Some(state) => {
                    identity!(
                        state,
                        execution_identity,
                        execution_identity_with_native_work
                    )
                    .as_deref()
                        != Some(current.as_ref())
                }
                None => true,
            };
            if !mismatch {
                mismatch = match &self.spawner {
                    Some(spawner) => {
                        identity!(
                            spawner,
                            execution_identity,
                            execution_identity_with_native_work
                        )
                        .as_deref()
                            != Some(current.as_ref())
                    }
                    None => true,
                };
            }
        }
        if mismatch {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "durable provider binding changed after native source configuration",
                });
            }
            return Err(Error::Conflict(
                "durable policy implementation changed after binding".into(),
            ));
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
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
            .is_some_and(|bound| std::ptr::eq(bound.as_ref(), host))
            && match (self.policy.as_deref(), policy) {
                (Some(bound), Some(policy)) => std::ptr::eq(bound, policy),
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
        let requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .collect::<Vec<_>>();
        if requirements.is_empty() {
            return Ok(None);
        }
        let runtime = self
            .extension_runtime
            .as_ref()
            .ok_or_else(|| Error::Unsupported("task requires a native extension runtime".into()))?;
        runtime.validate_admission(scope.extensions(), &mut None)?;
        Ok(Some(runtime.retain(requirements, false)?))
    }

    pub(super) fn retain_task_extensions_existing<I, O>(
        &self,
        definition: &TaskDefinition<I, O>,
        scope: &RuntimeScope,
    ) -> Result<Vec<crate::extension::ExtensionLease>> {
        let requirements = definition
            .requirements()
            .iter()
            .filter(|requirement| requirement.starts_with("extension:"))
            .cloned()
            .collect::<Vec<_>>();
        if requirements.is_empty() {
            return Ok(Vec::new());
        }
        let runtime = self.extension_runtime.as_ref().ok_or_else(|| {
            Error::Unsupported("retained task requires a native extension runtime".into())
        })?;
        runtime.validate_admission(scope.extensions(), &mut None)?;
        runtime.retain(requirements, true)
    }
}
