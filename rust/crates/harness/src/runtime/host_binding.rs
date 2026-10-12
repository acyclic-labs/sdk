use super::*;

impl AgentHarness {
pub(super) async fn validate_observed_admission(
        &self, observed: &TaskAdmissionRecord, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = task_original_work(io)? {
            let mut observer = crate::store::history::native_work_io(work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            observed.validate(&mut Some(&mut observer))?;
            self.tasks.validate_durable_admission(
                &observed.task, &observed.machine, Some(&observed.input_schema),
                &observed.output_schema, Some(&observed.input), &mut Some(&mut observer),
            )?;
            if observed.resource_proofs.is_empty() {
                self.scope.validate_narrowing_with_native_work(&observed.grants, &observed.limits, work)?;
                self.scope.validate_run_limit_attenuation(&observed.run_limits, &mut Some(&mut crate::store::native_work_io(work).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?))?;
            } else {
                let state = self.state.as_ref().ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "resource-derived admission state is not bound",
                })?;
                let admitted = state.resume_scope_with_native_work(
                    observed.task_id(), observed.operation_id, work,
                )?.await?;
                if !admitted.1.matches_work(work) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resumed scope differs from its original metadata ledger" });
                }
                let observed_digest = observed.canonical_digest_with_work(&mut Some(&mut observer))?;
                if admitted.0.created_access.as_ref().is_none_or(|access|
                    access.admission_digest != observed_digest)
                {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "resource scope seal differs from observed admission" });
                }
                self.validate_native_resumed_scope(&admitted.0, observed.operation_id, work)?;
            }
            if observed.policy != self.policy_identity
                || observed.extensions.as_ref() != self.scope.extensions()
            {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "retained admission changed owner bindings" });
            }
            return match (&self.execution, observed.execution.as_ref()) {
                (Some(provider), Some(placement)) if &placement.provider == provider.identity_with_native_work(work)? => Ok(()),
                (None, None) => Ok(()),
                _ => Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "retained execution route differs from runtime" }),
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = io;
        observed.validate(&mut None)?;
        self.tasks.validate_durable_admission(
            &observed.task,
            &observed.machine,
            Some(&observed.input_schema),
            &observed.output_schema,
            Some(&observed.input),
            &mut None,
        )?;
        if observed.resource_proofs.is_empty() {
            self.scope.narrow(observed.grants.clone(), observed.limits)?
                .with_run_limits(observed.run_limits, &mut None)?;
        } else {
            let state = self.state.as_ref().ok_or_else(|| {
                Error::Unsupported("resource-derived admission state is not bound".into())
            })?;
            let admitted = state.resume_scope(
                observed.task_id(), observed.operation_id,
            ).await?;
            let observed_digest = crate::contract::canonical_json_digest(
                &observed.canonical_value(),
            )?;
            if admitted.created_access.as_ref().is_none_or(|access| {
                access.admission_digest != observed_digest
            }) {
                return Err(Error::Unauthorized("resource scope seal differs from observed admission".into()));
            }
            self.scope.resume_sealed(admitted, observed.operation_id)?;
        }
        if observed.policy != self.policy_identity
            || observed.extensions.as_ref() != self.scope.extensions()
        {
            return Err(Error::Conflict(
                "retained admission changed owner bindings".into(),
            ));
        }
        match (&self.execution, observed.execution.as_ref()) {
            (Some(provider), Some(placement)) if placement.provider == provider.identity() => {
                placement.validate(&mut None)?;
            }
            (None, None) => {}
            _ => {
                return Err(Error::Conflict(
                    "retained execution route differs from runtime".into(),
                ));
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

pub(super) fn bind_execution(
        self: &Arc<Self>,
        execution: Arc<dyn ExecutionProvider>,
    ) -> Result<Arc<Self>> {
        let identity = execution.identity();
        validate_component_label(&identity.name, "execution provider name")?;
        validate_component_label(&identity.version, "execution provider version")?;
        if identity.digest == [0; 32] {
            return Err(Error::Invalid("execution provider digest is empty".into()));
        }
        if self
            .state
            .as_ref()
            .and_then(|state| state.execution_identity())
            .as_ref()
            != Some(&identity)
            || self
                .spawner
                .as_ref()
                .and_then(|spawner| spawner.execution_identity())
                .as_ref()
                != Some(&identity)
        {
            return Err(Error::Conflict(
                "execution provider, state, and spawner routes differ".into(),
            ));
        }
        let mut bound = self.as_ref().clone();
        bound.execution = Some(execution);
        Ok(Arc::new(bound))
    }

pub(super) fn bind_state(self: &Arc<Self>, state: Arc<dyn TaskStateProvider>) -> Result<Arc<Self>> {
        if state.policy_identity() != self.policy_identity {
            return Err(Error::Conflict("runtime and state policies differ".into()));
        }
        let mut bound = self.as_ref().clone();
        bound.state = Some(state);
        Ok(Arc::new(bound))
    }

pub(super) fn assert_durable_policy_bindings(&self, work: Option<&NativeWork>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.charge(acyclic_fs::WorkCounters::default())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
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
                { $provider.$ordinary().map(std::borrow::Cow::Owned) }
            }};
        }
        let mut mismatch = false;
        if let Some(host) = &self.host {
            let host: &dyn TaskStateProvider = host.as_ref();
            mismatch = identity!(host, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(state) = &self.state {
            mismatch = identity!(state, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(spawner) = &self.spawner {
            mismatch = identity!(spawner, policy_identity, policy_identity_with_native_work).as_deref()
                != self.policy_identity.as_ref();
        }
        if !mismatch && let Some(execution) = &self.execution {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let current = match work {
                Some(work) => std::borrow::Cow::Borrowed(execution.identity_with_native_work(work)?),
                None => std::borrow::Cow::Owned(execution.identity()),
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let current: std::borrow::Cow<'_, ComponentIdentity> = std::borrow::Cow::Owned(execution.identity());
            mismatch = match &self.state {
                Some(state) => identity!(state, execution_identity, execution_identity_with_native_work).as_deref() != Some(current.as_ref()),
                None => true,
            };
            if !mismatch {
                mismatch = match &self.spawner {
                    Some(spawner) => identity!(spawner, execution_identity, execution_identity_with_native_work).as_deref() != Some(current.as_ref()),
                    None => true,
                };
            }
        }
        if mismatch {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "durable provider binding changed after native source configuration" });
            }
            return Err(Error::Conflict("durable policy implementation changed after binding".into()));
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = work;
        Ok(())
    }

#[cfg(feature = "filesystem")]
pub(crate) fn original_host_binding_matches(
        &self, host: &dyn DurableTaskHost, policy: Option<&dyn ToolPolicy>,
    ) -> bool {
        self.host.as_ref().is_some_and(|bound| std::ptr::eq(bound.as_ref(), host))
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
            Ok(observed) if observed == *expected => self.validate_observed_admission(&observed, None).await,
            Ok(_) => Err(Error::Conflict(
                "spawner and state host admission bindings differ".into(),
            )),
            Err(Error::NotFound(_)) => Err(Error::Indeterminate(expected.operation_id)),
            Err(error) => Err(error),
        }
    }

/// Replaces only durable admission.

pub(super) fn bind_spawner(self: &Arc<Self>, spawner: Arc<dyn TaskSpawner>) -> Result<Arc<Self>> {
        if self.state.is_none() {
            return Err(Error::Invalid(
                "durable spawner requires a state/observation host".into(),
            ));
        }
        if spawner.policy_identity() != self.policy_identity {
            return Err(Error::Conflict(
                "runtime and spawner policies differ".into(),
            ));
        }
        let mut bound = self.as_ref().clone();
        bound.spawner = Some(spawner);
        Ok(Arc::new(bound))
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
