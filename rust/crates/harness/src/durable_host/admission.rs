//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Binds a trusted owner, its exact task/machine registries, and immutable
    /// admission payload provider. The host validates definitions even when a
    /// caller bypasses the high-level typed runtime.
    #[allow(
        clippy::too_many_arguments,
        reason = "each provider boundary is explicit at construction"
    )]
    pub fn new(
        coordinator: DistributedCoordinator<P>,
        stream: Arc<P>,
        payloads: Arc<dyn SchedulerPayloadStore>,
        reader: Arc<dyn ContentResidencyVerifier>,
        owner: Authority,
        owner_scope: Scope,
        verifier: AuthorityVerifier,
        root_scope: RuntimeScope,
        tasks: TaskRegistry,
        machines: MachineRegistry,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self> {
        verifier.verify_audience(&owner, &mut None)?;
        verifier.verify(&owner_scope, &mut None)?;
        for capability in [
            capability::OPERATION_DECLARE,
            capability::OPERATION_OBSERVE,
            capability::OPERATION_CANCEL,
        ] {
            if !owner_scope.capabilities().contains(capability) {
                return Err(Error::Unauthorized(format!(
                    "owner scope lacks {capability}"
                )));
            }
        }
        if !root_scope.grants().is_subset_of(owner_scope.capabilities()) {
            return Err(Error::Unauthorized(
                "runtime grants exceed the signed owner scope".into(),
            ));
        }
        Ok(Self {
            coordinator: Mutex::new(coordinator),
            stream,
            payloads,
            reader,
            owner,
            owner_scope,
            verifier,
            root_scope,
            tasks,
            machines,
            clock,
            policy: None,
            effects: None,
            session_limits: None,
        })
    }

    /// Requires these immutable shared ceilings for every root admitted by this
    /// host. Root admission publishes its declaration and configuration together.
    pub fn with_session_limits(mut self, limits: crate::scheduler::SessionLimits) -> Result<Self> {
        limits.validate(&mut None)?;
        self.session_limits = Some(limits);
        Ok(self)
    }

    /// Admits through the existing task path while retaining the parent's lease.
    pub async fn admit_owned(
        &self,
        admission: TaskAdmissionRecord,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<Admission<TaskId>> {
        let parent = admission
            .parent
            .ok_or_else(|| Error::Invalid("owned child admission has no parent".into()))?;
        self.verify_owner(parent, &fence, false).await?;
        self.admit_record(admission, Some(fence)).await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one complete immutable task admission validation path"
    )]
    pub(super) async fn admit_record(
        &self,
        admission: TaskAdmissionRecord,
        parent_fence: Option<crate::scheduler::LeaseFence>,
    ) -> Result<Admission<TaskId>> {
        let operation_id = admission.operation_id;
        let identity = admission.task.clone();
        let machine = admission.machine.clone();
        let output_schema = admission.output_schema.clone();
        let parent = admission.parent;
        let scope = RuntimeScope::new(admission.grants.clone(), admission.limits)?
            .with_run_limits(admission.run_limits, &mut None)?
            .with_replayed_extensions(admission.extensions.clone())?;
        self.root_scope
            .narrow(scope.grants().clone(), scope.limits())?
            .with_run_limits(scope.run_limits(), &mut None)?;
        self.validate_parent_scope(parent, &scope).await?;
        if parent.is_some() {
            require_descendant_grant(scope.grants(), &identity)?;
        }
        if let Some(extensions) = scope.extensions() {
            extensions.validate(&mut None)?;
        }
        if scope.extensions() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "task extensions differ from owner bindings".into(),
            ));
        }
        self.validate_local_admission_policy(&admission)?;
        if machine.name != identity.name
            || machine.version != identity.version
            || machine.digest == [0; 32]
            || self.machines.resolve(&machine, &mut None)?.is_none()
        {
            return Err(Error::Invalid(
                "task and machine identities disagree".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &identity,
            &machine,
            Some(&admission.input_schema),
            &output_schema,
            Some(&admission.input), &mut None)?;
        admission.validate(&mut None)?;
        let canonical = admission.canonical_value();
        let bytes = crate::contract::canonical_json_bytes(&canonical)?;
        if bytes.len() as u64 > scope.limits().file_bytes {
            return Err(Error::Invalid(
                "durable task admission exceeds its file limit".into(),
            ));
        }
        let key = format!("task-admission:{operation_id}");
        let (state, owner_scope) =
            if let Some(retained) = self.retained_declaration(operation_id).await? {
                if self.read_admission_json(&retained.state).await? != canonical {
                    return Err(Error::Conflict("task admission changed on retry".into()));
                }
                (retained.state, retained.owner_scope)
            } else {
                let state = self.payloads.stage(operation_id, &key, &bytes, None)?.await?;
                state.descriptor().verify(&bytes, &mut None)?;
                if self.read_json(&state).await? != canonical {
                    return Err(Error::Storage(
                        "staged task admission changed before publication".into(),
                    ));
                }
                (state, self.owner_scope.clone())
            };
        let spec = OperationSpec {
            operation_id,
            owner_scope,
            parent: parent.map(|parent| ParentLink {
                operation_id: OperationId::from_bytes(parent.into_bytes()),
                slot: operation_id.to_string(),
            }),
            owner: DurableOwner::Attached {
                authority: self.owner.clone(),
            },
            entrypoint: EntrypointRef {
                name: identity.name,
                version: identity.version,
                digest: identity.digest,
                result_schema: output_schema,
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: BTreeMap::new(),
            orchestration: Orchestration::Leaf,
            state,
        };
        let outcome = if let Some(fence) = parent_fence {
            self.coordinator
                .lock()
                .await
                .declare_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    IdempotencyKey::new(key)?,
                    Some(fence),
                    &mut None,
                )
                .await
        } else {
            self.declare_task(spec, IdempotencyKey::new(key)?).await
        };
        match outcome {
            Ok(_) => Ok(Admission::Accepted(TaskId::from_bytes(
                operation_id.into_bytes(),
            ))),
            Err(Error::Indeterminate(_)) => Ok(Admission::Indeterminate { operation_id }),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn declare_task(
        &self,
        spec: OperationSpec,
        key: IdempotencyKey,
    ) -> Result<bool> {
        let mut coordinator = self.coordinator.lock().await;
        if let Some(limits) = self.session_limits.filter(|_| spec.parent.is_none()) {
            coordinator
                .declare_session(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    limits,
                    key,
                    &mut None,
                )
                .await
        } else {
            coordinator
                .declare_operation(
                    &self.owner,
                    &self.owner_scope,
                    &self.verifier,
                    spec,
                    key,
                    None,
                    &mut None,
                )
                .await
        }
    }

    /// Pins the policy enforced by this owner, independently of caller contexts.
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>) -> Result<Self> {
        validate_policy_identity(&policy.identity(), None)?;
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    pub(super) async fn read_json(&self, reference: &FileRef) -> Result<Value> {
        reference.validate(&mut None)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("durable task payload is not JSON".into()));
        }
        let bytes = self.reader.read(reference, reference.descriptor().byte_length(), None)?.await?;
        reference.descriptor().verify(&bytes, &mut None)?;
        let value: Value = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if crate::contract::canonical_json_bytes(&value)? != bytes.as_ref() {
            return Err(Error::Invalid(
                "durable task payload is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    pub(super) async fn retained_declaration(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<OperationSpec>> {
        let declaration_key = IdempotencyKey::new(format!("task-admission:{operation_id}"))?;
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &declaration_key, &mut None)
        .await?;
        if indexed.is_some() {
            return Ok(indexed);
        }
        {
            // Missing/legacy locations are not evidence of missing admission.
            // Bootstrap through the existing authoritative reducer, then use
            // its verified declaration. This fallback remains a mutable read.
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(&mut None).await?;
            match coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id, &mut None) {
                Ok(operation) => Ok(Some(operation.spec.clone())),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    pub(super) async fn read_admission_json(&self, reference: &FileRef) -> Result<Value> {
        self.read_json(reference)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed task admission content is missing".into())
                }
                other => other,
            })
    }

    pub(super) async fn admission(&self, operation_id: OperationId) -> Result<TaskAdmissionRecord> {
        let spec = self
            .retained_declaration(operation_id)
            .await?
            .ok_or_else(|| Error::NotFound("task admission".into()))?;
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            // Session configuration and parent links are immutable. A cached
            // hierarchy can check these pinned ceilings without refreshing
            // unrelated lifecycle events; unknown descendants still bootstrap.
            let retained = match coordinator.scheduler().session_limits(operation_id, &mut None) {
                Err(Error::NotFound(_)) => {
                    coordinator.refresh(&mut None).await?;
                    coordinator.scheduler().session_limits(operation_id, &mut None)?
                }
                other => other?,
            };
            if retained != limits {
                return Err(Error::Conflict(
                    "retained session ceilings differ from owner bindings".into(),
                ));
            }
        }
        let value = self.read_admission_json(&spec.state).await?;
        let (admission, _) = TaskAdmissionRecord::from_canonical_value(value, &mut None)?;
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "durable task policy differs from its admission".into(),
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(&mut None)?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "retained task extensions differ from owner bindings".into(),
            ));
        }
        self.root_scope
            .narrow(admission.grants.clone(), admission.limits)?
            .with_run_limits(admission.run_limits, &mut None)?;
        if admission.operation_id != operation_id
            || admission.task.name != spec.entrypoint.name
            || admission.task.version != spec.entrypoint.version
            || admission.task.digest != spec.entrypoint.digest
            || admission.output_schema != spec.entrypoint.result_schema
            || spec.parent
                != admission.parent.map(|parent| ParentLink {
                    operation_id: OperationId::from_bytes(parent.into_bytes()),
                    slot: operation_id.to_string(),
                })
            || admission.machine.name != admission.task.name
            || admission.machine.version != admission.task.version
            || self.machines.resolve(&admission.machine, &mut None)?.is_none()
        {
            return Err(Error::Conflict(
                "durable task admission disagrees with its declaration".into(),
            ));
        }
        self.tasks.validate_durable_admission(
            &admission.task,
            &admission.machine,
            Some(&admission.input_schema),
            &admission.output_schema,
            Some(&admission.input), &mut None)?;
        Ok(admission)
    }

    pub(super) async fn validate_parent_scope(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
    ) -> Result<()> {
        if let Some(parent) = parent {
            let admitted = self
                .admission(OperationId::from_bytes(parent.into_bytes()))
                .await?;
            RuntimeScope::new(admitted.grants, admitted.limits)?
                .with_run_limits(admitted.run_limits, &mut None)?
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits(), &mut None)?;
        }
        Ok(())
    }

    pub(super) fn validate_local_admission_policy(
        &self,
        admission: &TaskAdmissionRecord,
    ) -> Result<()> {
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "task admission policy differs from owner".into(),
            ));
        }
        if admission.execution.is_some() {
            return Err(Error::Unsupported(
                "local durable host cannot admit a remote execution route".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn event_key(
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<StreamKey> {
        let identity = format!("harness/v2/{kind}/{task_id}/{operation_id}");
        StreamKey::new(OwnedBytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Invalid(error.to_string()))
    }
}
