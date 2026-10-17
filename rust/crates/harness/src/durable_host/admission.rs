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
            coordinator: Arc::new(crate::sync::Mutex::new(coordinator)),
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
            policy_binding: None,
            effects: None,
            session_limits: None,
            #[cfg(feature = "filesystem")]
            resource_creation: Vec::new(),
        })
    }

    /// Requires these immutable shared ceilings for every root admitted by this
    /// host. Root admission publishes its declaration and configuration together.
    pub fn with_session_limits(mut self, limits: crate::scheduler::SessionLimits) -> Result<Self> {
        limits.validate(&mut None)?;
        self.session_limits = Some(limits);
        Ok(self)
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
        #[cfg(feature = "filesystem")]
        let scope = self
            .authenticated_scope(&admission, &mut None)
            .await?
            .0
            .with_replayed_extensions(admission.extensions.clone())?;
        #[cfg(not(feature = "filesystem"))]
        let scope = {
            let scope = RuntimeScope::new(admission.grants.clone(), admission.limits)?
                .with_run_limits(admission.run_limits, &mut None)?
                .with_replayed_extensions(admission.extensions.clone())?;
            self.root_scope
                .narrow(scope.grants().clone(), scope.limits())?
                .with_run_limits(scope.run_limits(), &mut None)?;
            scope
        };
        self.validate_parent_scope(parent, &scope, Some(&identity), &mut None)
            .await?;
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
            Some(&admission.input),
            &mut None,
        )?;
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
            if let Some(retained) = self.retained_declaration(operation_id, &mut None).await? {
                if self.read_json(&retained.state, None).await? != canonical {
                    return Err(Error::Conflict("task admission changed on retry".into()));
                }
                (retained.state, retained.owner_scope)
            } else {
                let state = self
                    .payloads
                    .stage(operation_id, &key, &bytes, None)?
                    .await?;
                state.descriptor().verify(&bytes, &mut None)?;
                if self.read_json(&state, None).await? != canonical {
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
        let identity = policy.identity();
        validate_policy_identity(&identity, None)?;
        self.policy_binding = Some(identity);
        self.policy = Some(policy);
        Ok(self)
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    pub(super) async fn read_json(
        &self,
        reference: &FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Value> {
        #[cfg(feature = "filesystem")]
        let mut history = io.map(|io| io.history_io());
        #[cfg(feature = "filesystem")]
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn crate::store::HistoryIo> = None;
        reference.validate(&mut work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("durable task payload is not JSON".into()));
        }
        let bytes = self
            .reader
            .read(reference, reference.descriptor().byte_length(), None, io)?
            .await?;
        reference.descriptor().verify(&bytes, &mut work)?;
        let mut value: Value = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.decode_json(&bytes)?,
            _ => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?,
        };
        value.sort_all_objects();
        let canonical = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => io.encode(&value, reference.descriptor().byte_length(), false)?,
            _ => canonical_payload(&value, reference.descriptor().byte_length(), io)?,
        };
        if canonical.as_ref() != bytes.as_ref() {
            return Err(Error::Invalid(
                "durable task payload is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    pub(super) async fn retained_declaration(
        &self,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<OperationSpec>> {
        let text =
            crate::executor::preparation_format(format_args!("task-admission:{operation}"), work)?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, text.capacity() as u64);
        let key = IdempotencyKey::new(text)?;
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation,
            &key,
            quota.work(),
        )
        .await?;
        drop(key);
        if indexed.is_some() {
            return Ok(indexed);
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(quota.work()).await?;
        match coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation,
            quota.work(),
        ) {
            Ok(operation) => Ok(Some(operation.spec.clone_with_work(quota.work())?)),
            Err(Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn validate_parent_scope(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
        descendant: Option<&ComponentIdentity>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(parent) = parent {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let admitted = self
                .admission(OperationId::from_bytes(parent.into_bytes()), quota.work())
                .await?;
            if quota.work().is_some() {
                quota.adopt(
                    std::mem::size_of::<TaskAdmissionRecord>() as u64
                        + admitted.logical_owned_bytes()?,
                )?;
            }
            let parent = RuntimeScope::new(admitted.grants, admitted.limits)?
                .with_run_limits(admitted.run_limits, quota.work())?;
            if let Some(identity) = descendant {
                require_descendant_grant(parent.grants(), identity)?;
            }
            #[cfg(feature = "filesystem")]
            let created = scope.created_access();
            #[cfg(feature = "filesystem")]
            if let Some(access) = &created {
                crate::core::inspect_metadata(
                    &(&access.proofs, &scope.resource_proofs),
                    quota.work(),
                )?;
            }
            #[cfg(feature = "filesystem")]
            let created = created.filter(|access| access.proofs == scope.resource_proofs);
            for grant in scope.grants().iter() {
                crate::core::inspect_metadata(grant, quota.work())?;
                let permitted = parent.grants().contains(grant);
                #[cfg(feature = "filesystem")]
                let permitted = permitted
                    || created
                        .as_ref()
                        .is_some_and(|access| access.grants.contains(grant));
                if !permitted {
                    return Err(crate::store::guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Unauthorized,
                        "runtime scope cannot widen ancestor authority",
                    ));
                }
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(native) = quota
                .work()
                .as_deref()
                .and_then(|io| io.original_native_work())
            {
                parent.validate_narrowing_with_native_work(
                    parent.grants(),
                    &scope.limits(),
                    native,
                )?;
            } else {
                parent.narrow(parent.grants().clone(), scope.limits())?;
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            parent.narrow(parent.grants().clone(), scope.limits())?;
            parent.validate_run_limit_attenuation(&scope.run_limits(), quota.work())?;
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
        task: TaskId,
        operation: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<(StreamKey, u64)> {
        let identity = crate::executor::preparation_format(
            format_args!("harness/v2/{kind}/{task}/{operation}"),
            work,
        )?;
        let mut quota =
            crate::wire_codec::CodecQuota::from_retained(work, identity.capacity() as u64);
        if let Some(io) = quota.work().as_deref_mut() {
            io.hashed_bytes(identity.len() as u64)?;
        }
        let digest = blake3::hash(identity.as_bytes());
        drop(identity);
        crate::distributed::digest_key(digest.as_bytes(), quota.work())
    }
}
