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
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            verifier.verify_audience(&owner, work)?;
            verifier.verify(&owner_scope, work)
        })?;
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
            creation_providers: Vec::new(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: io.filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(io.owned_bytes()?)).transpose()?,
        })
    }

    /// Requires these immutable shared ceilings for every root admitted by this
    /// host. Root admission publishes its declaration and configuration together.
    pub fn with_session_limits(mut self, limits: crate::scheduler::SessionLimits, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        crate::executor::with_history_io(io, |work| limits.validate(work))?;
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
        self.verify_owner(parent, &fence, false, &mut None).await?;
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
            if let Some(retained) = self.retained_declaration(operation_id, None).await? {
                if self.read_admission_json(&retained.state, None).await? != canonical {
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
    pub fn with_policy(mut self, policy: Arc<dyn ToolPolicy>, io: Option<&crate::executor::PreparationIo>) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let paid = io.and_then(crate::executor::PreparationIo::original_native_work)
            .map(|work| policy.identity_with_native_work(work)).transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = paid.as_ref().map(|paid| &paid.0);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = None;
        let ordinary = identity.is_none().then(|| policy.identity());
        validate_policy_identity(identity.or(ordinary.as_ref()).expect("policy identity"), io)?;
        self.policy = Some(policy);
        Ok(self)
    }

    /// Registers exact resource creator providers.
    pub fn with_resource_creation_providers(
        mut self,
        providers: &[Arc<dyn crate::resource_preparation::ResourceCreationProvider>],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            if !self.creation_providers.is_empty() {
                return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Conflict,
                    "resource creation registry is already configured"));
            }
            for (index, provider) in providers.iter().enumerate() {
                provider.provider().validate(work)?;
                if providers[..index].iter().any(|bound| bound.provider() == provider.provider()) {
                    return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Invalid,
                        "resource creation provider is registered twice"));
                }
            }
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            self.creation_providers = crate::core::copy_vector(providers, &mut quota,
                |provider, _| Ok(Arc::clone(provider)))?;
            quota.transfer();
            Ok(())
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io.filter(|io| io.original_native_work().is_some()) {
            let mut added = io.take_native_custody(io.owned_bytes()?)?;
            self.native_custody.as_mut().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid, reason: "creator registry has no original owner custody",
            })?.absorb(&mut added)?;
        }
        Ok(self)
    }

    /// Binds the conversation-owned provider effect reconciler.
    #[must_use]
    pub fn with_effects(mut self, effects: Arc<dyn DurableEffectObserver>) -> Self {
        self.effects = Some(effects);
        self
    }

    pub(super) async fn read_json(&self, reference: &FileRef, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        #[cfg(feature = "filesystem")]
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work = None;
        reference.validate(&mut work)?;
        if reference.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("durable task payload is not JSON".into()));
        }
        let bytes = self
            .reader
            .read(reference, reference.descriptor().byte_length(), io)?
            .await?;
        reference.descriptor().verify(&bytes, &mut work)?;
        #[cfg(feature = "filesystem")]
        let value: Value = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<Value>, &bytes, crate::wire::ErrorCode::Invalid,
            )?),
            None => crate::contract::json_from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?,
        };
        #[cfg(not(feature = "filesystem"))]
        let value: Value = crate::contract::json_from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        let canonical = crate::store::history::canonical_bytes_with_work(&value, reference.descriptor().byte_length(), &mut work)?;
        let retained = canonical.capacity() as u64;
        let matches = canonical.as_slice() == bytes.as_ref();
        drop(canonical);
        if let Some(work) = work.as_deref_mut() { work.release_owned(retained)?; }
        if !matches {
            return Err(Error::Invalid(
                "durable task payload is not canonical JSON".into(),
            ));
        }
        Ok(value)
    }

    pub(super) async fn retained_declaration(
        &self,
        operation_id: OperationId,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<OperationSpec>> {
        #[cfg(feature = "filesystem")]
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work = None;
        let declaration_key = IdempotencyKey::new(format!("task-admission:{operation_id}"))?;
        let indexed = crate::distributed::observe_declaration(
            &self.stream,
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &declaration_key,
            &mut work,
        )
        .await?;
        if indexed.is_some() {
            return Ok(indexed);
        }
        {
            // Missing/legacy locations are not evidence of missing admission.
            // Bootstrap through the existing authoritative reducer, then use
            // its verified declaration. This fallback remains a mutable read.
            let mut coordinator = self.coordinator.lock().await;
            coordinator.refresh(&mut work).await?;
            match coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                &mut work,
            ) {
                Ok(operation) => Ok(Some(operation.spec.clone())),
                Err(Error::NotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }

    pub(super) async fn read_admission_json(&self, reference: &FileRef, io: Option<&crate::executor::PreparationIo>) -> Result<Value> {
        #[cfg(feature = "filesystem")]
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work = None;
        self.read_json(reference, io)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => {
                    Error::Storage("committed task admission content is missing".into())
                }
                other => other,
            })
    }

    pub(super) async fn admission(&self, operation_id: OperationId, io: Option<&crate::executor::PreparationIo>) -> Result<TaskAdmissionRecord> {
        #[cfg(feature = "filesystem")]
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work = None;
        let spec = self
            .retained_declaration(operation_id, io)
            .await?
            .ok_or_else(|| Error::NotFound("task admission".into()))?
            .clone();
        if let Some(limits) = self.session_limits {
            let mut coordinator = self.coordinator.lock().await;
            // Session configuration and parent links are immutable. A cached
            // hierarchy can check these pinned ceilings without refreshing
            // unrelated lifecycle events; unknown descendants still bootstrap.
            let retained = match coordinator
                .scheduler()
                .session_limits(operation_id, &mut work)
            {
                Err(Error::NotFound(_)) => {
                    coordinator.refresh(&mut work).await?;
                    coordinator
                        .scheduler()
                        .session_limits(operation_id, &mut work)?
                }
                other => other?,
            };
            if retained != limits {
                return Err(Error::Conflict(
                    "retained session ceilings differ from owner bindings".into(),
                ));
            }
        }
        let value = self.read_admission_json(&spec.state, io).await?;
        let admission = TaskAdmissionRecord::from_canonical_value(value, &mut work)?.0;
        if admission.policy != self.policy.as_ref().map(|policy| policy.identity()) {
            return Err(Error::Conflict(
                "durable task policy differs from its admission".into(),
            ));
        }
        if let Some(extensions) = &admission.extensions {
            extensions.validate(&mut work)?;
        }
        if admission.extensions.as_ref() != self.root_scope.extensions() {
            return Err(Error::Conflict(
                "retained task extensions differ from owner bindings".into(),
            ));
        }
        self.root_scope
            .narrow(admission.grants.clone(), admission.limits)?
            .with_run_limits(admission.run_limits, &mut work)?;
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
            || self
                .machines
                .resolve(&admission.machine, &mut work)?
                .is_none()
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
            Some(&admission.input),
            &mut work,
        )?;
        Ok(admission)
    }

    pub(super) async fn validate_parent_scope(
        &self,
        parent: Option<TaskId>,
        scope: &RuntimeScope,
    ) -> Result<()> {
        if let Some(parent) = parent {
            let admitted = self
                .admission(OperationId::from_bytes(parent.into_bytes()), None)
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
