use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) type LiveHandler<I, O> = dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>> + Send + Sync;

#[cfg(target_arch = "wasm32")]
pub(super) type LiveHandler<I, O> = dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>>;

pub(super) enum TaskImplementation<I, O> {
    Live(Arc<LiveHandler<I, O>>),
    Resumable(Arc<dyn ResumableMachine>),
}

/// Immutable typed task definition.

pub struct TaskDefinition<I, O> {
    pub(super) identity: ComponentIdentity,
    pub(super) input_schema: Value,
    pub(super) output_schema: Value,
    pub(super) requirements: BTreeSet<String>,
    pub(super) implementation: TaskImplementation<I, O>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_schema_custody: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) native_metadata_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<I, O> TaskDefinition<I, O> {
    /// Adapts an ordinary async function without claiming that its stack is durable.
    pub fn live<F, Fut>(
        name: impl Into<String>,
        version: impl Into<String>,
        handler: F,
    ) -> Result<Self>
    where
        F: Fn(TaskContext, I) -> Fut + acyclic_stream::ProviderPlatform + 'static,
        Fut: Future<Output = Result<O>> + acyclic_stream::ProviderTask + 'static,
    {
        let name = name.into();
        let version = version.into();
        validate_identity(&name, &version, None)?;
        let mut definition = Self {
            identity: ComponentIdentity {
                name,
                version,
                digest: [0; 32],
            },
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            requirements: BTreeSet::new(),
            implementation: TaskImplementation::Live(Arc::new(move |context, input| {
                Box::pin(handler(context, input))
            })),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_schema_custody: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        };
        definition.refresh_digest(None)?;
        Ok(definition)
    }

    /// Binds a pinned state machine and its exact admission/result schemas.
    pub fn resumable(
        machine: Arc<dyn ResumableMachine>,
        input_schema: Value,
        output_schema: Value,
    ) -> Result<Self> {
        Ok(Self::resumable_with_io(machine, input_schema, output_schema, None)?.0)
    }

    /// Moves genuinely guarded schema values into the original definition.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn resumable_with_native_work(
        machine: Arc<dyn ResumableMachine>, input_schema: Value, output_schema: Value,
        schema_custody: acyclic_fs::NativeAllocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let mut schemas = (input_schema, output_schema, schema_custody);
        if !machine.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine has no native definition callbacks",
            });
        }
        machine.identity_with_native_work(work)?;
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = Self::resumable_with_io(
            machine, std::mem::take(&mut schemas.0), std::mem::take(&mut schemas.1), Some(&io),
        );
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((mut definition, _), custody) = crate::workflow::finish_native_io(result, io, retained)?;
        definition.native_schema_custody = Some(schemas.2);
        definition.native_metadata_custody = Some(custody);
        Ok(definition)
    }

    pub(super) fn resumable_with_io(
        machine: Arc<dyn ResumableMachine>, input_schema: Value, output_schema: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Self, u64)> {
        let pinned = task_machine_identity(machine.as_ref(), io)?;
        validate_identity(&pinned.name, &pinned.version, io)?;
        if pinned.digest == [0; 32] {
            return Err(task_workflow_error(task_original_work(io)?, crate::wire::ErrorCode::Invalid,
                "resumable task requires an implementation digest"));
        }
        crate::executor::with_history_io(io, |work| validate_task_schemas(&input_schema, &output_schema, true, work))?;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        }
        let mut definition = Self {
            identity: ComponentIdentity {
                name: task_copy_text(&pinned.name, io)?,
                version: task_copy_text(&pinned.version, io)?,
                digest: [0; 32],
            },
            input_schema, output_schema, requirements: BTreeSet::new(),
            implementation: TaskImplementation::Resumable(machine),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_schema_custody: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        };
        let retained = crate::workflow::workflow_owned_bytes(io)?;
        definition.refresh_digest(io)?;
        Ok((definition, retained))
    }

    /// Pins both public JSON Schemas before registration.
    pub fn schemas(mut self, input: Value, output: Value) -> Result<Self> {
        validate_task_schemas(
            &input,
            &output,
            matches!(&self.implementation, TaskImplementation::Resumable(_)),
            &mut None,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_metadata_custody = None; }
        self.input_schema = input;
        self.output_schema = output;
        self.refresh_digest(None)?;
        Ok(self)
    }

    /// Declares a dependency available at admission.
    pub fn requires(mut self, requirement: impl Into<String>) -> Result<Self> {
        let requirement = requirement.into();
        if requirement.is_empty() || requirement.chars().any(char::is_whitespace) {
            return Err(Error::Invalid("task requirement is invalid".into()));
        }
        self.requirements.insert(requirement);
        self.refresh_digest(None)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.native_metadata_custody = None; }
        Ok(self)
    }

    pub(super) fn refresh_digest(&mut self, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
        let implementation = match &self.implementation {
            TaskImplementation::Live(_) => None,
            TaskImplementation::Resumable(machine) => Some(task_machine_identity(machine.as_ref(), io)?.digest),
        };
        self.identity.digest = task_definition_digest_with_io(
            &self.identity.name, &self.identity.version, &self.input_schema, &self.output_schema,
            &self.requirements, implementation, io,
        )?;
        Ok(())
    }

    /// Opens the registered task's retained workflow under its exact lease.
    pub async fn open(
        self: &Arc<Self>,
        context: &TaskContext,
        fence: crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableTaskSession>
    where
        I: 'static,
        O: 'static,
    {
        Ok(self.open_with_io(context, fence, journal, None).await?.0)
    }

    /// Opens the same registered workflow using the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn open_with_native_work(
        self: &Arc<Self>,
        context: Arc<NativeTaskContext>,
        fence: &crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession>
    where I: 'static, O: 'static,
    {
        self.require_native_identity(work)?;
        if !journal.supports_native_work() {
            return Err(task_workflow_error(Some(work), crate::wire::ErrorCode::Unsupported,
                "workflow journal has no native callbacks"));
        }
        let task = context.context().durable_task.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native workflow requires an original durable task context",
        })?;
        if !context.native_metadata_matches(work)
            || !context.context().original_task_binding_matches(task, fence)
            || journal.task_binding() != Some((task, fence))
        {
            return Err(task_workflow_error(Some(work), crate::wire::ErrorCode::Unauthorized,
                "task context or journal differs from its original native owner"));
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = async {
            let fence = task_copy_with_io(Some(&io), |quota| Ok(crate::scheduler::LeaseFence {
                reservation_id: crate::core::copy_text(&fence.reservation_id, quota)?,
                placement: crate::core::copy_text(&fence.placement, quota)?,
            }))?;
            let fence_retained = io.owned_bytes()?;
            let (session, retained) = self.open_with_io(
                context.context(), fence, journal, Some(&io),
            ).await?;
            Ok((session, fence_retained.checked_add(retained).ok_or(Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout, custody: None,
            })?))
        }.await;
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((mut session, _), custody) = crate::workflow::finish_native_io(result, io, retained)?;
        session.native_context = Some(context);
        session.native_metadata_custody = Some(custody);
        Ok(session)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_native_open(&self) -> Result<()> {
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "live tasks have no resumable workflow",
            });
        };
        if self.native_schema_custody.is_none() || self.native_metadata_custody.is_none()
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "task definition was not published with native custody",
            });
        }
        if !machine.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "machine has no native initialization or transition callback",
            });
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_native_identity(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.require_native_open()?;
        if let TaskImplementation::Resumable(machine) = &self.implementation {
            machine.identity_with_native_work(work)?;
        }
        Ok(())
    }

    pub(super) async fn open_with_io(
        self: &Arc<Self>, context: &TaskContext,
        fence: crate::scheduler::LeaseFence, journal: Arc<dyn WorkflowJournal>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ResumableTaskSession, u64)>
    where I: 'static, O: 'static,
    {
        let native = task_original_work(io)?;
        let registered = if io.is_some() {
            context.harness.tasks.0.iter().find(|((name, version), _)| {
                name == &self.identity.name && version == &self.identity.version
            }).filter(|(_, entry)| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                { entry.native_custody.is_some() }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { let _ = entry; true }
            })
                .and_then(|(_, entry)| task_definition_arc::<I, O>(entry))
                .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                    "task definition is not the registered version"))?
        } else {
            context.harness.tasks.get_version::<I, O>(&self.identity.name, &self.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] None)?
        };
        if !Arc::ptr_eq(&registered, self) {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task definition is not the registered version"));
        }
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "live tasks have no resumable workflow"));
        };
        let task_id = context.durable_task.ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unauthorized,
                "task workflow requires a durable task context")
        })?;
        let host = context.harness.host.as_ref().ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Unsupported,
                "task workflow ownership is not bound")
        })?;
        verify_task_execution_owner(host.as_ref(), task_id, &fence, io).await?;
        if journal.task_binding() != Some((task_id, &fence)) {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Unauthorized,
                "workflow journal is not bound to this task lease"));
        }
        let retained = observe_task_open_admission(host.as_ref(), task_id, io).await?;
        context.harness.validate_observed_admission(&retained.0, io).await?;
        if retained.0.task != self.identity
            || retained.0.machine != *task_machine_identity(machine.as_ref(), io)?
            || retained.0.input_schema != self.input_schema
            || retained.0.output_schema != self.output_schema
            || context.task_id != retained.0.operation_id
            || context.scope.grants() != &retained.0.grants
            || context.scope.limits() != retained.0.limits
            || context.scope.run_limits() != retained.0.run_limits
            || context.scope.extensions() != retained.0.extensions.as_ref()
        {
            return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task workflow differs from its admitted definition or scope"));
        }
        let request_digest = crate::executor::preparation_digest(&retained.0, io)?;
        let initialized = initialize_task_machine(machine.as_ref(), &retained.0.input, io)?;
        let admission = WorkflowAdmission {
            operation_id: retained.0.operation_id, request_digest,
            initial: MachineCheckpoint {
                machine: retained.0.machine, revision: 0, state: initialized.0,
            },
        };
        let mut registry = MachineRegistry::default();
        let before_registry = crate::workflow::workflow_owned_bytes(io)?;
        crate::executor::with_history_io(io, |work| registry.register(machine.clone(), work))?;
        let registry_retained = crate::workflow::workflow_owned_bytes(io)?.checked_sub(before_registry)
            .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow registry custody changed"))?;
        registry.validate_checkpoint(&admission.initial, io)?;
        match observe_workflow_open_admission(journal.as_ref(), io).await?.0 {
            Some(existing) if existing == admission => {}
            Some(_) => return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "task workflow admission changed")),
            None => {
                let copy = task_copy_with_io(io, |quota| Ok(WorkflowAdmission {
                    operation_id: admission.operation_id, request_digest: admission.request_digest,
                    initial: MachineCheckpoint {
                        machine: task_copy_machine(&admission.initial.machine, quota)?,
                        revision: admission.initial.revision,
                        state: crate::core::copy_value(&admission.initial.state, quota)?,
                    },
                }))?;
                if admit_workflow_open(journal.as_ref(), copy, io).await?.0 != admission {
                    return Err(task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                        "task workflow admission changed"));
                }
            }
        }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let workflow = if let Some(work) = native {
            DurableWorkflowHost::open_with_native_work(registry, admission.initial, journal, work).await?
        } else {
            DurableWorkflowHost::open(registry, admission.initial, journal).await?
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let workflow = DurableWorkflowHost::open(registry, admission.initial, journal).await?;
        validate_latest_task_transition(&workflow, &self.output_schema, io).await?;
        verify_task_execution_owner(host.as_ref(), task_id, &fence, io).await?;
        let before_schema = crate::workflow::workflow_owned_bytes(io)?;
        let output_schema = task_copy_value(&self.output_schema, io)?;
        let schema_retained = crate::workflow::workflow_owned_bytes(io)?.checked_sub(before_schema)
            .ok_or_else(|| task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow output schema custody changed"))?;
        let retained = registry_retained.checked_add(schema_retained).ok_or_else(|| {
            task_workflow_error(native, crate::wire::ErrorCode::Conflict,
                "workflow metadata custody overflow")
        })?;
        Ok((ResumableTaskSession {
            task_id, fence, host: host.clone(), workflow, output_schema,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_context: None,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_metadata_custody: None,
        }, retained))
    }

    /// Returns the version and implementation identity pinned at admission.
    #[must_use]
    pub const fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }

    /// Returns exact provider/task requirements pinned into this definition.
    #[must_use]
    pub(crate) fn requirements(&self) -> &BTreeSet<String> {
        &self.requirements
    }
}
