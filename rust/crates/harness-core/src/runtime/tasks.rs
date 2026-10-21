use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) type LiveHandler<I, O> =
    dyn Fn(TaskContext, I) -> BoxFuture<'static, Result<O>> + Send + Sync;

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
        machine: Arc<dyn ResumableMachine>,
        input_schema: Value,
        output_schema: Value,
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
            machine,
            std::mem::take(&mut schemas.0),
            std::mem::take(&mut schemas.1),
            Some(&io),
        );
        let retained = result.as_ref().map_or(0, |(_, retained)| *retained);
        let ((mut definition, _), custody) =
            crate::workflow::finish_native_io(result, io, retained)?;
        definition.native_schema_custody = Some(schemas.2);
        definition.native_metadata_custody = Some(custody);
        Ok(definition)
    }

    /// Publishes a funded copy of a resumable definition and its requirements.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn publish(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Self> {
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "live tasks have no resumable workflow",
            });
        };
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let result = (|| {
            machine.identity_with_native_work(work)?;
            let input = task_copy_value(&self.input_schema, Some(&io))?;
            let output = task_copy_value(&self.output_schema, Some(&io))?;
            let schemas = io.owned_bytes()?;
            let (mut definition, _) =
                Self::resumable_with_io(machine.clone(), input, output, Some(&io))?;
            definition.requirements = task_copy_with_io(Some(&io), |quota| {
                crate::core::copy(&self.requirements, quota, |value, _| {
                    crate::core::owned_bytes(value)
                })
            })?;
            let retained = io.owned_bytes()?;
            definition.refresh_digest(Some(&io))?;
            Ok((definition, schemas, retained))
        })();
        let retained = result.as_ref().map_or(0, |(_, _, retained)| *retained);
        let ((mut definition, schemas, _), mut custody) =
            crate::workflow::finish_native_io(result, io, retained)?;
        definition.native_schema_custody = Some(custody.split_off(schemas)?);
        definition.native_metadata_custody = Some(custody);
        Ok(definition)
    }

    pub(super) fn resumable_with_io(
        machine: Arc<dyn ResumableMachine>,
        input_schema: Value,
        output_schema: Value,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Self, u64)> {
        let pinned = task_machine_identity(machine.as_ref(), io)?;
        validate_identity(&pinned.name, &pinned.version, io)?;
        if pinned.digest == [0; 32] {
            return Err(crate::executor::stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "resumable task requires an implementation digest",
            ));
        }
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
            input_schema,
            output_schema,
            requirements: BTreeSet::new(),
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
        {
            self.native_metadata_custody = None;
        }
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
        {
            self.native_metadata_custody = None;
        }
        Ok(self)
    }

    pub(super) fn refresh_digest(
        &mut self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let implementation = match &self.implementation {
            TaskImplementation::Live(_) => None,
            TaskImplementation::Resumable(machine) => {
                Some(task_machine_identity(machine.as_ref(), io)?.digest)
            }
        };
        self.identity.digest = task_definition_digest(
            &self.identity.name,
            &self.identity.version,
            &self.input_schema,
            &self.output_schema,
            &self.requirements,
            implementation,
            io,
        )?;
        Ok(())
    }

    /// Opens the retained workflow using the context's original IO.
    pub fn open<'a>(
        self: &Arc<Self>,
        context: Arc<TaskContext>,
        fence: &'a crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<crate::store::HistoryContentFuture<'a, ResumableTaskSession>>
    where
        I: 'static,
        O: 'static,
    {
        let io = context.preparation_io()?;
        let definition = Arc::clone(self);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = task_original_work(io.as_ref())?.cloned() {
            self.require_native_identity(&work)?;
            if !journal.supports_native_work() {
                return Err(crate::executor::stock_boundary(
                    io.as_ref(),
                    crate::wire::ErrorCode::Unsupported,
                    "workflow journal has no native callbacks",
                ));
            }
            return crate::NativeFutureBundle::new(&work, || async move {
                definition.open_session(context, fence, journal, io).await
            })
            .map(crate::store::HistoryContentFuture::Native);
        }
        let future = async move { definition.open_session(context, fence, journal, io).await };
        Ok(crate::store::HistoryContentFuture::Ordinary {
            frame_bytes: std::mem::size_of_val(&future) as u64,
            future: Box::pin(future),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn require_native_open(&self) -> Result<()> {
        let TaskImplementation::Resumable(machine) = &self.implementation else {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "live tasks have no resumable workflow",
            });
        };
        if self.native_schema_custody.is_none() || self.native_metadata_custody.is_none() {
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
    pub(super) fn require_native_identity(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.require_native_open()?;
        if let TaskImplementation::Resumable(machine) = &self.implementation {
            machine.identity_with_native_work(work)?;
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Session opening keeps its owner fence and funded IO in one async frame."
    )]
    async fn open_session(
        self: &Arc<Self>,
        context: Arc<TaskContext>,
        fence: &crate::scheduler::LeaseFence,
        journal: Arc<dyn WorkflowJournal>,
        io: Option<crate::executor::PreparationIo>,
    ) -> Result<ResumableTaskSession>
    where
        I: 'static,
        O: 'static,
    {
        let observer = io.as_ref();
        let native = task_original_work(observer)?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = native;
        let error = |code, reason| crate::executor::stock_boundary(observer, code, reason);
        let result = async {
            let fence = task_copy_with_io(observer, |quota| {
                Ok(crate::scheduler::LeaseFence {
                    reservation_id: crate::core::copy_text(&fence.reservation_id, quota)?,
                    placement: crate::core::copy_text(&fence.placement, quota)?,
                })
            })?;
            let registered = context.harness.tasks.get_version::<I, O>(
                &self.identity.name,
                &self.identity.version,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                native,
            )?;
            if !Arc::ptr_eq(&registered, self) {
                return Err(error(
                    crate::wire::ErrorCode::Conflict,
                    "task definition is not the registered version",
                ));
            }
            let TaskImplementation::Resumable(machine) = &self.implementation else {
                return Err(error(
                    crate::wire::ErrorCode::Unsupported,
                    "live tasks have no resumable workflow",
                ));
            };
            let task_id = context.durable_task.ok_or_else(|| {
                error(
                    crate::wire::ErrorCode::Unauthorized,
                    "task workflow requires a durable task context",
                )
            })?;
            let host = context.harness.host.as_ref().ok_or_else(|| {
                error(
                    crate::wire::ErrorCode::Unsupported,
                    "task workflow ownership is not bound",
                )
            })?;
            host.verify_execution_owner(task_id, &fence, observer)?
                .await?;
            let owner_matches = journal.task_binding() == Some((task_id, &fence));
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owner_matches = owner_matches
                & (context.owned_mailbox.is_none()
                    || context.original_task_binding_matches(task_id, &fence))
                & native.is_none_or(|work| {
                    context.native_metadata_matches(work)
                        && context.original_task_binding_matches(task_id, &fence)
                });
            if !owner_matches {
                return Err(error(
                    crate::wire::ErrorCode::Unauthorized,
                    "workflow journal is not bound to this task lease",
                ));
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let original = context
                .owned_mailbox
                .as_ref()
                .map(|owner| owner.admission());
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let original: Option<(&TaskAdmissionRecord, &WorkflowAdmission)> = None;
            let observed;
            let retained = if let Some((retained, _)) = original {
                retained
            } else {
                observed = observe_task_open_admission(host.as_ref(), task_id, observer).await?;
                context
                    .harness
                    .validate_observed_admission(&observed, observer)
                    .await?;
                &observed
            };
            if retained.task != self.identity
                || retained.machine != *task_machine_identity(machine.as_ref(), observer)?
                || retained.input_schema != self.input_schema
                || retained.output_schema != self.output_schema
                || context.task_id != retained.operation_id
                || context.scope.grants() != &retained.grants
                || context.scope.limits() != retained.limits
                || context.scope.run_limits() != retained.run_limits
                || context.scope.extensions() != retained.extensions.as_ref()
            {
                return Err(error(
                    crate::wire::ErrorCode::Conflict,
                    "task workflow differs from its admitted definition or scope",
                ));
            }
            let admission = if let Some((_, workflow)) = original {
                WorkflowAdmission {
                    operation_id: workflow.operation_id,
                    request_digest: workflow.request_digest,
                    initial: workflow.initial.copy(observer)?,
                }
            } else {
                let request_digest = crate::executor::preparation_digest(retained, observer)?;
                let initialized =
                    initialize_task_machine(machine.as_ref(), &retained.input, observer)?;
                WorkflowAdmission {
                    operation_id: retained.operation_id,
                    request_digest,
                    initial: MachineCheckpoint {
                        machine: crate::workflow::workflow_copy_identity(
                            &retained.machine,
                            observer,
                        )?,
                        revision: 0,
                        state: initialized,
                    },
                }
            };
            let mut registry = MachineRegistry::default();
            crate::executor::with_history_io(observer, |work| {
                registry.register(machine.clone(), work)
            })?;
            registry.validate_checkpoint(&admission.initial, observer)?;
            match retain_task_value(journal.admission(observer)?.await?, observer)? {
                Some(existing) if existing == admission => {}
                Some(_) => {
                    return Err(error(
                        crate::wire::ErrorCode::Conflict,
                        "task workflow admission changed",
                    ));
                }
                None => {
                    let copy = WorkflowAdmission {
                        initial: admission.initial.copy(observer)?,
                        ..admission
                    };
                    if admit_workflow_open(journal.as_ref(), copy, observer).await? != admission {
                        return Err(error(
                            crate::wire::ErrorCode::Conflict,
                            "task workflow admission changed",
                        ));
                    }
                }
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let workflow = if let Some(work) = native {
                DurableWorkflowHost::open_with_native_work(
                    registry,
                    admission.initial,
                    journal,
                    work,
                )
                .await?
            } else {
                DurableWorkflowHost::open(registry, admission.initial, journal).await?
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let workflow = DurableWorkflowHost::open(registry, admission.initial, journal).await?;
            validate_latest_task_transition(&workflow, &self.output_schema, observer).await?;
            host.verify_execution_owner(task_id, &fence, observer)?
                .await?;
            Ok(ResumableTaskSession {
                task_id,
                fence,
                host: Arc::clone(host),
                owner: context.owned_mailbox.clone(),
                workflow,
                definition: Arc::clone(self) as Arc<dyn RegisteredTaskDefinition>,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                native_metadata_custody: None,
            })
        }
        .await;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            let retained = if result.is_ok() { io.owned_bytes()? } else { 0 };
            let (mut session, allocation) =
                crate::workflow::finish_native_io(result, io, retained)?;
            session.native_metadata_custody = Some(allocation);
            return Ok(session);
        }
        result
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
