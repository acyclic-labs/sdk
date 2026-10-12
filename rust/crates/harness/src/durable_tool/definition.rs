use super::*;

/// A version-pinned tool implemented as a deterministic serialized state machine.
#[derive(Clone)]
pub struct ResumableTool {
    /// Model-visible input/output contract.
    pub definition: ToolDefinition,
    /// Pinned implementation and durable state schema.
    pub machine: Arc<dyn ResumableMachine>,
}

impl ResumableTool {
    /// Rejects a tool whose machine could be silently swapped under the same visible revision.
    pub fn validate(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
            if !self.machine.supports_native_work() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unsupported,
                    reason: "resumable tool has no native machine producer",
                });
            }
            let identity = self.machine.identity_with_native_work(work)?;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |work| self.definition.validate(work))?;
            if identity.name != self.definition.name || identity.version != self.definition.revision
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Conflict,
                    reason: "resumable tool machine identity differs from its definition",
                });
            }
            let mut registry = MachineRegistry::default();
            let custody = registry.register_with_native_work(self.machine.clone(), work)?;
            drop(registry);
            drop(custody);
            return Ok(());
        }
        let _ = work;
        self.definition.validate(&mut None)?;
        let identity = self.machine.identity();
        if identity.name != self.definition.name || identity.version != self.definition.revision {
            return Err(Error::Conflict(
                "resumable tool machine identity differs from its definition".into(),
            ));
        }
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone(), &mut None)
    }

    /// Derives the complete original workflow envelope for this exact scoped tool invocation.
    pub fn workflow_admission(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<WorkflowAdmission> {
        #[cfg(feature = "filesystem")]
        let native = io.and_then(crate::executor::PreparationIo::original_native_work);
        #[cfg(not(feature = "filesystem"))]
        let native = None;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native {
            if !context.native_metadata_matches(work) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "resumable tool context lacks original native custody",
                });
            }
        }
        self.validate(native)?;
        crate::executor::with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        if invocation.name != self.definition.name {
            return Err(Error::Conflict(
                "resumable tool invocation names another definition".into(),
            ));
        }
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id().is_none()
        {
            return Err(Error::Unauthorized(
                "resumable tool requires its admitted task and exact call context".into(),
            ));
        }
        let scope = context.effective_scope();
        let required = crate::contract::preparation_format(
            format_args!("tool:call:{}", self.definition.name),
            io,
        )?;
        #[cfg(feature = "filesystem")]
        let _required = io
            .map(|io| io.guard_owned(required.capacity() as u64))
            .transpose()?;
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(
            &self.definition.input_schema,
            &invocation.arguments,
            "tool input",
            io,
        )?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = match native {
            Some(work) => self.machine.identity_with_native_work(work)?,
            None => self.machine.identity(),
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = self.machine.identity();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let state = match native {
            Some(work) => {
                let (state, custody) = self
                    .machine
                    .initialize_with_native_work(&invocation.arguments, work)?;
                io.expect("original funding")
                    .absorb_native_custody(custody)?;
                io.expect("original funding").retain_owned(
                    (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>())
                        as u64,
                )?;
                state
            }
            None => self.machine.initialize(&invocation.arguments)?,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let state = self.machine.initialize(&invocation.arguments)?;
        let initial = MachineCheckpoint {
            machine: crate::workflow::MachineIdentity {
                name: crate::executor::stock_string_copy(&identity.name, io)?,
                version: crate::executor::stock_string_copy(&identity.version, io)?,
                digest: identity.digest,
            },
            revision: 0,
            state,
        };
        validate_value(
            self.machine.state_schema(),
            &initial.state,
            "tool state",
            io,
        )?;
        let admission = WorkflowAdmission {
            operation_id: invocation.operation_id,
            request_digest: crate::executor::preparation_digest(
                &(
                    context.task().durable_task_id(),
                    scope.grants(),
                    scope.limits(),
                    scope.run_limits(),
                    scope.extensions(),
                    &self.definition,
                    invocation,
                    identity,
                ),
                io,
            )?,
            initial,
        };
        admission.validate(io)?;
        Ok(admission)
    }

    /// Admits one exact invocation before any transition is committed.
    pub async fn open(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableToolSession> {
        let admission = self.workflow_admission(&context, &invocation, None)?;
        if let Some(operation) = context
            .task()
            .authorize_tool(&self.definition, &invocation)
            .await?
        {
            return Err(Error::Indeterminate(operation));
        }
        if journal.admit(admission.clone()).await? != admission {
            return Err(Error::Conflict(
                "tool workflow identity belongs to another admission".into(),
            ));
        }
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone(), &mut None)?;
        let host = DurableWorkflowHost::open(registry, admission.initial, journal).await?;
        Ok(ResumableToolSession {
            definition: self.definition.clone(),
            invocation,
            host,
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn build_native_admission(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        identity: &crate::workflow::MachineIdentity,
        io: &crate::executor::PreparationIo,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(
        (WorkflowAdmission, acyclic_fs::NativeAllocation),
        MachineRegistry,
        acyclic_fs::NativeAllocation,
    )> {
        crate::executor::with_history_io(Some(io), |work| self.definition.validate(work))?;
        crate::executor::with_history_io(Some(io), |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        if identity.name != self.definition.name
            || identity.version != self.definition.revision
            || invocation.name != self.definition.name
        {
            return Err(tool_error(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "resumable tool identity differs from its admitted definition",
            ));
        }
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id().is_none()
        {
            return Err(tool_error(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "resumable tool requires its admitted task and exact call context",
            ));
        }
        let grant_hold;
        let (grant, hold) = work.display(&format_args!("tool:call:{}", self.definition.name))?;
        grant_hold = hold;
        let scope = context.effective_scope();
        if !scope.grants().contains(&grant) {
            return Err(tool_error(
                Some(io),
                crate::wire::ErrorCode::Unauthorized,
                "scope lacks resumable tool call grant",
            ));
        }
        drop(grant);
        drop(grant_hold);
        crate::contract::validate_json_schema_value(
            &self.definition.input_schema,
            &invocation.arguments,
            "tool input",
            Some(io),
        )?;
        let registry_custody;
        let mut registry = MachineRegistry::default();
        registry_custody = registry.register_with_native_work(self.machine.clone(), work)?;
        let mut admission_custody;
        let mut state_hold;
        let (state, hold) =
            registry.initialize_with_native_work(identity, &invocation.arguments, work)?;
        state_hold = hold;
        admission_custody = work.claim(
            (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>()) as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        admission_custody.absorb(&mut state_hold)?;
        let mut name_hold;
        let (name, hold) = work.display(&identity.name)?;
        name_hold = hold;
        admission_custody.absorb(&mut name_hold)?;
        let mut version_hold;
        let (version, hold) = work.display(&identity.version)?;
        version_hold = hold;
        admission_custody.absorb(&mut version_hold)?;
        let initial = MachineCheckpoint {
            machine: crate::workflow::MachineIdentity {
                name,
                version,
                digest: identity.digest,
            },
            revision: 0,
            state,
        };
        let request_digest = crate::executor::preparation_digest(
            &(
                context.task().durable_task_id(),
                scope.grants(),
                scope.limits(),
                scope.run_limits(),
                scope.extensions(),
                &self.definition,
                invocation,
                identity,
            ),
            Some(io),
        )?;
        let admission = (
            WorkflowAdmission {
                operation_id: invocation.operation_id,
                request_digest,
                initial,
            },
            admission_custody,
        );
        admission.0.validate(Some(io))?;
        Ok((admission, registry, registry_custody))
    }

    /// Admits the same exact workflow on its original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn open_with_native_context<'a>(
        &'a self,
        context: &'a ToolContext,
        invocation: &'a ToolInvocation,
        journal: Arc<dyn WorkflowJournal>,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, NativeResumableToolSession<'a>>> {
        work.remaining()?;
        if !context.native_metadata_matches(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumable tool context has no original native custody",
            });
        }
        if !self.machine.supports_native_work() || !journal.supports_native_work() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "resumable tool has no native machine or journal producer",
            });
        }
        let identity = self.machine.identity_with_native_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let built = self.build_native_admission(context, invocation, identity, &io, work)?;
            if let Some(operation) = context
                .authorize_tool(&self.definition, invocation, work)
                .await?
            {
                return Err(Error::Indeterminate(operation));
            }
            let copied_custody;
            let (copied, hold) = clone_admission(&built.0.0, work)?;
            copied_custody = hold;
            let (observed, observed_custody) =
                journal.admit_with_native_work(copied, work)?.await?;
            let observed = (observed, observed_custody);
            drop(copied_custody);
            if observed.0 != built.0.0 {
                return Err(tool_error(
                    Some(&io),
                    crate::wire::ErrorCode::Conflict,
                    "tool workflow identity belongs to another admission",
                ));
            }
            drop(observed);
            let metadata_custody = work.claim(
                std::mem::size_of::<NativeResumableToolSession<'a>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let host = DurableWorkflowHost::open_with_native_work(
                built.1,
                built.0.0.initial,
                journal,
                work,
            )
            .await?;
            Ok(NativeResumableToolSession {
                definition: &self.definition,
                invocation,
                host,
                registry_custody: built.2,
                metadata_custody,
            })
        })
    }
}
