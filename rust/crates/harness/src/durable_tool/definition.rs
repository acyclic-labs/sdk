use super::*;
/// A pinned deterministic tool state machine.
#[derive(Clone)]
pub struct ResumableTool {
    /// Model-visible contract.
    pub definition: ToolDefinition,
    /// Pinned implementation and state schema.
    pub machine: Arc<dyn ResumableMachine>,
}
impl ResumableTool {
    /// Validates definition, machine identity and native producer support.
    pub fn validate(&self, work: NativeWork<'_>) -> Result<()> {
        let io = preparation_io(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let identity = if let Some(work) = work {
            if !self.machine.supports_native_work() {
                return Err(boundary(
                    io.as_ref(),
                    Unsupported,
                    "resumable machine has no native producer",
                ));
            }
            self.machine.identity_with_native_work(work)?
        } else {
            self.machine.identity()
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let identity = self.machine.identity();
        with_history_io(io.as_ref(), |work| self.definition.validate(work))?;
        if identity.name != self.definition.name || identity.version != self.definition.revision {
            return Err(boundary(
                io.as_ref(),
                Conflict,
                "tool machine identity differs from definition",
            ));
        }
        let mut registry = MachineRegistry::default();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let custody = registry.register_with_native_work(self.machine.clone(), work)?;
            drop(registry);
            drop(custody);
            return Ok(());
        }
        registry.register(self.machine.clone(), &mut None)
    }
    /// Derives the admission for this exact scoped tool invocation.
    pub fn workflow_admission(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        io: Option<&PreparationIo>,
    ) -> Result<WorkflowAdmission> {
        let native = native_work(io);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = native
            && !context.native_metadata_matches(work)
        {
            return Err(boundary(
                io,
                Unauthorized,
                "tool context lacks original custody",
            ));
        }
        self.validate(native)?;
        with_history_io(io, |work| {
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work)
        })?;
        if invocation.name != self.definition.name {
            return Err(boundary(io, Conflict, "invocation names another tool"));
        }
        if context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id().is_none()
        {
            return Err(boundary(
                io,
                Unauthorized,
                "tool invocation context mismatch",
            ));
        }

        let scope = context.effective_scope();
        #[cfg(feature = "filesystem")]
        let _grant_hold;
        let grant = with_history_io(io, |work| {
            preparation_format(format_args!("tool:call:{}", self.definition.name), work)
        })?;
        #[cfg(feature = "filesystem")]
        {
            _grant_hold = io
                .map(|io| io.guard_owned(grant.capacity() as u64))
                .transpose()?;
        }
        if !scope.grants().contains(&grant) {
            return Err(boundary(io, Unauthorized, "scope lacks tool call grant"));
        }
        with_history_io(io, |work| {
            validate_json_schema_value(
                &self.definition.input_schema,
                &invocation.arguments,
                "tool input",
                work,
            )
        })?;
        let (identity, initial) = self.initialize_checkpoint(invocation, io)?;
        with_history_io(io, |work| {
            validate_json_schema_value(
                self.machine.state_schema(),
                &initial.state,
                "tool state",
                work,
            )
        })?;
        let admission = WorkflowAdmission {
            operation_id: invocation.operation_id,
            request_digest: preparation_digest(
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

    fn initialize_checkpoint<'a>(
        &'a self,
        invocation: &ToolInvocation,
        io: Option<&'a PreparationIo>,
    ) -> Result<(&'a crate::workflow::MachineIdentity, MachineCheckpoint)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let (identity, state) = if let Some(io) = io
            && let Some(work) = native_work(Some(io))
        {
            let identity = self.machine.identity_with_native_work(work)?;
            let (state, custody) = self
                .machine
                .initialize_with_native_work(&invocation.arguments, work)?;
            io.absorb_native_custody(custody)?;
            io.retain_owned(
                (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>()) as u64,
            )?;
            (identity, state)
        } else {
            (
                self.machine.identity(),
                self.machine.initialize(&invocation.arguments)?,
            )
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let (identity, state) = (
            self.machine.identity(),
            self.machine.initialize(&invocation.arguments)?,
        );
        let initial = MachineCheckpoint {
            machine: crate::workflow::MachineIdentity {
                name: stock_string_copy(&identity.name, io)?,
                version: stock_string_copy(&identity.version, io)?,
                digest: identity.digest,
            },
            revision: 0,
            state,
        };
        Ok((identity, initial))
    }
}
