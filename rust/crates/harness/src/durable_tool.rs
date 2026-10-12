//! Pinned, journaled tool calls for resumable tasks.

use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Outcome, Result, TaskId,
    conversation::Limits,
    executor::{
        ExecutionEvent, ExecutionJournal, ToolFailureKind, load_json, replay_execution, stage_json,
    },
    runtime::ToolContext,
    tool::{ToolDefinition, ToolInvocation, ToolRegistry, ToolResult, validate_value},
    workflow::{
        DurableWorkflowHost, MachineCheckpoint, MachineRegistry, MachineStatus, MachineTransition,
        ResumableMachine, WorkflowAdmission, WorkflowJournal,
    },
};
use serde_json::Value;

#[cfg(feature = "filesystem")]
impl crate::filesystem::UploadRecord for Value {
    fn diagnostic_label_bytes() -> u64 { 0 }
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::Root }
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::contract::json_value_logical_owned_bytes(self)
    }
}
use std::sync::Arc;

/// A version-pinned tool implemented as a deterministic serialized state machine.
#[derive(Clone)]
pub struct ResumableTool {
    /// Model-visible input/output contract.
    pub definition: ToolDefinition,
    /// Pinned implementation and durable state schema.
    pub machine: Arc<dyn ResumableMachine>,
}

/// Immutable registry of exact resumable tool revisions.
#[derive(Clone, Default)]
pub struct ResumableToolRegistry(Vec<ResumableTool>);

impl ResumableToolRegistry {
    /// Registers one version-pinned tool state machine.
    pub fn register(&mut self, tool: ResumableTool) -> Result<()> {
        tool.validate(None)?;
        let position = self.0.binary_search_by(|existing|
            (&existing.definition.name, &existing.definition.revision)
                .cmp(&(&tool.definition.name, &tool.definition.revision)));
        match position {
            Ok(_) => Err(Error::Conflict(format!(
                "resumable tool {}@{} is already registered",
                tool.definition.name, tool.definition.revision,
            ))),
            Err(position) => { self.0.insert(position, tool); Ok(()) },
        }
    }

    /// Resolves only the exact admitted revision.
    pub(crate) fn get(
        &self, name: &str, revision: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Option<&ResumableTool>> {
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        let mut lower = 0;
        let mut upper = self.0.len();
        while lower < upper {
            if let Some(work) = work.as_deref_mut() {
                work.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            let middle = lower + (upper - lower) / 2;
            let tool = &self.0[middle];
            match (tool.definition.name.as_str(), tool.definition.revision.as_str()).cmp(&(name, revision)) {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(tool)),
            }
        }
        Ok(None)
    }

    /// Iterates registered definitions in deterministic identity order.
    pub fn definitions(&self) -> impl Iterator<Item = &ToolDefinition> {
        self.0.iter().map(|tool| &tool.definition)
    }

    /// Validates installed revisions on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        // Reject an unaudited installed provider before any schema/registry allocation for other rows.
        for tool in &self.0 {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            if !tool.machine.supports_native_work() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "resumable registry contains an unaudited native machine" });
            }
            tool.machine.identity_with_native_work(work)?;
        }
        for tool in &self.0 {
            tool.validate(Some(work))?;
        }
        Ok(())
    }

    /// Copies installed definitions while retaining the same machine implementations.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        self.validate(work)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut copied = Self::default();
        let capacity = self.0.len().checked_mul(std::mem::size_of::<ResumableTool>())
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "resumable registry ownership overflow" })? as u64;
        io.retain_owned(capacity)?;
        if capacity != 0 { io.allocation_attempt(capacity)?; }
        copied.0.try_reserve_exact(self.0.len()).map_err(Error::Allocation)?;
        let backing = (copied.0.capacity() * std::mem::size_of::<ResumableTool>()) as u64;
        io.retain_owned(backing - capacity)?;
        for tool in &self.0 {
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            copied.0.push(ResumableTool {
                definition, machine: tool.machine.clone(),
            });
        }
        let mut retained = (std::mem::size_of::<Self>() as u64).checked_add(backing)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "resumable registry ownership overflow" })?;
        for tool in &copied.0 {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            let definition_bytes = tool.definition.logical_owned_bytes()?;
            retained = retained.checked_add(definition_bytes)
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "resumable registry ownership overflow" })?;
        }
        let custody = io.into_native_custody(retained)?;
        Ok((copied, custody))
    }

}

impl ResumableTool {
    /// Rejects a tool whose machine could be silently swapped under the same visible revision.
    pub fn validate(&self, work: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            work.remaining()?;
            if !self.machine.supports_native_work() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "resumable tool has no native machine producer" });
            }
            let identity = self.machine.identity_with_native_work(work)?;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            crate::executor::with_history_io(Some(&io), |work| self.definition.validate(work))?;
            if identity.name != self.definition.name || identity.version != self.definition.revision {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "resumable tool machine identity differs from its definition" });
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
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "resumable tool context lacks original native custody" });
            }
        }
        self.validate(native)?;
        crate::executor::with_history_io(io, |work|
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
        if invocation.name != self.definition.name {
            return Err(Error::Conflict("resumable tool invocation names another definition".into()));
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
        let required = crate::contract::preparation_format(format_args!("tool:call:{}", self.definition.name), io)?;
        #[cfg(feature = "filesystem")]
        let _required = io.map(|io| io.guard_owned(required.capacity() as u64)).transpose()?;
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(&self.definition.input_schema, &invocation.arguments, "tool input", io)?;
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
                let (state, custody) = self.machine.initialize_with_native_work(&invocation.arguments, work)?;
                io.expect("original funding").absorb_native_custody(custody)?;
                io.expect("original funding").retain_owned(
                    (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>()) as u64)?;
                state
            }
            None => self.machine.initialize(&invocation.arguments)?,
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let state = self.machine.initialize(&invocation.arguments)?;
        let initial = MachineCheckpoint {
            machine: crate::executor::with_history_io(io, |work| {
                let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
                let machine = crate::workflow::MachineIdentity {
                    name: crate::core::copy_text(&identity.name, &mut quota)?,
                    version: crate::core::copy_text(&identity.version, &mut quota)?, digest: identity.digest,
                };
                quota.transfer();
                Ok(machine)
            })?,
            revision: 0,
            state,
        };
        validate_value(self.machine.state_schema(), &initial.state, "tool state", io)?;
        let admission = WorkflowAdmission {
            operation_id: invocation.operation_id,
            request_digest: crate::executor::preparation_digest(&(
                context.task().durable_task_id(), scope.grants(), scope.limits(),
                scope.run_limits(), scope.extensions(), &self.definition,
                invocation, identity,
            ), io)?,
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
        if let Some(operation) = context.task().authorize_tool(&self.definition, &invocation).await? {
            return Err(Error::Indeterminate(operation));
        }
        if journal.admit(admission.clone()).await? != admission {
            return Err(Error::Conflict("tool workflow identity belongs to another admission".into()));
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
    ) -> Result<((WorkflowAdmission, acyclic_fs::NativeAllocation), MachineRegistry, acyclic_fs::NativeAllocation)> {
        crate::executor::with_history_io(Some(io), |work| self.definition.validate(work))?;
        crate::executor::with_history_io(Some(io), |work|
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
        if identity.name != self.definition.name || identity.version != self.definition.revision
            || invocation.name != self.definition.name
        {
            return Err(tool_error(Some(io), crate::wire::ErrorCode::Conflict,
                "resumable tool identity differs from its admitted definition"));
        }
        if context.operation_id() != invocation.operation_id || context.call_id() != invocation.call_id
            || context.task().durable_task_id().is_none()
        {
            return Err(tool_error(Some(io), crate::wire::ErrorCode::Unauthorized,
                "resumable tool requires its admitted task and exact call context"));
        }
        let grant_hold;
        let (grant, hold) = work.display(&format_args!("tool:call:{}", self.definition.name))?;
        grant_hold = hold;
        let scope = context.effective_scope();
        if !scope.grants().contains(&grant) {
            return Err(tool_error(Some(io), crate::wire::ErrorCode::Unauthorized,
                "scope lacks resumable tool call grant"));
        }
        drop(grant);
        drop(grant_hold);
        crate::contract::validate_json_schema_value(&self.definition.input_schema, &invocation.arguments, "tool input", Some(io))?;
        let registry_custody;
        let mut registry = MachineRegistry::default();
        registry_custody = registry.register_with_native_work(self.machine.clone(), work)?;
        let mut admission_custody;
        let mut state_hold;
        let (state, hold) = registry.initialize_with_native_work(identity, &invocation.arguments, work)?;
        state_hold = hold;
        admission_custody = work.claim(
            (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>()) as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
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
            machine: crate::workflow::MachineIdentity { name, version, digest: identity.digest },
            revision: 0, state,
        };
        let request_digest = crate::executor::preparation_digest(&(
            context.task().durable_task_id(), scope.grants(), scope.limits(),
            scope.run_limits(), scope.extensions(), &self.definition, invocation, identity,
        ), Some(io))?;
        let admission = (WorkflowAdmission {
            operation_id: invocation.operation_id, request_digest, initial,
        }, admission_custody);
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
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumable tool context has no original native custody" });
        }
        if !self.machine.supports_native_work() || !journal.supports_native_work() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "resumable tool has no native machine or journal producer" });
        }
        let identity = self.machine.identity_with_native_work(work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let built = self.build_native_admission(context, invocation, identity, &io, work)?;
            if let Some(operation) = context.authorize_tool(&self.definition, invocation, work).await? {
                return Err(Error::Indeterminate(operation));
            }
            let copied_custody;
            let (copied, hold) = clone_admission_with_native_work(&built.0.0, work)?;
            copied_custody = hold;
            let (observed, observed_custody) = journal.admit_with_native_work(copied, work)?.await?;
            let observed = (observed, observed_custody);
            drop(copied_custody);
            if observed.0 != built.0.0 {
                return Err(tool_error(Some(&io), crate::wire::ErrorCode::Conflict,
                    "tool workflow identity belongs to another admission"));
            }
            drop(observed);
            let metadata_custody = work.claim(std::mem::size_of::<NativeResumableToolSession<'a>>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let host = DurableWorkflowHost::open_with_native_work(
                built.1, built.0.0.initial, journal, work).await?;
            Ok(NativeResumableToolSession {
                definition: &self.definition, invocation, host,
                registry_custody: built.2, metadata_custody,
            })
        })
    }
}

/// Owner-bound durable session for one exact tool invocation.
pub struct ResumableToolSession {
    definition: ToolDefinition,
    invocation: ToolInvocation,
    host: DurableWorkflowHost,
}

impl ResumableToolSession {
    /// Returns the immutable admitted invocation.
    #[must_use]
    pub const fn invocation(&self) -> &ToolInvocation {
        &self.invocation
    }

    /// Returns the latest committed checkpoint.
    #[must_use]
    pub const fn checkpoint(&self) -> &MachineCheckpoint {
        self.host.checkpoint()
    }

    /// Commits one state transition and its complete ref-only command outbox.
    pub async fn step(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
    ) -> Result<MachineTransition> {
        let output_schema = &self.definition.output_schema;
        self.host
            .step(operation_id, idempotency_key, input, |transition| {
                if let MachineStatus::Completed { value } = &transition.status {
                    validate_value(output_schema, value, "tool output", None)?;
                }
                Ok(())
            })
            .await
    }
}

/// The same owner-bound workflow host, with borrowed original tool metadata.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub struct NativeResumableToolSession<'a> {
    definition: &'a ToolDefinition,
    invocation: &'a ToolInvocation,
    host: DurableWorkflowHost,
    registry_custody: acyclic_fs::NativeAllocation,
    metadata_custody: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeResumableToolSession<'_> {
    /// Borrows the exact original admitted invocation.
    pub const fn invocation(&self) -> &ToolInvocation { self.invocation }

    /// Borrows the actual latest committed checkpoint.
    pub const fn checkpoint(&self) -> &MachineCheckpoint { self.host.checkpoint() }

    /// Commits through the same native workflow host.
    pub fn step_with_native_work<'b>(
        &'b mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        input: Value,
        input_custody: acyclic_fs::NativeAllocation,
        work: &'b acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'b, (MachineTransition, acyclic_fs::NativeAllocation)>> {
        let input = ((idempotency_key, input), input_custody);
        if !self.metadata_custody.work().shares_ledger(work)
            || !self.registry_custody.work().shares_ledger(work)
            || !input.1.work().shares_ledger(work)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input belongs to another work" });
        }
        let required = (std::mem::size_of::<Value>() as u64)
            .checked_add(crate::contract::json_value_logical_owned_bytes(&input.0.1)?)
            .and_then(|bytes| bytes.checked_add(input.0.0.0.capacity() as u64)).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native resumable input ownership overflow" })?;
        if input.1.bytes() < required {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input or retry key has no paid custody" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let input = input;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let output_schema = &self.definition.output_schema;
            let result = self.host.step_checked_with_native_work(
                operation_id, input.0.0, input.0.1, work, |transition| {
                    if let MachineStatus::Completed { value } = &transition.status {
                        crate::contract::validate_json_schema_value(output_schema, value, "tool output", Some(&io))?;
                    }
                    Ok(())
                },
            ).await;
            drop(input.1);
            result
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn clone_admission_with_native_work(
    admission: &WorkflowAdmission,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(WorkflowAdmission, acyclic_fs::NativeAllocation)> {
    use crate::filesystem::UploadRecord as _;
    let io = crate::executor::PreparationIo::from_native_work(work)?;
    io.retain_owned(std::mem::size_of::<WorkflowAdmission>() as u64)?;
    let name = crate::executor::stock_string_copy(&admission.initial.machine.name, Some(&io))?;
    let version = crate::executor::stock_string_copy(&admission.initial.machine.version, Some(&io))?;
    let state = crate::model::clone_value(&admission.initial.state, Some(&io))?;
    let copied = WorkflowAdmission {
        operation_id: admission.operation_id,
        request_digest: admission.request_digest,
        initial: MachineCheckpoint {
            machine: crate::workflow::MachineIdentity { name, version, digest: admission.initial.machine.digest },
            revision: admission.initial.revision,
            state,
        },
    };
    let retained = (std::mem::size_of::<WorkflowAdmission>() as u64)
        .checked_add(copied.logical_owned_bytes()?)
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "native workflow admission ownership overflow" })?;
    let custody = io.into_native_custody(retained)?;
    Ok((copied, custody))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for WorkflowAdmission {
    fn diagnostic_label_bytes() -> u64 { crate::tool::tool_serde_diagnostic_label_bytes() }
    fn logical_owned_bytes(&self) -> Result<u64> {
        let deep = crate::contract::json_value_logical_owned_bytes(&self.initial.state)?;
        (self.initial.machine.name.capacity() as u64)
            .checked_add(self.initial.machine.version.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(deep))
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native workflow admission ownership overflow" })
    }
}

/// Executes only after a durable dispatch boundary; replay observes or reconciles that boundary and never calls `execute` a second time.
pub struct DurableToolRunner {
    tools: ToolRegistry,
    journal: Arc<dyn ExecutionJournal>,
    limits: Limits,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(
        tools: &ToolRegistry,
        journal: Arc<dyn ExecutionJournal>,
        _work: crate::NativeWork<'_>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = _work {
            if !journal.supports_native_execution(work)? {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "tool journal has no native execution producer" });
            }
            let metadata = (std::mem::size_of::<Self>() - std::mem::size_of::<ToolRegistry>()) as u64;
            let mut custody = work.claim(metadata, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let mut tools_custody;
            let (tools, hold) = tools.clone_with_native_work(work)?;
            tools_custody = hold;
            custody.absorb(&mut tools_custody)?;
            return Ok(Self { tools, journal, limits: Limits::default(), native_custody: Some(custody) });
        }
        Ok(Self {
            tools: tools.clone(), journal, limits: Limits::default(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        })
    }

    /// Narrows the maximum artifact and projection bytes before publication.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.native_custody {
            limits.validate(&mut Some(&mut crate::store::history::native_work_io(custody.work())?))?;
            self.limits = limits;
            return Ok(self);
        }
        limits.validate(&mut None)?;
        self.limits = limits;
        Ok(self)
    }

    /// Executes with the already admitted typed context supplied by a durable task host.
    pub async fn run_with_context(
        &self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: ToolDefinition,
        arguments: Value,
        context: ToolContext,
    ) -> Result<Outcome<Value>> {
        let tool = self.tools.get_version(&definition.name, &definition.revision, None)?
            .ok_or_else(|| Error::NotFound(format!("tool {}@{}", definition.name, definition.revision)))?;
        let invocation = (ToolInvocation {
            operation_id, call_id: operation_id.to_string(), name: definition.name.clone(), arguments,
        }, None);
        Ok(self.run_engine(task_id, operation_id, &definition, invocation, &context,
            tool, None, false).await?.0)
    }

    /// Runs the canonical dispatch/reconciliation engine on the original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn run_with_native_context<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &'a ToolDefinition,
        arguments: &'a Value,
        context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        self.native_run(task_id, operation_id, definition, arguments, context, work, false)
    }

    /// Only observes a retained dispatch proof. Missing intent never dispatches.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn reconcile_with_native_context<'a>(
        &'a self,
        task_id: TaskId,
        operation_id: OperationId,
        definition: &'a ToolDefinition,
        arguments: &'a Value,
        context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        self.native_run(task_id, operation_id, definition, arguments, context, work, true)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "original admitted tool envelope")]
    fn native_run<'a>(
        &'a self, task_id: TaskId, operation_id: OperationId,
        definition: &'a ToolDefinition, arguments: &'a Value, context: &'a ToolContext,
        work: &'a acyclic_fs::NativeMaterializationWork, reconcile_only: bool,
    ) -> Result<crate::NativeFutureBundle<'a, (Outcome<Value>, acyclic_fs::NativeAllocation)>> {
        work.remaining()?;
        if !context.native_metadata_matches(work)
            || !self.native_custody.as_ref().is_some_and(|guard| guard.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool context or runner metadata has no original custody" });
        }
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let tool = self.tools.get_version(&definition.name, &definition.revision, Some(&io))?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound,
                reason: "admitted tool revision is not registered" })?;
        tool.executor.supports_native_execution(work)?;
        tool.projection.supports_native_projection(Some(work))?;
        if !self.journal.supports_native_execution(work)? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "tool journal has no native execution producer" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let mut custody = work.claim(
                (std::mem::size_of::<ToolInvocation>() - std::mem::size_of::<Value>()) as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
            let mut call_hold;
            let (call_id, hold) = work.display(&context.call_id())?;
            call_hold = hold;
            custody.absorb(&mut call_hold)?;
            let mut name_hold;
            let (name, hold) = work.display(&definition.name)?;
            name_hold = hold;
            custody.absorb(&mut name_hold)?;
            let mut arguments_hold;
            let (arguments, hold) = crate::tool::clone_value(arguments, None, work)?;
            arguments_hold = hold;
            custody.absorb(&mut arguments_hold)?;
            let invocation = (ToolInvocation { operation_id, call_id, name, arguments }, Some(custody));
            let result = self.run_engine(task_id, operation_id, definition, invocation,
                context, tool, Some(&io), reconcile_only).await?;
            let guard = result.1.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "native tool outcome has no custody" })?;
            Ok((result.0, guard))
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines,
        reason = "one canonical ordered durable tool state machine")]
    async fn run_engine(
        &self, task_id: TaskId, operation_id: OperationId, definition: &ToolDefinition,
        invocation: (ToolInvocation, crate::NativeCustody), context: &ToolContext, tool: &crate::tool::Tool,
        io: Option<&crate::executor::PreparationIo>, reconcile_only: bool,
    ) -> Result<(Outcome<Value>, crate::NativeCustody)> {
        let invocation = &invocation.0;
        if tool.definition != *definition {
            return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                "durable tool definition changed after admission"));
        }
        crate::executor::with_history_io(io, |work|
            ToolInvocation::validate_identity(&invocation.call_id, Some(&invocation.name), work))?;
        if context.operation_id() != operation_id
            || context.call_id() != invocation.call_id
            || context.task().durable_task_id() != Some(task_id)
        {
            return Err(tool_error(io, crate::wire::ErrorCode::Unauthorized,
                "tool context is not bound to this admitted effect"));
        }
        let scope = context.effective_scope();
        let content_limit = self.limits.file_bytes.min(scope.limits().file_bytes);
        let projection_limit = self.limits.render_bytes.min(scope.limits().render_bytes);
        let projection_limits = Limits {
            file_bytes: content_limit, render_bytes: projection_limit,
            path_bytes: self.limits.path_bytes.min(scope.limits().path_bytes),
            attachments: self.limits.attachments.min(scope.limits().attachments),
            ..self.limits
        };
        tool_json_bound(invocation, content_limit, io)?;
        let digest = crate::executor::preparation_digest(
            &(&task_id, definition, invocation, projection_limits), io)?;
        loop {
            // Authentication is repeated BEFORE reading or publishing replay.
            self.authorize(tool, definition, invocation, context, io).await?;
            let records = self.replay(operation_id, io).await?;
            let mut started = false;
            let mut dispatched = false;
            let mut completed = None;
            let mut failed = None;
            for record in &records.0 {
                match &record.event {
                    ExecutionEvent::Started { request_digest }
                        if !started && !dispatched && completed.is_none()
                            && failed.is_none() && *request_digest == digest => started = true,
                    ExecutionEvent::ToolStarted { step: 0, call_id, invocation: reference }
                        if started && !dispatched && failed.is_none() && *call_id == invocation.call_id =>
                    {
                        if reference.descriptor().byte_length() > content_limit {
                            return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                                "durable tool invocation exceeds admitted descriptor limit"));
                        }
                        let pinned = self.load::<ToolInvocation>(reference, io).await?;
                        if pinned.0 != *invocation {
                            return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                                "durable tool call identity changed"));
                        }
                        dispatched = true;
                    }
                    ExecutionEvent::ToolCompleted { step: 0, call_id, result, projection }
                        if dispatched && completed.is_none() && failed.is_none()
                            && *call_id == invocation.call_id => completed = Some((result, projection)),
                    ExecutionEvent::ToolFailed { step: 0, call_id, reason }
                        if dispatched && completed.is_none() && failed.is_none()
                            && *call_id == invocation.call_id => failed = Some(*reason),
                    _ => return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                        "durable tool journal has an incompatible history")),
                }
            }
            if let Some((result, projection)) = completed {
                if result.descriptor().byte_length() > content_limit
                    || projection.descriptor().byte_length() > projection_limit
                {
                    return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                        "durable tool history exceeds admitted descriptor limits"));
                }
                let result = self.load::<ToolResult>(result, io).await?;
                let projection = self.load::<Value>(projection, io).await?;
                if let Err(error) = tool_json_bound(&result.0, content_limit, io)
                    .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
                {
                    if tool_propagate(&error, io) { return Err(error); }
                    return Err(Error::Conflict("durable tool history exceeds admitted limits".into()));
                }
                crate::contract::validate_json_schema_value(&definition.output_schema, &result.0.value, "tool output", io)?;
                tool_validate_projection(definition, &projection.0, projection_limits, io)?;
                return tool_success(result, io);
            }
            if let Some(reason) = failed {
                return tool_outcome(Outcome::Failed { message: String::new() }, Some(reason), io);
            }
            if reconcile_only && !dispatched {
                return tool_outcome(Outcome::Indeterminate { operation_id }, None, io);
            }
            drop(records);
            if !started {
                let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                let event = ExecutionEvent::Started { request_digest: digest };
                self.append(operation_id, "tool:started", &event, io).await?;
            }
            let mut claimed = false;
            if !dispatched {
                let reference = self.stage(operation_id, "tool:invocation", invocation, io).await?;
                let current = self.replay(operation_id, io).await?;
                if matches!(current.0.as_slice(), [record]
                    if matches!(&record.event, ExecutionEvent::Started { request_digest }
                        if *request_digest == digest))
                {
                    let key = tool_label("tool:claim", io)?;
                    let call = tool_string(&invocation.call_id, io)?;
                    let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                    let event = ExecutionEvent::ToolStarted {
                        step: 0, call_id: call.0, invocation: reference.0,
                    };
                    match self.append_tail(operation_id, 1, &key.0, &event, io).await {
                        Ok(true) => claimed = true,
                        Ok(false) => continue,
                        Err(error) if tool_propagate(&error, io) => return Err(error),
                        Err(Error::Indeterminate(_)) => return tool_outcome(
                            Outcome::Indeterminate { operation_id }, None, io),
                        Err(error) => return Err(error),
                    }
                } else if !matches!(current.0.get(1).map(|record| &record.event),
                    Some(ExecutionEvent::ToolStarted { step: 0, call_id, .. })
                        if *call_id == invocation.call_id)
                {
                    return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                        "durable tool journal changed before dispatch"));
                } else {
                    continue;
                }
            }
            macro_rules! fail {
                ($reason:expr) => {
                    if let Some(outcome) = self.fail(operation_id, invocation, $reason, io).await? {
                        return Ok(outcome);
                    }
                    continue;
                };
            }
            let result = if claimed {
                match self.invoke(tool, invocation, context, io).await {
                    Ok(result) => result,
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                        Outcome::Indeterminate { operation_id }, None, io),
                    Err(_) => { fail!(ToolFailureKind::ExecutorRejected); }
                }
            } else {
                match self.reconcile(tool, invocation, context, io).await {
                    Ok(Some(result)) => result,
                    Ok(None) => return tool_outcome(Outcome::Indeterminate { operation_id }, None, io),
                    Err(error) if tool_propagate(&error, io) => return Err(error),
                    Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                        Outcome::Indeterminate { operation_id }, None, io),
                    Err(error) => return Err(error),
                }
            };
            if let Err(error) = crate::contract::validate_json_schema_value(
                &definition.output_schema, &result.0.value, "tool output", io)
            {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::InvalidOutput);
            }
            if let Err(error) = tool_json_bound(&result.0, content_limit, io) {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::PublicationRejected);
            }
            let projection = match tool_project(tool, invocation, &result.0, io) {
                Ok(projection) => projection,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(_) => { fail!(ToolFailureKind::ProjectionRejected); }
            };
            if let Err(error) = tool_validate_projection(
                definition, &projection.0, projection_limits, io)
                .and_then(|()| tool_json_bound(&projection.0, projection_limit, io))
            {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::ProjectionRejected);
            }
            let result_ref = match self.stage(operation_id, "tool:result", &result.0, io).await {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                    Outcome::Indeterminate { operation_id }, None, io),
                Err(_) => { fail!(ToolFailureKind::PublicationRejected); }
            };
            let projection_ref = match self.stage(
                operation_id, "tool:projection", &projection.0, io).await
            {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                    Outcome::Indeterminate { operation_id }, None, io),
                Err(_) => { fail!(ToolFailureKind::PublicationRejected); }
            };
            let key = tool_label("tool:completed", io)?;
            let call = tool_string(&invocation.call_id, io)?;
            let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
            let event = ExecutionEvent::ToolCompleted {
                step: 0, call_id: call.0, result: result_ref.0, projection: projection_ref.0,
            };
            match self.append_tail(operation_id, 2, &key.0, &event, io).await {
                Ok(true) => return tool_success(result, io),
                Ok(false) => continue,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                    Outcome::Indeterminate { operation_id }, None, io),
                Err(error) => return Err(error),
            }
        }
    }

    async fn authorize(
        &self, tool: &crate::tool::Tool, definition: &ToolDefinition,
        invocation: &ToolInvocation, context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let scope = context.effective_scope();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let grant_hold;
            let (grant, hold) = work.display(&format_args!("tool:call:{}", definition.name))?;
            grant_hold = hold;
            if !scope.grants().contains(&grant) {
                return Err(tool_error(io, crate::wire::ErrorCode::Unauthorized, "scope lacks tool call grant"));
            }
            crate::contract::validate_json_schema_value(&definition.input_schema, &invocation.arguments, "tool input", io)?;
            tool.executor.authorize_with_native_context(context, invocation, work)?;
            drop(grant);
            drop(grant_hold);
            return match context.authorize_tool(definition, invocation, work).await? {
                Some(operation) => Err(Error::Indeterminate(operation)),
                None => Ok(()),
            };
        }
        let required = capability::tool_call(&definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(&definition.input_schema, &invocation.arguments, "tool input", None)?;
        tool.executor.authorize_with_context(context, invocation)?;
        match context.task().authorize_tool(definition, invocation).await? {
            Some(operation) => Err(Error::Indeterminate(operation)),
            None => Ok(()),
        }
    }

    async fn replay(
        &self, operation_id: OperationId, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(Vec<crate::executor::ExecutionRecord>, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (_, records, custody) = crate::executor::replay_execution_with_native_work(
                self.journal.as_ref(), operation_id, 4, |_| true, work)?.await?;
            return Ok((records, Some(custody)));
        }
        let (_, records) = replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, None).await?;
        Ok((records, None))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn load<T: crate::executor::PreparationRecord + crate::filesystem::UploadRecord + Send>(
        &self, reference: &crate::conversation::FileRef, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (value, custody) = crate::executor::load_json_with_native_work(
                self.journal.as_ref(), reference, work)?.await?;
            return Ok((value, Some(custody)));
        }
        Ok((load_json::<T>(self.journal.as_ref(), reference, None).await?.0, None))
    }

    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    async fn load<T: crate::executor::PreparationRecord + Send>(
        &self, reference: &crate::conversation::FileRef, _io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(T, crate::NativeCustody)> {
        Ok((load_json::<T>(self.journal.as_ref(), reference, None).await?.0, None))
    }

    async fn stage<T: serde::Serialize + Sync>(
        &self, operation_id: OperationId, key: &str, value: &T,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(crate::conversation::FileRef, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (reference, custody) = crate::executor::stage_json_with_native_work(
                self.journal.as_ref(), operation_id, key, value, work)?.await?;
            return Ok((reference, Some(custody)));
        }
        Ok((stage_json(self.journal.as_ref(), operation_id, key, value, None).await?, None))
    }

    async fn append(
        &self, operation_id: OperationId, key: &str, event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            return self.journal.append_with_native_work(operation_id, key, event, work)?.await;
        }
        self.journal.append(operation_id, key.into(), event.clone()).await
    }

    async fn append_tail(
        &self, operation_id: OperationId, tail: u64, key: &str, event: &ExecutionEvent,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<bool> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            return self.journal.append_if_tail_with_native_work(operation_id, tail, key, event, work)?.await;
        }
        self.journal.append_if_tail(operation_id, tail, key.into(), event.clone()).await
    }

    async fn invoke(
        &self, tool: &crate::tool::Tool, invocation: &ToolInvocation, context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(ToolResult, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool.executor.execute_with_native_context(context, invocation, work)?.await?;
            return Ok((result, Some(custody)));
        }
        Ok((tool.executor.execute_with_context(context.clone(), invocation.clone()).await?, None))
    }

    async fn reconcile(
        &self, tool: &crate::tool::Tool, invocation: &ToolInvocation, context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(ToolResult, crate::NativeCustody)>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool.executor.reconcile_with_native_context(context, invocation, work)?.await?;
            return Ok(result.map(|result| (result, Some(custody))));
        }
        Ok(tool.executor.reconcile_with_context(context.clone(), invocation.clone()).await?
            .map(|result| (result, None)))
    }

    async fn fail(
        &self, operation_id: OperationId, invocation: &ToolInvocation, reason: ToolFailureKind,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<(Outcome<Value>, crate::NativeCustody)>> {
        let key = tool_label("tool:failed", io)?;
        let call = tool_string(&invocation.call_id, io)?;
        let event = ExecutionEvent::ToolFailed { step: 0, call_id: call.0, reason };
        match self.append_tail(operation_id, 2, &key.0, &event, io).await {
            Ok(true) => tool_outcome(Outcome::Failed { message: String::new() }, Some(reason), io).map(Some),
            Ok(false) => Ok(None),
            Err(error) if tool_propagate(&error, io) => Err(error),
            Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) =>
                tool_outcome(Outcome::Indeterminate { operation_id }, None, io).map(Some),
            Err(error) => Err(error),
        }
    }
}

fn tool_propagate(error: &Error, io: Option<&crate::executor::PreparationIo>) -> bool {
    if tool_native(io) { return true; }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { error.is_native_failure() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = error; false }
}

// Tuple values precede their allocation guards, including on early error.
fn tool_metadata<T>(io: Option<&crate::executor::PreparationIo>) -> Result<((), crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return Ok(((), Some(work.claim(std::mem::size_of::<T>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?)));
    }
    Ok(((), None))
}

fn tool_native(io: Option<&crate::executor::PreparationIo>) -> bool {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    { io.and_then(crate::executor::PreparationIo::original_native_work).is_some() }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    { let _ = io; false }
}

fn tool_error(io: Option<&crate::executor::PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if tool_native(io) { return Error::NativeBoundary { code, reason }; }
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        _ => Error::Conflict(reason.into()),
    }
}

fn tool_json_bound<T: serde::Serialize>(
    value: &T, maximum: u64, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let encoded_hold;
        let encoded = io.encode(value, maximum, true)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        drop(encoded);
        drop(encoded_hold);
        return Ok(());
    }
    crate::contract::validate_json_byte_bound(value, maximum)
}

fn tool_validate_projection(
    definition: &ToolDefinition, value: &Value, limits: Limits,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let content = definition.validate_projection(value, limits, io)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io && io.original_native_work().is_some() {
        let bytes = content.logical_owned_bytes()?;
        drop(content);
        return io.release(bytes);
    }
    drop(content);
    Ok(())
}

fn tool_project(
    tool: &crate::tool::Tool, invocation: &ToolInvocation, result: &ToolResult,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = tool.projection.project(invocation, result, Some(work))?;
        let custody = custody.ok_or_else(|| tool_error(io, crate::wire::ErrorCode::Unsupported,
            "tool projection has no native producer"))?;
        return Ok((value, Some(custody)));
    }
    tool.projection.project(invocation, result, None)
}

fn tool_string(value: &str, io: Option<&crate::executor::PreparationIo>) -> Result<(String, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(value)?;
        return Ok((value, Some(custody)));
    }
    Ok((value.into(), None))
}

fn tool_label(prefix: &str, io: Option<&crate::executor::PreparationIo>) -> Result<(String, crate::NativeCustody)> {
    let id = OperationId::new();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(&format_args!("{prefix}:{id}"))?;
        return Ok((value, Some(custody)));
    }
    Ok((format!("{prefix}:{id}"), None))
}

fn tool_success(
    mut result: (ToolResult, crate::NativeCustody), io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let guard = result.1.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native tool result has no custody" })?;
        if !guard.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool result belongs to another work" });
        }
        let metadata = (std::mem::size_of::<Outcome<Value>>() - std::mem::size_of::<ToolResult>()) as u64;
        guard.resize_retained(guard.bytes().checked_add(metadata).ok_or(
            Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native tool outcome ownership overflow" })?)?;
        return Ok((Outcome::Succeeded(result.0.value), result.1));
    }
    Ok((Outcome::Succeeded(result.0.value), None))
}

fn tool_outcome(
    mut outcome: Outcome<Value>, failure: Option<ToolFailureKind>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Outcome<Value>, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let mut custody = work.claim(std::mem::size_of::<Outcome<Value>>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        if let Some(reason) = failure {
            let (message, mut hold) = work.display(reason.message())?;
            custody.absorb(&mut hold)?;
            outcome = Outcome::Failed { message };
        }
        return Ok((outcome, Some(custody)));
    }
    if let Some(reason) = failure { outcome = Outcome::Failed { message: reason.message().into() }; }
    Ok((outcome, None))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{MachineIdentity, ResumableMachine};
    use serde_json::json;

    struct TestMachine {
        identity: MachineIdentity,
    }

    impl ResumableMachine for TestMachine {
        fn identity(&self) -> &MachineIdentity {
            &self.identity
        }

        fn state_schema(&self) -> &Value {
            static SCHEMA: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
            SCHEMA.get_or_init(|| json!({"type": "object"}))
        }

        fn initialize(&self, _input: &Value) -> Result<Value> {
            Ok(json!({}))
        }

        fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
            Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Suspended,
            })
        }
    }

    fn definition(name: &str, revision: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.into(),
            revision: revision.into(),
            description: "test resumable tool".into(),
            input_schema: json!({"type": "object"}),
            output_schema: json!({"type": "object"}),
            projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
        }
    }

    fn tool(name: &str, revision: &str, machine_name: &str) -> ResumableTool {
        ResumableTool {
            definition: definition(name, revision),
            machine: Arc::new(TestMachine {
                identity: MachineIdentity {
                    name: machine_name.into(),
                    version: revision.into(),
                    digest: [7; 32],
                },
            }),
        }
    }

    #[test]
    fn resumable_tool_requires_machine_identity_to_match_definition() {
        assert!(matches!(
            tool("test.echo", "1", "test.other").validate(None),
            Err(Error::Conflict(_))
        ));
    }

    #[test]
    fn resumable_registry_rejects_ambiguous_exact_revisions() -> Result<()> {
        let mut registry = ResumableToolRegistry::default();
        registry.register(tool("test.echo", "1", "test.echo"))?;
        assert!(matches!(
            registry.register(tool("test.echo", "1", "test.echo")),
            Err(Error::Conflict(message)) if message.contains("already registered")
        ));
        Ok(())
    }
}
