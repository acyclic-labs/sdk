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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl crate::filesystem::UploadRecord for Value {
    fn diagnostic_label_bytes() -> u64 { 0 }
    fn json_value_schema() -> crate::contract::JsonValueSchema { crate::contract::JsonValueSchema::Root }
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(crate::tool::value_owned_bytes(self)? - std::mem::size_of::<Self>() as u64)
    }
}
use std::{collections::BTreeMap, sync::Arc};

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
pub struct ResumableToolRegistry(BTreeMap<(String, String), ResumableTool>);

impl ResumableToolRegistry {
    /// Registers one version-pinned tool state machine.
    pub fn register(&mut self, tool: ResumableTool) -> Result<()> {
        tool.validate()?;
        let key = (
            tool.definition.name.clone(),
            tool.definition.revision.clone(),
        );
        if self.0.contains_key(&key) {
            return Err(Error::Conflict(format!(
                "resumable tool {}@{} is already registered",
                key.0, key.1,
            )));
        }
        self.0.insert(key, tool);
        Ok(())
    }

    /// Resolves only the exact admitted revision.
    #[must_use]
    pub fn get(&self, name: &str, revision: &str) -> Option<&ResumableTool> {
        self.0.get(&(name.to_owned(), revision.to_owned()))
    }

    /// Iterates registered definitions in deterministic identity order.
    pub fn definitions(&self) -> impl Iterator<Item = &ToolDefinition> {
        self.0.values().map(|tool| &tool.definition)
    }

    /// Validates every installed revision through the genuine native machine identity and the same canonical machine registration engine.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        // Reject an unaudited installed provider before any schema/registry allocation for other rows.
        for tool in self.0.values() {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            if !tool.machine.supports_native_work() {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                    reason: "resumable registry contains an unaudited native machine" });
            }
            tool.machine.identity_with_native_work(work)?;
        }
        for ((name, revision), tool) in &self.0 {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: (name.len() as u64).saturating_add(revision.len() as u64).saturating_add(1),
                ..Default::default()
            })?;
            if name != &tool.definition.name || revision != &tool.definition.revision {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                    reason: "resumable registry key differs from its installed definition" });
            }
            tool.validate_with_native_work(work)?;
        }
        Ok(())
    }

    /// Copies the actual installed definitions and keys while retaining the same erased machine implementations.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::filesystem::UploadRecord as _;
        self.validate_with_native_work(work)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut copied = Self::default();
        let entry_bytes = std::mem::size_of::<((String, String), ResumableTool)>() as u64;
        for ((name, revision), tool) in &self.0 {
            io.retain_owned(entry_bytes)?;
            let name = crate::executor::stock_string_copy(name, Some(&io))?;
            let revision = crate::executor::stock_string_copy(revision, Some(&io))?;
            let definition = crate::model::clone_tool_definition(&tool.definition, Some(&io))?;
            copied.0.insert((name, revision), ResumableTool {
                definition, machine: tool.machine.clone(),
            });
        }
        let mut retained = std::mem::size_of::<Self>() as u64;
        for ((name, revision), tool) in &copied.0 {
            work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
            let definition_bytes = tool.definition.logical_owned_bytes()?;
            retained = retained.checked_add(entry_bytes)
                .and_then(|bytes| bytes.checked_add(name.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(revision.capacity() as u64))
                .and_then(|bytes| bytes.checked_add(definition_bytes))
                .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "resumable registry ownership overflow" })?;
        }
        let custody = io.into_native_custody(retained)?;
        Ok((copied, custody))
    }

    /// Uses the same exact-version tuple lookup with paid temporary keys; never invokes ordinary lookup or clones the installed tool/issued context.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn get_with_native_work<'a>(
        &'a self, name: &str, revision: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Option<&'a ResumableTool>> {
        work.remaining()?;
        let name_custody;
        let (name, hold) = work.display(name)?;
        name_custody = hold;
        let revision_custody;
        let (revision, hold) = work.display(revision)?;
        revision_custody = hold;
        work.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?;
        let key = (name, revision);
        let found = self.0.get(&key);
        drop(key);
        drop(revision_custody);
        drop(name_custody);
        Ok(found)
    }
}

impl ResumableTool {
    /// Rejects a tool whose machine could be silently swapped under the same visible revision.
    pub fn validate(&self) -> Result<()> {
        self.definition.validate()?;
        let identity = self.machine.identity();
        if identity.name != self.definition.name || identity.version != self.definition.revision {
            return Err(Error::Conflict(
                "resumable tool machine identity differs from its definition".into(),
            ));
        }
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone())
    }

    /// Validates native metadata without consulting ordinary machine identity or allocating an unaudited ordinary registration callback.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if !self.machine.supports_native_work() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "resumable tool has no native machine producer" });
        }
        let identity = self.machine.identity_with_native_work(work)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        self.definition.validate_with_io(Some(&io))?;
        if identity.name != self.definition.name || identity.version != self.definition.revision {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict,
                reason: "resumable tool machine identity differs from its definition" });
        }
        let registry_custody;
        let mut registry = MachineRegistry::default();
        registry_custody = registry.register_with_native_work(self.machine.clone(), work)?;
        drop(registry);
        drop(registry_custody);
        Ok(())
    }

    /// Derives the complete original workflow envelope for this exact scoped tool invocation.
    pub fn workflow_admission(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<WorkflowAdmission> {
        self.validate()?;
        invocation.validate()?;
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
        let required = capability::tool_call(&self.definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(&self.definition.input_schema, &invocation.arguments, "tool input", None)?;
        let initial = MachineCheckpoint {
            machine: self.machine.identity().clone(),
            revision: 0,
            state: self.machine.initialize(&invocation.arguments)?,
        };
        validate_value(self.machine.state_schema(), &initial.state, "tool state", None)?;
        let admission = WorkflowAdmission {
            operation_id: invocation.operation_id,
            request_digest: crate::contract::canonical_json_digest(&(
                context.task().durable_task_id(), scope.grants(), scope.limits(),
                scope.run_limits(), scope.extensions(), &self.definition,
                invocation, self.machine.identity(),
            ))?,
            initial,
        };
        admission.validate()?;
        Ok(admission)
    }

    /// Admits one exact invocation before any transition is committed.
    pub async fn open(
        &self,
        context: ToolContext,
        invocation: ToolInvocation,
        journal: Arc<dyn WorkflowJournal>,
    ) -> Result<ResumableToolSession> {
        let admission = self.workflow_admission(&context, &invocation)?;
        context.task().authorize_tool(&self.definition, &invocation).await?;
        if journal.admit(admission.clone()).await? != admission {
            return Err(Error::Conflict("tool workflow identity belongs to another admission".into()));
        }
        let mut registry = MachineRegistry::default();
        registry.register(self.machine.clone())?;
        let host = DurableWorkflowHost::open(registry, admission.initial, journal).await?;
        Ok(ResumableToolSession {
            definition: self.definition.clone(),
            invocation,
            host,
        })
    }

    /// Produces the same owner admission using the issued native context and original work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn workflow_admission_with_native_work(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(WorkflowAdmission, acyclic_fs::NativeAllocation)> {
        work.remaining()?;
        if !context.native_metadata_matches(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "resumable tool context has no original native custody" });
        }
        if !self.machine.supports_native_work() {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "resumable tool has no native machine producer" });
        }
        let identity = self.machine.identity_with_native_work(work)?;
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let built = self.build_native_admission(context, invocation, identity, &io, work)?;
        drop(built.registry);
        drop(built.registry_custody);
        Ok((built.admission.value, built.admission.custody
            .expect("native admission retains its original custody")))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn build_native_admission(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
        identity: &crate::workflow::MachineIdentity,
        io: &crate::executor::PreparationIo,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<NativeWorkflowAdmission> {
        self.definition.validate_with_io(Some(io))?;
        validate_tool_identity(invocation, Some(io))?;
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
        tool_validate_value(&self.definition.input_schema, &invocation.arguments, "tool input", Some(io))?;
        let registry_custody;
        let mut registry = MachineRegistry::default();
        registry_custody = registry.register_with_native_work(self.machine.clone(), work)?;
        let mut admission_custody;
        let mut state_hold;
        let (state, hold) = registry.initialize_with_native_work(identity, &invocation.arguments, work)?;
        state_hold = hold;
        admission_custody = work.retain_owned(
            (std::mem::size_of::<WorkflowAdmission>() - std::mem::size_of::<Value>()) as u64)?;
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
        let admission = ToolPayload::native(WorkflowAdmission {
            operation_id: invocation.operation_id, request_digest, initial,
        }, admission_custody);
        admission.value.validate_with_io(Some(io))?;
        Ok(NativeWorkflowAdmission { admission, registry, registry_custody })
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
            context.authorize_tool_with_native_work(&self.definition, invocation, work).await?;
            let copied_custody;
            let (copied, hold) = clone_admission_with_native_work(&built.admission.value, work)?;
            copied_custody = hold;
            let (observed, observed_custody) = journal.admit_with_native_work(copied, work)?.await?;
            let observed = ToolPayload::native(observed, observed_custody);
            drop(copied_custody);
            if observed.value != built.admission.value {
                return Err(tool_error(Some(&io), crate::wire::ErrorCode::Conflict,
                    "tool workflow identity belongs to another admission"));
            }
            drop(observed);
            let metadata_custody = work.retain_owned(std::mem::size_of::<NativeResumableToolSession<'a>>() as u64)?;
            let host = DurableWorkflowHost::open_with_native_work(
                built.registry, built.admission.value.initial, journal, work).await?;
            Ok(NativeResumableToolSession {
                definition: &self.definition, invocation, host,
                registry_custody: built.registry_custody, metadata_custody,
            })
        })
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeWorkflowAdmission {
    admission: ToolPayload<WorkflowAdmission>,
    registry: MachineRegistry,
    registry_custody: acyclic_fs::NativeAllocation,
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
            .step_checked(operation_id, idempotency_key, input, |transition| {
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
        let input = ToolPayload::native((idempotency_key, input), input_custody);
        if !self.metadata_custody.work().shares_ledger(work)
            || !self.registry_custody.work().shares_ledger(work)
            || !input.custody.as_ref().is_some_and(|guard| guard.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input belongs to another work" });
        }
        let required = crate::tool::value_owned_bytes(&input.value.1)?
            .checked_add(input.value.0.0.capacity() as u64).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "native resumable input ownership overflow" })?;
        if !input.custody.as_ref().is_some_and(|guard| guard.bytes() >= required) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native resumable input or retry key has no paid custody" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let input = input;
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let output_schema = &self.definition.output_schema;
            let result = self.host.step_checked_with_native_work(
                operation_id, input.value.0, input.value.1, work, |transition| {
                    if let MachineStatus::Completed { value } = &transition.status {
                        tool_validate_value(output_schema, value, "tool output", Some(&io))?;
                    }
                    Ok(())
                },
            ).await;
            drop(input.custody);
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
        let deep = crate::tool::value_owned_bytes(&self.initial.state)? - std::mem::size_of::<Value>() as u64;
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

// Owned data precedes its original-ledger guard, including on early error.
struct ToolPayload<T> {
    value: T,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    custody: Option<acyclic_fs::NativeAllocation>,
}

impl<T> ToolPayload<T> {
    fn ordinary(value: T) -> Self {
        Self { value, #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] custody: None }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native(value: T, custody: acyclic_fs::NativeAllocation) -> Self {
        Self { value, custody: Some(custody) }
    }
}

impl DurableToolRunner {
    /// Pins the registered definitions and the owner-bound ref-only journal.
    pub fn new(tools: ToolRegistry, journal: Arc<dyn ExecutionJournal>) -> Self {
        Self {
            tools, journal, limits: Limits::default(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            native_custody: None,
        }
    }

    /// Pins those same registered provider objects with paid SDK metadata.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        tools: &ToolRegistry,
        journal: Arc<dyn ExecutionJournal>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        if !journal.supports_native_execution(work)? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "tool journal has no native execution producer" });
        }
        let metadata = (std::mem::size_of::<Self>() - std::mem::size_of::<ToolRegistry>()) as u64;
        let mut custody = work.retain_owned(metadata)?;
        let mut tools_custody;
        let (tools, hold) = tools.clone_with_native_work(work)?;
        tools_custody = hold;
        custody.absorb(&mut tools_custody)?;
        Ok(Self { tools, journal, limits: Limits::default(), native_custody: Some(custody) })
    }

    /// Narrows the maximum artifact and projection bytes before publication.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.native_custody {
            limits.validate_with_native_work(custody.work())?;
            self.limits = limits;
            return Ok(self);
        }
        limits.validate()?;
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
        let tool = self.tools.get_version(&definition.name, &definition.revision)
            .ok_or_else(|| Error::NotFound(format!("tool {}@{}", definition.name, definition.revision)))?;
        let invocation = ToolPayload::ordinary(ToolInvocation {
            operation_id, call_id: operation_id.to_string(), name: definition.name.clone(), arguments,
        });
        Ok(self.run_engine(task_id, operation_id, &definition, invocation, &context,
            tool, None, false).await?.value)
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
        let tool = self.tools.get_version_with_native_work(&definition.name, &definition.revision, work)?
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::NotFound,
                reason: "admitted tool revision is not registered" })?;
        tool.executor.supports_native_execution(work)?;
        tool.projection.supports_native_projection(work)?;
        if !self.journal.supports_native_execution(work)? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "tool journal has no native execution producer" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let io = crate::executor::PreparationIo::from_native_work(work)?;
            let mut custody = work.retain_owned(
                (std::mem::size_of::<ToolInvocation>() - std::mem::size_of::<Value>()) as u64)?;
            let mut call_hold;
            let (call_id, hold) = work.display(&context.call_id())?;
            call_hold = hold;
            custody.absorb(&mut call_hold)?;
            let mut name_hold;
            let (name, hold) = work.display(&definition.name)?;
            name_hold = hold;
            custody.absorb(&mut name_hold)?;
            let mut arguments_hold;
            let (arguments, hold) = crate::tool::clone_value_with_native_work(arguments, work)?;
            arguments_hold = hold;
            custody.absorb(&mut arguments_hold)?;
            let invocation = ToolPayload::native(
                ToolInvocation { operation_id, call_id, name, arguments }, custody);
            let result = self.run_engine(task_id, operation_id, definition, invocation,
                context, tool, Some(&io), reconcile_only).await?;
            let guard = result.custody.ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage, reason: "native tool outcome has no custody" })?;
            Ok((result.value, guard))
        })
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines,
        reason = "one canonical ordered durable tool state machine")]
    async fn run_engine(
        &self, task_id: TaskId, operation_id: OperationId, definition: &ToolDefinition,
        invocation: ToolPayload<ToolInvocation>, context: &ToolContext, tool: &crate::tool::Tool,
        io: Option<&crate::executor::PreparationIo>, reconcile_only: bool,
    ) -> Result<ToolPayload<Outcome<Value>>> {
        let invocation = &invocation.value;
        if tool.definition != *definition {
            return Err(tool_error(io, crate::wire::ErrorCode::Conflict,
                "durable tool definition changed after admission"));
        }
        validate_tool_identity(invocation, io)?;
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
            for record in &records.value {
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
                        if pinned.value != *invocation {
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
                if let Err(error) = tool_json_bound(&result.value, content_limit, io)
                    .and_then(|()| tool_json_bound(&projection.value, projection_limit, io))
                {
                    if tool_propagate(&error, io) { return Err(error); }
                    return Err(Error::Conflict("durable tool history exceeds admitted limits".into()));
                }
                tool_validate_value(&definition.output_schema, &result.value.value, "tool output", io)?;
                tool_validate_projection(definition, &projection.value, projection_limits, io)?;
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
                if matches!(current.value.as_slice(), [record]
                    if matches!(&record.event, ExecutionEvent::Started { request_digest }
                        if *request_digest == digest))
                {
                    let key = tool_label("tool:claim", io)?;
                    let call = tool_string(&invocation.call_id, io)?;
                    let _event_metadata = tool_metadata::<ExecutionEvent>(io)?;
                    let event = ExecutionEvent::ToolStarted {
                        step: 0, call_id: call.value, invocation: reference.value,
                    };
                    match self.append_tail(operation_id, 1, &key.value, &event, io).await {
                        Ok(true) => claimed = true,
                        Ok(false) => continue,
                        Err(error) if tool_propagate(&error, io) => return Err(error),
                        Err(Error::Indeterminate(_)) => return tool_outcome(
                            Outcome::Indeterminate { operation_id }, None, io),
                        Err(error) => return Err(error),
                    }
                } else if !matches!(current.value.get(1).map(|record| &record.event),
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
            if let Err(error) = tool_validate_value(
                &definition.output_schema, &result.value.value, "tool output", io)
            {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::InvalidOutput);
            }
            if let Err(error) = tool_json_bound(&result.value, content_limit, io) {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::PublicationRejected);
            }
            let projection = match tool_project(tool, invocation, &result.value, io) {
                Ok(projection) => projection,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(_) => { fail!(ToolFailureKind::ProjectionRejected); }
            };
            if let Err(error) = tool_validate_projection(
                definition, &projection.value, projection_limits, io)
                .and_then(|()| tool_json_bound(&projection.value, projection_limit, io))
            {
                if tool_propagate(&error, io) { return Err(error); }
                fail!(ToolFailureKind::ProjectionRejected);
            }
            let result_ref = match self.stage(operation_id, "tool:result", &result.value, io).await {
                Ok(reference) => reference,
                Err(error) if tool_propagate(&error, io) => return Err(error),
                Err(Error::Indeterminate(_)) | Err(Error::Storage(_)) => return tool_outcome(
                    Outcome::Indeterminate { operation_id }, None, io),
                Err(_) => { fail!(ToolFailureKind::PublicationRejected); }
            };
            let projection_ref = match self.stage(
                operation_id, "tool:projection", &projection.value, io).await
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
                step: 0, call_id: call.value, result: result_ref.value, projection: projection_ref.value,
            };
            match self.append_tail(operation_id, 2, &key.value, &event, io).await {
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
            tool_validate_value(&definition.input_schema, &invocation.arguments, "tool input", io)?;
            tool.executor.authorize_with_native_context(context, invocation, work)?;
            drop(grant);
            drop(grant_hold);
            return context.authorize_tool_with_native_work(definition, invocation, work).await;
        }
        let required = capability::tool_call(&definition.name);
        if !scope.grants().contains(&required) {
            return Err(Error::Unauthorized(format!("scope lacks {required}")));
        }
        validate_value(&definition.input_schema, &invocation.arguments, "tool input", None)?;
        tool.executor.authorize_with_context(context, invocation)?;
        context.task().authorize_tool(definition, invocation).await
    }

    async fn replay(
        &self, operation_id: OperationId, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ToolPayload<Vec<crate::executor::ExecutionRecord>>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (_, records, custody) = crate::executor::replay_execution_with_native_work(
                self.journal.as_ref(), operation_id, 4, |_| true, work)?.await?;
            return Ok(ToolPayload::native(records, custody));
        }
        let (_, records) = replay_execution(self.journal.as_ref(), operation_id, 4, |_| true, None).await?;
        Ok(ToolPayload::ordinary(records))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    async fn load<T: crate::executor::PreparationRecord + crate::filesystem::UploadRecord + Send>(
        &self, reference: &crate::conversation::FileRef, io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ToolPayload<T>> {
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (value, custody) = crate::executor::load_json_with_native_work(
                self.journal.as_ref(), reference, work)?.await?;
            return Ok(ToolPayload::native(value, custody));
        }
        Ok(ToolPayload::ordinary(load_json(self.journal.as_ref(), reference, None).await?.into_retained()))
    }

    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    async fn load<T: crate::executor::PreparationRecord + Send>(
        &self, reference: &crate::conversation::FileRef, _io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ToolPayload<T>> {
        Ok(ToolPayload::ordinary(load_json(self.journal.as_ref(), reference, None).await?.into_retained()))
    }

    async fn stage<T: serde::Serialize + Sync>(
        &self, operation_id: OperationId, key: &str, value: &T,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ToolPayload<crate::conversation::FileRef>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (reference, custody) = crate::executor::stage_json_with_native_work(
                self.journal.as_ref(), operation_id, key, value, work)?.await?;
            return Ok(ToolPayload::native(reference, custody));
        }
        Ok(ToolPayload::ordinary(stage_json(self.journal.as_ref(), operation_id, key, value, None).await?))
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
    ) -> Result<ToolPayload<ToolResult>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool.executor.execute_with_native_context(context, invocation, work)?.await?;
            return Ok(ToolPayload::native(result, custody));
        }
        Ok(ToolPayload::ordinary(tool.executor.execute_with_context(context.clone(), invocation.clone()).await?))
    }

    async fn reconcile(
        &self, tool: &crate::tool::Tool, invocation: &ToolInvocation, context: &ToolContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<ToolPayload<ToolResult>>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            let (result, custody) = tool.executor.reconcile_with_native_context(context, invocation, work)?.await?;
            return Ok(result.map(|result| ToolPayload::native(result, custody)));
        }
        Ok(tool.executor.reconcile_with_context(context.clone(), invocation.clone()).await?
            .map(ToolPayload::ordinary))
    }

    async fn fail(
        &self, operation_id: OperationId, invocation: &ToolInvocation, reason: ToolFailureKind,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Option<ToolPayload<Outcome<Value>>>> {
        let key = tool_label("tool:failed", io)?;
        let call = tool_string(&invocation.call_id, io)?;
        let event = ExecutionEvent::ToolFailed { step: 0, call_id: call.value, reason };
        match self.append_tail(operation_id, 2, &key.value, &event, io).await {
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

fn tool_metadata<T>(io: Option<&crate::executor::PreparationIo>) -> Result<ToolPayload<()>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return Ok(ToolPayload::native((), work.retain_owned(std::mem::size_of::<T>() as u64)?));
    }
    Ok(ToolPayload::ordinary(()))
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

fn validate_tool_identity(invocation: &ToolInvocation, io: Option<&crate::executor::PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        return invocation.validate_with_native_work(work);
    }
    invocation.validate()
}

fn tool_validate_value(
    schema: &Value, value: &Value, label: &str, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    crate::contract::validate_json_schema_value(schema, value, label, io)
}

fn tool_json_bound<T: serde::Serialize>(
    value: &T, maximum: u64, io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let encoded_hold;
        let encoded = io.encode_json(value, maximum, true)?;
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (content, custody) = definition.validate_projection_with_native_work(value, limits, work)?;
        drop(content);
        drop(custody);
        return Ok(());
    }
    definition.validate_projection(value)?.validate_limits(limits, &mut None)
}

fn tool_project(
    tool: &crate::tool::Tool, invocation: &ToolInvocation, result: &ToolResult,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<ToolPayload<Value>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = tool.projection.project_with_native_work(invocation, result, work)?;
        return Ok(ToolPayload::native(value, custody));
    }
    Ok(ToolPayload::ordinary(tool.projection.project(invocation, result)?))
}

fn tool_string(value: &str, io: Option<&crate::executor::PreparationIo>) -> Result<ToolPayload<String>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(value)?;
        return Ok(ToolPayload::native(value, custody));
    }
    Ok(ToolPayload::ordinary(value.into()))
}

fn tool_label(prefix: &str, io: Option<&crate::executor::PreparationIo>) -> Result<ToolPayload<String>> {
    let id = OperationId::new();
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let (value, custody) = work.display(&format_args!("{prefix}:{id}"))?;
        return Ok(ToolPayload::native(value, custody));
    }
    Ok(ToolPayload::ordinary(format!("{prefix}:{id}")))
}

fn tool_success(
    mut result: ToolPayload<ToolResult>, io: Option<&crate::executor::PreparationIo>,
) -> Result<ToolPayload<Outcome<Value>>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let guard = result.custody.as_mut().ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native tool result has no custody" })?;
        if !guard.work().shares_ledger(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native tool result belongs to another work" });
        }
        let metadata = (std::mem::size_of::<Outcome<Value>>() - std::mem::size_of::<ToolResult>()) as u64;
        guard.resize_retained(guard.bytes().checked_add(metadata).ok_or(
            Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "native tool outcome ownership overflow" })?)?;
        return Ok(ToolPayload { value: Outcome::Succeeded(result.value.value), custody: result.custody });
    }
    Ok(ToolPayload::ordinary(Outcome::Succeeded(result.value.value)))
}

fn tool_outcome(
    mut outcome: Outcome<Value>, failure: Option<ToolFailureKind>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<ToolPayload<Outcome<Value>>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
        let mut custody = work.retain_owned(std::mem::size_of::<Outcome<Value>>() as u64)?;
        if let Some(reason) = failure {
            let (message, mut hold) = work.display(reason.message())?;
            custody.absorb(&mut hold)?;
            outcome = Outcome::Failed { message };
        }
        return Ok(ToolPayload::native(outcome, custody));
    }
    if let Some(reason) = failure { outcome = Outcome::Failed { message: reason.message().into() }; }
    Ok(ToolPayload::ordinary(outcome))
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
            tool("test.echo", "1", "test.other").validate(),
            Err(Error::Conflict(message)) if message.contains("machine identity")
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
