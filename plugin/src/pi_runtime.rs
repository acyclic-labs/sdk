//! Pi's registered root task over the existing durable runtime.

use super::{
    PiScratchAllocation, PiTaskBinding,
    pi_root::PiRootIntent,
    pi_task::{PiTaskExecution, PiTaskInput, PiTaskMachine},
};
use acyclic_harness::{
    Admission, AgentId, OperationId, TaskId,
    conversation::{FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, AuthorityIssuer, AuthorityVerifier, Scope},
    filesystem::{FilesystemHost, FilesystemResourceCreationProvider, FilesystemTaskRuntime},
    runtime::{RuntimeScope, TaskDefinition, TaskRegistry},
    scheduler::SessionLimits,
    tool::ToolRegistry,
    workflow::MachineRegistry,
};
use acyclic_stream::{StreamProvider};
use std::sync::Arc;

/// Trusted service configuration, supplied independently of route bytes.
pub(crate) struct PiRuntimeAuthority {
    /// Original coordinator issuer, supplied only by protected root bootstrap.
    pub(crate) issuer: AuthorityIssuer,
    pub(crate) verifier: AuthorityVerifier,
    pub(crate) signed: Scope,
    pub(crate) scope: RuntimeScope,
    pub(crate) session_limits: SessionLimits,
    pub(crate) concurrency: usize,
    pub(crate) maximum_payload_bytes: u64,
}

impl PiRuntimeAuthority {
    pub(crate) fn validate(
        &self,
        journal: &VolumeRef,
    ) -> acyclic_harness::Result<(AgentId, TaskId)> {
        let authority = self;
        authority.verifier.verify(&authority.signed)?;
        authority.issuer.verifier().verify(&authority.signed)?;
        authority.scope.limits().validate()?;
        authority.session_limits.validate()?;
        let (agent, root_task) = authority.validate_identity_and_bounds(journal)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let grant = journal.capability(operation)?;
            if !authority.signed.capabilities().contains(&grant) {
                return Err(invalid("Pi runtime owner lacks its journal grant"));
            }
            if authority.scope.grants().contains(&grant) {
                return Err(invalid("Pi task grants expose owner journal storage"));
            }
        }
        if authority
            .scope
            .grants()
            .contains(&journal.directory_read_capability("")?)
        {
            return Err(invalid("Pi task grants expose owner journal storage"));
        }
        Ok((agent, root_task))
    }

    /// Pure owner/policy validation shared with the ordinary infrastructure path.
    fn validate_identity_and_bounds(&self, journal: &VolumeRef)
        -> acyclic_harness::Result<(AgentId, TaskId)> {
        let agent = self.signed.agent()
            .ok_or_else(|| invalid("Pi root owner has no agent"))?;
        if self.verifier.audience().kind != AggregateKind::Task
            || journal.class() != VolumeClass::AgentPrivate
            || journal.owner() != &VolumeOwner::Agent(agent)
            || !self.scope.grants().is_subset_of(self.signed.capabilities())
            || self.concurrency == 0
            || self.maximum_payload_bytes == 0
            || self.maximum_payload_bytes > self.scope.limits().file_bytes {
            return Err(invalid("Pi runtime authority or limits are inconsistent"));
        }
        let root_task = TaskId::parse(&self.verifier.audience().id)
            .map_err(|_| invalid("Pi runtime audience is not its root task identity"))?;
        if root_task.into_bytes() == [0; 16] {
            return Err(invalid("Pi runtime root task identity is empty"));
        }
        Ok((agent, root_task))
    }

    /// Verifies the original configured issuer and exact journal grant names through SDK native producers on the supplied original work.
    pub(crate) fn validate_with_native_work(
        &self, journal: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(AgentId, TaskId)> {
        self.verifier.verify_with_native_work(&self.signed, work)?;
        let original = self.issuer.verifier_with_native_work(work)?;
        let verification = original.0.verify_with_native_work(&self.signed, work);
        drop(original.0);
        drop(original.1);
        verification?;
        RuntimeScope::validate_limits_with_native_work(self.scope.limits(), work)?;
        RuntimeScope::validate_session_limits_with_native_work(&self.session_limits, work)?;
        let identities = self.validate_identity_and_bounds(journal)?;
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let grant = journal.capability_with_native_work(operation, work)?;
            if !self.signed.capabilities().contains(&grant.0) {
                return Err(native_invalid("Pi runtime owner lacks its journal grant"));
            }
            if self.scope.grants().contains(&grant.0) {
                return Err(native_invalid("Pi task grants expose owner journal storage"));
            }
        }
        let directory = journal.directory_read_capability_with_native_work("", work)?;
        if self.scope.grants().contains(&directory.0) {
            return Err(native_invalid("Pi task grants expose owner journal storage"));
        }
        Ok(identities)
    }
}

pub(crate) struct PiTaskRuntime<P, A, O> {
    pub(crate) runtime: FilesystemTaskRuntime<P, A, O>,
    pub(crate) definition: Arc<TaskDefinition<PiTaskInput, FileRef>>,
    pub(crate) fork_definition: Arc<TaskDefinition<acyclic_harness::resource_preparation::ResourcePreparationRequest, acyclic_harness::fork::ForkReport>>,
    policy: super::pi_owner::PiOwnerPolicy,
    storage_scope: Scope,
    _native_custody: acyclic_fs::NativeAllocation,
}

impl<P, A, O> PiTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    /// Uses one protected owner snapshot loaded by the service and authenticates every original task before accessing selected native workspaces.
    pub(crate) async fn reopen(
        owner: &super::pi_owner::PiOwnerConfig,
        state: &super::AdapterState,
        stream: std::sync::Arc<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Self, Vec<super::pi_recovery::PiTaskObservation>, acyclic_fs::NativeAllocation)> {
        super::validate_state_version(state).map_err(acyclic_harness::Error::Invalid)?;
        super::validate_state_bounds(state).map_err(acyclic_harness::Error::Invalid)?;
        let super::PluginTaskBinding::Pi(root) = &state.task_binding else {
            return Err(invalid("Pi runtime requires a Pi root binding"));
        };
        owner.validate_root(root, work)?;
        let runtime = Self::open_with_native_work(
            stream, filesystem, &owner.policy().journal,
            owner.authority_with_native_work(work)?, work,
        ).await?;
        let (observed, custody) = super::pi_recovery::observe_pi_session(
            &runtime.runtime, state, work,
        ).await?;
        Ok((runtime, observed, custody))
    }

    /// Clean native startup over the same protected owner, registered machines and SDK runtime engine.
    pub(crate) async fn open_with_native_work(
        stream: std::sync::Arc<P>, filesystem: Arc<FilesystemHost<A, O>>,
        journal: &VolumeRef,
        authority: (PiRuntimeAuthority, acyclic_fs::NativeAllocation),
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        use acyclic_fs::WorkCounters;
        work.admit(WorkCounters::UNCHARGED)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        // Source authority data precedes its original decoder/constructor AL.
        let (agent, root_task) = authority.0.validate_with_native_work(journal, work)?;
        let mut tools = ToolRegistry::new_with_native_work(work)?;
        let builtin = super::pi_builtin::builtin_tool_definition_with_native_work(work)?;
        let executor = super::pi_builtin::builtin_tool_executor_with_native_work(work)?;
        let projection = Arc::new(acyclic_harness::tool::JsonToolProjection);
        tools.0.register_with_native_work(
            builtin.0, builtin.1, executor, projection, &mut tools.1, work,
        )?;
        let resumable_tools = acyclic_harness::durable_tool::ResumableToolRegistry::default();
        let slot = work.retain_owned(std::mem::size_of::<Self>() as u64)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let retained_journal = journal.clone_with_native_work(work)?;
        let grants = authority.0.scope.grants().clone_with_native_work(work)?;
        let signed = authority.0.signed.clone_with_native_work(work)?;
        let mut retained = (
            super::pi_owner::PiOwnerPolicy {
                root_task, agent, journal: retained_journal.0, grants: grants.0,
                limits: authority.0.scope.limits(), run_limits: authority.0.scope.run_limits(),
                session_limits: authority.0.session_limits, concurrency: authority.0.concurrency,
                maximum_payload_bytes: authority.0.maximum_payload_bytes,
            },
            signed.0, slot, retained_journal.1, grants.1, signed.1,
        );
        retained.2.absorb(&mut retained.3)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        retained.2.absorb(&mut retained.4)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        retained.2.absorb(&mut retained.5)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;

        let machine = PiTaskMachine::new(work)?;
        let mut tasks = TaskRegistry::default();
        let definition = machine.definition(work)?;
        tasks.register_with_native_work(definition.0, definition.1, work)?;
        let fork_machine = acyclic_harness::filesystem::ForkPreparationMachine::new_with_native_work(work)?;
        let fork = fork_machine.definition_with_native_work(work)?;
        tasks.register_with_native_work(fork.0, fork.1, work)?;
        let fork_definition = tasks.get_version_with_native_work("acyclic.fork_preparation", "1", work)?;
        let definition = tasks.get_version_with_native_work::<PiTaskInput, FileRef>("acyclic.pi", "1", work)?;
        let mut machines = (MachineRegistry::default(), None, None);
        machines.1 = Some(machines.0.register_with_native_work(machine, work)?);
        machines.2 = Some(machines.0.register_with_native_work(fork_machine, work)?);
        let creation = FilesystemResourceCreationProvider::new_with_native_work(
            Arc::clone(&filesystem), &authority.0.issuer, work,
        )?.into_native_shared(work)?;
        let creation_providers: [Arc<dyn acyclic_harness::resource_preparation::ResourceCreationProvider>; 1] = [creation];
        let clock = Arc::new(acyclic_stream::SystemUnixMillisClock);
        let runtime = FilesystemTaskRuntime::open_with_creation_providers_with_native_work(
            stream, filesystem, journal, &authority.0.verifier, &authority.0.signed,
            &authority.0.scope, &tasks, &machines.0, &tools.0, &resumable_tools,
            authority.0.session_limits, authority.0.concurrency, authority.0.maximum_payload_bytes,
            clock, None, &creation_providers, work,
        ).await?;
        Ok(Self {
            runtime, definition, fork_definition,
            policy: retained.0, storage_scope: retained.1, _native_custody: retained.2,
        })
    }


    pub(crate) fn verify_owner_with_native_work(
        &self, owner: &super::pi_owner::PiOwnerConfig,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?;
        let denied = || acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi owner differs from the original runtime credentials or policy",
        };
        if owner.policy() != &self.policy {
            return Err(denied());
        }
        let original = owner.authority_with_native_work(work)?;
        let same = original.0.signed == self.storage_scope;
        drop(original.0);
        drop(original.1);
        if !same {
            return Err(denied());
        }
        Ok(())
    }

    /// The same original admission operation and sealed task input, with SDK ownership retained on the caller's original native invocation.
    pub(crate) async fn admit_root(
        &self, binding: &PiTaskBinding, root: &PiRootIntent,
        preparation: &acyclic_harness::workflow::WorkflowCommand,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Admission<TaskId>, acyclic_fs::NativeAllocation)> {
        binding.validate_with_native_work(None, binding.project_volumes.keys().map(String::as_str), true, work)?;
        root.validate_with_native_work(binding, work)?;
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || !matches!(binding.scratch.allocation, PiScratchAllocation::Preparing) {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi root differs from its authenticated owner or allocation intent",
            });
        }
        for volume in binding.project_volumes.values().chain(std::iter::once(&binding.scratch.volume)) {
            let grant = volume.capability_with_native_work(VolumeOperation::Read, work)?;
            if !self.policy.grants.contains(&grant.0) {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi root selection lacks its admitted read grant",
                });
            }
        }
        let write = binding.scratch.volume.capability_with_native_work(VolumeOperation::Write, work)?;
        if !self.policy.grants.contains(&write.0) {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root scratch lacks its admitted write grant",
            });
        }

        let operation = OperationId::from_bytes(binding.task_id.into_bytes());
        let input = PiTaskInput { binding, execution: PiTaskExecution::Host { root, preparation } };
        let value = super::pi_root_provider::guarded_pi_value(
            &input, self.policy.maximum_payload_bytes, work,
        )?;
        let record = self.runtime.harness().registered_root_admission_with_native_work(
            operation, &self.definition, value, work,
        )?;
        self.runtime.task_host().admit_with_native_work(record, work)?.await
    }


}

fn invalid(message: &str) -> acyclic_harness::Error {
    acyclic_harness::Error::Invalid(message.to_owned())
}

fn native_invalid(reason: &'static str) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid, reason,
    }
}
