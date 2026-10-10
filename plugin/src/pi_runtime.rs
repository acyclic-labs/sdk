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
    runtime::{RuntimeScope, RuntimeTask, TaskDefinition, TaskRegistry},
    scheduler::SessionLimits,
    tool::ToolRegistry,
    workflow::MachineRegistry,
};
use acyclic_stream::{StreamClient, StreamProvider};
use std::sync::Arc;

/// Trusted service configuration, supplied independently of route bytes. The
/// signed storage scope may include the journal; task grants remain explicitly
/// selected by `scope`. Neither credentials nor grants belong in route records.
pub(crate) struct PiRuntimeAuthority {
    /// Original coordinator issuer, supplied only by protected root bootstrap.
    /// Recursive requests and retained routes never supply signing authority.
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
        let agent = authority
            .signed
            .agent()
            .ok_or_else(|| invalid("Pi root owner has no agent"))?;
        if authority.verifier.audience().kind != AggregateKind::Task
            || journal.class() != VolumeClass::AgentPrivate
            || journal.owner() != &VolumeOwner::Agent(agent)
            || !authority
                .scope
                .grants()
                .is_subset_of(authority.signed.capabilities())
            || authority.concurrency == 0
            || authority.maximum_payload_bytes == 0
            || authority.maximum_payload_bytes > authority.scope.limits().file_bytes
        {
            return Err(invalid("Pi runtime authority or limits are inconsistent"));
        }
        // The audience is covered by the scope proof. Binding the root here
        // prevents a caller from obtaining a new session budget with a fresh ID.
        let root_task = TaskId::parse(&authority.verifier.audience().id)
            .map_err(|_| invalid("Pi runtime audience is not its root task identity"))?;
        if root_task.into_bytes() == [0; 16] {
            return Err(invalid("Pi runtime root task identity is empty"));
        }
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
}

pub(crate) struct PiTaskRuntime<P, A, O> {
    pub(crate) runtime: FilesystemTaskRuntime<P, A, O>,
    pub(crate) definition: Arc<TaskDefinition<PiTaskInput, FileRef>>,
    policy: super::pi_owner::PiOwnerPolicy,
    storage_scope: Scope,
}

impl<P, A, O> PiTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    /// Uses one protected owner snapshot loaded by the service and authenticates
    /// every original task before accessing selected native workspaces.
    /// Provider handles are existing storage infrastructure, not route grants.
    pub(crate) async fn reopen(
        owner: &super::pi_owner::PiOwnerConfig,
        state: &super::AdapterState,
        stream: StreamClient<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
    ) -> Result<(Self, Vec<super::pi_recovery::PiTaskObservation>), String> {
        super::validate_state_version(state)?;
        super::validate_state_bounds(state)?;
        let super::PluginTaskBinding::Pi(root) = &state.task_binding else {
            return Err("Pi runtime requires a Pi root binding".to_owned());
        };
        owner.validate_root(root)?;
        let runtime = Self::open(
            stream,
            filesystem,
            owner.policy().journal.clone(),
            owner.authority()?,
        )
        .await
        .map_err(super::display)?;
        let observed = super::pi_recovery::observe_pi_session(&runtime.runtime, state).await?;
        Ok((runtime, observed))
    }

    /// Reuses admitted providers and storage. Opening registers code but does
    /// not allocate a selected workspace, admit a task, or claim execution.
    pub(crate) async fn open(
        stream: StreamClient<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        journal: VolumeRef,
        authority: PiRuntimeAuthority,
    ) -> acyclic_harness::Result<Self> {
        let (agent, root_task) = authority.validate(&journal)?;
        let policy = super::pi_owner::PiOwnerPolicy {
            root_task,
            agent,
            journal: journal.clone(),
            grants: authority.scope.grants().clone(),
            limits: authority.scope.limits(),
            run_limits: authority.scope.run_limits(),
            session_limits: authority.session_limits,
            concurrency: authority.concurrency,
            maximum_payload_bytes: authority.maximum_payload_bytes,
        };
        let storage_scope = authority.signed.clone();
        let machine = Arc::new(PiTaskMachine::new());
        let mut tasks = TaskRegistry::default();
        tasks.register(machine.definition()?)?;
        let definition = tasks.get_version::<PiTaskInput, FileRef>("acyclic.pi", "1")?;
        let mut machines = MachineRegistry::default();
        machines.register(machine)?;
        let scope = authority.scope;
        let creation = Arc::new(FilesystemResourceCreationProvider::new(
            filesystem.clone(), authority.issuer,
        )?);
        let runtime = FilesystemTaskRuntime::open_with_creation_providers(
            stream,
            filesystem,
            journal,
            authority.verifier,
            authority.signed,
            scope,
            tasks,
            machines,
            ToolRegistry::default(),
            authority.session_limits,
            authority.concurrency,
            authority.maximum_payload_bytes,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
            vec![creation],
        )
        .await?;
        Ok(Self {
            runtime,
            definition,
            policy,
            storage_scope,
        })
    }

    /// A service reader must use the same original key and complete policy as
    /// this runtime, rather than another valid owner with matching route IDs.
    pub(crate) fn verify_owner(
        &self,
        owner: &super::pi_owner::PiOwnerConfig,
    ) -> Result<(), String> {
        if owner.policy() != &self.policy || owner.authority()?.signed != self.storage_scope {
            return Err(
                "Pi owner differs from the original runtime credentials or policy".to_owned(),
            );
        }
        Ok(())
    }

    /// Uses the original persisted task identity as the admission operation.
    /// Only the authenticated service owner may register a root; descendants
    /// require the shared parent-linked resource admission path.
    pub(crate) async fn admit_root(
        &self,
        binding: PiTaskBinding,
        root: PiRootIntent,
        preparation: acyclic_harness::workflow::WorkflowCommand,
    ) -> acyclic_harness::Result<Admission<RuntimeTask<FileRef>>> {
        binding
            .validate(
                None,
                binding.project_volumes.keys().map(String::as_str).collect(),
                true,
            )
            .map_err(acyclic_harness::Error::Invalid)?;
        root.validate(&binding)
            .map_err(acyclic_harness::Error::Invalid)?;
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || !matches!(binding.scratch.allocation, PiScratchAllocation::Preparing)
        {
            return Err(invalid(
                "Pi root differs from its authenticated owner or allocation intent",
            ));
        }
        for volume in binding
            .project_volumes
            .values()
            .chain(std::iter::once(&binding.scratch.volume))
        {
            if !self
                .policy
                .grants
                .contains(&volume.capability(VolumeOperation::Read)?)
            {
                return Err(invalid("Pi root selection lacks its admitted read grant"));
            }
        }
        if !self
            .policy
            .grants
            .contains(&binding.scratch.volume.capability(VolumeOperation::Write)?)
        {
            return Err(invalid("Pi root scratch lacks its admitted write grant"));
        }
        self.runtime
            .harness()
            .admit(
                OperationId::from_bytes(binding.task_id.into_bytes()),
                &self.definition,
                PiTaskInput {
                    binding,
                    execution: PiTaskExecution::Host { root, preparation },
                },
                None,
            )
            .await
    }
}

fn invalid(message: &str) -> acyclic_harness::Error {
    acyclic_harness::Error::Invalid(message.to_owned())
}
