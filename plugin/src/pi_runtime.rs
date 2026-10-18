//! Pi's registered root task over the existing durable runtime.

use super::{
    PiScratchAllocation, PiTaskBinding,
    pi_root::PiRootIntent,
    pi_task::{PiTaskExecution, PiTaskInput, PiTaskMachine},
};
use acyclic_harness::filesystem::PreparationIo;
use acyclic_harness::{
    Admission, AgentId, OperationId, TaskId,
    conversation::{FileRef, VolumeClass, VolumeOperation, VolumeOwner, VolumeRef},
    core::{AggregateKind, AuthorityIssuer, AuthorityVerifier, Scope},
    filesystem::{FilesystemHost, FilesystemResourceCreationProvider, FilesystemTaskRuntime},
    runtime::{RuntimeScope, TaskDefinition, TaskRegistry, TaskSpawner},
    scheduler::SessionLimits,
    tool::ToolRegistry,
    workflow::MachineRegistry,
};
use acyclic_stream::LocalStream;
use std::sync::Arc;

mod workflow;

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
    pub(crate) custody: acyclic_fs::NativeAllocation,
}

impl PiRuntimeAuthority {
    pub(crate) fn validate(
        &self,
        journal: &VolumeRef,
        io: &mut Option<&mut dyn acyclic_harness::store::HistoryIo>,
    ) -> acyclic_harness::Result<(AgentId, TaskId)> {
        self.verifier.verify(&self.signed, io)?;
        self.issuer.verifier(io)?.verify(&self.signed, io)?;
        self.scope.limits().validate(io)?;
        self.session_limits.validate(io)?;
        let agent = self
            .signed
            .agent()
            .ok_or_else(|| authority_error(io, "Pi root owner has no agent"))?;
        if self.verifier.audience().kind != AggregateKind::Task
            || journal.class() != VolumeClass::AgentPrivate
            || journal.owner() != &VolumeOwner::Agent(agent)
            || !self.scope.grants().is_subset_of(self.signed.capabilities())
            || self.concurrency == 0
            || self.maximum_payload_bytes == 0
            || self.maximum_payload_bytes > self.scope.limits().file_bytes
        {
            return Err(authority_error(
                io,
                "Pi runtime authority or limits are inconsistent",
            ));
        }
        let root_task = TaskId::parse(&self.verifier.audience().id).map_err(|_| {
            authority_error(io, "Pi runtime audience is not its root task identity")
        })?;
        if root_task.into_bytes() == [0; 16] {
            return Err(authority_error(
                io,
                "Pi runtime root task identity is empty",
            ));
        }
        for operation in [VolumeOperation::Read, VolumeOperation::Write] {
            let grant = journal.capability(operation, io)?;
            if !self.signed.capabilities().contains(&grant) {
                return Err(authority_error(
                    io,
                    "Pi runtime owner lacks its journal grant",
                ));
            }
            if self.scope.grants().contains(&grant) {
                return Err(authority_error(
                    io,
                    "Pi task grants expose owner journal storage",
                ));
            }
        }
        let directory = journal.directory_read_capability("", io)?;
        if self.scope.grants().contains(&directory) {
            return Err(authority_error(
                io,
                "Pi task grants expose owner journal storage",
            ));
        }
        Ok((agent, root_task))
    }
}

fn authority_error(
    io: &mut Option<&mut dyn acyclic_harness::store::HistoryIo>,
    reason: &'static str,
) -> acyclic_harness::Error {
    if io.is_some() {
        acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason,
        }
    } else {
        invalid(reason)
    }
}

pub(crate) struct PiTaskRuntime {
    pub(crate) runtime:
        FilesystemTaskRuntime<LocalStream, crate::LocalAuthorityBackend, crate::LocalObjectBackend>,
    pub(crate) definition: Arc<TaskDefinition<PiTaskInput, FileRef>>,
    pub(crate) authority: PiRuntimeAuthority,
}

impl PiTaskRuntime {
    pub(crate) async fn reopen(
        owner: &super::pi_owner::PiOwnerConfig,
        state: &super::AdapterState,
        stream: Arc<LocalStream>,
        filesystem: Arc<FilesystemHost<crate::LocalAuthorityBackend, crate::LocalObjectBackend>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(
        Self,
        Vec<super::pi_recovery::PiTaskObservation>,
        acyclic_fs::NativeAllocation,
    )> {
        super::validate_state_version(state).map_err(acyclic_harness::Error::Invalid)?;
        super::validate_state_bounds(state).map_err(acyclic_harness::Error::Invalid)?;
        let super::PluginTaskBinding::Pi(root) = &state.task_binding else {
            return Err(invalid("Pi runtime requires a Pi root binding"));
        };
        owner.validate_root(root)?;
        let runtime = Self::open_with_native_work(
            stream,
            filesystem,
            &owner.policy().journal,
            owner.authority(work)?,
            work,
        )
        .await?;
        let (observed, custody) =
            super::pi_recovery::observe_pi_session(&runtime.runtime, state, work).await?;
        Ok((runtime, observed, custody))
    }

    pub(crate) async fn open_with_native_work(
        stream: Arc<LocalStream>,
        filesystem: Arc<FilesystemHost<crate::LocalAuthorityBackend, crate::LocalObjectBackend>>,
        journal: &VolumeRef,
        mut authority: PiRuntimeAuthority,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Self> {
        authority.validate(
            journal,
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
        )?;
        let (tools, _tools_custody) = ToolRegistry::new(work)?;
        let resumable_tools = acyclic_harness::durable_tool::ResumableToolRegistry::default();
        let mut slot = work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        authority.custody.absorb(&mut slot)?;
        let machine = PiTaskMachine::new(work)?;
        let fork_machine = Arc::new(acyclic_harness::filesystem::ForkPreparationMachine::new());
        let mut tasks = TaskRegistry::default();
        tasks.register(machine.definition(work)?)?;
        tasks.register(
            fork_machine
                .definition::<serde_json::Value, serde_json::Value>()?
                .publish(work)?,
        )?;
        let definition =
            tasks.get_version::<PiTaskInput, FileRef>("acyclic.pi", "1", Some(work))?;
        let mut machines = (MachineRegistry::default(), None, None);
        machines.1 = Some(machines.0.register_with_native_work(machine, work)?);
        machines.2 = Some(machines.0.register_with_native_work(fork_machine, work)?);
        let creation = FilesystemResourceCreationProvider::new(
            filesystem.clone(),
            &authority.issuer,
            Some(work),
        )?
        .into_native_shared(work)?;
        let creation_providers: [Arc<
            dyn acyclic_harness::resource_preparation::ResourceCreationProvider,
        >; 1] = [creation];
        let io = PreparationIo::from_native_work(work)?;
        let runtime = FilesystemTaskRuntime::open(
            stream,
            filesystem,
            journal,
            // Pi tasks publish into their created scratch volume, never journal storage.
            None,
            &authority.verifier,
            &authority.signed,
            &authority.scope,
            &tasks,
            &machines.0,
            &tools,
            authority.session_limits,
            authority.concurrency,
            authority.maximum_payload_bytes,
            Arc::new(acyclic_stream::SystemUnixMillisClock),
            None,
            &creation_providers,
            &resumable_tools,
            Some(&io),
        )
        .await?;
        Ok(Self {
            runtime,
            definition,
            authority,
        })
    }

    pub(crate) async fn admit_root(
        &self,
        binding: &PiTaskBinding,
        root: &PiRootIntent,
        preparation: &acyclic_harness::workflow::WorkflowCommand,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Admission<TaskId>, acyclic_fs::NativeAllocation)> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(|key| key.as_str()),
            true,
        )?;
        root.validate(binding, work)?;
        let authority = &self.authority;
        let task = TaskId::parse(&authority.verifier.audience().id)
            .map_err(|error| invalid(&error.to_string()))?;
        if binding.task_id != task
            || Some(binding.agent_id) != authority.signed.agent()
            || !matches!(binding.scratch.allocation, PiScratchAllocation::Preparing)
        {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi root differs from its authenticated owner or allocation intent",
            });
        }
        for (volume, operation) in binding
            .project_volumes
            .values()
            .chain(std::iter::once(&binding.scratch.volume))
            .map(|volume| (volume, VolumeOperation::Read))
            .chain(std::iter::once((
                &binding.scratch.volume,
                VolumeOperation::Write,
            )))
        {
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let grant = volume.capability(operation, &mut Some(&mut io))?;
            if !authority.scope.grants().contains(&grant) {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi root selection lacks its admitted volume grant",
                });
            }
        }
        let operation = OperationId::from_bytes(binding.task_id.into_bytes());
        let input = PiTaskInput {
            binding,
            execution: PiTaskExecution::Host { root, preparation },
        };
        let value = super::pi_root_provider::guarded_pi_value(
            &input,
            authority.maximum_payload_bytes,
            work,
        )?;
        let io = PreparationIo::from_native_work(work)?;
        acyclic_harness::store::HistoryIo::absorb_native_custody(&mut &io, value.1)?;
        let (record, retained) = self.runtime.harness().registered_admission(
            operation,
            &self.definition.identity().name,
            &self.definition.identity().version,
            value.0,
            None,
            Some(self.definition.identity().digest),
            Some(&io),
        )?;
        let record_custody = io.take_native_custody(retained)?;
        let admission = self.runtime.task_host().admit(record).await?;
        Ok((admission, record_custody))
    }
}

fn invalid(message: &str) -> acyclic_harness::Error {
    acyclic_harness::Error::Invalid(message.to_owned())
}
