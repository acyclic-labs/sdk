//! Passive, selected inspection over the canonical task owner and content reader.

use super::*;
use crate::conversation::{FileRef, PrivateDirectoryPage};
use crate::resources::GenerationRef;
use crate::runtime::{TaskAdmissionRecord, TaskChildrenPage};

/// A retained source identity and a separately authorized reader. Selecting the
/// source never adopts its grants, mounts its resources, or acquires a lease.
pub struct TaskSourceInspection {
    source: TaskAdmissionRecord,
    reader: TaskContext,
    volumes: Vec<crate::durable_host::TaskVolumeSnapshot>,
}

impl TaskSourceInspection {
    /// Exact owner-authenticated source admission, not an executable context.
    #[must_use]
    pub const fn source(&self) -> &TaskAdmissionRecord { &self.source }

    /// Original allocation snapshots, not mutable source heads or read grants.
    #[must_use]
    pub fn volumes(&self) -> &[crate::durable_host::TaskVolumeSnapshot] { &self.volumes }

    /// Resolves one unambiguous retained source project for fork composition.
    pub fn project(&self) -> Result<&crate::durable_host::TaskVolumeSnapshot> {
        self.selected_volume(crate::conversation::VolumeClass::Project)
    }

    /// Resolves authenticated scratch identity without adopting its read authority.
    pub fn private_volume(&self) -> Result<&crate::durable_host::TaskVolumeSnapshot> {
        self.selected_volume(crate::conversation::VolumeClass::AgentPrivate)
    }

    /// The actual retained scratch owner; NEVER derived by casting the task ID.
    pub fn agent(&self) -> Result<crate::AgentId> {
        match self.private_volume()?.volume.owner() {
            crate::conversation::VolumeOwner::Agent(agent) => Ok(*agent),
            _ => Err(Error::Unauthorized("source scratch does not identify an agent".into())),
        }
    }

    fn selected_volume(&self, class: crate::conversation::VolumeClass) -> Result<&crate::durable_host::TaskVolumeSnapshot> {
        let mut selected = self.volumes.iter().filter(|snapshot| snapshot.volume.class() == class);
        let volume = selected.next().ok_or_else(|| Error::NotFound("selected source has no authenticated volume allocation".into()))?;
        if selected.next().is_some() {
            return Err(Error::Conflict("selected source volume is ambiguous".into()));
        }
        Ok(volume)
    }

    /// Reads one exact immutable file under the CALLER's retained authority.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>> {
        self.reader.read_file(file).await
    }

    /// Lists only the selected directory at the explicit immutable generation.
    /// Carry both generation and the last name when continuing a page.
    pub async fn list(
        &self, volume: &VolumeRef, granted_prefix: &str, path: &str,
        generation: &GenerationRef, after: Option<&str>, maximum_entries: u32,
    ) -> Result<PrivateDirectoryPage> {
        if after.is_some_and(|name| name.is_empty() || name.contains('/') || name.contains('\\')) {
            return Err(Error::Invalid("invalid selected directory cursor".into()));
        }
        self.reader.list_private_directory(
            volume, granted_prefix, path, Some(generation), after, maximum_entries,
        ).await
    }

    /// Reads just one selected path at the explicit immutable generation.
    pub async fn read_path(
        &self, volume: &VolumeRef, granted_prefix: &str, path: &str,
        generation: &GenerationRef,
    ) -> Result<(FileRef, Vec<u8>)> {
        self.reader.read_private_path(volume, granted_prefix, path, Some(generation)).await
    }

    /// Compares two selected immutable files, authenticating BOTH references.
    /// This deliberately does not scan directories or resolve mutable heads.
    pub async fn diff(&self, before: &FileRef, after: &FileRef) -> Result<bool> {
        let before_bytes = self.read_file(before).await?;
        let after_bytes = self.read_file(after).await?;
        Ok(before_bytes != after_bytes)
    }
}

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// Reads one canonical direct-child index page; no source or swarm is hydrated.
    pub async fn children(
        &self, parent: TaskId, expected_revision: Option<u64>, after_slot: Option<String>, maximum: usize,
    ) -> Result<TaskChildrenPage> {
        if after_slot.is_some() && expected_revision.is_none() {
            return Err(Error::Invalid("child continuation requires its observed revision".into()));
        }
        self.harness.children(parent, expected_revision, after_slot, maximum).await
    }

    /// Resolves retained source metadata while retaining only the caller's read
    /// authority. Both identities are authenticated by the existing task owner.
    pub async fn inspect_source(&self, caller: TaskId, source: TaskId) -> Result<TaskSourceInspection> {
        let caller_admission = self.host.observe_admission(caller).await?;
        let source = self.host.observe_admission(source).await?;
        let reader = self.harness.durable_context(caller, caller_admission.operation_id).await?;
        let volumes = self.host.observe_volume_snapshots(TaskId::from_bytes(source.operation_id.into_bytes())).await?;
        Ok(TaskSourceInspection { source, reader, volumes })
    }
}

impl<P, A, O> FilesystemTaskExecution<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// One public stock model step with the same task-owned journal, scope,
    /// accounting and deadline as `execute`. Async stacks are not checkpoints.
    pub async fn model_step(
        &self, input: &TurnInput, step: u32, prior_messages: &[crate::model::ModelMessage],
    ) -> Result<Vec<crate::model::ModelEvent>> {
        self.validate_input(input)?;
        deadline::run(&self.host, self.task, Some(&self.fence), self.run_limits.deadline_epoch_ms,
            self.operation_id, self.executor.model_step(&self.journal, input, step, prior_messages)).await
    }

    /// Settles only the tools admitted by a retained model step through the
    /// ordinary stock resolver. Pending/rejected approval is not completion.
    pub async fn resolve_model_tools(
        &self, input: &TurnInput, step: u32,
    ) -> Result<crate::executor::ModelToolsProgress> {
        self.validate_input(input)?;
        deadline::run(&self.host, self.task, Some(&self.fence), self.run_limits.deadline_epoch_ms,
            self.operation_id, self.executor.resolve_model_tools(&self.journal, input, step)).await
    }
}
