//! Passive, selected inspection over the canonical task owner and content reader.

use super::*;
use crate::filesystem::PreparationIo;
use crate::conversation::{FileRef, PrivateDirectoryPage};
use crate::resources::GenerationRef;
use crate::runtime::{TaskAdmissionRecord, TaskChildrenPage};

mod project;
use project::ProjectSourceReader;

enum SourceReader<P, A, O> {
    Caller(TaskContext),
    Project(ProjectSourceReader<P, A, O>),
}

/// A retained source identity and a separately authorized reader.
pub struct TaskSourceInspection<P, A, O> {
    source: TaskAdmissionRecord,
    volumes: Vec<crate::durable_host::TaskVolumeSnapshot>,
    reader: SourceReader<P, A, O>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_metadata_custody: Option<(
        acyclic_fs::NativeAllocation, acyclic_fs::NativeAllocation, acyclic_fs::NativeAllocation,
    )>,
}

impl<P, A, O> TaskSourceInspection<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
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
    pub async fn read_file(&self, file: &FileRef) -> Result<acyclic_stream::OwnedBytes> {
        match &self.reader {
            SourceReader::Caller(reader) => reader.read_file(file).await,
            SourceReader::Project(reader) => reader.read_file(file).await,
        }
    }

    /// Lists only the selected directory at the explicit immutable generation.
    pub async fn list(
        &self, volume: &VolumeRef, granted_prefix: &str, path: &str,
        generation: &GenerationRef, after: Option<&str>, maximum_entries: u32,
    ) -> Result<PrivateDirectoryPage> {
        if after.is_some_and(|name| name.is_empty() || name.contains('/') || name.contains('\\')) {
            return Err(Error::Invalid("invalid selected directory cursor".into()));
        }
        match &self.reader {
            SourceReader::Caller(reader) => reader.list_private_directory(
                volume, granted_prefix, path, Some(generation), after, maximum_entries,
            ).await,
            SourceReader::Project(reader) => reader.list(
                volume, granted_prefix, path, generation, after, maximum_entries,
            ).await,
        }
    }

    /// Reads just one selected path at the explicit immutable generation.
    pub async fn read_path(
        &self, volume: &VolumeRef, granted_prefix: &str, path: &str,
        generation: &GenerationRef,
    ) -> Result<(FileRef, acyclic_stream::OwnedBytes)> {
        match &self.reader {
            SourceReader::Caller(reader) => reader.read_private_path(volume, granted_prefix, path, Some(generation)).await,
            SourceReader::Project(reader) => reader.read_path(volume, granted_prefix, path, generation).await,
        }
    }

    /// Compares two selected immutable files, authenticating BOTH references.
    pub async fn diff(&self, before: &FileRef, after: &FileRef) -> Result<bool> {
        if let SourceReader::Project(reader) = &self.reader {
            return reader.diff(before, after).await;
        }
        let before_bytes = self.read_file(before).await?;
        let after_bytes = self.read_file(after).await?;
        Ok(before_bytes != after_bytes)
    }

    /// Immutable operation attestation, not a serialized filesystem read grant.
    #[must_use]
    pub fn project_authorization(&self) -> Option<&crate::source_authorization::ProjectSourceAuthorization> {
        match &self.reader {
            SourceReader::Caller(_) => None,
            SourceReader::Project(reader) => Some(reader.authorization()),
        }
    }

    /// Authenticates only the original selected Project READ before or after an SDK source boundary.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn require_native_project_selection_with_native_work(
        &self, source: TaskId, project: &VolumeRef, generation: &GenerationRef,
        frozen: Option<acyclic_fs::LazySnapshotRef>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if self.source.task_id() != source {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native source loan changed its authenticated source task" });
        }
        match &self.reader {
            SourceReader::Project(reader) => reader.require_native_project_selection_with_native_work(
                source, project, generation, frozen, work,
            ).await,
            SourceReader::Caller(_) => Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native source loan requires the private operation-bound Project reader",
            }),
        }
    }

    /// Cumulative measured filesystem/SDK-buffer work for this selected reader, including failed boundaries.
    #[must_use]
    pub fn project_work(&self) -> Option<acyclic_fs::WorkCounters> {
        match &self.reader {
            SourceReader::Caller(_) => None,
            SourceReader::Project(reader) => Some(reader.work()),
        }
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

    /// Resolves retained source metadata while retaining only the caller's read authority.
    pub async fn inspect_source(&self, caller: TaskId, source: TaskId) -> Result<TaskSourceInspection<P, A, O>> {
        let caller_admission = self.host.observe_admission(caller).await?;
        let source = self.host.observe_admission(source).await?;
        let reader = self.harness.durable_context(caller, caller_admission.operation_id).await?;
        let volumes = self.host.observe_volume_snapshots(source.task_id()).await?;
        Ok(TaskSourceInspection { source, reader: SourceReader::Caller(reader), volumes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata_custody: None,
        })
    }

    /// Opens one operation-authorized project selection without changing caller grants or granting transcript/private access.
    #[allow(clippy::too_many_arguments, reason = "original owner, exact selection and finite budget are explicit")]
    pub async fn inspect_project_source(
        &self, context: &TaskContext, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        handle: crate::source_authorization::ProjectSourceReadHandle,
        operation: OperationId, source: TaskId,
        snapshot: &crate::source_authorization::ProjectSourceSnapshot,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<TaskSourceInspection<P, A, O>> {
        handle.require_selection(operation, source, &snapshot.project, &snapshot.generation, snapshot.native_snapshot.as_ref())?;
        if !budget.has_finite_allowances() {
            return Err(Error::Invalid("selected inspection requires finite caller allowances".into()));
        }
        budget.verify(crate::filesystem::finite_budget(
            handle.authorization().maximum_work, handle.authorization().maximum_bytes,
        )).map_err(|error| Error::Invalid(error.to_string()))?;
        owner.verify_project_source_handle(context, &handle).await?;
        let admission = self.host.observe_admission(source).await?;
        owner.verify(false).await?;
        let volumes = self.host.observe_volume_snapshots(admission.task_id()).await?;
        owner.verify(false).await?;
        let io = PreparationIo::from_budget(budget, owner.invocation_cancellation().ok_or_else(||
            Error::Unauthorized("selected source has no original invocation token".into()))?)?;
        let reader = ProjectSourceReader::open(self.filesystem.clone(), owner, handle, context.scope().limits(), io).await?;
        Ok(TaskSourceInspection { source: admission, reader: SourceReader::Project(reader), volumes,
            #[cfg(not(target_arch = "wasm32"))]
            _native_metadata_custody: None,
        })
    }

    /// Opens the same sealed selected-project reader under the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::too_many_arguments, reason = "original owner, exact selection and native work are explicit")]
    pub async fn inspect_project_source_with_native_work(
        &self, context: &TaskContext, owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        handle: (crate::source_authorization::ProjectSourceReadHandle, acyclic_fs::NativeAllocation),
        operation: OperationId, source: TaskId,
        snapshot: &crate::source_authorization::ProjectSourceSnapshot,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<TaskSourceInspection<P, A, O>> {
        work.remaining()?;
        if !handle.1.matches_work(work) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "selected source handle custody belongs to another original work" });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        handle.0.require_selection_with_native_work(
            operation, source, &snapshot.project, &snapshot.generation,
            snapshot.native_snapshot.as_ref(), work,
        )?;
        io.remaining()?.verify(crate::filesystem::finite_budget(
            handle.0.authorization().maximum_work, handle.0.authorization().maximum_bytes,
        )).map_err(|_| Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "selected native inspection exceeds its original source allowances",
        })?;
        owner.verify_project_source_handle_with_native_work(context, &handle.0, work).await?;
        let admission = self.host.observe_admission_with_native_work(source, work).await?;
        owner.verify_with_native_work(false, work).await?;
        let volumes = self.host.observe_volume_snapshots_with_native_work(admission.0.task_id(), work).await?;
        owner.verify_with_native_work(false, work).await?;
        let reader = ProjectSourceReader::open(
            self.filesystem.clone(), owner, handle.0, context.scope().limits(), io,
        ).await?;
        Ok(TaskSourceInspection {
            source: admission.0, volumes: volumes.0, reader: SourceReader::Project(reader),
            _native_metadata_custody: Some((admission.1, volumes.1, handle.1)),
        })
    }
}

impl<P, A, O> FilesystemTaskExecution<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    /// One public stock model step with the same task-owned journal, scope, accounting and deadline as `execute`.
    pub async fn model_step(
        &self, input: &TurnInput, step: u32, prior_messages: &[crate::model::ModelMessage],
    ) -> Result<Vec<crate::model::ModelEvent>> {
        self.validate_input(input)?;
        deadline::run(&self.host, self.task, Some(&self.fence), self.run_limits.deadline_epoch_ms,
            self.operation_id, self.executor.model_step(&self.journal, input, step, prior_messages)).await
    }

    /// Settles only the tools admitted by a retained model step through the ordinary stock resolver.
    pub async fn resolve_model_tools(
        &self, input: &TurnInput, step: u32,
    ) -> Result<crate::executor::ModelToolsProgress> {
        self.validate_input(input)?;
        deadline::run(&self.host, self.task, Some(&self.fence), self.run_limits.deadline_epoch_ms,
            self.operation_id, self.executor.resolve_model_tools(&self.journal, input, step)).await
    }
}
