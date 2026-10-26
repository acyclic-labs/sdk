use super::*;
use crate::{
    conversation::FileRef,
    core::{Reducer, SchemaRegistry},
    distributed::WorkPull,
    filesystem::{FilesystemContentVerifier, FilesystemForkPreparer},
    fork::{ForkPreparation, ForkRequest, ForkSelection, ResourceRevision},
    resource_preparation::ResourcePreparationRequest,
    resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef},
    runtime::{TaskAdmissionRecord, TaskContext},
    task_volume::{ChildRequest, Component},
};
use acyclic_fs::facade::FsWork as _;
use acyclic_native_runtime::spawn_blocking;
use serde_json::Value;

pub(super) struct Plan {
    parent: TaskId,
    reference: Component,
    operation: OperationId,
    preparation: OperationId,
    fork: OperationId,
    checkpoint: OperationId,
    child_agent: AgentId,
    project: VolumeRef,
    private: VolumeRef,
}

impl Plan {
    pub(super) fn new(
        harness: &LocalHarness,
        volume: &VolumeRef,
        parent: TaskId,
        reference: &Component,
    ) -> Result<Self> {
        let operation = child_operation(volume, reference.as_str(), b"child")?;
        let child_agent = AgentId::from_bytes(
            child_operation(volume, reference.as_str(), b"agent")?.into_bytes(),
        );
        Ok(Self {
            parent,
            reference: reference.clone(),
            operation,
            child_agent,
            preparation: child_operation(volume, reference.as_str(), b"preparation")?,
            fork: child_operation(volume, reference.as_str(), b"fork")?,
            checkpoint: child_operation(volume, reference.as_str(), b"source")?,
            project: VolumeRef::new(
                harness.project.provider().clone(),
                format!("agent-project-{operation}"),
                VolumeClass::Project,
                harness.project.owner().clone(),
                &mut None,
            )?,
            private: VolumeRef::new(
                harness.project.provider().clone(),
                format!("agent-private-{operation}"),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(child_agent),
                &mut None,
            )?,
        })
    }
}
struct Parent {
    context: TaskContext,
    narrowed: RuntimeScope,
    fence: LeaseFence,
    project: VolumeRef,
    private: VolumeRef,
}
impl Parent {
    async fn open<P: StreamProvider + ?Sized>(
        harness: &LocalHarness,
        plan: &Plan,
        files: &TaskFiles<P>,
    ) -> Result<Self> {
        let host = harness.runtime.task_host();
        let WorkPull::Claimed(lease) = host.recover_work(plan.parent, &mut None).await? else {
            return Err(Error::Unauthorized(
                "child request parent has no live lease".into(),
            ));
        };
        let fence = LeaseFence::from(&lease.reservation);
        let context = harness
            .runtime
            .task_context(plan.parent, fence.clone())
            .await?;
        let binding = files.for_created_task(&context, context.scope()).await?;
        let expected = binding
            .as_ref()
            .map_or_else(|| harness.task_files.volume(), TaskFiles::volume);
        if files.volume() != expected {
            return Err(Error::Unauthorized(
                "child request belongs to another private volume".into(),
            ));
        }
        let request_file = files
            .snapshot(
                &Query::File(FileKey::Child {
                    reference: plan.reference.clone(),
                    file: ChildFile::Request,
                }),
                1,
            )
            .await
            .map_err(storage)?;
        let request = ChildRequest::parse(
            &request_file
                .files
                .first()
                .ok_or_else(|| Error::NotFound("accepted child request absent".into()))?
                .bytes,
            context.scope().limits().file_bytes,
        )?;
        let narrowed = crate::task_volume::child_scope(
            context.scope(),
            request.tools.as_deref(),
            request.grants.as_deref(),
        )?;
        let (project, private) = source_volumes(&context, &harness.project, &harness.scratch)?;
        Ok(Self {
            context,
            narrowed,
            fence,
            project,
            private,
        })
    }

    async fn preparer(
        &self,
        harness: &LocalHarness,
    ) -> Result<Arc<FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>>> {
        let VolumeOwner::Agent(agent) = self.private.owner() else {
            return Err(Error::Unauthorized(
                "child source private volume has no agent".into(),
            ));
        };
        let task = self.context.owned_task_binding(None)?.0;
        let signed = harness
            .runtime
            .fork_source_scope(task, self.fence.clone())
            .await?;
        if signed.agent() != Some(*agent) || signed.capabilities() != self.context.scope().grants()
        {
            return Err(Error::Unauthorized(
                "child source differs from its retained admission".into(),
            ));
        }
        let verifier = harness.verifier.clone();
        let reducer = Reducer::new(
            verifier.audience().clone(),
            verifier.clone(),
            SchemaRegistry::new(),
        );
        let resolver = FilesystemContentVerifier::new(
            harness.files.clone(),
            &verifier,
            &signed,
            self.narrowed.limits().file_bytes,
            None,
        )?;
        Ok(Arc::new(FilesystemForkPreparer::new(
            harness.files.clone(),
            reducer,
            verifier,
            signed,
            self.project.clone(),
            ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None)?,
            resolver,
            None,
            None,
            None,
        )?))
    }
}

pub(super) struct Admission<'a> {
    harness: &'a LocalHarness,
    plan: Plan,
    parent: Parent,
    workspace: &'a LocalWorkspace,
    io: PreparationIo,
    _invocation: crate::filesystem::InvocationCancellation,
    source: workspace::View,
    preparer: Arc<FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>>,
    publisher: Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>,
}
impl<'a> Admission<'a> {
    pub(super) async fn open<P: StreamProvider + ?Sized>(
        harness: &'a LocalHarness,
        plan: Plan,
        files: &TaskFiles<P>,
    ) -> Result<Self> {
        let parent = Parent::open(harness, &plan, files).await?;
        let workspace = harness
            .workspace
            .as_deref()
            .ok_or_else(|| Error::Unsupported("child agents require a lazy workspace".into()))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let invocation = crate::filesystem::InvocationCancellation(cancellation.clone());
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, harness.limits.file_bytes),
            &cancellation,
        )?;
        let preparer = parent.preparer(harness).await?;
        let publisher = harness.input_publisher_with_limit(parent.narrowed.limits().file_bytes)?;
        let source_workspace = io.sdk(
            workspace
                .filesystem
                .open_workspace(parent.project.storage_name(&mut None)?, &io, &cancellation)
                .await,
        )?;
        let source = {
            let io_ref = &io;
            let mut accounting = io_ref.accounting(&cancellation);
            let opened = workspace
                .view()
                .open_related(source_workspace, &mut accounting)
                .await;
            io.sdk(io_ref.finish(opened, &accounting))?
        };
        Ok(Self {
            harness,
            plan,
            parent,
            workspace,
            io,
            _invocation: invocation,
            source,
            preparer,
            publisher,
        })
    }

    async fn checkpoint(&self) -> Result<(FileRef, acyclic_fs::LazySnapshotRef)> {
        let operation = self.plan.checkpoint;
        let maximum = self.parent.narrowed.limits().file_bytes;
        let (retained, _) = self
            .publisher
            .observe_staged_once(
                operation,
                maximum,
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        if let Some(checkpoint) = retained {
            let bytes = self.parent.context.read_file(&checkpoint, None).await?;
            return Ok((
                checkpoint,
                crate::contract::json_from_slice(&bytes).map_err(storage)?,
            ));
        }
        let cut = {
            let io = &self.io;
            let cancellation = io.cancellation();
            let mut accounting = io.accounting(cancellation);
            let cut = self.source.snapshot(&mut accounting).await;
            io.sdk(io.finish(cut, &accounting))?
        };
        let bytes = serde_json::to_vec(&cut).map_err(storage)?;
        let (checkpoint, _) = self
            .publisher
            .stage_once(
                operation,
                &format!("inputs/agents/{}/source.json", self.plan.operation),
                &bytes,
                "application/json",
                "source.json",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        Ok((checkpoint, cut))
    }

    async fn fork(&self, cut: acyclic_fs::LazySnapshotRef) -> Result<ForkRequest> {
        let generation = GenerationRef::from_resource(
            ResourceRef::new(
                ResourceKind::Generation,
                self.parent.project.provider().clone(),
                cut.authored_generation.digest().as_bytes().to_vec(),
                None,
                &mut None,
            )?,
            &mut None,
        )?;
        let private = self
            .harness
            .files
            .resolve(
                &crate::filesystem::workspace_ref(
                    self.parent.private.provider().clone(),
                    &self.parent.private.storage_name(&mut None)?,
                )?,
                None,
            )
            .await?;
        Ok(ForkRequest {
            operation_id: self.plan.fork,
            authorized_source: None,
            parent: self.harness.verifier.audience().clone(),
            parent_revision: 0,
            child: Authority {
                kind: AggregateKind::Task,
                id: TaskId::from_bytes(self.plan.operation.into_bytes()).to_string(),
            },
            child_agent: self.plan.child_agent,
            attached_agents: Vec::new(),
            boundary: None,
            preparation: ForkPreparation {
                child_project_volume: self.plan.project.clone(),
                child_private_volume: self.plan.private.clone(),
                child_result_volume: None,
                summary: None,
                inherited_through_sequence: 0,
                maximum_inherited_messages: 1,
                maximum_inherited_bytes: self.parent.narrowed.limits().file_bytes,
                maximum_inherited_references: 1,
            },
            selections: vec![
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::Project {
                        volume: self.parent.project.clone().into(),
                        generation: generation.into(),
                    },
                },
                ForkSelection {
                    required: true,
                    revision: ResourceRevision::PrivateVolume {
                        volume: self.parent.private.clone().into(),
                        generation: private.generation.into(),
                        paths: Vec::new(),
                    },
                },
            ],
        })
    }

    async fn child(&self, fork: &ForkRequest) -> Result<(TaskAdmissionRecord, Vec<FileRef>)> {
        self.harness
            .prepare_inheritance(
                crate::runtime::task_id_for_admission(self.plan.operation),
                Some(self.plan.parent),
            )
            .await?;
        let (input, inputs, kind) = {
            let (receive, model, manifest) = self
                .harness
                .stage_turn_with_output(
                    self.plan.operation,
                    "Follow the received child request.",
                    self.parent.narrowed.limits(),
                    Some(&self.plan.project),
                )?
                .await?;
            let inputs = vec![receive.payload.clone(), model.payload.clone(), manifest];
            (
                serde_json::json!({"receive":receive,"model":model}),
                inputs,
                "acyclic.child_agent@1",
            )
        };
        let definition = self.harness.runtime.harness().task::<Value, Value>(kind)?;
        let mut child = self
            .harness
            .runtime
            .harness()
            .registered_admission(
                self.plan.operation,
                &definition.identity().name,
                &definition.identity().version,
                input,
                Some((self.plan.parent, self.parent.context.scope())),
                None,
                None,
            )?
            .0;
        child.grants = self.child_grants(&inputs)?;
        child = child.with_resource_proofs(self.preparer.creation_allocations(
            fork,
            TaskId::from_bytes(self.plan.preparation.into_bytes()),
            Some(&self.io),
        )?)?;
        Ok((child, inputs))
    }

    fn child_grants(&self, inputs: &[FileRef]) -> Result<Capabilities> {
        let mut excluded = [&self.parent.project, &self.parent.private]
            .into_iter()
            .flat_map(|volume| {
                [VolumeOperation::Read, VolumeOperation::Write]
                    .into_iter()
                    .map(move |operation| volume.capability(operation, &mut None))
            })
            .collect::<Result<Vec<_>>>()?;
        for volume in [&self.parent.project, &self.parent.private] {
            excluded.push(volume.directory_read_capability("", &mut None)?);
        }
        // A child's own preparation proofs replace ancestor input readers.
        if let Some(access) = self.parent.context.scope().created_access.as_ref() {
            for (file, _) in &access.references {
                excluded.push(file.read_capability(&mut None)?);
            }
        }
        Ok(Capabilities::new(
            self.parent
                .narrowed
                .grants()
                .iter()
                .filter(|grant| !excluded.iter().any(|denied| denied == grant))
                .map(str::to_owned)
                .chain(
                    [&self.plan.project, &self.plan.private]
                        .into_iter()
                        .flat_map(|volume| {
                            [VolumeOperation::Read, VolumeOperation::Write]
                                .into_iter()
                                .map(move |operation| volume.capability(operation, &mut None))
                        })
                        .collect::<Result<Vec<_>>>()?,
                )
                .chain(
                    inputs
                        .iter()
                        .map(|file| file.read_capability(&mut None))
                        .collect::<Result<Vec<_>>>()?,
                ),
        ))
    }

    async fn stage(
        &self,
    ) -> Result<(
        TaskAdmissionRecord,
        ResourcePreparationRequest,
        acyclic_fs::LazySnapshotRef,
    )> {
        let host = self.harness.runtime.task_host();
        if let Some((_, record)) = host.reconcile_admission(self.plan.preparation).await? {
            if record.parent != Some(self.plan.parent) {
                return Err(Error::Conflict("child preparation parent changed".into()));
            }
            let request: ResourcePreparationRequest =
                serde_json::from_value(record.input.clone()).map_err(storage)?;
            let checkpoint = request
                .source_checkpoint
                .as_ref()
                .ok_or_else(|| Error::Conflict("child source checkpoint missing".into()))?;
            let bytes = self.parent.context.read_file(checkpoint, None).await?;
            let cut = crate::contract::json_from_slice(&bytes).map_err(storage)?;
            return Ok((record, request, cut));
        }
        let (checkpoint, cut) = self.checkpoint().await?;
        let fork = self.fork(cut).await?;
        let (child, inputs) = self.child(&fork).await?;
        let (payload, _) = self
            .publisher
            .stage_once(
                self.plan.fork,
                &format!("inputs/agents/{}/fork.json", self.plan.operation),
                &serde_json::to_vec(&fork).map_err(storage)?,
                "application/json",
                "fork.json",
                crate::conversation::ContentAccess::default(),
            )?
            .await?;
        let request = ResourcePreparationRequest {
            provider: self.plan.project.provider().clone(),
            child,
            creation_grant: "fork:publish".into(),
            maximum_resources: 2,
            maximum_work: 65_536,
            maximum_bytes: self.parent.narrowed.limits().file_bytes,
            payload: serde_json::to_value(&fork).map_err(storage)?,
            command: Some(WorkflowCommand {
                operation_id: self.plan.fork,
                kind: crate::filesystem::FORK_PREPARE_TASK_COMMAND_KIND.into(),
                payload,
            }),
            source_checkpoint: Some(checkpoint),
            input_files: inputs,
            read_authorization: None,
        };
        let definition = self
            .harness
            .runtime
            .harness()
            .task::<ResourcePreparationRequest, crate::fork::ForkReport>(
                "acyclic.fork_preparation@1",
            )?;
        let record = self
            .harness
            .runtime
            .harness()
            .registered_admission(
                self.plan.preparation,
                &definition.identity().name,
                &definition.identity().version,
                serde_json::to_value(&request).map_err(storage)?,
                Some((self.plan.parent, self.parent.context.scope())),
                None,
                None,
            )?
            .0;
        Ok((record, request, cut))
    }

    pub(super) async fn finish(self) -> Result<TaskId> {
        // Always fence the accepted input prefix before admission, including errors.
        let (record, request, cut) =
            super::super::preparation::fenced_inputs(self.stage(), async {
                let filesystem = self.workspace.filesystem.clone();
                spawn_blocking(move || filesystem.flush_deferred_authority())
                    .await
                    .map_err(storage)?
                    .map_err(storage)
            })
            .await?;
        let host = self.harness.runtime.task_host();
        match host.admit_owned(record, self.parent.fence.clone()).await? {
            crate::Admission::Accepted(_) => {}
            crate::Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            crate::Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        }
        let commands = super::super::preparation::Commands {
            harness: self.harness,
            source: &self.source,
            cut,
            preparer: &self.preparer,
            request: &request,
            workspace: self.workspace,
            io: &self.io,
        };
        crate::package::harness::worker::advance(
            self.harness,
            TaskId::from_bytes(self.plan.preparation.into_bytes()),
            &commands,
        )
        .await?;
        if !matches!(
            host.terminal_status(TaskId::from_bytes(self.plan.preparation.into_bytes()))
                .await?,
            Some(crate::Outcome::Succeeded(_))
        ) {
            return Err(Error::Indeterminate(self.plan.preparation));
        }
        #[cfg(all(test, not(acyclic_harness_loom)))]
        super::super::cuts::stop("prepared", self.plan.operation)?;
        let child_id = self
            .workspace
            .filesystem
            .workspace_id(self.plan.project.storage_name(&mut None)?)
            .map_err(storage)?;
        acyclic_fs::GitCompatRepository::new(
            self.source.workspace().id(),
            self.workspace.store.clone(),
        )
        .register_branch_workspace(
            format!("agents/{}", self.plan.reference.as_str()),
            child_id,
            None,
            false,
        )
        .await
        .map_err(storage)?;
        let task = match host.admit_owned(request.child, self.parent.fence).await? {
            crate::Admission::Accepted(task) => task,
            crate::Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            crate::Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        #[cfg(all(test, not(acyclic_harness_loom)))]
        super::super::cuts::stop("admitted", self.plan.operation)?;
        Ok(task)
    }
}
