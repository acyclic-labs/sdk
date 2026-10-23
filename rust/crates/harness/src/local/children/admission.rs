use super::*;
use crate::{
    core::{Reducer, SchemaRegistry},
    distributed::WorkPull,
    filesystem::{FilesystemContentVerifier, FilesystemForkPreparer, PreparationIo},
    fork::{ForkPreparation, ForkRequest, ForkSelection, ResourceRevision},
    resource_preparation::ResourcePreparationRequest,
    resources::{GenerationRef, ProviderRef, ResourceKind, ResourceRef},
    runtime::TaskStateProvider,
    task_volume::{ChildFile, ChildRequest, FileKey, FileValue, Query, TaskFiles},
};
use acyclic_fs::facade::FsWork as _;
use acyclic_native_runtime::spawn_blocking;
use acyclic_stream::StreamProvider;

impl LocalHarness {
    pub(in crate::local) async fn admit_child_task<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        file: &FileValue,
    ) -> Result<TaskId> {
        let FileKey::Child {
            reference,
            file: ChildFile::Request,
        } = &file.key
        else {
            return Err(Error::Invalid(
                "child admission requires a request file".into(),
            ));
        };
        let observed = files
            .inner
            .snapshot_files(&[file.key.clone()], &Query::File(file.key.clone()))
            .await
            .map_err(storage)?;
        if observed.files.as_slice() != std::slice::from_ref(file) {
            return Err(Error::Conflict(
                "child request differs from its accepted file".into(),
            ));
        }
        let parent = file.requester.ok_or_else(|| {
            Error::Unauthorized("child request has no authenticated parent".into())
        })?;
        let parent_record = self.runtime.task_host().observe_admission(parent).await?;
        let native_host = parent_record.task.name == "acyclic.native_host";
        let operation = child_operation(files.volume(), reference.as_str(), b"child")?;
        let admitted = match self
            .runtime
            .task_host()
            .observe_admission(TaskId::from_bytes(operation.into_bytes()))
            .await
        {
            Ok(record) => Some(record),
            Err(Error::NotFound(_)) => None,
            Err(error) => return Err(error),
        };
        if let Some(admitted) = admitted {
            if if native_host {
                admitted.task.name != "acyclic.native_host" || admitted.task.version != "1"
            } else {
                !crate::filesystem::child_agent::is_child(&admitted)
            } {
                return Err(Error::Conflict(
                    "child request task definition changed".into(),
                ));
            }
            if admitted.parent != Some(parent) {
                return Err(Error::Conflict("child parent changed".into()));
            }
            return Ok(admitted.task_id());
        }

        let preparation_operation =
            child_operation(files.volume(), reference.as_str(), b"preparation")?;
        let fork_operation = child_operation(files.volume(), reference.as_str(), b"fork")?;
        let checkpoint_operation = child_operation(files.volume(), reference.as_str(), b"source")?;
        let child_agent = AgentId::from_bytes(
            child_operation(files.volume(), reference.as_str(), b"agent")?.into_bytes(),
        );
        let project = VolumeRef::new(
            self.project.provider().clone(),
            format!("agent-project-{operation}"),
            VolumeClass::Project,
            self.project.owner().clone(),
            &mut None,
        )?;
        let private = VolumeRef::new(
            self.project.provider().clone(),
            format!("agent-private-{operation}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(child_agent),
            &mut None,
        )?;
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("child agents require a lazy workspace".into()))?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let _invocation = crate::filesystem::InvocationCancellation(cancellation.clone());
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, self.limits.file_bytes),
            &cancellation,
        )?;

        let io_ref = &io;

        let host = self.runtime.task_host();
        let WorkPull::Claimed(parent_lease) = host.recover_work(parent, &mut None).await? else {
            return Err(Error::Unauthorized(
                "child request parent has no live lease".into(),
            ));
        };
        let fence = LeaseFence::from(&parent_lease.reservation);
        let context = self.runtime.task_context(parent, fence.clone()).await?;
        let binding = files.for_created_task(&context, context.scope()).await?;
        let expected_volume = binding
            .as_ref()
            .map_or_else(|| self.task_files.volume(), TaskFiles::volume);
        if files.volume() != expected_volume {
            return Err(Error::Unauthorized(
                "child request belongs to another private volume".into(),
            ));
        }
        let request = ChildRequest::parse(&file.bytes, context.scope().limits().file_bytes)?;
        let spawn_grant = if native_host {
            "task:spawn:acyclic.native_host@1"
        } else {
            "tool:call:spawn"
        };
        if !context.scope().grants().contains(spawn_grant) {
            return Err(Error::Unauthorized(
                "child parent lacks the spawn capability".into(),
            ));
        }
        let narrowed = crate::task_volume::child_scope(
            context.scope(),
            request.tools.as_deref(),
            request.grants.as_deref(),
        )?;
        let (source_project, source_private) =
            source_volumes(&context, &self.project, &self.scratch)?;
        let source_agent = match source_private.owner() {
            VolumeOwner::Agent(agent) => *agent,
            _ => {
                return Err(Error::Unauthorized(
                    "child source private volume has no agent".into(),
                ));
            }
        };
        let issuer = self
            .issuer
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("child source has no issuing authority".into()))?;
        let signed = issuer.issue(
            "child-source",
            context.scope().grants(),
            Some(source_agent),
            None,
            &mut None,
        )?;
        let verifier = self.verifier.clone();
        let reducer = Reducer::new(
            verifier.audience().clone(),
            verifier.clone(),
            SchemaRegistry::new(),
        );
        let resolver = FilesystemContentVerifier::new(
            self.files.clone(),
            &verifier,
            &signed,
            narrowed.limits().file_bytes,
            None,
        )?;
        let preparer = Arc::new(FilesystemForkPreparer::new(
            self.files.clone(),
            reducer,
            verifier,
            signed,
            source_project.clone(),
            ProviderRef::new("local".into(), "stream".into(), "2".into(), &mut None)?,
            resolver,
            None,
            None,
            None,
        )?);
        let publisher = self.input_publisher_with_limit(narrowed.limits().file_bytes)?;
        let source_workspace = io.sdk(
            workspace
                .filesystem
                .open_workspace(source_project.storage_name(&mut None)?, &io, &cancellation)
                .await,
        )?;
        let mut accounting = io_ref.accounting(&cancellation);
        let source = workspace
            .view()
            .open_related(source_workspace, &mut accounting)
            .await;
        let source = io.sdk(io_ref.finish(source, &accounting))?;
        drop(accounting);

        let retained = host.reconcile_admission(preparation_operation).await?;
        // Staging has no external execution: retain every input immediately,
        // then fence their authority log together before admitting preparation.
        let (preparation_record, preparation_request, cut) = preparation::fenced_inputs(
            async {
                let prepared = if let Some((_, record)) = retained {
                    if record.parent != Some(parent) {
                        return Err(Error::Conflict("child preparation parent changed".into()));
                    }
                    let request: ResourcePreparationRequest =
                        serde_json::from_value(record.input.clone()).map_err(storage)?;
                    let checkpoint = request
                        .source_checkpoint
                        .as_ref()
                        .ok_or_else(|| Error::Conflict("child source checkpoint missing".into()))?;
                    let bytes = context.read_file(checkpoint, None).await?;
                    let cut = crate::contract::json_from_slice(&bytes).map_err(storage)?;
                    (record, request, cut)
                } else {
                    let (checkpoint, _) = publisher
                        .observe_staged_once(
                            checkpoint_operation,
                            narrowed.limits().file_bytes,
                            None,
                        )?
                        .await?;
                    let (checkpoint, cut) = if let Some(checkpoint) = checkpoint {
                        let bytes = context.read_file(&checkpoint, None).await?;
                        let cut = crate::contract::json_from_slice(&bytes).map_err(storage)?;
                        (checkpoint, cut)
                    } else {
                        let mut accounting = io_ref.accounting(&cancellation);
                        let cut = source.snapshot(&mut accounting).await;
                        let cut = io.sdk(io_ref.finish(cut, &accounting))?;
                        drop(accounting);
                        let bytes = serde_json::to_vec(&cut).map_err(storage)?;
                        let (checkpoint, _) = publisher
                            .stage_once(
                                checkpoint_operation,
                                &format!("inputs/agents/{operation}/source.json"),
                                &bytes,
                                "application/json",
                                "source.json",
                                None,
                            )?
                            .await?;
                        (checkpoint, cut)
                    };
                    let source_generation = GenerationRef::from_resource(
                        ResourceRef::new(
                            ResourceKind::Generation,
                            source_project.provider().clone(),
                            cut.authored_generation.digest().as_bytes().to_vec(),
                            None,
                            &mut None,
                        )?,
                        &mut None,
                    )?;
                    let private_head = self
                        .files
                        .resolve(
                            &crate::filesystem::workspace_ref(
                                source_private.provider().clone(),
                                &source_private.storage_name(&mut None)?,
                            )?,
                            None,
                        )
                        .await?;
                    let fork = ForkRequest {
                        operation_id: fork_operation,
                        authorized_source: None,
                        parent: self.verifier.audience().clone(),
                        parent_revision: 0,
                        child: Authority {
                            kind: AggregateKind::Task,
                            id: TaskId::from_bytes(operation.into_bytes()).to_string(),
                        },
                        child_agent,
                        attached_agents: Vec::new(),
                        boundary: None,
                        preparation: ForkPreparation {
                            child_project_volume: project.clone(),
                            child_private_volume: private.clone(),
                            child_result_volume: None,
                            summary: None,
                            inherited_through_sequence: 0,
                            maximum_inherited_messages: 1,
                            maximum_inherited_bytes: narrowed.limits().file_bytes,
                            maximum_inherited_references: 1,
                        },
                        selections: vec![
                            ForkSelection {
                                required: true,
                                revision: ResourceRevision::Project {
                                    volume: (source_project.clone()).into(),
                                    generation: (source_generation).into(),
                                },
                            },
                            ForkSelection {
                                required: true,
                                revision: ResourceRevision::PrivateVolume {
                                    volume: (source_private.clone()).into(),
                                    generation: (private_head.generation).into(),
                                    paths: Vec::new(),
                                },
                            },
                        ],
                    };
                    self.prepare_inheritance(
                        crate::runtime::task_id_for_admission(operation),
                        Some(parent),
                    )
                    .await?;
                    let (input, inputs, task_kind) = if native_host {
                        (serde_json::json!({"host":parent_record.input.get("host").ok_or_else(|| Error::Invalid("native parent host is absent".into()))?, "session":reference.as_str()}),
                            Vec::new(), super::super::host_task::TASK)
                    } else {
                    let (receive, model, manifest) = self
                        .stage_turn_with_output(
                            operation,
                            "Follow the received child request.",
                            narrowed.limits(),
                            Some(&project),
                        )
                        .await?;
                    let inputs = vec![receive.payload.clone(), model.payload.clone(), manifest];
                    (serde_json::json!({"receive":receive,"model":model}), inputs, "acyclic.child_agent@1")
                    };
                    let definition = self
                        .runtime
                        .harness()
                        .task::<serde_json::Value, serde_json::Value>(task_kind)?;
                    let mut child = self
                        .runtime
                        .harness()
                        .registered_admission(
                            operation,
                            &definition.identity().name,
                            &definition.identity().version,
                            input,
                            Some((parent, context.scope())),
                            None,
                            None,
                        )?
                        .0;
                    let mut excluded = [&source_project, &source_private]
                        .into_iter()
                        .flat_map(|volume| {
                            [VolumeOperation::Read, VolumeOperation::Write]
                                .into_iter()
                                .map(move |operation| volume.capability(operation, &mut None))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    for volume in [&source_project, &source_private] {
                        excluded.push(volume.directory_read_capability("", &mut None)?);
                    }
                    // New preparation proofs authenticate this child's own inputs.
                    // Ancestor-only input readers belong to the parent's proof set.
                    if let Some(access) = context.scope().created_access.as_ref() {
                        for (file, _) in &access.references {
                            excluded.push(file.read_capability(&mut None)?);
                        }
                    }
                    child.grants = Capabilities::new(
                        narrowed
                            .grants()
                            .iter()
                            .filter(|grant| !excluded.iter().any(|denied| denied == grant))
                            .map(str::to_owned)
                            .chain(
                                [&project, &private]
                                    .into_iter()
                                    .flat_map(|volume| {
                                        [VolumeOperation::Read, VolumeOperation::Write]
                                            .into_iter()
                                            .map(move |operation| {
                                                volume.capability(operation, &mut None)
                                            })
                                    })
                                    .collect::<Result<Vec<_>>>()?,
                            )
                            .chain(
                                inputs
                                    .iter()
                                    .map(|file| file.read_capability(&mut None))
                                    .collect::<Result<Vec<_>>>()?,
                            ),
                    );
                    child = child.with_resource_proofs(preparer.creation_allocations(
                        &fork,
                        TaskId::from_bytes(preparation_operation.into_bytes()),
                        Some(&io),
                    )?)?;
                    let fork_value = serde_json::to_value(&fork).map_err(storage)?;
                    let bytes = serde_json::to_vec(&fork).map_err(storage)?;
                    let (payload, _) = publisher
                        .stage_once(
                            fork_operation,
                            &format!("inputs/agents/{operation}/fork.json"),
                            &bytes,
                            "application/json",
                            "fork.json",
                            None,
                        )?
                        .await?;
                    let request = ResourcePreparationRequest {
                        provider: project.provider().clone(),
                        child,
                        creation_grant: "fork:publish".into(),
                        maximum_resources: 2,
                        maximum_work: 65_536,
                        maximum_bytes: narrowed.limits().file_bytes,
                        payload: fork_value,
                        command: Some(WorkflowCommand {
                            operation_id: fork_operation,
                            kind: crate::filesystem::FORK_PREPARE_TASK_COMMAND_KIND.into(),
                            payload,
                        }),
                        source_checkpoint: Some(checkpoint),
                        input_files: inputs,
                        read_authorization: None,
                    };
                    let definition = self
                        .runtime
                        .harness()
                        .task::<ResourcePreparationRequest, crate::fork::ForkReport>(
                            "acyclic.fork_preparation@1",
                        )?;
                    let record = self
                        .runtime
                        .harness()
                        .registered_admission(
                            preparation_operation,
                            &definition.identity().name,
                            &definition.identity().version,
                            serde_json::to_value(&request).map_err(storage)?,
                            Some((parent, context.scope())),
                            None,
                            None,
                        )?
                        .0;
                    (record, request, cut)
                };
                Ok(prepared)
            },
            async {
                let filesystem = workspace.filesystem.clone();
                spawn_blocking(move || filesystem.flush_deferred_authority())
                    .await
                    .map_err(storage)?
                    .map_err(storage)
            },
        )
        .await?;
        match host.admit_owned(preparation_record, fence.clone()).await? {
            Admission::Accepted(_) => {}
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        }
        let commands = preparation::Commands {
            harness: self,
            source: &source,
            cut,
            preparer: &preparer,
            request: &preparation_request,
            workspace,
            io: &io,
        };
        self.advance(
            TaskId::from_bytes(preparation_operation.into_bytes()),
            &commands,
        )
        .await?;
        if !matches!(
            host.terminal_status(TaskId::from_bytes(preparation_operation.into_bytes()))
                .await?,
            Some(crate::Outcome::Succeeded(_))
        ) {
            return Err(Error::Indeterminate(preparation_operation));
        }
        #[cfg(all(test, not(acyclic_harness_loom)))]
        super::cuts::stop("prepared", operation)?;
        let child_id = workspace
            .filesystem
            .workspace_id(project.storage_name(&mut None)?)
            .map_err(storage)?;
        acyclic_fs::GitCompatRepository::new(source.workspace().id(), workspace.store.clone())
            .register_branch_workspace(
                format!("agents/{}", reference.as_str()),
                child_id,
                None,
                false,
            )
            .await
            .map_err(storage)?;
        let task = match host.admit_owned(preparation_request.child, fence).await? {
            Admission::Accepted(task) => task,
            Admission::Rejected { reason } => return Err(Error::Conflict(reason)),
            Admission::Indeterminate { operation_id } => {
                return Err(Error::Indeterminate(operation_id));
            }
        };
        #[cfg(all(test, not(acyclic_harness_loom)))]
        super::cuts::stop("admitted", operation)?;
        Ok(task)
    }
    #[cfg(test)]
    pub(in crate::local) async fn admit_child_request<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        file: &FileValue,
    ) -> Result<(TaskId, workspace::View)> {
        let task = self.admit_child_task(files, file).await?;
        let workspace = self.workspace.as_ref().unwrap();
        let project = VolumeRef::new(
            self.project.provider().clone(),
            format!("agent-project-{task}"),
            VolumeClass::Project,
            self.project.owner().clone(),
            &mut None,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, self.limits.file_bytes),
            &cancellation,
        )?;
        let child = io.sdk(
            workspace
                .filesystem
                .open_workspace(project.storage_name(&mut None)?, &io, &cancellation)
                .await,
        )?;
        let io_ref = &io;
        let mut accounting = io_ref.accounting(&cancellation);
        let opened = workspace.view().open_related(child, &mut accounting).await;
        let view = io.sdk(io_ref.finish(opened, &accounting))?;
        let work = io.receipt();
        acyclic_test_support::differential::record(("view-items", work.items_examined));
        acyclic_test_support::differential::record((
            "view-allocations",
            work.allocation_operations,
        ));
        Ok((task, view))
    }
}

pub(super) fn child_operation(
    volume: &VolumeRef,
    reference: &str,
    phase: &[u8],
) -> Result<OperationId> {
    let mut digest = blake3::Hasher::new();
    digest.update(b"acyclic.local.child-file/v1\0");
    digest.update(volume.storage_name(&mut None)?.as_bytes());
    digest.update(&[0]);
    digest.update(reference.as_bytes());
    digest.update(&[0]);
    digest.update(phase);
    Ok(OperationId::from_bytes(
        digest.finalize().as_bytes()[..16].try_into().unwrap(),
    ))
}

fn source_volumes(
    context: &crate::runtime::TaskContext,
    project: &VolumeRef,
    private: &VolumeRef,
) -> Result<(VolumeRef, VolumeRef)> {
    let Some(access) = context.scope().created_access.as_ref() else {
        return Ok((project.clone(), private.clone()));
    };
    let find = |class| -> Result<VolumeRef> {
        let mut bindings = access
            .content
            .iter()
            .filter(|binding| binding.volume.class() == class);
        let volume = bindings
            .next()
            .ok_or_else(|| Error::Unauthorized("child source volume missing".into()))?;
        if bindings.next().is_some() {
            return Err(Error::Unauthorized("child source volume ambiguous".into()));
        }
        Ok(volume.volume.clone())
    };
    Ok((
        find(VolumeClass::Project)?,
        find(VolumeClass::AgentPrivate)?,
    ))
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod work_tests;
