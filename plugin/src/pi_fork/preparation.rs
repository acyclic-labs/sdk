//! Preparation of the original Pi fork.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

impl<'a> PiForkCoordinator<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "actual original owner and native cut are independent authorities"
    )]
    pub(crate) fn begin<'b>(
        &'b self,
        shared: &'b Arc<acyclic_harness::runtime::TaskContext>,
        owner: &'b Arc<TaskJournalOwner<LocalStream>>,
        fence: &'b LeaseFence,
        source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef,
        mut commit: RetainedSpawnCommit,
        limits: ForkPreparationLimits,
    ) -> impl std::future::Future<Output = Result<PiForkPlan>> + Send + 'b {
        async move {
            let context = shared.as_ref();
            let work = &self.work;
            let cancel = work.cancellation();
            let admission = self.authenticate(context, owner, fence).await?;
            let caller_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self
                .decode_input_value(
                    &admission.0.input,
                    acyclic_harness::JsonValueSchema::None,
                    crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
                )?;
            let caller = &caller_input.0;
            let original = original_operation(
                self.owner.policy().root_task,
                owner.task_id(),
                &commit.sdk_operation,
                &self.work,
            )?;
            let operations = PiForkOperations::new(original, &self.work)?;
            if commit.sdk_operation.is_empty()
                || commit.alias.is_empty()
                || commit.caller != owner.task_id()
                || commit.alias != commit.launch.agent_alias
                || commit.child.task_id != task_id_for_admission(operations.child)
                || commit.child.agent_id != operations.agent
                || commit.child.native_session_id != commit.launch.child_session_id
                || commit.prompt != commit.launch.invocation.prompt
                || commit.launch.source.parent_session_id != caller.binding.native_session_id
                || !matches!(
                    commit.child.scratch.allocation,
                    PiScratchAllocation::Preparing
                )
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi spawn changed its original SDK, native, or task identity",
                });
            }
            commit.launch.validate(work)?;
            commit
                .launch
                .invocation
                .environment
                .retain(|key, _| !crate::pi_caller::is_environment_key(key));
            self.require_input_read(context)?;
            if let Some(plan) = self.load_original_plan(original).await? {
                self.validate_plan(&plan, owner.task_id())?;
                if plan.commit != commit {
                    return Err(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi SDK retry changed its immutable spawn commit",
                    });
                }
                self.prepare_native_source(owner, &plan, source).await?;
                return Ok(plan);
            }
            let authority = self.owner.conversation_authority_with_native_work(
                owner.task_id(),
                caller.binding.agent_id,
                context.scope().grants(),
                work,
            )?;
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let verifier = authority.0.1.verifier(&mut Some(&mut io))?;
            let mut parent = StreamAggregate::open(
                &self.stream,
                (
                    io.clone_authority(&authority.0.0)?,
                    verifier.copy(&mut Some(&mut io))?,
                    SchemaRegistry::new(),
                ),
                None,
                acyclic_harness::store::default_projection_read_limits(),
                acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
                None,
                &mut Some(&mut io),
            )
            .await?;
            io.return_staged_output()?;
            parent.set_limits(context.scope().limits())?;
            let signed = &authority.0.2;
            let caller_id = work.display(&owner.task_id())?;
            if parent.reducer().authority().kind != AggregateKind::Conversation
                || parent.reducer().authority().id != caller_id.0
                || parent
                    .reducer()
                    .conversation()
                    .and_then(|state| state.agent)
                    != Some(caller.binding.agent_id)
            {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi fork requires the actual bound caller conversation",
                });
            }
            self.selected_graph(context, owner, commit.source_task)
                .await?;
            let source_admission = self
                .runtime
                .runtime
                .task_host()
                .observe_admission_with_native_work(commit.source_task, &self.work)?
                .await?;
            let source_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self
                .decode_input_value(
                    &source_admission.0.input,
                    acyclic_harness::JsonValueSchema::None,
                    crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
                )?;
            let source_input = &source_input.0;
            let mut selected_project = None;
            for (key, volume) in &source_input.binding.project_volumes {
                let name = {
                    let mut io = acyclic_harness::store::HistoryWork::new(
                        PreparationIo::from_native_work(work)?,
                    );
                    let value = volume.storage_name(&mut Some(&mut io))?;
                    (value, io.into_custody()?)
                };
                if name.0 == source.workspace().name().as_str() {
                    selected_project = Some((key, volume));
                    break;
                }
            }
            let (root_key, project) = selected_project.ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi source handle is not its originally admitted project",
            })?;
            let mut generation = acyclic_harness::resources::GenerationRef::new(
                project.provider(),
                frozen.authored_generation.digest().as_bytes(),
                None,
                Some(work),
            )
            .and_then(|(reference, custody)| {
                Ok((
                    reference,
                    custody.ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?,
                ))
            })?;
            let scratch = &caller.binding.scratch.volume;
            let mut private_generation = self
                .native_generation(context, owner, scratch, work)
                .await?;
            let original_caller_project = caller
                .binding
                .project_volumes
                .get(root_key)
                .or_else(|| caller.binding.project_volumes.values().next())
                .ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi caller has no actual project binding",
                })?;
            let source_read = if commit.source_task == owner.task_id() {
                None
            } else {
                Some(
                    self.authorize_selected_project_with_native_work(
                        context,
                        owner,
                        commit.source_task,
                        source.clone(),
                        frozen,
                        operations.fork,
                        limits.work,
                        limits.bytes,
                        work,
                    )
                    .await?,
                )
            };
            if commit.child.project_volumes.len() != 1 {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi fork requires its one actually selected child project",
                });
            }
            let child_project = commit.child.project_volumes.get(root_key).ok_or_else(|| {
                Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi child changed its selected root key",
                }
            })?;
            let results = commit
                .child
                .results
                .as_ref()
                .ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi child requires its separate unmounted result allocation",
                })?;
            if !matches!(results.allocation, PiScratchAllocation::Preparing) {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "Pi immutable result allocation must remain Preparing",
                });
            }
            let mut child_volume_io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let child_project = child_project.copy(&mut Some(&mut child_volume_io))?;
            let child_private = commit
                .child
                .scratch
                .volume
                .copy(&mut Some(&mut child_volume_io))?;
            let child_result = results.volume.copy(&mut Some(&mut child_volume_io))?;
            let mut child_volume_custody = child_volume_io.into_custody()?;
            let mut child_id = work.display(&commit.child.task_id)?;
            let history_limits = HistoryReadLimits {
                maximum_events: u32::try_from(limits.work).unwrap_or(u32::MAX),
                maximum_bytes: limits.bytes,
            };
            let checkpoint_snapshot = {
                let mut io = acyclic_harness::store::HistoryWork::new(
                    PreparationIo::from_native_work(work)?,
                );
                let snapshot = parent.reducer().snapshot(usize::MAX, &mut Some(&mut io))?.0;
                (snapshot, io.into_custody()?)
            };
            let checkpoint_bytes = crate::pi_root_provider::guarded_pi_bytes(
                &checkpoint_snapshot.0,
                self.owner.policy().maximum_payload_bytes,
                work,
            )?;
            let checkpoint_path =
                work.display(&format_args!("pi-fork/{original}/source-checkpoint.json"))?;
            let checkpoint = {
                let publication_io = PreparationIo::from_native_work(&self.work)?;
                (context)
                    .stage_file_once(
                        derived(original, "source-checkpoint", &self.work)?,
                        &checkpoint_path.0,
                        &checkpoint_bytes.0,
                        "application/json",
                        "Pi original caller cut",
                        Some(&publication_io),
                    )
                    .await
                    .and_then(|(file, retained)| {
                        Ok((file, publication_io.take_native_custody(retained)?))
                    })
            }?;
            let resolver = acyclic_harness::runtime::content_residency_verifier(
                Arc::clone(shared),
                Arc::clone(owner),
                Some(&PreparationIo::from_native_work(work)?),
            )?;
            let mut provider_io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let provider = acyclic_harness::resources::ProviderRef::new(
                provider_io.clone_text("local")?,
                provider_io.clone_text("stream")?,
                provider_io.clone_text("1")?,
                &mut Some(&mut provider_io),
            )?;
            let stream_provider = (provider, provider_io.into_custody()?);
            let authorized_source = source_read.as_ref().map(|pair| {
            self.decode_input_value::<acyclic_harness::source_authorization::ProjectSourceAuthorization>(
                pair.0.authorization(), acyclic_harness::JsonValueSchema::None, FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )
        }).transpose()?;
            let mut request_io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let reference_provider = acyclic_harness::resources::ProviderRef::new(
                request_io.clone_text(stream_provider.0.namespace())?,
                request_io.clone_text(stream_provider.0.family())?,
                request_io.clone_text(stream_provider.0.version())?,
                &mut Some(&mut request_io),
            )?;
            let mut history_version = work.display(&parent.reducer().revision())?;
            let history = acyclic_harness::resources::StreamRef::new(
                reference_provider,
                parent
                    .reducer()
                    .authority()
                    .stream_path(&mut Some(&mut request_io))?
                    .into_bytes(),
                Some(history_version.0),
            )?;
            let mut selections = crate::pi_native::pi_native_vec(3, work)?;
            let mut selection_boxes = work.claim(
                (std::mem::size_of::<acyclic_harness::resources::StreamRef>()
                    + 2 * std::mem::size_of::<acyclic_harness::conversation::VolumeRef>()
                    + 2 * std::mem::size_of::<acyclic_harness::resources::GenerationRef>())
                    as u64,
                5,
                acyclic_fs::WorkCounters::default(),
            )?;
            for revision in [
                acyclic_harness::fork::ResourceRevision::History(Box::new(history)),
                acyclic_harness::fork::ResourceRevision::Project {
                    volume: Box::new(project.copy(&mut Some(&mut request_io))?),
                    generation: Box::new(generation.0),
                },
                acyclic_harness::fork::ResourceRevision::PrivateVolume {
                    volume: Box::new(scratch.copy(&mut Some(&mut request_io))?),
                    generation: Box::new(private_generation.0),
                    paths: Vec::new(),
                },
            ] {
                selections.0.push(acyclic_harness::fork::ForkSelection {
                    required: true,
                    revision,
                });
            }
            let (authorized_source, mut authorization_custody) = match authorized_source {
                Some((authorization, custody)) => (Some(authorization), Some(custody)),
                None => (None, None),
            };
            let request = ForkRequest {
                operation_id: operations.fork,
                parent: request_io.clone_authority(parent.reducer().authority())?,
                parent_revision: parent.reducer().revision(),
                child: Authority {
                    kind: AggregateKind::Conversation,
                    id: child_id.0,
                },
                child_agent: operations.agent,
                attached_agents: Vec::new(),
                preparation: acyclic_harness::fork::ForkPreparation {
                    child_project_volume: child_project,
                    child_private_volume: child_private,
                    child_result_volume: Some(child_result),
                    summary: None,
                    inherited_through_sequence: 0,
                    maximum_inherited_messages: context.scope().limits().context_messages as u64,
                    maximum_inherited_bytes: limits.bytes,
                    maximum_inherited_references: limits.resources,
                },
                selections: selections.0,
                boundary: None,
                authorized_source,
            }
            .with_history_policy(
                parent.reducer(),
                if commit.source_task == owner.task_id() {
                    acyclic_harness::fork::ForkHistoryPolicy::default()
                } else {
                    acyclic_harness::fork::ForkHistoryPolicy::Fresh
                },
                &mut Some(&mut request_io),
            )?;
            let mut request_custody = request_io.into_custody()?;
            for custody in [
                &mut child_volume_custody,
                &mut child_id.1,
                &mut history_version.1,
                &mut selections.1,
                &mut selection_boxes,
                &mut generation.1,
                &mut private_generation.1,
            ]
            .into_iter()
            .chain(authorization_custody.as_mut())
            {
                request_custody.absorb(custody)?;
            }
            let source_binding = acyclic_harness::filesystem::restore_recursive_fork_source(
                self.filesystem.clone(),
                &self.stream,
                (
                    parent.into_reducer(),
                    Some(PreparationIo::from_native_custody(io.into_custody()?)),
                ),
                &verifier,
                signed,
                original_caller_project,
                &stream_provider.0,
                request,
                history_limits,
                source_read.as_ref().map(|pair| &pair.0),
                resolver,
                cancel,
            )
            .await?;
            source
                .validate_snapshot(
                    frozen,
                    &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                        work,
                        cancellation: work.cancellation(),
                    },
                )
                .await
                .map_err(crate::pi_fork_host::native_lazy_error)?;
            let request = source_binding.request;
            let preparer = source_binding.preparer;
            let mut source_read_custody = source_read.map(|pair| pair.1);
            let input_read = {
                let mut io = acyclic_harness::store::HistoryWork::new(
                    PreparationIo::from_native_work(work)?,
                );
                let value = self.owner.policy().journal.directory_read_capability(
                    crate::pi_root_inputs::ROOT_INPUT_DIRECTORY,
                    &mut Some(&mut io),
                )?;
                (value, io.into_custody()?)
            };
            if !context.scope().grants().contains(&input_read.0) {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi caller lacks immutable input-directory READ",
                });
            }
            let mut launch_reference = crate::pi_root_provider::stage_pi_input(
                &self.owner,
                &self.filesystem,
                derived(original, "native-input", &self.work)?,
                "native-input",
                &commit.launch,
                &self.work,
            )
            .await?;
            #[derive(Serialize)]
            struct Command<'a> {
                operation_id: OperationId,
                kind: &'a str,
                payload: &'a FileRef,
            }
            let input = crate::pi_root_provider::guarded_pi_value(
                &PiTaskInput::<_, crate::pi_root::PiRootIntent, _> {
                    binding: &commit.child,
                    execution: PiTaskExecution::Child {
                        launch: Command {
                            operation_id: operations.native,
                            kind: NATIVE_LAUNCH_TASK_COMMAND_KIND,
                            payload: &launch_reference.0,
                        },
                    },
                },
                self.owner.policy().maximum_payload_bytes,
                work,
            )?;
            let admission_io = PreparationIo::from_native_work(work)?;
            acyclic_harness::store::HistoryIo::absorb_native_custody(&mut &admission_io, input.1)?;
            let (child, retained) = self.runtime.runtime.harness().registered_admission(
                operations.child,
                "acyclic.pi",
                "1",
                input.0,
                Some((owner.task_id(), context.scope())),
                None,
                Some(&admission_io),
            )?;
            let mut child = (child, admission_io.take_native_custody(retained)?);
            let mut child_grants = Capabilities::copy(
                child
                    .0
                    .grants
                    .iter()
                    .filter(|grant| {
                        !grant.starts_with("volume:")
                            && !grant.starts_with("directory:")
                            && !grant.starts_with("file:")
                    })
                    .chain(std::iter::once(input_read.0.as_str())),
                work,
            )?;
            child.0.grants = child_grants.0;
            child.0.resource_proofs.clear();
            child.1.absorb(&mut child_grants.1)?;
            let bytes = crate::pi_root_provider::guarded_pi_bytes(
                &request,
                self.owner.policy().maximum_payload_bytes,
                work,
            )?;
            let fork_path = work.display(&format_args!("pi-fork/{original}/fork-input.json"))?;
            let mut fork_input = {
                let publication_io = PreparationIo::from_native_work(&self.work)?;
                (context)
                    .stage_file_once(
                        derived(original, "fork-input", &self.work)?,
                        &fork_path.0,
                        &bytes.0,
                        "application/json",
                        "Pi original fork input",
                        Some(&publication_io),
                    )
                    .await
                    .and_then(|(file, retained)| {
                        Ok((file, publication_io.take_native_custody(retained)?))
                    })
            }?;
            let command_kind = work.display(&FORK_PREPARE_TASK_COMMAND_KIND)?;
            let command = WorkflowCommand {
                operation_id: operations.fork,
                kind: command_kind.0,
                payload: fork_input.0,
            };
            let mut checkpoint_copy = self.decode_input_value::<FileRef>(
                &checkpoint.0,
                acyclic_harness::JsonValueSchema::None,
                FORK_PLAN_DIAGNOSTIC_LABEL_BYTES,
            )?;
            drop(checkpoint);
            let io = PreparationIo::from_native_work(work)?;
            let prepared_request = acyclic_harness::filesystem::fork_preparation_request(
                &preparer,
                &request,
                task_id_for_admission(operations.preparation),
                child.0,
                command,
                limits,
                Some(checkpoint_copy.0),
                Vec::new(),
                Some(&io),
            )?;
            let mut prepared_request = (
                prepared_request,
                acyclic_harness::store::HistoryWork::new(io).into_custody()?,
            );
            let input = crate::pi_root_provider::guarded_pi_value(
                &prepared_request.0,
                self.owner.policy().maximum_payload_bytes,
                work,
            )?;
            let admission_io = PreparationIo::from_native_work(work)?;
            acyclic_harness::store::HistoryIo::absorb_native_custody(&mut &admission_io, input.1)?;
            let (prepared_admission, retained) =
                self.runtime.runtime.harness().registered_admission(
                    operations.preparation,
                    "acyclic.fork_preparation",
                    "1",
                    input.0,
                    Some((owner.task_id(), context.scope())),
                    None,
                    Some(&admission_io),
                )?;
            let mut prepared_admission = (
                prepared_admission,
                admission_io.take_native_custody(retained)?,
            );
            request_custody.absorb(&mut prepared_request.1)?;
            request_custody.absorb(&mut prepared_admission.1)?;
            request_custody.absorb(&mut child.1)?;
            request_custody.absorb(&mut checkpoint_copy.1)?;
            let request = prepared_request.0;
            let preparation = prepared_admission.0;
            cancel.check().map_err(|_| Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                custody: None,
            })?;
            (owner)
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(&self.work)?),
                )
                .await?;
            drop(launch_reference.0);
            let mut plan = PiForkPlan {
                original,
                commit,
                preparation,
                request,
                frozen,
                _input_allocations: Some(request_custody),
            };
            let allocations = plan
                ._input_allocations
                .as_mut()
                .ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?;
            for part in [&mut fork_input.1, &mut launch_reference.1]
                .into_iter()
                .chain(source_read_custody.as_mut())
            {
                allocations.absorb(part)?;
            }
            let mut publication = crate::pi_root_provider::stage_pi_input(
                &self.owner,
                &self.filesystem,
                original,
                "fork-plan",
                &plan,
                &self.work,
            )
            .await?;
            drop(publication.0);
            plan._input_allocations
                .as_mut()
                .ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?
                .absorb(&mut publication.1)?;
            self.prepare_native_source(owner, &plan, source).await?;
            Ok(plan)
        }
    }
}
