//! Preparation of the original Pi fork.
use super::*;

impl<'a> PiForkCoordinator<'a> {
    #[allow(
        clippy::too_many_arguments,
        reason = "actual original owner, canonical parent, and native cut are independent authorities"
    )]
    pub(crate) async fn begin(
        &self,
        shared: &Arc<acyclic_harness::runtime::TaskContext>,
        owner: &Arc<TaskJournalOwner<LocalStream>>,
        fence: &LeaseFence,
        parent: StreamAggregate<LocalStream>,
        source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef,
        mut commit: RetainedSpawnCommit,
        limits: ForkPreparationLimits,
        budget: WorkBudget,
        cancel: CancellationToken,
    ) -> Result<PiForkPlan> {
        let context = shared.as_ref();
        let work = &self.work;
        let admission = self.authenticate(context, owner, fence).await?;
        let caller_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
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
        if let Some(plan) = self.load_plan(context, original).await? {
            self.validate_plan(&plan, owner.task_id())?;
            if plan.commit != commit {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi SDK retry changed its immutable spawn commit",
                });
            }
            self.prepare_native_source(owner, &plan, source, &cancel)
                .await?;
            return Ok(plan);
        }
        let authority = self.owner.conversation_authority_with_native_work(
            owner.task_id(),
            caller.binding.agent_id,
            context.scope().grants(),
            work,
        )?;
        let verifier = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
            let value = authority.0.1.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
        };
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
        verifier.0.verify_audience(
            parent.reducer().authority(),
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)),
        )?;
        self.selected_graph(context, owner, commit.source_task)
            .await?;
        let source_admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(commit.source_task, &self.work)
            .await?;
        let source_input: (PiTaskInput, acyclic_fs::NativeAllocation) = self.decode_input_value(
            &source_admission.0.input,
            acyclic_harness::JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
        )?;
        let source_input = &source_input.0;
        let mut selected_project = None;
        for (key, volume) in &source_input.binding.project_volumes {
            let name = {
                let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
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
        let generation = acyclic_harness::resources::GenerationRef::new_with_native_work(
            project.provider(),
            frozen.authored_generation.digest().as_bytes(),
            None,
            work,
        )?;
        let scratch = &caller.binding.scratch.volume;
        let private_generation = self
            .native_generation(context, owner, scratch, work)
            .await?;
        let mut source_snapshot = self.native_source_snapshot(
            owner.task_id(),
            project,
            &generation.0,
            scratch,
            &private_generation.0,
            work,
        )?;
        let original_caller_project = caller
            .binding
            .project_volumes
            .get(root_key)
            .or_else(|| caller.binding.project_volumes.values().next())
            .ok_or_else(|| Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi caller has no actual project binding",
            })?;
        let caller_generation = if original_caller_project == project {
            acyclic_harness::resources::GenerationRef::new_with_native_work(
                project.provider(),
                frozen.authored_generation.digest().as_bytes(),
                None,
                work,
            )?
        } else {
            self.native_generation(context, owner, original_caller_project, work)
                .await?
        };
        let caller_snapshot = self.native_source_snapshot(
            owner.task_id(),
            original_caller_project,
            &caller_generation.0,
            scratch,
            &private_generation.0,
            work,
        )?;
        let source_read = if commit.source_task != owner.task_id() {
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
        } else {
            None
        };
        if commit.child.project_volumes.len() != 1 {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "Pi fork requires its one actually selected child project",
            });
        }
        let child_project =
            commit
                .child
                .project_volumes
                .get(root_key)
                .ok_or_else(|| Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi child changed its selected root key",
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
        let mut child_volume_io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
        let child_project = child_project.copy(&mut Some(&mut child_volume_io))?;
        let child_private = commit
            .child
            .scratch
            .volume
            .copy(&mut Some(&mut child_volume_io))?;
        let child_result = results.volume.copy(&mut Some(&mut child_volume_io))?;
        let _child_volume_custody = child_volume_io.into_custody()?;
        let child_id = work.display(&commit.child.task_id)?;
        #[derive(Serialize)]
        struct Arguments<'a> {
            id: AgentId,
            msg: &'a str,
            source: Option<TaskId>,
        }
        let arguments = self.native_copy::<acyclic_harness::filesystem::ForkToolArguments, _>(
            &Arguments {
                id: operations.agent,
                msg: &commit.prompt,
                source: Some(commit.source_task),
            },
            acyclic_harness::JsonValueSchema::None,
            work,
        )?;
        let history_limits = HistoryReadLimits {
            maximum_events: limits.work.min(u64::from(u32::MAX)) as u32,
            maximum_bytes: limits.bytes,
        };
        let config = RecursiveForkConfig {
            child: Authority {
                kind: AggregateKind::Conversation,
                id: child_id.0,
            },
            child_project,
            child_private,
            child_result_volume: Some(child_result),
            limits: context.scope().limits(),
            inherited_messages: context.scope().limits().context_messages as u64,
            inherited_bytes: limits.bytes,
            inherited_references: limits.resources,
            history_limits,
        };
        let checkpoint_snapshot = {
            let mut io = acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
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
        let checkpoint = (context)
            .stage_file_once_with_native_work(
                derived(original, "source-checkpoint", &self.work)?,
                &checkpoint_path.0,
                &checkpoint_bytes.0,
                "application/json",
                "Pi original caller cut",
                &self.work,
            )
            .await?;
        let resolver = acyclic_harness::runtime::content_residency_verifier(
            Arc::clone(shared),
            Arc::clone(owner),
            Some(&acyclic_harness::filesystem::PreparationIo::from_native_work(work)?),
        )?;
        let stream_provider = acyclic_harness::resources::ProviderRef::new_with_native_work(
            "local", "stream", "1", work,
        )?;
        let source_binding = acyclic_harness::filesystem::recursive_fork_source_with_native_work(
            self.filesystem.clone(),
            &self.stream,
            parent.into_reducer(),
            &verifier.0,
            signed,
            context,
            &caller_snapshot.0,
            &stream_provider.0,
            operations.fork,
            &arguments.0,
            &config,
            source_read.as_ref().map(|pair| &pair.0),
            resolver,
            &cancel,
            work,
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
        let request = source_binding.0.request;
        let mut request_custody = source_binding.1;
        let preparer = source_binding.0.preparer;
        let mut source_read_custody = source_read.map(|pair| pair.1);
        let snapshot = source_snapshot.0;
        request_custody.absorb(&mut source_snapshot.1)?;
        let input_read = {
            let mut io = acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?);
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
        let mut child = self
            .runtime
            .runtime
            .harness()
            .registered_child_admission_with_native_work(
                operations.child,
                "acyclic.pi",
                "1",
                input,
                owner.task_id(),
                context.scope(),
                work,
            )?;
        let mut child_grants = Capabilities::new_with_native_work(
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
        let mut fork_input = (context)
            .stage_file_once_with_native_work(
                derived(original, "fork-input", &self.work)?,
                &fork_path.0,
                &bytes.0,
                "application/json",
                "Pi original fork input",
                &self.work,
            )
            .await?;
        let command_kind = work.display(&FORK_PREPARE_TASK_COMMAND_KIND)?;
        let command = WorkflowCommand {
            operation_id: operations.fork,
            kind: command_kind.0,
            payload: fork_input.0,
        };
        let project_creation = preparer.project_creation_operation(&request)?;
        let mut checkpoint_copy = self.native_copy::<FileRef, _>(
            &checkpoint.0,
            acyclic_harness::JsonValueSchema::None,
            work,
        )?;
        drop(checkpoint);
        let io = acyclic_harness::filesystem::PreparationIo::from_native_work(work)?;
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
        let acyclic_harness::filesystem::PreparationIo::Native { custody, .. } = io else {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "fork preparation requires its original native ledger",
            });
        };
        let custody = custody
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut prepared_request = (prepared_request, custody);
        let input = crate::pi_root_provider::guarded_pi_value(
            &prepared_request.0,
            self.owner.policy().maximum_payload_bytes,
            work,
        )?;
        let mut prepared_admission = self
            .runtime
            .runtime
            .harness()
            .registered_child_admission_with_native_work(
                operations.preparation,
                "acyclic.fork_preparation",
                "1",
                input,
                owner.task_id(),
                context.scope(),
                work,
            )?;
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
            .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(&self.work)?)))
            .await?;
        drop(launch_reference.0);
        let mut plan = PiForkPlan {
            original,
            commit,
            preparation,
            request,
            source: snapshot,
            frozen,
            project_creation,
            _input_allocations: Some(request_custody),
        };
        let allocations = plan._input_allocations.as_mut().expect("new plan custody");
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
            .expect("new plan custody")
            .absorb(&mut publication.1)?;
        self.prepare_native_source(owner, &plan, source, &cancel)
            .await?;
        Ok(plan)
    }
}
