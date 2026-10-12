//! One native view and process preparation path.
use super::*;

impl PiNativeCommandHost {
    pub(super) async fn prepare_effect(
        &self,
        shared: Arc<NativeTaskContext>,
        input: &PiTaskInput,
        owner: Arc<TaskJournalOwner<LocalStream>>,
        command: &WorkflowCommand,
        mut retained_intent: (PiNativeLaunchIntent, NativeAllocation),
        existing_only: bool,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<BoundEffect>> {
        let intent = &mut retained_intent.0;
        let context = shared.context();
        self.hydrate_child(
            &shared,
            &owner,
            &context.owned_task_fence()?,
            work.cancellation(),
            work,
        )
        .await?;
        let prepared = self.prepared(owner.task_id()).ok_or_else(|| {
            unauthorized(
                "Pi native first dispatch requires actual reopened sealed preparation capabilities",
            )
        })?;
        validate_child_binding(&prepared.binding, &input.binding)?;
        intent.validate(work)?;
        if intent
            .invocation
            .environment
            .keys()
            .any(|key| pi_caller::is_environment_key(key))
        {
            return Err(unauthorized(
                "Pi retained invocation supplied a replacement caller credential",
            ));
        }
        let root_admission = self
            .runtime
            .runtime
            .task_host()
            .observe_admission_with_native_work(self.protected.policy().root_task, work)
            .await?;
        let limits = pi_native::capture_limits(
            root_admission.0.limits,
            self.protected.policy().maximum_payload_bytes,
        )?;
        if intent.invocation.timeout_ms != limits.timeout_ms
            || intent.invocation.control_timeout_ms != limits.control_timeout_ms
            || intent.invocation.cancellation_poll_ms != limits.cancellation_poll_ms
            || intent.invocation.maximum_output_bytes != limits.maximum_output_bytes
            || intent.invocation.maximum_result_bytes != limits.maximum_result_bytes
        {
            return Err(unauthorized(
                "Pi native capture differs from its original protected root bounds",
            ));
        }
        let maximum = self
            .protected
            .policy()
            .maximum_payload_bytes
            .min(root_admission.0.limits.file_bytes);
        let root_bytes =
            crate::pi_root_provider::canonical_pi_value(&root_admission.0.input, maximum, work)?;
        let root = crate::pi_root_provider::guarded_pi_decode::<PiTaskInput>(
            &root_bytes.0,
            maximum,
            self.protected.policy().maximum_payload_bytes,
            acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None,
            crate::pi_task::PI_TASK_INPUT_DIAGNOSTIC_LABEL_BYTES,
            work,
        )?;
        let root_session = &root.0.binding.native_session_id;
        let caller_environment =
            pi_caller::issued_environment(&self.protected, &root_session, &prepared.binding, work)?;
        let mut invocation = self.protected.native_runtime()?.bind_invocation(
            intent,
            &caller_environment.0,
            work,
        )?;
        retained_intent.1.absorb(&mut invocation.1)?;
        intent.invocation = invocation.0;
        drop(caller_environment);
        let entry = self.registered_entry(owner.task_id())?;
        let mut handles = entry.handles.lock().await;
        if let Some(bound) = handles.effect.as_ref()
            && bound.staged.is_some()
        {
            return Ok(bound.clone());
        }
        if handles.preparation_owner.is_none() {
            handles.preparation_owner =
                Some((owner.clone(), Arc::clone(&shared), command.operation_id));
        }
        let view = if let Some(view) = self.live_view(owner.task_id()) {
            view
        } else {
            if handles.root.as_ref().is_none_or(|root| root.view.is_none()) {
                let cancellation = work.cancellation();
                let budget = crate::pi_root_provider::allocation_budget();
                let _recovered = if existing_only {
                    Some(
                        self.original_allocation(&owner, command.operation_id, work)
                            .await?,
                    )
                } else {
                    None
                };
                let scratch_context = Arc::clone(&shared)
                    .with_output_volume_with_native_work(&prepared.binding.scratch.volume, work)?;
                if handles.session.is_none() {
                    handles.session = Some(
                        intent
                            .publish_session(scratch_context.context(), command.operation_id, work)
                            .await?,
                    );
                }
                let generation = generation(
                    &handles
                        .session
                        .as_ref()
                        .ok_or_else(|| {
                            unauthorized("Pi session has no original publication custody")
                        })?
                        .0,
                )?;
                let mut conversation = self.protected.conversation_authority_with_native_work(
                    owner.task_id(),
                    input.binding.agent_id,
                    context.scope().grants(),
                    work,
                )?;
                let mut verifier = {
                    let mut io = acyclic_harness::store::history::native_work_io(work)?;
                    let value = conversation.0.1.verifier(&mut Some(&mut io))?;
                    (value, io.into_custody())
                };
                conversation.1.absorb(&mut verifier.1)?;
                let (_, _, scope) = conversation.0;
                let authority = (verifier.0, scope, conversation.1);
                let namespace = NativeVolumeNamespace::MaterializedDirectory;
                let scratch_workspace = accept(
                    self.filesystem
                        .open_measured(
                            &workspace_ref(
                                prepared.binding.scratch.volume.provider().clone(),
                                &prepared.binding.scratch.volume.storage_name()?,
                            )?,
                            remaining(work)?,
                            cancellation,
                        )
                        .await,
                    work,
                )?;
                let scratch_generation = GenerationId::new(Digest::from_bytes(
                    generation
                        .as_resource()
                        .key()
                        .try_into()
                        .map_err(|_| unauthorized("Pi scratch generation is not canonical"))?,
                ));
                let scratch_permit = handles
                    .scratch_gate
                    .reserve(
                        &owner,
                        &scratch_workspace,
                        &prepared.binding.scratch.volume,
                        scratch_generation,
                        command.operation_id,
                        work,
                        cancellation,
                        &entry.cleanup_cancellation,
                        existing_only,
                    )
                    .await?;
                let options = pi_native::native_view_options(
                    &self.resources,
                    root_session,
                    owner.task_id(),
                    2,
                    view_volume_share(budget, 2)?,
                    work,
                )?;
                let scratch_binding = BorrowedNativeVolumeBinding {
                    host: &self.filesystem,
                    volume: &prepared.binding.scratch.volume,
                    generation: &generation,
                    path: "scratch",
                    writable: true,
                    verifier: &authority.0,
                    scope: &authority.1,
                    publication: scratch_permit,
                    namespace: &namespace,
                };
                if handles.root.is_none() {
                    let session = &handles
                        .session
                        .as_ref()
                        .ok_or_else(|| {
                            unauthorized("Pi session has no original publication custody")
                        })?
                        .0;
                    handles.root = Some(
                        pi_native::native_root(
                            &self.resources,
                            root_session,
                            &self.protected,
                            &owner,
                            &shared,
                            &prepared.binding,
                            &options.0,
                            intent,
                            session,
                            std::slice::from_ref(&scratch_binding),
                            work,
                        )
                        .await?,
                    );
                }
                {
                    let LiveHandles { root, session, .. } = &mut *handles;
                    let session = &session
                        .as_ref()
                        .ok_or_else(|| {
                            unauthorized("Pi session has no original publication custody")
                        })?
                        .0;
                    root.as_mut()
                        .ok_or(Error::Indeterminate(command.operation_id))?
                        .create_directories_into_with_work(
                            &owner,
                            &shared,
                            &prepared.binding,
                            &options.0,
                            intent,
                            session,
                            std::slice::from_ref(&scratch_binding),
                            work,
                        )
                        .await?;
                }
                if handles.project_work.is_none() {
                    handles.project_work =
                        Some(work.native_volume_work(options.0.work_per_volume)?);
                }
                let (root_key, project) = prepared
                    .binding
                    .project_volumes
                    .iter()
                    .next()
                    .ok_or_else(|| unauthorized("Pi actual fork has no selected project"))?;
                let project_generation = GenerationRef::new_with_native_work(
                    project.provider(),
                    prepared
                        .native_lazy
                        .authored_generation
                        .digest()
                        .into_bytes(),
                    None,
                    work,
                )?;
                let write_capability = {
                    let mut io = acyclic_harness::store::history::native_work_io(work)?;
                    let value = project.capability(VolumeOperation::Write, &mut Some(&mut io))?;
                    (value, io.into_custody())
                };
                let writable = context.scope().grants().contains(&write_capability.0);
                let project_permit = handles
                    .project_gate
                    .reserve(
                        &owner,
                        prepared.workspace.workspace(),
                        project,
                        prepared.native_lazy.authored_generation,
                        command.operation_id,
                        work,
                        cancellation,
                        &entry.cleanup_cancellation,
                        existing_only,
                    )
                    .await?;
                let project_capture = {
                    let LiveHandles {
                        root,
                        project: retained_mount,
                        project_allocation,
                        project_startup_owner,
                        project_work,
                        ..
                    } = &mut *handles;
                    let root = root
                        .as_ref()
                        .ok_or(Error::Indeterminate(command.operation_id))?;
                    root.mount_project_with_work(
                        &owner,
                        &shared,
                        &prepared.binding,
                        root_key,
                        &prepared.workspace,
                        self.filesystem.clone(),
                        &project_generation.0,
                        &authority,
                        project_permit,
                        writable,
                        retained_mount,
                        project_allocation,
                        project_startup_owner,
                        project_work
                            .as_ref()
                            .ok_or(Error::Indeterminate(command.operation_id))?,
                        work,
                        cancellation,
                    )
                    .await?
                };
                let route = project_capture
                    .0
                    .path
                    .strip_prefix(&options.0.root)
                    .map_err(|_| Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Unauthorized,
                        reason: "Pi mounted project escaped its original native root",
                    })?
                    .to_str()
                    .ok_or(Error::NativeBoundary {
                        code: acyclic_harness::wire::ErrorCode::Invalid,
                        reason: "Pi native project route is not UTF-8",
                    })?;
                let bindings = [
                    scratch_binding,
                    BorrowedNativeVolumeBinding {
                        host: &self.filesystem,
                        volume: project,
                        generation: &project_generation.0,
                        path: route,
                        writable,
                        verifier: &authority.0,
                        scope: &authority.1,
                        publication: project_permit,
                        namespace: &project_capture.0.namespace,
                    },
                ];
                let scratch_guard = shared
                    .volume_callback_guard_with_native_work(&owner, work)
                    .await?;
                let guards = [scratch_guard, project_capture.2];
                let existing_lazy = [None, Some(&project_capture.1)];
                handles
                    .root
                    .as_mut()
                    .ok_or(Error::Indeterminate(command.operation_id))?
                    .prepare(&owner, &options.0, &bindings, &guards, &existing_lazy, work)
                    .await?;
            }
            Arc::clone(
                handles
                    .root
                    .as_ref()
                    .and_then(|root| root.view.as_ref())
                    .ok_or(Error::Indeterminate(command.operation_id))?,
            )
        };
        let original_view_owner = handles
            .preparation_owner
            .as_ref()
            .ok_or(Error::Indeterminate(command.operation_id))?
            .0
            .clone();
        let working_view = view
            .task_working_view_with_native_work(&original_view_owner, work)?
            .await?;
        shared
            .bind_working_view_with_native_work(working_view, work)
            .await?;
        *entry
            .view
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&view));
        let result_volume = &prepared
            .binding
            .results
            .as_ref()
            .ok_or_else(|| unauthorized("Pi result selection vanished"))?
            .volume;
        let result_context =
            Arc::clone(&shared).with_output_volume_with_native_work(result_volume, work)?;
        let request = intent.request_with_work(
            self.protected.native_runtime()?,
            &prepared.binding,
            view.manifest(),
            work,
        )?;
        let staged = pi_native::stage_command(
            result_context.context(),
            command.operation_id,
            &request.0,
            work,
        )
        .await?;
        let journal = self
            .journal(&owner, context, input, command.operation_id, work)
            .await?;
        let approval = pi_native::approve_command(
            &self.protected,
            &owner,
            context,
            &prepared.binding,
            &intent.invocation,
            view.manifest(),
            command.operation_id,
            &journal,
            work,
        )
        .await?;
        let authority = self.authority(context, input, journal.clone(), work)?;
        let provider = NativeProcessProvider::new(
            owner.clone(),
            command.operation_id,
            &staged.1,
            view,
            approval,
            authority,
            work,
        )?
        .await?
        .into_arc_with_native_work(work)?;
        let effects = self.effects(context, input, &provider, work)?;
        let bound = Arc::new(BoundEffect {
            provider,
            work: work.clone(),
            effects,
            owner,
            journal,
            context: shared,
            command: command.operation_id,
            staged: Some(staged.0),
            plan: (staged.1, staged.2),
        });
        handles.effect = Some(bound.clone());
        Ok(bound)
    }
}
