//! Original allocation, scratch and conversation observations.
use super::*;

/// Reopens historical successful creation after the root released its lease.
#[allow(
    clippy::too_many_arguments,
    reason = "original archived owner, producers and finite observation"
)]
pub(crate) async fn observe_root_allocation(
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    preparation: PiRootPreparation,
    work: &NativeMaterializationWork,
) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
    let observer = root_provider(
        owner,
        resources,
        filesystem,
        stream,
        preparation,
        None,
        work.clone(),
    )?;
    let attempt =
        EffectAttemptId::from_bytes(observer.effect_id_with_native_work(work)?.into_bytes());
    observer
        .observe_allocation_with_native_work(attempt, work)
        .await
}

/// Root output is its genuinely created private scratch, never the protected bootstrap journal.
#[allow(
    clippy::too_many_arguments,
    reason = "actual caller, original protected creator and bounded producers"
)]
pub(crate) async fn bind_root_output_context_with_native_work(
    context: Arc<acyclic_harness::runtime::NativeTaskContext>,
    current_owner: &Arc<TaskJournalOwner<LocalStream>>,
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    binding: &crate::PiTaskBinding,
    root: &PiRootIntent,
    work: &NativeMaterializationWork,
) -> Result<Arc<acyclic_harness::runtime::NativeTaskContext>> {
    work.admit(WorkCounters::UNCHARGED)?;
    let invalid = || Error::NativeBoundary {
        code: ErrorCode::Unauthorized,
        reason: "root output context differs from its original admitted task scope",
    };
    if context.context().durable_task_id() != Some(current_owner.task_id())
        || current_owner.task_id() != owner.policy().root_task
        || context.context().scope().grants() != &owner.policy().grants
        || binding.task_id != owner.policy().root_task
    {
        return Err(invalid());
    }
    current_owner.require_volume_grant(
        &binding.scratch.volume,
        VolumeOperation::Write,
        &mut Some(&mut native_work_io(work)?),
    )?;
    current_owner
        .verify(false, &mut Some(&mut native_work_io(work)?))
        .await?;
    let preparation = load_original_preparation_with_native_work(
        &owner,
        &filesystem,
        binding,
        root,
        Arc::clone(&context),
        work,
    )
    .await?;
    let observer = root_provider(
        owner,
        resources,
        Arc::clone(&filesystem),
        stream,
        preparation,
        None,
        work.clone(),
    )?;
    let effect = observer.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    let (scratch, operation, _scratch_custody) = observer
        .prove_scratch_allocation_with_native_work(attempt, work)
        .await?
        .ok_or_else(invalid)?;
    let binding = &observer.preparation.request.binding;
    let (authority, custody) = observer.owner.conversation_authority_with_native_work(
        binding.task_id,
        binding.agent_id,
        &observer.owner.policy().grants,
        work,
    )?;
    crate::pi_conversation::bind_task_conversation(
        &observer.stream,
        authority,
        custody,
        current_owner,
        &binding.scratch.volume,
        operation,
        observer.owner.policy().limits,
        work,
    )
    .await?;
    if current_owner.authenticated_binding_agent_with_native_work(work)? != Some(binding.agent_id) {
        return Err(invalid());
    }
    let generation = observer
        .creation_with_native_work(
            effect,
            attempt,
            "scratch",
            &binding.scratch.volume,
            VolumeConfig::portable(Lifecycle::Durable),
            work,
        )
        .await?
        .ok_or_else(invalid)?;
    let actual = observer.scratch_creation_with_native_work(&generation, work)?;
    if actual.0.workspace_id != scratch.workspace_id || actual.0.generation != scratch.generation {
        return Err(invalid());
    }
    let authority = observer.owner.conversation_authority_with_native_work(
        binding.task_id,
        binding.agent_id,
        &observer.owner.policy().grants,
        work,
    )?;
    let verifier = {
        let mut io = native_work_io(work)?;
        let value = authority.0.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    let source = filesystem
        .root_output_source(
            current_owner,
            &binding.scratch.volume,
            operation,
            &generation,
            &verifier.0,
            &authority.0.2,
            work,
        )
        .await?;
    context
        .bind_root_output_context_with_native_work(source, work)
        .await?;
    Ok(context)
}
impl PiRootProvider {
    pub(crate) async fn observe_allocation_with_native_work(
        &self,
        attempt: EffectAttemptId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        self.authenticate_with_native_work(attempt, true, work)
            .await?;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(mut reference) = self
            .original_artifact_with_native_work(effect, attempt, "result", work)
            .await?
        else {
            return Ok(None);
        };
        let mut recorded = self
            .read_with_native_work::<PiRootAllocation>(&reference.0, work)
            .await?;
        let Some(actual) = self
            .observe_resources_with_native_work(effect, attempt, work)
            .await?
        else {
            return Ok(None);
        };
        if !same_allocation(&recorded.0, &actual.0)
            || !self
                .observe_conversation_with_native_work(effect, attempt, work)
                .await?
        {
            return Err(Self::native_failure(
                "successful root output differs from original resources or binding",
            ));
        }
        reference.1.absorb(&mut recorded.1)?;
        Ok(Some(((reference.0, recorded.0), reference.1)))
    }

    pub(crate) async fn prove_scratch_allocation_with_native_work(
        &self,
        attempt: EffectAttemptId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(PiScratchCreation, acyclic_fs::OperationId, NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        self.authenticate_with_native_work(attempt, true, work)
            .await?;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(output) = self
            .original_artifact_with_native_work(effect, attempt, "result", work)
            .await?
        else {
            return Ok(None);
        };
        let recorded = self
            .read_with_native_work::<PiRootAllocation>(&output.0, work)
            .await?;
        let binding = &self.preparation.request.binding;
        recorded.0.validate(binding)?;
        if binding.scratch.volume.class()
            != acyclic_harness::conversation::VolumeClass::AgentPrivate
            || binding.scratch.volume.owner()
                != &acyclic_harness::conversation::VolumeOwner::Agent(binding.agent_id)
        {
            return Err(Self::native_failure(
                "root scratch is not its original agent-private output resource",
            ));
        }
        let Some(generation) = self
            .creation_with_native_work(
                effect,
                attempt,
                "scratch",
                &binding.scratch.volume,
                VolumeConfig::portable(Lifecycle::Durable),
                work,
            )
            .await?
        else {
            return Ok(None);
        };
        let scratch = self.scratch_creation_with_native_work(&generation, work)?;
        if recorded.0.scratch != scratch.0
            || !self
                .observe_conversation_with_native_work(effect, attempt, work)
                .await?
        {
            return Err(Self::native_failure(
                "root scratch output differs from original creation or binding",
            ));
        }
        let operation =
            self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
        Ok(Some((scratch.0, operation, scratch.1)))
    }

    pub(super) async fn creation_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        resource: &str,
        volume: &VolumeRef,
        config: VolumeConfig,
        work: &NativeMaterializationWork,
    ) -> Result<Option<LocalGeneration>> {
        let operation =
            self.creation_operation_with_native_work(effect, attempt, resource, work)?;
        let name = {
            let mut io = native_work_io(work)?;
            let value = volume.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        self.fs
            .workspace_for_creation_operation_with_native_work(
                &name.0,
                config,
                operation,
                work,
                work.cancellation(),
            )
            .await
            .map_err(|(source, custody)| Error::NativeWorkspace { source, custody })
    }

    pub(super) fn scratch_creation_with_native_work(
        &self,
        generation: &LocalGeneration,
        work: &NativeMaterializationWork,
    ) -> Result<(PiScratchCreation, NativeAllocation)> {
        let digest = generation.id().digest().into_bytes();
        let volume = &self.preparation.request.binding.scratch.volume;
        let (reference, custody) =
            GenerationRef::new_with_native_work(volume.provider(), &digest, None, work)?;
        let scratch = (
            PiScratchCreation {
                workspace_id: generation.workspace().id().into_bytes(),
                generation: reference,
            },
            custody,
        );
        scratch.0.validate(volume)?;
        Ok(scratch)
    }

    pub(super) async fn observe_resources_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(PiRootAllocation, NativeAllocation)>> {
        let mut roots = (
            BTreeMap::new(),
            work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?,
        );
        if let PiRootIntent::Project { paths, .. } = &self.preparation.request.root {
            let Some(reference) = self
                .original_artifact_with_native_work(effect, attempt, "attachments", work)
                .await?
            else {
                return Ok(None);
            };
            let inputs = self
                .read_with_native_work::<BTreeMap<String, (SourceReference, [u8; 16])>>(
                    &reference.0,
                    work,
                )
                .await?;
            if !inputs.0.keys().eq(paths.keys()) {
                return Err(Self::native_failure(
                    "root attachment selection differs from original admission",
                ));
            }
            for (key, path) in paths {
                let volume = &self.preparation.request.binding.project_volumes[key];
                let input = &inputs.0[key];
                self.validate_attachment(input)?;
                let Some(generation) = self
                    .creation_with_native_work(
                        effect,
                        attempt,
                        key,
                        volume,
                        VolumeConfig::native(Lifecycle::Durable),
                        work,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                let Some(state) = self
                    .store
                    .load_lazy_workspace_with_native_work(generation.workspace().id(), work)
                    .await?
                else {
                    return Ok(None);
                };
                state
                    .as_state()
                    .validate_root_binding(generation.workspace().id(), input.0)?;
                let observed = work.root_identity(Path::new(path))?;
                if observed.to_bytes() != input.1 {
                    return Err(Self::native_failure("root native identity was substituted"));
                }
                let owned_key = work.display(key)?;
                let mut slot = work.claim(
                    std::mem::size_of::<(String, RootCreation)>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                roots.1.absorb(&mut slot)?;
                let (value, mut _custody) = owned_key;
                roots.1.absorb(&mut _custody)?;
                roots.0.insert(
                    value,
                    RootCreation {
                        repository_workspace_id: generation.workspace().id().into_bytes(),
                        source_identity: input.0.identity,
                        source_epoch: input.0.epoch,
                        native_root_identity: input.1,
                    },
                );
            }
        }
        let Some(generation) = self
            .creation_with_native_work(
                effect,
                attempt,
                "scratch",
                &self.preparation.request.binding.scratch.volume,
                VolumeConfig::portable(Lifecycle::Durable),
                work,
            )
            .await?
        else {
            return Ok(None);
        };
        let scratch = self.scratch_creation_with_native_work(&generation, work)?;
        let (scratch, mut _custody) = scratch;
        roots.1.absorb(&mut _custody)?;
        let allocation = (
            PiRootAllocation {
                roots: roots.0,
                scratch,
            },
            roots.1,
        );
        allocation.0.validate(&self.preparation.request.binding)?;
        Ok(Some(allocation))
    }

    pub(super) async fn observe_result_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        state: &EffectState,
        stage_missing: bool,
        work: &NativeMaterializationWork,
    ) -> Result<Option<(EffectObservation, NativeAllocation)>> {
        let Some(actual) = self
            .observe_resources_with_native_work(effect, attempt, work)
            .await?
        else {
            return Ok(None);
        };
        if !self
            .observe_conversation_with_native_work(effect, attempt, work)
            .await?
        {
            return Ok(None);
        }
        let result = match self
            .original_artifact_with_native_work(effect, attempt, "result", work)
            .await?
        {
            Some(reference) => {
                let recorded = self
                    .read_with_native_work::<PiRootAllocation>(&reference.0, work)
                    .await?;
                if !same_allocation(&recorded.0, &actual.0) {
                    return Err(Self::native_failure(
                        "root output substitutes original filesystem resources",
                    ));
                }
                reference
            }
            None if stage_missing => {
                self.stage_with_native_work(effect, attempt, "result", &actual.0, work)
                    .await?
            }
            None => return Ok(None),
        };
        let mut provider = work.display(ROOT_ALLOCATION_PROVIDER)?;
        let mut result = result;
        result.1.absorb(&mut provider.1)?;
        Ok(Some((
            EffectObservation {
                provider: provider.0,
                effect_id: effect,
                attempt_id: attempt,
                request_digest: state.request_digest,
                guarantee: state.guarantee,
                status: EffectStatus::Succeeded { result: result.0 },
            },
            result.1,
        )))
    }

    pub(super) async fn bind_conversation_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        original: &NativeMaterializationWork,
    ) -> Result<()> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request.binding.task_id,
            self.preparation.request.binding.agent_id,
            &self.owner.policy().grants,
            original,
        )?;
        let original_creation =
            self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        crate::pi_conversation::bind_task_conversation(
            &self.stream,
            authority.0,
            authority.1,
            self.execution_owner()?,
            &self.preparation.request.binding.scratch.volume,
            original_creation,
            self.owner.policy().limits,
            original,
        )
        .await
    }

    pub(super) async fn observe_conversation_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        original: &NativeMaterializationWork,
    ) -> Result<bool> {
        let authority = self.owner.conversation_authority_with_native_work(
            self.preparation.request.binding.task_id,
            self.preparation.request.binding.agent_id,
            &self.owner.policy().grants,
            original,
        )?;
        let original_creation =
            self.creation_operation_with_native_work(effect, attempt, "scratch", original)?;
        crate::pi_conversation::observe_task_conversation(
            &self.stream,
            authority.0,
            authority.1,
            &self.preparation.request.binding.scratch.volume,
            original_creation,
            self.maximum(),
            original,
        )
        .await
    }
}
