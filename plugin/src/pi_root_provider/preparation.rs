//! Original allocation, scratch and conversation observations.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

/// Root output is its genuinely created private scratch, never the protected bootstrap journal.
pub(crate) async fn bind_root_output_context(
    current_owner: &Arc<TaskJournalOwner<LocalStream>>,
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    preparation: PiRootPreparation,
    work: &NativeMaterializationWork,
) -> Result<Arc<acyclic_harness::runtime::TaskContext>> {
    let context = Arc::clone(&preparation.context);
    let binding = &preparation.request.binding;
    work.admit(WorkCounters::UNCHARGED)?;
    let invalid = || Error::NativeBoundary {
        code: ErrorCode::Unauthorized,
        reason: "root output context differs from its original admitted task scope",
    };
    if context.durable_task_id() != Some(current_owner.task_id())
        || current_owner.task_id() != owner.policy().root_task
        || context.scope().grants() != &owner.policy().grants
        || binding.task_id != owner.policy().root_task
    {
        return Err(invalid());
    }
    current_owner.require_volume_grant(
        &binding.scratch.volume,
        VolumeOperation::Write,
        &mut Some(&mut &PreparationIo::from_native_work(work)?),
    )?;
    current_owner
        .verify(
            false,
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
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
    let (_scratch, operation, _scratch_custody) = observer
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
    let verifier = {
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let value = authority.1.verifier(&mut Some(&mut io))?;
        (value, io.into_custody()?)
    };
    let io = PreparationIo::from_native_work(work)?;
    let maximum = context.scope().limits().file_bytes.min(observer.maximum());
    let output = acyclic_harness::runtime::ContentBindings {
        reader: acyclic_harness::filesystem::FilesystemContentVerifier::new(
            Arc::clone(&filesystem),
            &verifier.0,
            &authority.2,
            maximum,
            Some(&io),
        )?,
        writer: Some(
            acyclic_harness::filesystem::FilesystemContentPublisher::new(
                filesystem,
                &binding.scratch.volume,
                &verifier.0,
                &authority.2,
                maximum,
                Some(&io),
            )?,
        ),
    };
    crate::pi_conversation::bind_task_conversation(
        &observer.stream,
        (authority, custody),
        current_owner,
        &binding.scratch.volume,
        operation,
        observer.owner.policy().limits,
        work,
    )
    .await?;
    context.with_output_bindings(output).await
}
impl PiRootProvider {
    pub(crate) async fn observe_allocation_with_native_work(
        &self,
        attempt: EffectAttemptId,
        work: &NativeMaterializationWork,
    ) -> Result<Option<((FileRef, PiRootAllocation), NativeAllocation)>> {
        self.check_native_ledger(work)?;
        let _state = self.state.lock().await;
        let effect = self.effect_id_with_native_work(work)?;
        let Some(mut reference) = self
            .original_artifact_with_native_work(effect, attempt, "result", work)
            .await?
        else {
            return Ok(None);
        };
        self.authenticate_with_native_work(attempt, true, work)
            .await?;
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
        let effect = self.effect_id_with_native_work(work)?;
        let Some(output) = self
            .original_artifact_with_native_work(effect, attempt, "result", work)
            .await?
        else {
            return Ok(None);
        };
        self.authenticate_with_native_work(attempt, true, work)
            .await?;
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

    pub(super) fn creation_with_native_work<'a>(
        &'a self,
        effect: EffectId,
        attempt: EffectAttemptId,
        resource: &'a str,
        volume: &'a VolumeRef,
        config: VolumeConfig,
        work: &'a NativeMaterializationWork,
    ) -> impl std::future::Future<Output = Result<Option<LocalGeneration>>> + Send + 'a {
        async move {
            let operation =
                self.creation_operation_with_native_work(effect, attempt, resource, work)?;
            let name = {
                let mut io = acyclic_harness::store::HistoryWork::new(
                    PreparationIo::from_native_work(work)?,
                );
                let value = volume.storage_name(&mut Some(&mut io))?;
                (value, io.into_custody()?)
            };
            let io = PreparationIo::from_native_work(work)?;
            let (generation, recorded) = self
                .fs
                .workspace_for_creation_operation(
                    name.0,
                    config,
                    operation,
                    &io,
                    work.cancellation(),
                )
                .await;
            recorded?;
            generation.map_err(Error::from)
        }
    }

    pub(super) fn scratch_creation_with_native_work(
        &self,
        generation: &LocalGeneration,
        work: &NativeMaterializationWork,
    ) -> Result<(PiScratchCreation, NativeAllocation)> {
        let digest = generation.id().digest().into_bytes();
        let volume = &self.preparation.request.binding.scratch.volume;
        let (reference, custody) =
            GenerationRef::new(volume.provider(), &digest, None, Some(work))?;
        let scratch = (
            PiScratchCreation {
                workspace_id: generation.workspace().id().into_bytes(),
                generation: reference,
            },
            custody.ok_or(acyclic_fs::NativeTreeMaterializationError::MissingCustody)?,
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
                let volume = self
                    .preparation
                    .request
                    .binding
                    .project_volumes
                    .get(key)
                    .ok_or_else(|| {
                        Self::native_failure("root is absent from its original selected volumes")
                    })?;
                let input = inputs
                    .0
                    .get(key)
                    .ok_or_else(|| Self::native_failure("root attachment input is missing"))?;
                let Some(root) = self
                    .observe_project(effect, attempt, key, volume, path, input, work)
                    .await?
                else {
                    return Ok(None);
                };
                let owned_key = work.display(key)?;
                let mut slot = work.claim(
                    std::mem::size_of::<(String, RootCreation)>() as u64,
                    0,
                    acyclic_fs::WorkCounters::UNCHARGED,
                )?;
                roots.1.absorb(&mut slot)?;
                let (value, mut _custody) = owned_key;
                roots.1.absorb(&mut _custody)?;
                roots.0.insert(value, root);
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

    pub(super) async fn observe_project(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        key: &str,
        volume: &VolumeRef,
        path: &str,
        input: &(SourceReference, [u8; 16]),
        work: &NativeMaterializationWork,
    ) -> Result<Option<RootCreation>> {
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
            .load_lazy_workspace(
                generation.workspace().id(),
                &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work,
                    cancellation: work.cancellation(),
                },
            )
            .await?
        else {
            return Ok(None);
        };
        state.validate_root_binding(generation.workspace().id(), input.0)?;
        if work.root_identity(Path::new(path))?.to_bytes() != input.1 {
            return Err(Self::native_failure("root native identity was substituted"));
        }
        Ok(Some(RootCreation {
            repository_workspace_id: generation.workspace().id().into_bytes(),
            source_identity: input.0.identity,
            source_epoch: input.0.epoch,
            native_root_identity: input.1,
        }))
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
        let mut result_box = work.claim(
            std::mem::size_of::<acyclic_harness::conversation::FileRef>() as u64,
            1,
            acyclic_fs::WorkCounters::default(),
        )?;
        result.1.absorb(&mut provider.1)?;
        result.1.absorb(&mut result_box)?;
        Ok(Some((
            EffectObservation {
                provider: provider.0,
                effect_id: effect,
                attempt_id: attempt,
                request_digest: state.request_digest,
                guarantee: state.guarantee,
                status: EffectStatus::Succeeded {
                    result: Box::new(result.0),
                },
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
            (authority.0, authority.1),
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
