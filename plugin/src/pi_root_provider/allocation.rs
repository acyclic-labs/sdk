//! Idempotent creation of the selected root resources.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

impl PiRootProvider {
    pub(super) async fn capture_inputs_with_native_work(
        &self,
        effect: EffectId,
        attempt: EffectAttemptId,
        work: &NativeMaterializationWork,
    ) -> Result<(
        BTreeMap<String, (SourceReference, [u8; 16])>,
        NativeAllocation,
    )> {
        let mut inputs = (BTreeMap::new(), work.claim(0, 0, WorkCounters::UNCHARGED)?);
        let PiRootIntent::Project { paths, .. } = &self.preparation.request.root else {
            return Ok(inputs);
        };
        for key in paths
            .keys()
            .map(|key| key.as_str())
            .chain(std::iter::once("scratch"))
        {
            let (volume, config) = if key == "scratch" {
                (
                    &self.preparation.request.binding.scratch.volume,
                    VolumeConfig::portable(Lifecycle::Durable),
                )
            } else {
                (
                    self.preparation
                        .request
                        .binding
                        .project_volumes
                        .get(key)
                        .ok_or_else(|| {
                            Self::native_failure(
                                "root is absent from its original selected volumes",
                            )
                        })?,
                    VolumeConfig::native(Lifecycle::Durable),
                )
            };
            if self
                .creation_with_native_work(effect, attempt, key, volume, config, work)
                .await?
                .is_some()
            {
                return Err(Self::native_failure(
                    "original creation exists without immutable root attachment input",
                ));
            }
        }
        for (key, path) in paths {
            self.execution_owner()?
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            let physical = self
                .acquire_original_source_with_native_work(
                    key,
                    path,
                    SharedRootAdmission::Fresh,
                    None,
                    work,
                )
                .await?;
            let mut key = work.display(key)?;
            let mut slot = work.claim(
                std::mem::size_of::<(String, (SourceReference, [u8; 16]))>() as u64,
                0,
                WorkCounters::UNCHARGED,
            )?;
            inputs.1.absorb(&mut key.1)?;
            inputs.1.absorb(&mut slot)?;
            inputs.0.insert(
                key.0,
                (
                    physical.source.reference(),
                    physical.source.root_identity().to_bytes(),
                ),
            );
        }
        #[cfg(test)]
        self.test_fault_native("before-attachments-stage")?;
        self.stage_with_native_work(effect, attempt, "attachments", &inputs.0, work)
            .await?;
        Ok(inputs)
    }

    pub(super) fn allocate_with_native_work<'a>(
        &'a self,
        effect: EffectId,
        attempt: EffectAttemptId,
        original: &'a EffectState,
        state: &'a mut ProviderState,
        work: &'a NativeMaterializationWork,
    ) -> impl std::future::Future<Output = Result<(EffectObservation, NativeAllocation)>> + Send + 'a
    {
        async move {
            self.check_native_ledger(work)?;
            let cancellation = work.cancellation().clone();
            let owner = self.execution_owner()?;
            owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            self.bind_conversation_with_native_work(effect, attempt, work)
                .await?;
            #[cfg(test)]
            self.test_fault_native("after-conversation-bind")?;

            if let PiRootIntent::Project { paths, .. } = &self.preparation.request.root {
                let inputs = match self
                    .original_artifact_with_native_work(effect, attempt, "attachments", work)
                    .await?
                {
                    Some(reference) => {
                        self.read_with_native_work::<BTreeMap<String, (SourceReference, [u8; 16])>>(
                            &reference.0,
                            work,
                        )
                        .await?
                    }
                    None => {
                        self.capture_inputs_with_native_work(effect, attempt, work)
                            .await?
                    }
                };
                if !inputs.0.keys().eq(paths.keys()) {
                    return Err(Self::native_failure(
                        "root attachment selection differs from original creator",
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
                            Self::native_failure(
                                "root is absent from its original selected volumes",
                            )
                        })?;
                    let input = inputs
                        .0
                        .get(key)
                        .ok_or_else(|| Self::native_failure("root attachment input is missing"))?;
                    self.validate_attachment(input)?;
                    owner
                        .verify(
                            false,
                            &mut Some(&mut &PreparationIo::from_native_work(work)?),
                        )
                        .await?;
                    let identity = guard_root_preparation_source_with_native_work(
                        self.preparation.context.as_ref(),
                        owner,
                        &self.preparation,
                        key,
                        None,
                        work,
                    )
                    .await?;
                    if identity.to_bytes() != input.1 {
                        return Err(Self::native_failure(
                            "original root native identity was replaced",
                        ));
                    }
                    let generation = self
                        .creation_with_native_work(
                            effect,
                            attempt,
                            key,
                            volume,
                            VolumeConfig::native(Lifecycle::Durable),
                            work,
                        )
                        .await?;
                    let mut persisted_source = input.0;
                    let mut existing_workspace = None;
                    if let Some(generation) = generation {
                        if let Some(binding) = self
                            .store
                            .load_lazy_workspace(
                                generation.workspace().id(),
                                &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                                    work,
                                    cancellation: &cancellation,
                                },
                            )
                            .await?
                        {
                            binding.validate_root_binding(generation.workspace().id(), input.0)?;
                            persisted_source = binding.source;
                            existing_workspace = Some(generation.workspace());
                        }
                        if state.handles.contains_key(key) {
                            continue;
                        }
                    }
                    let physical = self
                        .acquire_original_source_with_native_work(
                            key,
                            path,
                            SharedRootAdmission::Resume(persisted_source),
                            Some(identity),
                            work,
                        )
                        .await?;
                    if physical.source.reference().identity != input.0.identity
                        || physical.source.root_identity().to_bytes() != input.1
                    {
                        return Err(Self::native_failure(
                            "resumed root source differs from original allocation",
                        ));
                    }
                    owner
                        .verify(
                            false,
                            &mut Some(&mut &PreparationIo::from_native_work(work)?),
                        )
                        .await?;
                    let workspace = if let Some(workspace) = existing_workspace {
                        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                            work,
                            cancellation: &cancellation,
                        };
                        let original = acyclic_fs::LazyWorkspaceStore::load_lazy_workspace(
                            &self.store,
                            workspace.id(),
                            &mut accounting,
                        )
                        .await
                        .map_err(crate::pi_fork_host::native_lazy_error)?
                        .ok_or_else(|| {
                            crate::pi_fork_host::native_lazy_error(
                                acyclic_fs::LazyWorkspaceError::OriginalSnapshotUnavailable,
                            )
                        })?;
                        original
                            .validate_root_binding(workspace.id(), input.0)
                            .map_err(crate::pi_fork_host::native_lazy_error)?;
                        LocalLazyWorkspace::open(
                            workspace,
                            Arc::clone(&physical.source),
                            self.store.clone(),
                            &mut accounting,
                        )
                        .await
                        .map_err(crate::pi_fork_host::native_lazy_error)?
                    } else {
                        let operation =
                            self.creation_operation_with_native_work(effect, attempt, key, work)?;
                        let name = {
                            let mut io = acyclic_harness::store::HistoryWork::new(
                                PreparationIo::from_native_work(work)?,
                            );
                            let value = volume.storage_name(&mut Some(&mut io))?;
                            (value, io.into_custody()?)
                        };
                        acyclic_fs::LazyWorkspace::attach(
                            &self.fs,
                            &name.0,
                            Arc::clone(&physical.source),
                            self.store.clone(),
                            acyclic_fs::VolumeInitialization {
                                config: VolumeConfig::native(Lifecycle::Durable),
                                access_policy: Some(
                                    acyclic_harness::filesystem::INITIAL_ACCESS_POLICY,
                                ),
                            },
                            operation,
                            &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                                work,
                                cancellation: &cancellation,
                            },
                        )
                        .await
                        .map_err(crate::pi_fork_host::native_lazy_error)?
                        .0
                    };
                    let (key, handle) = PiRootHandle::from_observed_with_native_work(
                        workspace,
                        physical,
                        work.display(key)?,
                        work,
                    )?;
                    state.handles.insert(key, handle);
                    #[cfg(test)]
                    self.test_fault_native("after-root-creation")?;
                }
            }
            owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            let volume = &self.preparation.request.binding.scratch.volume;
            owner.require_volume_grant(
                volume,
                VolumeOperation::Write,
                &mut Some(&mut &PreparationIo::from_native_work(work)?),
            )?;
            if self
                .creation_with_native_work(
                    effect,
                    attempt,
                    "scratch",
                    volume,
                    VolumeConfig::portable(Lifecycle::Durable),
                    work,
                )
                .await?
                .is_none()
            {
                owner
                    .verify(
                        false,
                        &mut Some(&mut &PreparationIo::from_native_work(work)?),
                    )
                    .await?;
                let operation =
                    self.creation_operation_with_native_work(effect, attempt, "scratch", work)?;
                let name = {
                    let mut io = acyclic_harness::store::HistoryWork::new(
                        PreparationIo::from_native_work(work)?,
                    );
                    let value = volume.storage_name(&mut Some(&mut io))?;
                    (value, io.into_custody()?)
                };
                let owner = Arc::clone(self.journal_owner.as_ref().ok_or_else(|| {
                    Self::native_failure("root creation requires its original execution owner")
                })?);
                let mut guard =
                    acyclic_fs::NativeAuthorityGuard(move |work: &NativeMaterializationWork| {
                        let owner = Arc::clone(&owner);
                        let work = work.clone();
                        async move {
                            owner
                                .verify(
                                    false,
                                    &mut Some(&mut &PreparationIo::from_native_work(&work)?),
                                )
                                .await
                        }
                    });
                let io = PreparationIo::from_native_work(work)?;
                let (created, recorded) = self
                    .fs
                    .create_workspace_with_operation::<_, _, Error>(
                        name.0,
                        acyclic_fs::VolumeInitialization {
                            config: VolumeConfig::portable(Lifecycle::Durable),
                            access_policy: Some(acyclic_harness::filesystem::INITIAL_ACCESS_POLICY),
                        },
                        operation,
                        &io,
                        &cancellation,
                        Some(&mut guard),
                    )
                    .await;
                recorded?;
                created?;
            }
            #[cfg(test)]
            self.test_fault_native("before-result-stage")?;
            owner
                .verify(
                    false,
                    &mut Some(&mut &PreparationIo::from_native_work(work)?),
                )
                .await?;
            let observation = self
                .observe_result_with_native_work(effect, attempt, original, true, work)
                .await?
                .ok_or(Self::native_failure(
                    "root creator lacks authentic original resource acknowledgement",
                ))?;
            #[cfg(test)]
            self.test_fault_native("after-result-stage")?;
            Ok(observation)
        }
    }
}
