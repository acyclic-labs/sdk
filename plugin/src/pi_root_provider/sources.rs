//! Original physical source identity and selected project proofs.
use super::*;

/// Fresh creator's source gate.
pub(crate) async fn guard_root_preparation_source_with_native_work(
    context: &TaskContext,
    current_owner: &TaskJournalOwner<LocalStream>,
    preparation: &PiRootPreparation,
    root_key: &str,
    expected: Option<acyclic_fs::NativeRootIdentity>,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    let request = &preparation.request;
    let project = request
        .binding
        .project_volumes
        .get(root_key)
        .ok_or(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "root source is not selected by its original admission",
        })?;
    current_owner.require_volume_grant(project, VolumeOperation::Read, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))?;
    context
        .verify_original_volume_read_with_native_work(current_owner, project, work)
        .await?;
    if current_owner.task_id() != request.binding.task_id
        || preparation.command.operation_id
            != crate::pi_root_inputs::preparation_operation(request.binding.task_id, work)?
        || preparation.plan.provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan.effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan.guarantee != EffectGuarantee::AtMostOnce
    {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "root source differs from its original admitted creator operation",
        });
    }
    let PiRootIntent::Project { paths, .. } = &request.root else {
        return Err(Error::NativeBoundary {
            code: ErrorCode::Unauthorized,
            reason: "scratch-only admission has no native project source",
        });
    };
    let path = paths.get(root_key).ok_or(Error::NativeBoundary {
        code: ErrorCode::Unauthorized,
        reason: "root source has no original configured binding",
    })?;
    current_owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    let identity = work.root_identity(Path::new(path))?;
    if let Some(expected) = expected {
        if identity != expected {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::AllocationChanged {
                    expected,
                    actual: identity,
                },
                custody: None,
            });
        }
    }
    context
        .verify_original_volume_read_with_native_work(current_owner, project, work)
        .await?;
    current_owner
        .verify(false, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))
        .await?;
    Ok(identity)
}

impl PiRootProvider {
    /// Verifies a created root against its original output and current readonly owner.
    pub(crate) async fn verify_created_source(
        &self,
        current_read_owner: &TaskJournalOwner<LocalStream>,
        key: &str,
        expected_allocation: &PiRootAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeRootIdentity> {
        work.admit(WorkCounters::UNCHARGED)?;
        let context = &self.preparation.context;
        let request = &self.preparation.request;
        if current_read_owner.task_id() != request.binding.task_id
            || context.durable_task_id() != Some(request.binding.task_id)
        {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "created root source differs from the original readonly task",
            });
        }
        for volume in request.binding.project_volumes.values() {
            context
                .verify_original_volume_read_with_native_work(current_read_owner, volume, work)
                .await?;
        }
        if !request.binding.project_volumes.contains_key(key) {
            return Err(Error::NativeBoundary {
                code: ErrorCode::Unauthorized,
                reason: "created source is not an originally selected root",
            });
        }
        expected_allocation.validate(&request.binding)?;
        let effect = self.effect_id_with_native_work(work)?;
        let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
        let observed = self
            .observe_allocation_with_native_work(attempt, work)
            .await?
            .ok_or_else(|| {
                PiRootProvider::native_failure("created root lacks its original successful output")
            })?;
        if !same_allocation(expected_allocation, &observed.0.1) {
            return Err(PiRootProvider::native_failure(
                "created root substitutes original allocation evidence",
            ));
        }
        let PiRootIntent::Project { paths, .. } = &request.root else {
            return Err(PiRootProvider::native_failure(
                "scratch-only allocation has no created native source",
            ));
        };
        let path = paths.get(key).ok_or_else(|| {
            PiRootProvider::native_failure("created source has no original native selection")
        })?;
        let identity = work.root_identity(Path::new(path))?;
        let expected = acyclic_fs::NativeRootIdentity::from_bytes(
            observed.0.1.roots[key].native_root_identity,
        );
        if identity != expected {
            return Err(Error::NativeWork {
                cause: NativeTreeMaterializationError::AllocationChanged {
                    expected,
                    actual: identity,
                },
                custody: None,
            });
        }
        context
            .verify_original_volume_read_with_native_work(
                current_read_owner,
                &request.binding.project_volumes[key],
                work,
            )
            .await?;
        Ok(identity)
    }
}

/// Selected original-root source evidence under the caller's remaining quota.
#[allow(
    clippy::too_many_arguments,
    reason = "independent original source and bounded observer arguments"
)]
pub(crate) async fn prove_root_project_source_with_native_work(
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    binding: &crate::PiTaskBinding,
    intent: &PiRootIntent,
    project: &VolumeRef,
    snapshot: acyclic_fs::LazySnapshotRef,
    context: Arc<acyclic_harness::runtime::TaskContext>,
    current_owner: &TaskJournalOwner<LocalStream>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    current_owner.require_volume_grant(project, VolumeOperation::Read, &mut Some(&mut acyclic_harness::store::HistoryWork::new(acyclic_harness::filesystem::PreparationIo::from_native_work(work)?)))?;
    context
        .verify_original_volume_read_with_native_work(current_owner, project, work)
        .await?;
    let preparation = load_original_preparation_with_native_work(
        &owner,
        &filesystem,
        binding,
        intent,
        Arc::clone(&context),
        work,
    )
    .await?;
    let provider = root_provider(
        owner,
        resources,
        filesystem,
        stream,
        preparation,
        None,
        work.clone(),
    )?;
    let effect = provider.effect_id_with_native_work(work)?;
    let attempt = EffectAttemptId::from_bytes(effect.into_bytes());
    provider
        .authenticate_with_native_work(attempt, true, work)
        .await?;
    let reference = provider
        .original_artifact_with_native_work(effect, attempt, "result", work)
        .await?
        .ok_or_else(|| {
            PiRootProvider::native_failure(
                "root source has no canonical successful allocation output",
            )
        })?;
    let recorded = provider
        .read_with_native_work::<PiRootAllocation>(&reference.0, work)
        .await?;
    recorded.0.validate(binding)?;
    let key = binding
        .project_volumes
        .iter()
        .find_map(|(key, volume)| (volume == project).then_some(key))
        .ok_or_else(|| {
            PiRootProvider::native_failure("project is not an originally selected root source")
        })?;
    let root = &recorded.0.roots[key];
    if snapshot.workspace_id.into_bytes() != root.repository_workspace_id
        || snapshot.source.identity != root.source_identity
        || snapshot.source.epoch < root.source_epoch
    {
        return Err(PiRootProvider::native_failure(
            "snapshot substitutes original root workspace or source",
        ));
    }
    let inputs_reference = provider
        .original_artifact_with_native_work(effect, attempt, "attachments", work)
        .await?
        .ok_or_else(|| {
            PiRootProvider::native_failure("root source has no original attachment input")
        })?;
    let inputs = provider
        .read_with_native_work::<BTreeMap<String, (SourceReference, [u8; 16])>>(
            &inputs_reference.0,
            work,
        )
        .await?;
    let PiRootIntent::Project { paths, .. } = intent else {
        return Err(PiRootProvider::native_failure(
            "scratch-only root has no native project source",
        ));
    };
    if !inputs.0.keys().eq(paths.keys()) {
        return Err(PiRootProvider::native_failure(
            "root source selection differs from original attachment",
        ));
    }
    let input = &inputs.0[key];
    provider.validate_attachment(input)?;
    if input.0.identity != root.source_identity
        || input.0.epoch != root.source_epoch
        || input.1 != root.native_root_identity
    {
        return Err(PiRootProvider::native_failure(
            "root source allocation differs from original attachment",
        ));
    }
    let generation = provider
        .creation_with_native_work(
            effect,
            attempt,
            key,
            project,
            VolumeConfig::native(Lifecycle::Durable),
            work,
        )
        .await?
        .ok_or_else(|| {
            PiRootProvider::native_failure("root source filesystem creation is absent")
        })?;
    if generation.workspace().id() != snapshot.workspace_id {
        return Err(PiRootProvider::native_failure(
            "root source snapshot substitutes actual creation",
        ));
    }
    let state = provider
        .store
        .load_lazy_workspace_with_native_work(snapshot.workspace_id, work)
        .await?
        .ok_or_else(|| PiRootProvider::native_failure("root source lazy binding is absent"))?;
    state
        .as_state()
        .validate_root_binding(snapshot.workspace_id, input.0)?;
    let identity = work.root_identity(Path::new(&paths[key]))?;
    if identity.to_bytes() != root.native_root_identity
        || !provider
            .observe_conversation_with_native_work(effect, attempt, work)
            .await?
    {
        return Err(PiRootProvider::native_failure(
            "root source native identity or canonical binding differs",
        ));
    }
    provider
        .preparation
        .context
        .verify_original_volume_read_with_native_work(current_owner, project, work)
        .await?;
    Ok(())
}
impl PiRootProvider {
    pub(super) async fn acquire_original_source_with_native_work(
        &self,
        key: &str,
        path: &str,
        admission: SharedRootAdmission,
        expected: Option<acyclic_fs::NativeRootIdentity>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<SharedPhysicalRoot>> {
        let owner = self.execution_owner()?;
        let pinned = std::sync::Mutex::new(expected);
        self.shared_roots
            .acquire_with_native_work(Path::new(path), admission, work, || {
                let pinned = &pinned;
                async move {
                    let previous = *pinned.lock().map_err(|_| {
                        Self::native_failure("root source identity guard is poisoned")
                    })?;
                    let identity = guard_root_preparation_source_with_native_work(
                        self.preparation.context.as_ref(),
                        owner,
                        &self.preparation,
                        key,
                        previous,
                        work,
                    )
                    .await?;
                    let mut original = pinned.lock().map_err(|_| {
                        Self::native_failure("root source identity guard is poisoned")
                    })?;
                    if original.is_some_and(|previous| previous != identity) {
                        return Err(Self::native_failure(
                            "root identity changed between original source guards",
                        ));
                    }
                    *original = Some(identity);
                    Ok(identity)
                }
            })
            .await
    }
}
