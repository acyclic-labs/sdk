//! Physical roots checked against the canonical allocation observer.
use super::*;
use acyclic_harness::filesystem::PreparationIo;

pub(crate) async fn guard_root_preparation_source_with_native_work(
    context: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    preparation: &PiRootPreparation,
    key: &str,
    expected: Option<acyclic_fs::NativeRootIdentity>,
    work: &NativeMaterializationWork,
) -> Result<acyclic_fs::NativeRootIdentity> {
    let request = &preparation.request;
    let invalid =
        || PiRootProvider::native_failure("root source differs from its admitted creator");
    let project = request
        .binding
        .project_volumes
        .get(key)
        .ok_or_else(invalid)?;
    let io = PreparationIo::from_native_work(work)?;
    owner.require_volume_grant(project, VolumeOperation::Read, &mut Some(&mut &io))?;
    if context.durable_task_id() != Some(request.binding.task_id)
        || owner.task_id() != request.binding.task_id
        || preparation.command.operation_id
            != crate::pi_root_inputs::preparation_operation(request.binding.task_id, work)?
        || preparation.plan.provider != ROOT_ALLOCATION_PROVIDER
        || preparation.plan.effect_kind != ROOT_ALLOCATION_EFFECT
        || preparation.plan.guarantee != EffectGuarantee::AtMostOnce
    {
        return Err(invalid());
    }
    let PiRootIntent::Project { paths, .. } = &request.root else {
        return Err(invalid());
    };
    let path = paths.get(key).ok_or_else(invalid)?;
    owner.verify(false, &mut Some(&mut &io)).await?;
    let identity = work.root_identity(Path::new(path))?;
    if let Some(expected) = expected
        && identity != expected
    {
        return Err(Error::NativeWork {
            cause: NativeTreeMaterializationError::AllocationChanged {
                expected,
                actual: identity,
            },
            custody: None,
        });
    }
    owner.verify(false, &mut Some(&mut &io)).await?;
    Ok(identity)
}

impl PiRootProvider {
    pub(crate) async fn verify_created_source(
        &self,
        key: &str,
        expected: &PiRootAllocation,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeRootIdentity> {
        let request = &self.preparation.request;
        let invalid =
            || Self::native_failure("created source differs from its original allocation");
        let context = &self.preparation.context;
        if context.durable_task_id() != Some(request.binding.task_id) {
            return Err(invalid());
        }
        require_read(
            context,
            request
                .binding
                .project_volumes
                .get(key)
                .ok_or_else(invalid)?,
            work,
        )?;
        expected.validate(&request.binding)?;
        let observed;
        let allocation = if self.observed_allocation(expected, work)? {
            let creation = expected.roots.get(key).ok_or_else(invalid)?;
            self.verify_live_binding(key, creation, work).await?;
            expected
        } else {
            let effect = self.effect_id_with_native_work(work)?;
            observed = self
                .observe_allocation_with_native_work(
                    EffectAttemptId::from_bytes(effect.into_bytes()),
                    work,
                )
                .await?
                .ok_or_else(invalid)?;
            &observed.0.1
        };
        if !same_allocation(expected, allocation) {
            return Err(invalid());
        }
        let PiRootIntent::Project { paths, .. } = &request.root else {
            return Err(invalid());
        };
        let identity = work.root_identity(Path::new(paths.get(key).ok_or_else(invalid)?))?;
        let expected = acyclic_fs::NativeRootIdentity::from_bytes(
            allocation
                .roots
                .get(key)
                .ok_or_else(invalid)?
                .native_root_identity,
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
        Ok(identity)
    }

    pub(super) async fn acquire_original_source_with_native_work(
        &self,
        key: &str,
        path: &str,
        admission: SharedRootAdmission,
        expected: Option<acyclic_fs::NativeRootIdentity>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<SharedPhysicalRoot>> {
        let owner = self.execution_owner()?;
        self.shared_roots
            .acquire_with_native_work(Path::new(path), admission, work, || {
                guard_root_preparation_source_with_native_work(
                    &self.preparation.context,
                    owner,
                    &self.preparation,
                    key,
                    expected,
                    work,
                )
            })
            .await
    }
}

pub(crate) async fn prove_root_project_source_with_native_work(
    owner: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<Host>,
    stream: Arc<LocalStream>,
    binding: &crate::PiTaskBinding,
    intent: &PiRootIntent,
    project: &VolumeRef,
    snapshot: acyclic_fs::LazySnapshotRef,
    context: Arc<TaskContext>,
    work: &NativeMaterializationWork,
) -> Result<()> {
    require_read(&context, project, work)?;
    let preparation = load_original_preparation_with_native_work(
        &owner,
        &filesystem,
        binding,
        intent,
        context,
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
    let invalid =
        || PiRootProvider::native_failure("snapshot differs from its original root allocation");
    let key = binding
        .project_volumes
        .iter()
        .find_map(|(key, volume)| (volume == project).then_some(key))
        .ok_or_else(invalid)?;
    let effect = provider.effect_id_with_native_work(work)?;
    let allocation = provider
        .observe_allocation_with_native_work(EffectAttemptId::from_bytes(effect.into_bytes()), work)
        .await?
        .ok_or_else(invalid)?;
    allocation.0.1.validate(binding)?;
    let root = allocation.0.1.roots.get(key).ok_or_else(invalid)?;
    if snapshot.workspace_id.into_bytes() != root.repository_workspace_id
        || snapshot.source.identity != root.source_identity
        || snapshot.source.epoch < root.source_epoch
    {
        return Err(invalid());
    }
    Ok(())
}

fn require_read(
    context: &TaskContext,
    project: &VolumeRef,
    work: &NativeMaterializationWork,
) -> Result<()> {
    let mut io = acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
    let read = project.capability(VolumeOperation::Read, &mut Some(&mut io))?;
    if !context.scope().grants().contains(&read) {
        return Err(PiRootProvider::native_failure(
            "root source is outside the original task read scope",
        ));
    }
    Ok(())
}
