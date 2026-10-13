use super::*;

#[allow(
    clippy::too_many_arguments,
    reason = "the original creator, current owner and native source remain explicit"
)]
pub(crate) async fn freeze_created_root_source(
    protected: Arc<PiOwnerConfig>,
    resources: &ServiceResources,
    filesystem: Arc<PiNativeFilesystem>,
    stream: std::sync::Arc<LocalStream>,
    shared: &Arc<TaskContext>,
    owner: &Arc<TaskJournalOwner<LocalStream>>,
    input: &PiTaskInput,
    key: &str,
    actual: &PiRootHandle,
    work: &NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    let PiTaskExecution::Host { root, .. } = &input.execution else {
        return Err(unauthorized(
            "Root source substituted its original admitted bootstrap input",
        ));
    };
    let preparation = crate::pi_root_provider::load_original_preparation_with_native_work(
        &protected,
        &filesystem,
        &input.binding,
        root,
        shared.clone(),
        work,
    )
    .await?;
    let provider = crate::pi_root_provider::root_provider(
        protected.clone(),
        resources,
        filesystem.clone(),
        stream.clone(),
        preparation,
        None,
        work.clone(),
    )?;
    let attempt = acyclic_harness::EffectAttemptId::from_bytes(
        provider.effect_id_with_native_work(work)?.into_bytes(),
    );
    let original = provider
        .observe_allocation_with_native_work(attempt, work)
        .await?
        .ok_or_else(|| unauthorized("Root freeze has no actual original created allocation"))?;
    let mut guard = || async {
        owner
            .verify(
                false,
                &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                    acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await?;
        provider
            .verify_created_source(owner, key, &original.0.1, work)
            .await?;
        owner
            .verify(
                false,
                &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                    acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
                )),
            )
            .await
    };
    if owner.task_id() != protected.policy().root_task
        || shared.durable_task_id() != Some(protected.policy().root_task)
    {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Root snapshot requires its actual original Root context and owner",
        });
    }
    let observed = actual
        .physical
        .observe_with_native_work(work, &mut guard)
        .await?;
    guard().await?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    };
    let rebound = actual
        .workspace
        .rebind_source(&mut accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if rebound.source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source rebind changed its guarded watcher observation",
        });
    }
    let frozen = actual
        .workspace
        .snapshot(&mut accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if frozen.source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source freeze changed its guarded watcher observation",
        });
    }
    drop(rebound);
    drop(observed.0);
    Ok((frozen, observed.1))
}
/// Only an active source's ORIGINAL native view owner may checkpoint its dirty mounts.
pub(crate) async fn freeze_child_source(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<LocalStream>,
    view: &acyclic_harness::filesystem::NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>,
    source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source checkpoint requires its actual source task context",
        });
    }
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
    let checkpoint = view
        .checkpoint_projects_with_native_work(owner, work)?
        .await?;
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::store::HistoryWork::new(
                acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
            )),
        )
        .await?;
    let snapshot = match checkpoint.0.as_slice() {
        [(_, snapshot)] if snapshot.workspace_id == source.workspace().id() => *snapshot,
        _ => {
            return Err(unauthorized(
                "Pi checkpoint must name its unique selected workspace",
            ));
        }
    };
    drop(checkpoint.0);
    Ok((snapshot, checkpoint.1))
}
