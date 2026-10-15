//! Canonical selected import under the original SDK publication callback.
use super::*;

pub(crate) async fn import_native_project<F, Fut>(
    source: &PiNativeSelectedSource,
    workspace_mount_volume_checkpoint: (
        &crate::LocalLazyWorkspace,
        Option<&crate::LocalSingleMount>,
        &VolumeRef,
        LazySnapshotRef,
    ),
    paths: &[String],
    operation_permit_maximum_paths_maximum_bytes: (
        acyclic_harness::OperationId,
        acyclic_fs::PublicationPermit,
        usize,
        u64,
    ),
    work: &NativeMaterializationWork,
    mut before_publication: F,
) -> Result<LazySnapshotRef>
where
    F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let (workspace, mount, volume, checkpoint) = workspace_mount_volume_checkpoint;
    let (operation, permit, maximum_paths, maximum_bytes) =
        operation_permit_maximum_paths_maximum_bytes;
    before_publication(work).await?;
    let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
    let name = volume.storage_name(&mut Some(&mut io))?;
    if paths.is_empty()
        || maximum_paths == 0
        || paths.len() > maximum_paths
        || maximum_bytes == 0
        || workspace.workspace().name().as_str() != name
        || checkpoint.workspace_id != workspace.workspace().id()
    {
        return Err(selected_unauthorized(
            "import changed its destination or selected bounds",
        ));
    }
    let (names, mut names_custody) = crate::pi_native::pi_native_vec::<&str>(paths.len(), work)?;
    let mut requested = {
        let (paths, mut custody) = crate::pi_native::pi_native_vec(paths.len(), work)?;
        custody.absorb(&mut names_custody)?;
        (paths, names, custody)
    };
    requested.1.extend(paths.iter().map(String::as_str));
    requested.1.sort_unstable();
    if requested
        .1
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left == right))
    {
        return Err(selected_invalid("import repeats a selected path"));
    }
    for path in paths {
        let absolute = source.path(path, work).await?;
        source.owner.verify(false, &mut Some(&mut io)).await?;
        let parsed = acyclic_fs::kernel::NamespacePath::parse_in_profile(
            &absolute.0,
            source.workspace.workspace().profile(),
            source.workspace.workspace().limits(),
            &mut acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work,
                cancellation: work.cancellation(),
            },
            work.cancellation(),
        )?;
        let acyclic_fs::kernel::KernelOwnedAllocation::NativeOriginal(mut custody) = parsed.1
        else {
            return Err(selected_invalid("selected path lost its original ledger"));
        };
        requested.2.absorb(&mut custody)?;
        requested.0.push(parsed.0);
    }
    source
        .inspection
        .require_project_selection(source.snapshot, work)
        .await?;
    let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
        work,
        cancellation: work.cancellation(),
    };
    let plan = source
        .workspace
        .prepare_selected_import(source.snapshot, &requested.0, &mut accounting)
        .await
        .map_err(crate::pi_fork_host::native_lazy_error)?;
    source
        .inspection
        .require_project_selection(source.snapshot, work)
        .await?;
    before_publication(work).await?;
    let (logical_bytes, maximum_regular_file_bytes) = plan
        .0
        .byte_limits(&mut accounting)
        .map_err(crate::pi_fork_host::native_lazy_error)?;
    if logical_bytes > maximum_bytes
        || maximum_regular_file_bytes > source.context.scope().limits().file_bytes
    {
        return Err(selected_invalid(
            "import exceeds its selected byte allowance",
        ));
    }
    let acyclic_fs::kernel::KernelOwnedAllocation::NativeOriginal(custody) = plan.1 else {
        return Err(selected_invalid("selected plan lost its original ledger"));
    };
    let operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
    if let Some(mount) = mount {
        mount
            .import_selected(
                checkpoint,
                (plan.0, custody),
                operation,
                permit,
                &acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                    work,
                    cancellation: work.cancellation(),
                },
                before_publication,
            )
            .await
    } else {
        workspace
            .import_selected(
                checkpoint,
                (plan.0, custody),
                operation,
                permit,
                work,
                before_publication,
            )
            .await
    }
}
