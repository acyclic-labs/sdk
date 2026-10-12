//! Canonical selected import under the original SDK publication callback.
use super::*;

pub(crate) async fn import_native_project<F, Fut>(
    source: &PiNativeSelectedSource,
    workspace: &crate::LocalLazyWorkspace,
    mount: Option<&crate::LocalSingleMount>,
    volume: &VolumeRef,
    checkpoint: LazySnapshotRef,
    paths: &[String],
    operation: acyclic_harness::OperationId,
    permit: acyclic_fs::PublicationPermit,
    maximum_paths: usize,
    maximum_bytes: u64,
    work: &NativeMaterializationWork,
    mut before_publication: F,
) -> Result<LazySnapshotRef>
where
    F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
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
    if requested.1.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(selected_invalid("import repeats a selected path"));
    }
    for path in paths {
        let absolute = source.path(path, work)?;
        source.owner.verify(false, &mut Some(&mut io)).await?;
        let mut parsed = source
            .workspace
            .workspace()
            .namespace_path_with_native_work(&absolute.0, work, work.cancellation())?;
        requested.2.absorb(&mut parsed.1)?;
        requested.0.push(parsed.0);
    }
    let plan = source
        .inspection
        .prepare_native_selected_import_with_native_work(
            &source.workspace,
            source.snapshot,
            &requested.0,
            work,
        )
        .await?;
    before_publication(work).await?;
    if plan.0.logical_bytes() > maximum_bytes
        || plan.0.maximum_regular_file_bytes() > source.context.scope().limits().file_bytes
    {
        return Err(selected_invalid(
            "import exceeds its selected byte allowance",
        ));
    }
    let operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
    if let Some(mount) = mount {
        mount
            .import_selected(
                checkpoint,
                plan.0,
                plan.1,
                operation,
                permit,
                work,
                work.cancellation(),
                before_publication,
            )
            .await
    } else {
        let accounting =
            acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(work, work.cancellation())?;
        workspace
            .import_selected(
                checkpoint,
                plan.0,
                plan.1,
                operation,
                permit,
                &accounting,
                before_publication,
            )
            .await
    }
}
