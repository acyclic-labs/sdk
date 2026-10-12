use super::*;

/// Independently authenticates both selections under the ORIGINAL root's retained admission tree.
pub(crate) async fn authorize_selected_source(
    runtime: &FilesystemTaskRuntime<
        LocalStream,
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
    >,
    original_root: TaskId,
    caller: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    source: TaskId,
    maximum_depth: u32,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    let mut io = HistoryWork::new(PreparationIo::from_native_work(native_work)?);
    owner.verify(false, &mut Some(&mut io)).await?;
    if caller.durable_task_id() != Some(owner.task_id()) {
        return Err(selected_unauthorized(
            "selected graph changed its ORIGINAL caller context",
        ));
    }
    if maximum_depth == 0 {
        return Err(selected_invalid("selected ancestry bound is zero"));
    }
    for selected in [owner.task_id(), source] {
        let mut task = selected;
        let mut authenticated = false;
        for _ in 0..maximum_depth {
            let admission_custody = runtime
                .task_host()
                .observe_admission_with_native_work(task, native_work)
                .await?;
            let admission = &admission_custody.0;
            if admission.task_id() != task {
                return Err(selected_unauthorized(
                    "selected admission substituted its task identity",
                ));
            }
            if task == original_root {
                if admission.parent.is_some() {
                    return Err(selected_unauthorized(
                        "selected original root is itself a child",
                    ));
                }
                authenticated = true;
                break;
            }
            task = admission.parent.ok_or_else(|| {
                selected_unauthorized("selected task belongs to another original root")
            })?;
        }
        if !authenticated {
            return Err(selected_unauthorized(
                "selected task ancestry exceeds its authenticated bound",
            ));
        }
    }
    owner.verify(false, &mut Some(&mut io)).await
}
