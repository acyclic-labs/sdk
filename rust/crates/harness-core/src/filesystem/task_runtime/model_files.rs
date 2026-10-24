use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn verify_native_model_files(
    context: &TaskContext,
    content: &crate::model::ModelContent,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    // Keep every direct and nested reference behind one authority check.
    for (file, policy) in content.file_parts() {
        verify_native_model_file(context, file, policy, work).await?;
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn verify_native_model_file(
    context: &TaskContext,
    file: &crate::conversation::FileRef,
    policy: &crate::model::FileProjectionPolicy,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if matches!(policy, crate::model::FileProjectionPolicy::Reference) {
        return context.authorize_file_reference(
            file,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        );
    }
    let body = context
        .read_file(
            file,
            Some(&crate::executor::PreparationIo::from_native_work(work)?),
        )
        .await?;
    drop(body);
    if let crate::model::FileProjectionPolicy::Native(policy) = policy
        && let Some(binding) = &policy.configuration
    {
        let body = context
            .read_file(
                &binding.configuration.content,
                Some(&crate::executor::PreparationIo::from_native_work(work)?),
            )
            .await?;
        drop(body);
    }
    Ok(())
}
