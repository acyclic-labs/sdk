use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn verify_native_model_files(
    context: &TaskContext,
    content: &crate::model::ModelContent,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    use crate::model::{ModelContentPart, ModelDataPart, ToolResultContent};
    for part in content.parts() {
        match part {
            ModelContentPart::File { file, policy } => {
                verify_native_model_file(context, file, policy, work).await?;
            }
            ModelContentPart::ToolResult {
                content: ToolResultContent::Parts { parts },
                ..
            } => {
                for part in parts {
                    if let ModelDataPart::File { file, policy } = part {
                        verify_native_model_file(context, file, policy, work).await?;
                    }
                }
            }
            _ => {}
        }
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
