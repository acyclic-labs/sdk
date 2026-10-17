//! Native attachments retain their identity through the stock child preparation.
use super::*;
use crate::{
    conversation::FileRef,
    external_executor::ChildAdmission,
    resource_preparation::ResourcePreparationRequest,
    runtime::{TaskAdmissionRecord, TaskContext},
    task_volume::Component,
};
use acyclic_stream::BoxProviderFuture;

impl ChildAdmission for LocalHarness {
    fn native_workspace(&self, task: TaskId) -> Result<PathBuf> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("native child has no workspace".into()))?;
        Ok(crate::external_executor::launch::process_directory(
            &workspace.mount_directory,
            task,
        ))
    }
    fn admit<'a>(
        &'a self,
        context: &'a TaskContext,
        fence: &'a LeaseFence,
        reference: &'a Component,
        child: TaskAdmissionRecord,
        inputs: Vec<FileRef>,
        reconcile: bool,
    ) -> BoxProviderFuture<'a, Result<TaskId>> {
        Box::pin(async move {
            if !Arc::ptr_eq(&context.harness, self.runtime.harness()) {
                return Err(Error::Unauthorized(
                    "native child uses another runtime".into(),
                ));
            }
            let parent = context.durable_task_id().ok_or_else(|| {
                Error::Unauthorized("native child parent has no admitted task".into())
            })?;
            self.runtime
                .task_host()
                .journal_owner(parent, fence, None)
                .await?
                .verify(reconcile, &mut None)
                .await?;
            self.admit_child(
                &self.task_files,
                reference,
                parent,
                admission::ChildInput::Native {
                    context,
                    fence,
                    child,
                    inputs,
                    reconcile,
                },
            )
            .await
        })
    }
}

pub(super) fn validate_template(
    context: &TaskContext,
    child: &TaskAdmissionRecord,
) -> Result<RuntimeScope> {
    child.validate(&mut None)?;
    if context.durable_task_id().is_none()
        || child.parent != context.durable_task_id()
        || !child.resource_proofs.is_empty()
        || !context.scope().run_limits().allows(&child.run_limits)
    {
        return Err(Error::Unauthorized(
            "native child template differs from its parent".into(),
        ));
    }
    context.scope().narrow(child.grants.clone(), child.limits)
}

pub(super) fn verify_retained(
    context: &TaskContext,
    template: &TaskAdmissionRecord,
    inputs: &[FileRef],
    admitted: &TaskAdmissionRecord,
    prepared: &ResourcePreparationRequest,
    volumes: [&VolumeRef; 4],
) -> Result<()> {
    let narrowed = validate_template(context, template)?;
    let mut expected = template.clone();
    expected.grants = admission::child_grants(context, &narrowed, volumes, inputs)?;
    expected.resource_proofs = prepared.child.resource_proofs.clone();
    if &prepared.child != admitted || prepared.child != expected || prepared.input_files != inputs {
        return Err(Error::Conflict("native child preparation changed".into()));
    }
    Ok(())
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
