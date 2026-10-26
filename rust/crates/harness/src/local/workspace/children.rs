//! Authenticated child allocation shared by workspace clients and executors.
use super::*;
use crate::runtime::{RuntimeScope, TaskContext};

impl LocalWorkspace {
    pub(in crate::local) async fn project_view(&self, project: &VolumeRef) -> Result<View> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let project_workspace = self
            .filesystem
            .open_workspace(
                project.storage_name(&mut None)?,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value;
        self.view()
            .open_related(
                project_workspace,
                &mut acyclic_fs::WorkBudget::UNBOUNDED.accounting(&cancellation),
            )
            .await
            .map_err(storage)
    }
}

pub(in crate::local) fn created_project<'a>(
    task: &'a TaskContext,
    scope: &RuntimeScope,
) -> Result<&'a VolumeRef> {
    let owner = task
        .owned_mailbox
        .as_ref()
        .ok_or_else(|| Error::Unauthorized("child workspace has no original owner".into()))?;
    let (child, fence) = owner.task_binding();
    if !task.original_task_binding_matches(child, fence) {
        return Err(Error::Unauthorized("child workspace owner differs".into()));
    }
    let access = task.scope().created_access.as_ref().ok_or_else(|| {
        Error::Unauthorized("workspace has no authenticated child allocation".into())
    })?;
    if access.operation.into_bytes() != child.into_bytes() {
        return Err(Error::Unauthorized(
            "child workspace seal belongs to another task".into(),
        ));
    }
    let mut selected = None;
    for binding in &access.content {
        if binding.volume.class() == VolumeClass::Project
            && [VolumeOperation::Read, VolumeOperation::Write]
                .into_iter()
                .map(|operation| binding.volume.capability(operation, &mut None))
                .collect::<Result<Vec<_>>>()?
                .iter()
                .all(|grant| scope.grants().contains(grant))
            && selected.replace(&binding.volume).is_some()
        {
            return Err(Error::Unauthorized(
                "child project binding is ambiguous".into(),
            ));
        }
    }
    selected.ok_or_else(|| Error::Unauthorized("child project is not writable".into()))
}
