use super::*;
use crate::{
    filesystem::PreparationIo,
    runtime::TaskStateProvider,
    task_volume::{ChildFile, FileKey, FileValue, Query, TaskFiles},
};
mod staging;

#[cfg(test)]
use acyclic_fs::facade::FsWork as _;
use acyclic_stream::{BoxProviderFuture, StreamProvider};

impl LocalHarness {
    // Child admission owns the complete fork/preparation state machine. Build
    // it in a separate frame which returns before the controller polls it.
    // Otherwise each nested controller embeds and constructs that whole state
    // on the native caller's stack, including in LTO product builds.
    #[inline(never)]
    pub(in crate::local) fn admit_child_task<'a, P: StreamProvider + ?Sized>(
        &'a self,
        files: &'a TaskFiles<P>,
        file: &'a FileValue,
    ) -> BoxProviderFuture<'a, Result<TaskId>> {
        Box::pin(self.admit_child_task_body(files, file))
    }

    async fn admit_child_task_body<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        file: &FileValue,
    ) -> Result<TaskId> {
        let FileKey::Child {
            reference,
            file: ChildFile::Request,
        } = &file.key
        else {
            return Err(Error::Invalid(
                "child admission requires a request file".into(),
            ));
        };
        let observed = files
            .inner
            .snapshot_files(&[file.key.clone()], &Query::File(file.key.clone()))
            .await
            .map_err(storage)?;
        if observed.files.as_slice() != std::slice::from_ref(file) {
            return Err(Error::Conflict(
                "child request differs from its accepted file".into(),
            ));
        }
        let parent = file.requester.ok_or_else(|| {
            Error::Unauthorized("child request has no authenticated parent".into())
        })?;
        let parent_record = self.runtime.task_host().observe_admission(parent).await?;
        let native_host = parent_record.task.name == "acyclic.native_host";
        if let Some(task) = self.admitted_child(files, reference, parent).await? {
            return Ok(task);
        }

        let plan = staging::Plan::new(
            self,
            files.volume(),
            parent,
            reference,
            native_host,
            &parent_record.input,
        )?;
        staging::Admission::open(self, plan, files)
            .await?
            .finish()
            .await
    }
    #[cfg(test)]
    pub(in crate::local) async fn admit_child_request<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        file: &FileValue,
    ) -> Result<(TaskId, workspace::View)> {
        let task = self.admit_child_task(files, file).await?;
        let workspace = self.workspace.as_ref().unwrap();
        let project = VolumeRef::new(
            self.project.provider().clone(),
            format!("agent-project-{task}"),
            VolumeClass::Project,
            self.project.owner().clone(),
            &mut None,
        )?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, self.limits.file_bytes),
            &cancellation,
        )?;
        let child = io.sdk(
            workspace
                .filesystem
                .open_workspace(project.storage_name(&mut None)?, &io, &cancellation)
                .await,
        )?;
        let io_ref = &io;
        let mut accounting = io_ref.accounting(&cancellation);
        let opened = workspace.view().open_related(child, &mut accounting).await;
        let view = io.sdk(io_ref.finish(opened, &accounting))?;
        let work = io.receipt();
        acyclic_test_support::differential::record(("view-items", work.items_examined));
        acyclic_test_support::differential::record((
            "view-allocations",
            work.allocation_operations,
        ));
        Ok((task, view))
    }
}

pub(super) fn child_operation(
    volume: &VolumeRef,
    reference: &str,
    phase: &[u8],
) -> Result<OperationId> {
    let mut digest = blake3::Hasher::new();
    digest.update(b"acyclic.local.child-file/v1\0");
    digest.update(volume.storage_name(&mut None)?.as_bytes());
    digest.update(&[0]);
    digest.update(reference.as_bytes());
    digest.update(&[0]);
    digest.update(phase);
    Ok(OperationId::from_bytes(
        digest.finalize().as_bytes()[..16].try_into().unwrap(),
    ))
}

fn source_volumes(
    context: &crate::runtime::TaskContext,
    project: &VolumeRef,
    private: &VolumeRef,
) -> Result<(VolumeRef, VolumeRef)> {
    let Some(access) = context.scope().created_access.as_ref() else {
        return Ok((project.clone(), private.clone()));
    };
    let find = |class| -> Result<VolumeRef> {
        let mut bindings = access
            .content
            .iter()
            .filter(|binding| binding.volume.class() == class);
        let volume = bindings
            .next()
            .ok_or_else(|| Error::Unauthorized("child source volume missing".into()))?;
        if bindings.next().is_some() {
            return Err(Error::Unauthorized("child source volume ambiguous".into()));
        }
        Ok(volume.volume.clone())
    };
    Ok((
        find(VolumeClass::Project)?,
        find(VolumeClass::AgentPrivate)?,
    ))
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod work_tests;
