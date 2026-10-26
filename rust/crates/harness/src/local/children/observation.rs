//! Read-only lookup shares admission's identity and ownership check.
use super::*;
use crate::runtime::TaskStateProvider;
use crate::task_volume::{
    ChildObservation, ChildResultWindow, ChildTurnResult, Component, TaskFiles,
};
use acyclic_stream::StreamProvider;

impl LocalHarness {
    pub(in crate::local) async fn admitted_child<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        reference: &Component,
        parent: TaskId,
    ) -> Result<Option<TaskId>> {
        let operation = admission::child_operation(files.volume(), reference.as_str(), b"child")?;
        let task = TaskId::from_bytes(operation.into_bytes());
        let admitted = match self.runtime.task_host().observe_admission(task).await {
            Ok(record) => record,
            Err(Error::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        if admitted.parent != Some(parent) {
            return Err(Error::Conflict("child parent changed".into()));
        }
        if admitted.task_id() != task {
            return Err(Error::Conflict(
                "child request task definition changed".into(),
            ));
        }
        if crate::filesystem::child_agent::is_child(&admitted) {
            return Ok(Some(task));
        }
        let parent_record = self.runtime.task_host().observe_admission(parent).await?;
        let native =
            parent_record.task.name == "acyclic.native_host" && parent_record.task.version == "1";
        if !native || admitted.task.name != "acyclic.native_host" || admitted.task.version != "1" {
            return Err(Error::Conflict(
                "child request task definition changed".into(),
            ));
        }
        Ok(Some(task))
    }

    /// Reads one already admitted direct child of the authenticated parent.
    /// This cannot prepare resources, create a fork, publish branch metadata or
    /// admit an unknown child. Existing journal continuation finishes inside
    /// this read; it cannot report a committed mutation as missing progress.
    pub async fn observe_admitted_child<P: StreamProvider + ?Sized>(
        &self,
        files: &TaskFiles<P>,
        reference: &Component,
        parent: TaskId,
        window: ChildResultWindow,
        maximum: usize,
    ) -> Result<(TaskId, ChildObservation)> {
        if maximum == 0 {
            return Err(Error::Invalid(
                "child observation requires a result bound".into(),
            ));
        }
        let task = self
            .admitted_child(files, reference, parent)
            .await?
            .ok_or_else(|| Error::NotFound("child is not admitted".into()))?;
        let observation = files
            .inner
            .observe_child(reference, window, maximum)
            .await
            .map_err(storage)?;
        if observation.status.is_some_and(|status| status.task != task) {
            return Err(Error::Conflict(
                "child status has a foreign admission".into(),
            ));
        }
        for file in &observation.results {
            let result = ChildTurnResult::parse(&file.bytes).map_err(storage)?;
            if result.task != task {
                return Err(Error::Conflict(
                    "child result has a foreign admission".into(),
                ));
            }
        }
        Ok((task, observation))
    }
}
