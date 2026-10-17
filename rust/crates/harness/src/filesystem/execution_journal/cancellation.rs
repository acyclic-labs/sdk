//! Cancellation is an ordinary request followed at the model-stream boundary.
use super::*;
use crate::{
    client::{CommandRequest, CommandResolution},
    task_volume::{Component, FileKey, Query, TaskFile},
};
use futures::StreamExt as _;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn follow_cancellation(&self) -> Result<()> {
        let (Some(files), Some((owner, _))) = (&self.task_files, &self.owner) else {
            return std::future::pending().await;
        };
        let task = owner.task_id();
        let mut observed = files
            .inner
            .observe(
                Query::File(FileKey::Task {
                    task: Component::new(task.to_string())?,
                    file: TaskFile::Request(Component::new(format!("cancel-{task}"))?),
                }),
                None,
                1,
            )
            .await?;
        let cancelled = |file: &crate::task_volume::FileValue| {
            let request = serde_json::from_slice::<CommandResolution>(&file.bytes)
                .ok()
                .and_then(|value| match value {
                    CommandResolution::Submit { request } => Some(request),
                    _ => None,
                })
                .or_else(|| serde_json::from_slice::<CommandRequest>(&file.bytes).ok());
            matches!(request, Some(CommandRequest::Cancel { task: target }) if target == task)
        };
        if observed
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.files.iter().any(cancelled))
        {
            return Ok(());
        }
        while let Some(change) = observed.changes.next().await {
            let change = change?;
            if change.file.iter().chain(&change.files).any(cancelled) {
                return Ok(());
            }
        }
        Err(Error::Storage("cancellation follow ended".into()))
    }
}
