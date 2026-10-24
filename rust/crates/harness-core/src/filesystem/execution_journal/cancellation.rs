//! Cancellation is an ordinary request followed during cancellable execution waits.
use super::*;
use crate::task_volume::{Query, cancellation_key, is_cancellation};
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
            .observe(Query::File(cancellation_key(task)?), None, 1)
            .await?;
        let cancelled =
            |file: &crate::task_volume::FileValue| is_cancellation(task, &file.key, &file.bytes);
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
