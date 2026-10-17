use crate::task_volume::{Error, FileValue, Query, Result, TaskVolume};
use acyclic_stream::{CommitCondition, StreamProvider};

/// Publishes an immutable file without changing its original ownership fence.
pub(crate) async fn publish<P: StreamProvider + ?Sized>(
    requests: &TaskVolume<P>,
    file: &FileValue,
    condition: Option<CommitCondition>,
    deadline: Option<u64>,
) -> Result<()> {
    loop {
        let snapshot = requests.snapshot(&Query::File(file.key.clone()), 1).await?;
        if let Some(original) = snapshot.files.first() {
            return if original == file {
                Ok(())
            } else {
                Err(Error::ImmutableConflict(file.key.clone()))
            };
        }
        let mut conditions = Vec::with_capacity(1 + usize::from(condition.is_some()));
        conditions.extend(condition.iter().cloned());
        match requests
            .append_if(snapshot.generation, file, conditions, deadline, None)
            .await
        {
            Ok(_) => return Ok(()),
            // Every retry requires actual accepted progress, never a timer or
            // a repeated check of unchanged task state.
            Err(Error::Conflict(next))
                if next.volume == snapshot.generation.volume
                    && next.epoch == snapshot.generation.epoch
                    && next.position > snapshot.generation.position => {}
            Err(error) => return Err(error),
        }
    }
}
