use crate::{
    Error, Result,
    task_volume::{FileValue, TaskVolume},
};
use acyclic_stream::{CommitCondition, StreamProvider};

// The owner condition and the volume head must be checked by the same provider
// transaction. Retries retain the exact owner condition and immutable bytes.
pub(crate) async fn publish<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    file: &FileValue,
    condition: CommitCondition,
    deadline: Option<u64>,
) -> Result<()> {
    super::append::publish(volume, file, Some(condition), deadline)
        .await
        .map_err(|error| match error {
            crate::task_volume::Error::ImmutableConflict(_) => {
                Error::Conflict("task request publication changed on retry".into())
            }
            error => storage(error),
        })
}

pub(super) fn storage(error: crate::task_volume::Error) -> Error {
    Error::Storage(error.to_string())
}
