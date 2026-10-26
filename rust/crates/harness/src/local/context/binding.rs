//! The admitted task's protected pointer to its exact configuration manifest.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, TaskFile, TaskVolume};
use acyclic_stream::StreamProvider;

fn key(task: TaskId) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string())?,
        file: TaskFile::Context(Component::new("context-manifest")?),
    })
}

pub(in crate::local) async fn retain<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    manifest: &FileRef,
    maximum: u64,
) -> Result<()> {
    let bytes = serde_json::to_vec(manifest).map_err(storage)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid(
            "manifest reference exceeds file bound".into(),
        ));
    }
    let generation = volume.head().await?;
    crate::task_volume::publish(
        volume,
        generation,
        &FileValue::new(key(task)?, bytes),
        || std::future::ready(Ok::<_, Error>((Vec::new(), None))),
    )
    .await?;
    Ok(())
}
