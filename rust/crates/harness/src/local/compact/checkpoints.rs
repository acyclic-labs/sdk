use super::*;
use crate::conversation::ContentResidencyVerifier;
use crate::{
    context::{SummaryCheckpoint, SummarySelection},
    task_volume::{FileKey, TaskVolume},
};
use acyclic_stream::StreamProvider;

fn continuation_key(task: TaskId) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string()).map_err(storage)?,
        file: TaskFile::Context(Component::new("selected-conversation").map_err(storage)?),
    })
}

pub(super) fn checkpoint_key(
    task: TaskId,
    position: u64,
    operation: OperationId,
) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string()).map_err(storage)?,
        file: TaskFile::Context(
            Component::new(format!("compaction-{position:020}-{operation}")).map_err(storage)?,
        ),
    })
}

fn selection(bytes: &[u8], maximum: u64) -> Result<Option<SummarySelection>> {
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid(
            "summary selection exceeds file bound".into(),
        ));
    }
    let value: Value = serde_json::from_slice(bytes).map_err(storage)?;
    if value.get("checkpoint").is_none() {
        return Ok(None);
    }
    serde_json::from_value(value).map(Some).map_err(storage)
}

// The source's predecessor and observation fence are both immutable. Replays
// cannot replace a descendant, nor undo a later explicit rewind selection.
pub(in crate::local) fn advances<const CHECK: bool>(
    current: Option<&SummarySelection>,
    next: &SummaryCheckpoint,
    key: &FileKey,
) -> bool {
    if !CHECK {
        return current.is_none_or(|current| current.checkpoint != *key);
    }
    current.map(|current| &current.checkpoint) == next.boundary.previous.as_ref()
        && current.is_none_or(|current| next.boundary.captured_at > current.selected_at)
}

pub(in crate::local) async fn select<P: StreamProvider + ?Sized, const CHECK: bool>(
    volume: &TaskVolume<P>,
    target: TaskId,
    next: &SummaryCheckpoint,
    key: &FileKey,
    maximum: u64,
) -> Result<bool> {
    let head = commands::compact_key(target)?;
    let head = &head;
    select_with::<_, _, _, CHECK>(
        next,
        key,
        move || async move {
            let cut = volume
                .snapshot(&Query::File(head.clone()), 1)
                .await
                .map_err(storage)?;
            let current = match cut.files.first() {
                Some(file) => selection(&file.bytes, maximum)?,
                None => volume
                    .snapshot(&Query::File(continuation_key(target)?), 1)
                    .await
                    .map_err(storage)?
                    .files
                    .first()
                    .map(|file| selection(&file.bytes, maximum))
                    .transpose()?
                    .flatten(),
            };
            Ok((cut.generation, current))
        },
        move |generation, value| async move {
            let bytes = serde_json::to_vec(&value).map_err(storage)?;
            match volume
                .append(generation, &FileValue::new(head.clone(), bytes))
                .await
            {
                Ok(_) => Ok(true),
                Err(crate::task_volume::Error::Conflict(_)) => Ok(false),
                Err(error) => Err(storage(error)),
            }
        },
    )
    .await
}

// The production selection protocol depends only on the volume's atomic
// snapshot and conditional-append contract. Keep decoding/indexing outside the
// protocol model; TaskVolume models verify those implementation details.
async fn select_with<G, Read, Append, const CHECK: bool>(
    next: &SummaryCheckpoint,
    key: &FileKey,
    read: impl Fn() -> Read + Send + Sync,
    append: impl Fn(G, SummarySelection) -> Append + Send + Sync,
) -> Result<bool>
where
    G: Send,
    Read: Future<Output = Result<(G, Option<SummarySelection>)>> + Send,
    Append: Future<Output = Result<bool>> + Send,
{
    loop {
        let (generation, current) = read().await?;
        if current
            .as_ref()
            .is_some_and(|current| current.checkpoint == *key)
        {
            return Ok(true);
        }
        if !advances::<CHECK>(current.as_ref(), next, key) {
            return Ok(false);
        }
        let value = SummarySelection {
            parent: None,
            checkpoint: key.clone(),
            selected_at: next.boundary.captured_at,
            through: next.boundary.frozen.then(|| next.boundary.through.clone()),
        };
        if append(generation, value).await? {
            return Ok(true);
        }
    }
}

impl LocalHarness {
    pub(in crate::local) async fn selected_checkpoint(
        &self,
        task: TaskId,
    ) -> Result<Option<(SummarySelection, SummaryCheckpoint)>> {
        let snapshot = self
            .task_files
            .inner
            .snapshot(&Query::File(commands::compact_key(task)?), 1)
            .await
            .map_err(storage)?;
        let selected = snapshot
            .files
            .first()
            .map(|file| selection(&file.bytes, self.limits.file_bytes))
            .transpose()?
            .flatten();
        let selected = match selected {
            Some(value) => Some(value),
            None => self.continuation_selection(task).await?,
        };
        match selected {
            Some(value) => {
                let checkpoint = self.read_checkpoint(&value.checkpoint).await?;
                Ok(Some((value, checkpoint)))
            }
            None => Ok(None),
        }
    }

    pub(in crate::local) async fn continuation_selection(
        &self,
        task: TaskId,
    ) -> Result<Option<SummarySelection>> {
        let snapshot = self
            .task_files
            .inner
            .snapshot(&Query::File(continuation_key(task)?), 1)
            .await
            .map_err(storage)?;
        snapshot
            .files
            .first()
            .map(|file| selection(&file.bytes, self.limits.file_bytes))
            .transpose()
            .map(Option::flatten)
    }

    pub(in crate::local) async fn retain_continuation(
        &self,
        task: TaskId,
        parent: TaskId,
        mut value: SummarySelection,
    ) -> Result<(SummarySelection, SummaryCheckpoint)> {
        value.parent = Some(parent);
        value.through = None; // A continuation has its own live history subtree.
        let key = continuation_key(task)?;
        let bytes = serde_json::to_vec(&value).map_err(storage)?;
        loop {
            let snapshot = self
                .task_files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            if let Some(file) = snapshot.files.first() {
                let retained = selection(&file.bytes, self.limits.file_bytes)?
                    .ok_or_else(|| Error::Invalid("continuation has no checkpoint".into()))?;
                if retained.parent != Some(parent) {
                    return Err(Error::Conflict(
                        "continuation belongs to another conversation".into(),
                    ));
                }
                let checkpoint = self.read_checkpoint(&retained.checkpoint).await?;
                return Ok((retained, checkpoint));
            }
            match self
                .task_files
                .inner
                .create(
                    snapshot.generation,
                    &FileValue::new(key.clone(), bytes.clone()),
                )
                .await
            {
                Ok(_) => {
                    let checkpoint = self.read_checkpoint(&value.checkpoint).await?;
                    return Ok((value, checkpoint));
                }
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
    }

    pub(in crate::local) async fn read_checkpoint(
        &self,
        key: &FileKey,
    ) -> Result<SummaryCheckpoint> {
        if !matches!(key, FileKey::Task { file: TaskFile::Context(name), .. } if name.as_str().starts_with("compaction-"))
        {
            return Err(Error::Invalid(
                "selection is not a protected summary checkpoint".into(),
            ));
        }
        let snapshot = self
            .task_files
            .inner
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        let file = snapshot
            .files
            .first()
            .ok_or_else(|| Error::NotFound("summary checkpoint".into()))?;
        if file.bytes.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid("checkpoint exceeds file bound".into()));
        }
        let value: SummaryCheckpoint = serde_json::from_slice(&file.bytes).map_err(storage)?;
        for file in [&value.source, &value.summary, &value.boundary.manifest] {
            self.limits.validate_file(file, &mut None)?;
        }
        Ok(value)
    }

    pub(in crate::local) async fn checkpoint_text(
        &self,
        selected: &SummarySelection,
    ) -> Result<String> {
        let checkpoint = self.read_checkpoint(&selected.checkpoint).await?;
        self.summary_text(&checkpoint).await
    }

    pub(in crate::local) async fn summary_text(
        &self,
        checkpoint: &SummaryCheckpoint,
    ) -> Result<String> {
        let bytes = self
            .context_reader()?
            .read(&checkpoint.summary, self.limits.file_bytes, None)?
            .await?;
        String::from_utf8(bytes.as_ref().to_vec()).map_err(storage)
    }

    pub(super) async fn publish_checkpoint(
        &self,
        operation: OperationId,
        target: TaskId,
        checkpoint: &SummaryCheckpoint,
    ) -> Result<(FileKey, bool)> {
        if checkpoint.boundary.task != target {
            return Err(Error::Conflict(
                "summary belongs to another conversation".into(),
            ));
        }
        let key = checkpoint_key(target, checkpoint.boundary.through.position, operation)?;
        let bytes = serde_json::to_vec(checkpoint).map_err(storage)?;
        if bytes.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid("checkpoint exceeds file bound".into()));
        }
        loop {
            let snapshot = self
                .task_files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            if let Some(file) = snapshot.files.first() {
                if file.bytes.as_ref() != bytes {
                    return Err(Error::Conflict(
                        "summary checkpoint identity changed".into(),
                    ));
                }
                break;
            }
            match self
                .task_files
                .inner
                .create(
                    snapshot.generation,
                    &FileValue::new(key.clone(), bytes.clone()),
                )
                .await
            {
                Ok(_) => break,
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
        let selected = select::<_, true>(
            &self.task_files.inner,
            target,
            checkpoint,
            &key,
            self.limits.file_bytes,
        )
        .await?;
        Ok((key, selected))
    }
}

#[cfg(all(test, acyclic_harness_loom))]
mod model;
