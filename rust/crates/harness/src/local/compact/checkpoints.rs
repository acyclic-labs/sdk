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
    identity: Option<[u8; 32]>,
    next: &SummaryCheckpoint,
    key: &FileKey,
) -> bool {
    if !CHECK {
        return current.is_none_or(|current| current.checkpoint != *key);
    }
    identity == next.boundary.previous
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
    loop {
        let (cut, current, identity) = capture_selection(volume, target, maximum).await?;
        if current
            .as_ref()
            .is_some_and(|current| current.checkpoint == *key)
        {
            return Ok(true);
        }
        if !advances::<CHECK>(current.as_ref(), identity, next, key) {
            return Ok(false);
        }
        let value = SummarySelection {
            continuation: None,
            checkpoint: key.clone(),
            selected_at: next.boundary.captured_at,
            through: next.boundary.frozen.then(|| next.boundary.through.clone()),
        };
        let bytes = serde_json::to_vec(&value).map_err(storage)?;
        match volume
            .append(cut.generation, &FileValue::new(head.clone(), bytes))
            .await
        {
            Ok(_) => return Ok(true),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
}

// Selection and history are sampled under the same real task-volume index lock.
// Both capture and publication use this observation, including continuation
// fallback; a concurrent rewind cannot be paired with a later history cut.
pub(in crate::local) async fn capture_selection<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    maximum: u64,
) -> Result<(
    crate::task_volume::Snapshot,
    Option<SummarySelection>,
    Option<[u8; 32]>,
)> {
    let keys = [commands::compact_key(task)?, continuation_key(task)?];
    let query = Query::Turns(Component::new(task.to_string()).map_err(storage)?);
    let cut = volume
        .snapshot_files(&keys, &query)
        .await
        .map_err(storage)?;
    let mut selected = None;
    let mut identity = None;
    for file in &cut.files {
        if let Some(value) = selection(&file.bytes, maximum)? {
            selected = Some(value);
            identity = Some(*blake3::hash(&file.bytes).as_bytes());
            break;
        }
    }
    Ok((cut, selected, identity))
}

impl LocalHarness {
    #[cfg(test)]
    pub(in crate::local) async fn selected_checkpoint(
        &self,
        task: TaskId,
    ) -> Result<Option<(SummarySelection, SummaryCheckpoint)>> {
        let (_, selected, _) =
            capture_selection(&self.task_files.inner, task, self.limits.file_bytes).await?;
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
        through: crate::task_volume::Cursor,
        prompt: &str,
    ) -> Result<(SummarySelection, SummaryCheckpoint)> {
        value.continuation = Some(crate::context::SummaryContinuation {
            parent,
            through,
            frozen: value.through.is_some(),
            prompt: prompt.into(),
        });
        value.through = None; // A continuation has its own live history subtree.
        let key = continuation_key(task)?;
        let bytes = serde_json::to_vec(&value).map_err(storage)?;
        if bytes.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid("continuation exceeds file bound".into()));
        }
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
                if retained.continuation.as_ref().map(|turn| turn.parent) != Some(parent) {
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
        self.retained_text::<true>(selected, &checkpoint).await
    }

    pub(in crate::local) async fn retained_text<const FROZEN: bool>(
        &self,
        selected: &SummarySelection,
        checkpoint: &SummaryCheckpoint,
    ) -> Result<String> {
        let mut text = self.summary_text(checkpoint).await?;
        let mut tails = Vec::new();
        let mut current = selected.clone();
        while let Some(turn) = current.continuation {
            if tails.len() == self.limits.context_messages {
                return Err(Error::Invalid(
                    "continuation history exceeds message bound".into(),
                ));
            }
            if turn.parent == checkpoint.boundary.task {
                self.append_turns(
                    &mut text,
                    turn.parent,
                    &checkpoint.boundary.through,
                    &turn.through,
                    true,
                )
                .await?;
                if !checkpoint.boundary.outcome_included && (!FROZEN || !turn.frozen) {
                    self.append_completed_outcome(&mut text, turn.parent)
                        .await?;
                }
                break;
            }
            let parent = self
                .continuation_selection(turn.parent)
                .await?
                .ok_or_else(|| Error::Invalid("continuation parent lost its history".into()))?;
            let original = parent
                .continuation
                .as_ref()
                .ok_or_else(|| Error::Invalid("continuation parent has no prompt".into()))?;
            let mut from = original.through.clone();
            from.query = turn.through.query;
            tails.push((
                turn.parent,
                original.prompt.clone(),
                from,
                turn.through,
                FROZEN && turn.frozen,
            ));
            current = parent;
        }
        for (task, prompt, from, through, frozen) in tails.into_iter().rev() {
            super::append_source(&mut text, &prompt, self.limits)?;
            // The immutable authenticated outcome owns the completed answer;
            // chunks are optional projections and may not yet have arrived.
            // Retain intervening tool results at the admitted volume cut.
            self.append_turns(&mut text, task, &from, &through, frozen)
                .await?;
            if !frozen {
                self.append_completed_outcome(&mut text, task).await?;
            }
        }
        Ok(text)
    }

    async fn append_completed_outcome(&self, text: &mut String, task: TaskId) -> Result<()> {
        use crate::runtime::TaskStateProvider;
        let outcome = self
            .runtime
            .task_host()
            .outcome(task)
            .await?
            .ok_or_else(|| {
                Error::Invalid("continued conversation has no terminal outcome".into())
            })?;
        super::append_source(
            text,
            &serde_json::to_string(&outcome).map_err(storage)?,
            self.limits,
        )
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

    pub(in crate::local) async fn publish_checkpoint(
        &self,
        operation: OperationId,
        target: TaskId,
        checkpoint: &SummaryCheckpoint,
    ) -> Result<(FileKey, bool)> {
        publish(
            &self.task_files.inner,
            operation,
            target,
            checkpoint,
            self.limits.file_bytes,
        )
        .await
    }
}

#[cfg(test)]
thread_local! {
    /// Fault switch: treat a retained checkpoint file as completed publication,
    /// skipping the selection write that follows it.
    static RETAINED_MEANS_SELECTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Two durable writes: the immutable checkpoint file, then its selection. A
// replay after either cut reruns both steps; neither implies the other.
pub(in crate::local) async fn publish<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    operation: OperationId,
    target: TaskId,
    checkpoint: &SummaryCheckpoint,
    maximum: u64,
) -> Result<(FileKey, bool)> {
    if checkpoint.boundary.task != target {
        return Err(Error::Conflict(
            "summary belongs to another conversation".into(),
        ));
    }
    let key = checkpoint_key(target, checkpoint.boundary.through.position, operation)?;
    let bytes = serde_json::to_vec(checkpoint).map_err(storage)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::Invalid("checkpoint exceeds file bound".into()));
    }
    loop {
        let snapshot = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if let Some(file) = snapshot.files.first() {
            if file.bytes.as_ref() != bytes {
                return Err(Error::Conflict(
                    "summary checkpoint identity changed".into(),
                ));
            }
            #[cfg(test)]
            if RETAINED_MEANS_SELECTED.with(std::cell::Cell::get) {
                return Ok((key, true));
            }
            break;
        }
        match volume
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
    let selected = select::<_, true>(volume, target, checkpoint, &key, maximum).await?;
    Ok((key, selected))
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod crash_cut_tests;
#[cfg(test)]
#[cfg(acyclic_harness_loom)]
mod model;
