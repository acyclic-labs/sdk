use super::*;
use crate::{
    context::{Context, SummaryBoundary, SummarySource},
    runtime::TaskStateProvider,
    task_volume::{Channel, FileKey, ToolFile},
};
use futures::TryStreamExt;

impl LocalHarness {
    pub(super) async fn summary_source(&self, target: TaskId) -> Result<SummarySource> {
        let selected = self.selected_checkpoint(target).await?;
        let query = Query::Turns(Component::new(target.to_string()).map_err(storage)?);
        let now = self.task_files.inner.cut(&query).await.map_err(storage)?;
        let frozen = selected.as_ref().and_then(|(selection, checkpoint)| {
            (checkpoint.boundary.task == target)
                .then_some(selection.through.as_ref())
                .flatten()
        });
        let through = frozen.cloned().unwrap_or_else(|| now.clone());
        let same_task = selected
            .as_ref()
            .filter(|(_, checkpoint)| checkpoint.boundary.task == target);
        let admission = self.runtime.task_host().observe_admission(target).await?;
        let owner = self
            .runtime
            .harness()
            .durable_context(target, admission.operation_id, None)
            .await?;
        let command: WorkflowCommand = serde_json::from_value(
            admission
                .input
                .get("model")
                .ok_or_else(|| Error::Invalid("selected task has no model context".into()))?
                .clone(),
        )
        .map_err(storage)?;
        let bytes = owner.read_file(&command.payload, None).await?;
        let input: ModelTaskCommand = serde_json::from_slice(&bytes).map_err(storage)?;
        let prior = match &selected {
            Some((_, checkpoint)) => Some(self.summary_text(checkpoint).await?),
            None => self.resolved_value(target, "summary").await?,
        };
        let mut text = crate::context::summary_source::retained_source::<true>(
            prior.as_deref(),
            same_task.is_none().then_some(&input.input),
            self.limits,
        )?;
        if let Some((_, checkpoint)) = same_task {
            let mut cursor = checkpoint.boundary.through.clone();
            if cursor.position > through.position {
                return Err(Error::Invalid("rewind precedes selected checkpoint".into()));
            }
            if cursor.position < through.position {
                let mut changes = self
                    .task_files
                    .inner
                    .follow(query.clone(), cursor.clone())
                    .await
                    .map_err(storage)?;
                while cursor.position < through.position {
                    let change = changes.try_next().await.map_err(storage)?.ok_or_else(|| {
                        Error::Invalid("summary history ended before its cut".into())
                    })?;
                    for file in change.file.iter().chain(&change.files) {
                        append_file(&mut text, file, self.limits)?;
                    }
                    cursor = change.cursor;
                }
            }
        } else {
            let snapshot = self
                .task_files
                .inner
                .heads(&query, frozen.cloned(), self.limits.context_messages)
                .await
                .map_err(storage)?;
            // `now` is the boundary for this capture. A concurrent writer must not
            // contribute records newer than it; snapshot_at handles that cut.
            let snapshot = if snapshot.cursor.position == through.position {
                snapshot
            } else {
                self.task_files
                    .inner
                    .heads(&query, Some(through.clone()), self.limits.context_messages)
                    .await
                    .map_err(storage)?
            };
            for file in snapshot.files {
                for bytes in file.segments {
                    append_bytes(
                        &mut text,
                        &file.key,
                        Some(Channel::Text),
                        &bytes,
                        self.limits,
                    )?;
                }
            }
        }
        let mut outcome_included =
            same_task.is_some_and(|(_, checkpoint)| checkpoint.boundary.outcome_included);
        // An explicit rewind is an exact history view, including its outcome.
        if !outcome_included && frozen.is_none() {
            if let Some(outcome) = self.runtime.task_host().outcome(target).await? {
                append_source(
                    &mut text,
                    &serde_json::to_string(&outcome).map_err(storage)?,
                    self.limits,
                )?;
                outcome_included = true;
            }
        }
        let mut context = Context::default();
        context.messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(text),
        });
        Ok(SummarySource {
            context,
            boundary: SummaryBoundary {
                task: target,
                through,
                captured_at: now.position,
                previous: selected
                    .as_ref()
                    .map(|(selection, _)| selection.checkpoint.clone()),
                outcome_included,
                frozen: frozen.is_some(),
                manifest: input.context_manifest,
                model: self.command_model(Some(target)).await?.name,
            },
        })
    }
}

fn append_file(text: &mut String, file: &FileValue, limits: Limits) -> Result<()> {
    append_bytes(text, &file.key, file.channel, &file.bytes, limits)
}
fn append_bytes(
    text: &mut String,
    key: &FileKey,
    channel: Option<Channel>,
    bytes: &[u8],
    limits: Limits,
) -> Result<()> {
    if bytes.len() as u64 > limits.file_bytes {
        return Err(Error::Invalid("compaction turn exceeds file bound".into()));
    }
    match key {
        FileKey::Task {
            file: TaskFile::TurnRequest(_),
            ..
        } => {
            // Decode only messages: schemas and definitions are irrelevant to a
            // summary and must never be copied into its source.
            #[derive(serde::Deserialize)]
            struct Request {
                messages: Vec<ModelMessage>,
            }
            let request: Request = serde_json::from_slice(bytes).map_err(storage)?;
            if let Some(message) = request
                .messages
                .iter()
                .rev()
                .find(|m| m.role == ModelRole::User)
            {
                append_source(
                    text,
                    &serde_json::to_string(&message.content).map_err(storage)?,
                    limits,
                )?;
            }
        }
        FileKey::Task {
            file:
                TaskFile::Chunk { .. }
                | TaskFile::TextHead {
                    channel: Channel::Text,
                    ..
                },
            ..
        } if channel == Some(Channel::Text) => {
            let delta = std::str::from_utf8(bytes).map_err(storage)?;
            if text.len().saturating_add(delta.len()) as u64 > limits.file_bytes {
                return Err(Error::Invalid(
                    "compaction source exceeds file bound".into(),
                ));
            }
            text.push_str(delta);
        }
        FileKey::Task {
            file:
                TaskFile::Tool {
                    file: ToolFile::Result,
                    ..
                },
            ..
        } => append_source(text, std::str::from_utf8(bytes).map_err(storage)?, limits)?,
        _ => {}
    }
    Ok(())
}
