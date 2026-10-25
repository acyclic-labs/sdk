use super::*;
use crate::{
    context::{Context, SummaryBoundary, SummarySource},
    runtime::TaskStateProvider,
    task_volume::{Channel, FileKey, ToolFile},
};
use futures::TryStreamExt;

mod output;
use output::OutputStreams;

impl LocalHarness {
    pub(super) async fn summary_source(&self, target: TaskId) -> Result<SummarySource> {
        let (cut, selection, identity) = super::checkpoints::capture_selection(
            &self.task_files.inner,
            target,
            self.limits.file_bytes,
        )
        .await?;
        let selected = match selection {
            Some(selection) => {
                let checkpoint = self.read_checkpoint(&selection.checkpoint).await?;
                Some((selection, checkpoint))
            }
            None => None,
        };
        let query = Query::Turns(Component::new(target.to_string()).map_err(storage)?);
        let now = cut.cursor;
        let frozen = selected.as_ref().and_then(|(selection, checkpoint)| {
            (checkpoint.boundary.task == target)
                .then_some(selection.through.as_ref())
                .flatten()
        });
        let through = frozen.cloned().unwrap_or_else(|| now.clone());
        let same_task = selected
            .as_ref()
            .filter(|(_, checkpoint)| checkpoint.boundary.task == target);
        let input = self.summary_input(target).await?;
        let prior = match &selected {
            Some((selection, checkpoint)) => {
                Some(self.retained_text::<true>(selection, checkpoint).await?)
            }
            None => self.resolved_value(target, "summary").await?,
        };
        let raw_prompt = selected
            .as_ref()
            .and_then(|(selection, _)| selection.continuation.as_ref())
            .map(|turn| ModelContent::Text(turn.prompt.clone()));
        let mut text = crate::context::summary_source::retained_source::<true>(
            prior,
            same_task
                .is_none()
                .then_some(raw_prompt.as_ref().unwrap_or(&input.input)),
            self.limits,
        )?;
        if let Some((_, checkpoint)) = same_task {
            self.append_turns(
                &mut text,
                target,
                &checkpoint.boundary.through,
                &through,
                true,
            )
            .await?;
        } else {
            self.append_snapshot(&mut text, &query, &through, raw_prompt.is_some())
                .await?;
        }
        let mut outcome_included =
            same_task.is_some_and(|(_, checkpoint)| checkpoint.boundary.outcome_included);
        // An explicit rewind is an exact history view, including its outcome.
        if !outcome_included
            && frozen.is_none()
            && let Some(outcome) = self.runtime.task_host().outcome(target).await?
        {
            append_source(
                &mut text,
                &serde_json::to_string(&outcome).map_err(storage)?,
                self.limits,
            )?;
            outcome_included = true;
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
                previous: identity,
                outcome_included,
                frozen: frozen.is_some(),
                manifest: input.context_manifest,
                model: self.command_model(Some(target)).await?.name,
            },
        })
    }

    async fn summary_input(&self, target: TaskId) -> Result<ModelTaskCommand> {
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
        serde_json::from_slice(&bytes).map_err(storage)
    }

    async fn append_snapshot(
        &self,
        text: &mut String,
        query: &Query,
        through: &crate::task_volume::Cursor,
        skip_requests: bool,
    ) -> Result<()> {
        // Reuse the current materialized index when it is the admitted cut.
        // A later producer may also overflow the file bound, so retry that
        // case at the immutable cut rather than rejecting an earlier history.
        let snapshot = match self
            .task_files
            .inner
            .heads(query, None, self.limits.context_messages)
            .await
        {
            Ok(snapshot) if snapshot.cursor == *through => snapshot,
            Ok(_) | Err(crate::task_volume::Error::LimitExceeded) => self
                .task_files
                .inner
                .heads(query, Some(through.clone()), self.limits.context_messages)
                .await
                .map_err(storage)?,
            Err(cause) => return Err(storage(cause)),
        };
        let mut output = OutputStreams::default();
        for file in snapshot.files {
            if skip_requests
                && matches!(
                    file.key,
                    FileKey::Task {
                        file: TaskFile::TurnRequest(_),
                        ..
                    }
                )
            {
                continue;
            }
            for bytes in file.segments {
                output.observe(&file.key, &bytes, self.limits)?;
                append_bytes(text, &file.key, Some(Channel::Text), &bytes, self.limits)?;
            }
        }
        output.append(text, self.limits)
    }

    pub(in crate::local) async fn append_turns(
        &self,
        text: &mut String,
        task: TaskId,
        from: &crate::task_volume::Cursor,
        through: &crate::task_volume::Cursor,
        requests: bool,
    ) -> Result<()> {
        if from.position > through.position {
            return Err(Error::Invalid("rewind precedes selected checkpoint".into()));
        }
        if from.position == through.position {
            return Ok(());
        }
        let query = Query::Turns(Component::new(task.to_string()).map_err(storage)?);
        let mut cursor = from.clone();
        let mut output = OutputStreams::default();
        let mut changes = self
            .task_files
            .inner
            .follow(query, cursor.clone())
            .await
            .map_err(storage)?;
        while cursor.position < through.position {
            let change = changes
                .try_next()
                .await
                .map_err(storage)?
                .ok_or_else(|| Error::Invalid("summary history ended before its cut".into()))?;
            for file in change.file.iter().chain(&change.files) {
                output.observe(&file.key, &file.bytes, self.limits)?;
                if requests
                    || matches!(
                        file.key,
                        FileKey::Task {
                            file: TaskFile::Tool {
                                file: ToolFile::Result,
                                ..
                            },
                            ..
                        }
                    )
                {
                    append_file(text, file, self.limits)?;
                }
            }
            cursor = change.cursor;
        }
        output.append(text, self.limits)
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
        } => {
            let value: serde_json::Value = serde_json::from_slice(bytes).map_err(storage)?;
            let metadata = match &value {
                serde_json::Value::Object(fields) => {
                    std::borrow::Cow::Owned(crate::local::tool_metadata::encode(fields)?)
                }
                _ => std::borrow::Cow::Borrowed(bytes),
            };
            append_source(
                text,
                std::str::from_utf8(&metadata).map_err(storage)?,
                limits,
            )?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_process_results_render_canonical_streams_once() {
        let limits = Limits {
            file_bytes: 256,
            ..Limits::default()
        };
        let task = Component::new("t").unwrap();
        let call = Component::new("x").unwrap();
        let key = |file| FileKey::Task {
            task: task.clone(),
            file: TaskFile::Tool {
                turn: 1,
                call: call.clone(),
                file,
            },
        };
        let stdout = "O".repeat(64);
        let stderr = "E".repeat(64);
        let result = serde_json::to_vec(&serde_json::json!({"success":true,"exit_code":0,"stdout":stdout,"stderr":stderr,"stop":null})).unwrap();
        let mut text = String::new();
        append_bytes(
            &mut text,
            &key(ToolFile::Result),
            Some(Channel::Text),
            &result,
            limits,
        )
        .unwrap();
        let mut output = OutputStreams::default();
        output
            .observe(&key(ToolFile::Stdout(0)), stdout.as_bytes(), limits)
            .unwrap();
        output
            .observe(&key(ToolFile::Stderr(0)), stderr.as_bytes(), limits)
            .unwrap();
        output
            .append(&mut text, limits)
            .expect("canonical stream projection must fit the unchanged file bound");
        assert_eq!(text.matches(stdout.as_str()).count(), 1);
        assert_eq!(text.matches(stderr.as_str()).count(), 1);
        assert!(text.contains("success"));
        assert!(text.contains("exit_code"));
    }

    #[test]
    fn non_object_terminal_results_keep_their_original_summary() {
        let key = FileKey::Task {
            task: Component::new("t").unwrap(),
            file: TaskFile::Tool {
                turn: 1,
                call: Component::new("x").unwrap(),
                file: ToolFile::Result,
            },
        };
        let mut text = String::new();
        append_bytes(
            &mut text,
            &key,
            Some(Channel::Text),
            br#""opaque stdout and stderr""#,
            Limits::default(),
        )
        .unwrap();
        assert!(text.contains("opaque stdout and stderr"));
    }
}
