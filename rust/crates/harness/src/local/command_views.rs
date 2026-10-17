//! Lifecycle and attention files projected from the authenticated task owner.
use super::*;
use crate::Outcome;
use crate::{
    runtime::TaskStateProvider,
    task_volume::{Component, FileKey, FileValue, Query, TaskFile},
};
use serde_json::{Value, json};

impl LocalHarness {
    /// Refreshes lifecycle files from the authenticated task owner after progress.
    pub async fn refresh_command_task(&self, task: TaskId) -> Result<()> {
        let admission = self.runtime.task_host().observe_admission(task).await?;
        let key = FileKey::Task {
            task: Component::new(task.to_string()).map_err(storage)?,
            file: TaskFile::Status,
        };
        loop {
            // Read the publication cut before observing canonical state. A
            // competing refresh forces both reads again, never a stale rewrite.
            let snapshot = self
                .task_files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            let mut error = None;
            let mut state = match self.runtime.task_host().outcome(task).await? {
                Some(Outcome::Succeeded(_)) => "completed",
                Some(Outcome::Failed { message }) => {
                    error = Some(message);
                    "failed"
                }
                Some(Outcome::Cancelled) => "cancelled",
                Some(Outcome::Indeterminate { .. }) => "indeterminate",
                None => "pending",
            };
            let settings_key =
                FileKey::parse(&format!(".ui/task-{task}/settings.json")).map_err(storage)?;
            let settings = self
                .task_files
                .inner
                .snapshot(&Query::File(settings_key), 1)
                .await
                .map_err(storage)?;
            if settings
                .files
                .first()
                .map(|file| serde_json::from_slice::<Value>(&file.bytes).map_err(storage))
                .transpose()?
                .is_some_and(|value| value["deleted"] == true)
            {
                state = "deleted";
            }
            let attention = admission.task.name == "acyclic.stock_turn"
                && matches!(state, "completed" | "failed" | "indeterminate");
            let mut value = json!({"task":task,"parent":admission.parent,"state":state,"attention":attention,"owner_agent":self.signed.agent()});
            if let Some(error) = error {
                value["error"] = Value::String(error);
            }
            let bytes = serde_json::to_vec(&value).map_err(storage)?;
            match self
                .task_files
                .inner
                .append(snapshot.generation, &FileValue::new(key.clone(), bytes))
                .await
            {
                Ok(_) => {
                    self.project_attention(task).await?;
                    return Ok(());
                }
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
    }
    async fn project_attention(&self, task: TaskId) -> Result<()> {
        let key = FileKey::parse(".ui/config/attention.json").map_err(storage)?;
        loop {
            let snapshot = self
                .task_files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            let mut tasks: Vec<TaskId> = snapshot
                .files
                .first()
                .map(|file| -> Result<_> {
                    let value: Value = serde_json::from_slice(&file.bytes).map_err(storage)?;
                    serde_json::from_value(value["tasks"].clone()).map_err(storage)
                })
                .transpose()?
                .unwrap_or_default();
            let status_key = FileKey::Task {
                task: Component::new(task.to_string()).map_err(storage)?,
                file: TaskFile::Status,
            };
            let status = self
                .task_files
                .inner
                .snapshot(&Query::File(status_key), 1)
                .await
                .map_err(storage)?;
            let attention = status
                .files
                .first()
                .map(|file| serde_json::from_slice::<Value>(&file.bytes).map_err(storage))
                .transpose()?
                .is_some_and(|value| value["attention"] == true);
            tasks.retain(|old| *old != task);
            if attention {
                tasks.push(task);
            }
            let excess = tasks.len().saturating_sub(self.limits.context_messages);
            tasks.drain(..excess);
            let bytes = serde_json::to_vec(&json!({"tasks":tasks})).map_err(storage)?;
            if bytes.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid("attention exceeds owner byte bound".into()));
            }
            match self
                .task_files
                .inner
                .append(snapshot.generation, &FileValue::new(key.clone(), bytes))
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
    }
}
