//! Session boundaries only. Native subagents and native tools are unmanaged.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, TaskFile};

impl HostWorkspaceOwner {
    pub(super) async fn hook(
        &mut self,
        operation: OperationId,
        host: &str,
        event: &str,
        input: Value,
    ) -> Result<Value> {
        if !matches!(
            event,
            "SessionStart" | "Stop" | "SessionEnd" | "NativeSession"
        ) {
            return Ok(json!({}));
        }
        let (task, managed) = if let Some(task) = input.get("task") {
            let task = serde_json::from_value(task.clone()).map_err(storage)?;
            (task, self.require_native(task).await?.is_some())
        } else {
            let session = input
                .get("session_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("native session identity absent".into()))?;
            let operation = context::publication_operation(
                b"acyclic.native-root/v1\0",
                &serde_json::to_vec(&(host, session)).map_err(storage)?,
            );
            (
                self.local.admit_host_task(operation, host, session).await?,
                false,
            )
        };
        // A managed boundary is an acknowledgement, not view admission or
        // capture. Only its physical process owner can retire and capture it.
        if managed && matches!(event, "Stop" | "SessionEnd") {
            return Ok(json!({}));
        }
        self.attach(task).await?;
        if event == "NativeSession" {
            if let Some(session) = input.get("native_session") {
                let object = session.as_object().ok_or_else(|| {
                    Error::Invalid("native session reference must be an object".into())
                })?;
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Invalid("native session id absent".into()))?;
                if id.is_empty()
                    || id.len() > 256
                    || id.chars().any(char::is_control)
                    || object
                        .keys()
                        .any(|key| !matches!(key.as_str(), "id" | "file"))
                    || object.get("file").is_some_and(|value| {
                        !value.is_null()
                            && !value
                                .as_str()
                                .is_some_and(|file| Path::new(file).is_absolute())
                    })
                {
                    return Err(Error::Invalid("invalid native session reference".into()));
                }
                self.publish(
                    task,
                    FileValue::new(
                        context_key(task, "session")?,
                        serde_json::to_vec(session).map_err(storage)?,
                    ),
                )
                .await?;
                return Ok(session.clone());
            }
            let snapshot = self
                .files(task)
                .await?
                .snapshot(&Query::File(context_key(task, "session")?), 1)
                .await
                .map_err(storage)?;
            return snapshot.files.first().map_or_else(
                || Err(Error::NotFound("native session reference absent".into())),
                |file| serde_json::from_slice(&file.bytes).map_err(storage),
            );
        }
        if matches!(event, "Stop" | "SessionEnd") {
            let attached = self.attached(task)?;
            attached
                .workspace
                .capture(&attached.context, operation)
                .await?;
            return Ok(json!({}));
        }
        Ok(
            json!({"hookSpecificOutput":{"hookEventName":event,"additionalContext":format!("Harness task {task}. Use Acyclic spawn/send/wait/archive for isolated sessions and acyclic fs merge agents/<ref> to merge. Native subagents retain their host behavior and are not harness forks.")}}),
        )
    }

    pub(super) async fn locate(&mut self, cwd: &Path) -> Result<Value> {
        let cwd = std::fs::canonicalize(cwd).map_err(storage)?;
        // A native session enters only after its view has been attached. Cwd
        // discovery observes those held admissions; it must never reconstruct
        // the task tree or acquire unrelated views as a side effect of a read.
        let mut selected = self
            .views
            .iter()
            .filter(|(_, attached)| cwd.starts_with(attached.workspace.path()))
            .collect::<Vec<_>>();
        selected.sort_by_key(|(_, attached)| {
            std::cmp::Reverse(attached.workspace.path().components().count())
        });
        let Some((task, first)) = selected.first() else {
            return Err(Error::NotFound(
                "no native task selects this directory".into(),
            ));
        };
        if selected
            .get(1)
            .is_some_and(|(_, second)| second.workspace.path() == first.workspace.path())
        {
            return Err(Error::Conflict(
                "multiple native sessions select this directory; use --task".into(),
            ));
        }
        if cwd.starts_with(first.workspace.path().join(host_workspace::HOST_VIEW_ROOT)) {
            return Err(Error::NotFound(
                "native cwd selects no held agent view".into(),
            ));
        }
        first.workspace.verify(&first.context).await?;
        self.location(**task)
    }

    pub(super) async fn publish(&self, task: TaskId, file: FileValue) -> Result<()> {
        let owner = self
            .attached(task)?
            .context
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("native owner absent".into()))?;
        let files = self.files(task).await?;
        let snapshot = files
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .map_err(storage)?;
        crate::task_volume::publish(&files.inner, snapshot.generation, &file, || async {
            let (condition, deadline) = owner.file_publication_condition().await?;
            Ok::<_, Error>((vec![condition], deadline))
        })
        .await?;
        Ok(())
    }
}

pub(super) fn context_key(task: TaskId, name: &str) -> Result<FileKey> {
    Ok(FileKey::Task {
        task: Component::new(task.to_string()).map_err(storage)?,
        file: TaskFile::Context(Component::new(format!("native-{name}")).map_err(storage)?),
    })
}
