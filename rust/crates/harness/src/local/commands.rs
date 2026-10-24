//! Local request-file effects and the stock command-owner binding.
use super::*;
use crate::{
    client::{CommandControl, CommandRequest, CommandResolution, CommandTaskScope, CommandTool},
    filesystem::{TaskCommandHost, TaskCommandProgress},
    runtime::{TaskContext, TaskStateProvider},
    scheduler::LeaseFence,
    task_volume::{Component, FileKey, FileValue, Query},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::{Value, json};

impl LocalHarness {
    /// Dispatch already-claimed file data with its captured task identity.
    pub async fn dispatch_command(
        &self,
        operation: OperationId,
        target: Option<TaskId>,
        resolution: CommandResolution,
    ) -> Result<Value> {
        self.dispatch_configured_command(operation, target, resolution, &json!({}))
            .await
    }
    pub(crate) fn dispatch_configured_command<'a>(
        &'a self,
        operation: OperationId,
        target: Option<TaskId>,
        resolution: CommandResolution,
        settings: &'a Value,
    ) -> BoxProviderFuture<'a, Result<Value>> {
        // Select the operation before constructing its future. Unrelated
        // command branches must never enter this owner's polling frame.
        Box::pin(async move {
            self.dispatch_effect(operation, target, resolution, settings)?
                .await
        })
    }
    fn dispatch_effect<'a>(
        &'a self,
        operation: OperationId,
        target: Option<TaskId>,
        resolution: CommandResolution,
        settings: &'a Value,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Value>> {
        use crate::conversation::BoundedContentFuture;
        match resolution {
            CommandResolution::Tool { tool, input } => {
                BoundedContentFuture::new(None, || self.direct_tool(operation, target, tool, input))
            }
            resolution @ CommandResolution::Prompt { .. } => {
                BoundedContentFuture::new(None, || async move {
                    let text = self.retain_prompt_input(operation, resolution).await?;
                    self.queue_prompt(operation, target, &text, settings)?.await
                })
            }
            CommandResolution::Submit { request } => {
                self.dispatch_submission(operation, target, request, settings)
            }
            _ => Err(Error::Invalid(
                "renderer selection is not a harness request".into(),
            )),
        }
    }
    fn dispatch_submission<'a>(
        &'a self,
        operation: OperationId,
        target: Option<TaskId>,
        request: CommandRequest,
        settings: &'a Value,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Value>> {
        use crate::conversation::BoundedContentFuture;
        match request {
            CommandRequest::Replace { message, text } => {
                BoundedContentFuture::new(None, || async move {
                    let original = crate::runtime::task_id_for_admission(message);
                    self.runtime.task_host().observe_admission(original).await?;
                    let mut result = self.queue_prompt(operation, None, &text, settings)?.await?;
                    result["replaces"] = json!(message);
                    Ok(result)
                })
            }
            CommandRequest::Cancel { task } => BoundedContentFuture::new(None, || async move {
                self.runtime.task_host().cancel(task).await?;
                self.refresh_command_task(task).await?;
                Ok(json!({"task":task, "cancel_requested":true}))
            }),
            CommandRequest::New => {
                BoundedContentFuture::new(None, || async { Ok(json!({"selected_task":null})) })
            }
            CommandRequest::Delete => BoundedContentFuture::new(None, || async move {
                self.delete_commands(Some(required(target)?)).await
            }),
            CommandRequest::ClearAll => {
                BoundedContentFuture::new(None, || self.delete_commands(None))
            }
            CommandRequest::Export => {
                BoundedContentFuture::new(None, || self.export(operation, target))
            }
            CommandRequest::Compact { instructions } => {
                BoundedContentFuture::new(None, || async move {
                    self.compact(operation, required(target)?, &instructions)
                        .await
                })
            }
            CommandRequest::Rewind => BoundedContentFuture::new(None, || async move {
                self.rewind_checkpoint(operation, required(target)?).await
            }),
            request => BoundedContentFuture::new(None, || self.command_controls(target, request)),
        }
    }
    async fn command_controls(
        &self,
        target: Option<TaskId>,
        request: CommandRequest,
    ) -> Result<Value> {
        let (task, paused) = match request {
            CommandRequest::TogglePause => (Some(required(target)?), None),
            CommandRequest::Pause { scope } => (
                match scope {
                    CommandTaskScope::All => None,
                    _ => Some(required(target)?),
                },
                Some(true),
            ),
            CommandRequest::Resume { scope } => (
                match scope {
                    CommandTaskScope::All => None,
                    _ => Some(required(target)?),
                },
                Some(false),
            ),
            _ => {
                return Err(Error::Invalid(
                    "presentation changes are applied before owner dispatch".into(),
                ));
            }
        };
        if let Some(task) = task {
            self.runtime.task_host().observe_admission(task).await?;
        }
        let paused = CommandControl::change_matching(
            &self.task_files.inner,
            task,
            |scope| Box::pin(self.command_has_ancestor(task, scope)),
            paused,
            self.limits.file_bytes,
        )
        .await?;
        Ok(json!({"paused":paused,"scope":task}))
    }
    pub(super) async fn wait_command_control(&self, task: TaskId) -> Result<()> {
        CommandControl::wait_matching(
            &self.task_files.inner,
            |scope| Box::pin(self.command_has_ancestor(Some(task), scope)),
            self.limits.file_bytes,
        )
        .await
    }
    pub(super) async fn prompt_context(
        &self,
        target: Option<TaskId>,
        prompt: &str,
    ) -> Result<String> {
        if let Some(target) = target {
            if let Some(summary) = self.resolved_value(target, "summary").await? {
                let text = format!(
                    "Previous compacted conversation:\n{summary}\nCurrent request:\n{prompt}"
                );
                if text.len() as u64 > self.limits.file_bytes {
                    return Err(Error::Invalid(
                        "prompt with compacted context exceeds file bound".into(),
                    ));
                }
                return Ok(text);
            }
        }
        Ok(prompt.into())
    }
    async fn delete_commands(&self, target: Option<TaskId>) -> Result<Value> {
        if let Some(task) = target {
            let deleted = self.delete_conversation(task).await?;
            CommandControl::change(
                &self.task_files.inner,
                Some(task),
                &[task],
                Some(false),
                self.limits.file_bytes,
            )
            .await?;
            return Ok(
                json!({"selected_task":null,"deleted":if deleted { vec![task] } else { vec![] },"cleared":false}),
            );
        }
        // Every stock admission retains its input before becoming runnable.
        // UI settings and post-worker status projections are optional. Pin one
        // input-directory generation and visit only one bounded page at a time.
        let grant = crate::conversation::ContentGrant::verify_directory_read(
            &self.verifier,
            &self.signed,
            &self.journal,
            "inputs",
            &mut None,
        )?;
        let mut generation = None;
        let mut after = None;
        let mut deleted = 0_u64;
        loop {
            let (cut, page) = match self
                .files
                .list_private_directory(
                    &self.journal,
                    &grant,
                    "inputs",
                    crate::filesystem::DirectoryOptions {
                        generation: generation.as_ref(),
                        after: after.as_ref(),
                        maximum_entries: self.limits.context_messages.min(64) as u32,
                        ceiling: None,
                    },
                )
                .await
            {
                Ok(page) => page,
                Err(Error::NotFound(_)) => break,
                Err(error) => return Err(error),
            };
            generation = Some(cut);
            after = page.entries.last().map(|entry| entry.name.clone());
            for entry in page.entries {
                if entry.kind != acyclic_fs::kernel::FileKind::Regular {
                    continue;
                }
                let name = entry
                    .name
                    .unicode_text(
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                    )
                    .map_err(storage)?
                    .value;
                let Some(task) = name
                    .strip_suffix(".json")
                    .and_then(|name| TaskId::parse(name).ok())
                else {
                    continue;
                };
                let admission = match self.runtime.task_host().observe_admission(task).await {
                    Ok(admission) => admission,
                    Err(Error::NotFound(_)) => continue,
                    Err(error) => return Err(error),
                };
                if admission.task.name == "acyclic.stock_turn"
                    && self.delete_conversation(task).await?
                {
                    deleted = deleted.checked_add(1).ok_or_else(|| {
                        Error::Invalid("deleted conversation count overflow".into())
                    })?;
                }
            }
            if !page.has_more {
                break;
            }
            tokio::task::yield_now().await;
        }
        CommandControl::change(
            &self.task_files.inner,
            None,
            &[],
            Some(false),
            self.limits.file_bytes,
        )
        .await?;
        Ok(json!({"selected_task":null,"deleted_count":deleted,"cleared":true}))
    }
    async fn delete_conversation(&self, task: TaskId) -> Result<bool> {
        match self.runtime.task_host().cancel(task).await {
            Ok(()) => {}
            Err(Error::NotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        }
        let key = FileKey::Ui {
            directory: Component::new(format!("task-{task}")).map_err(storage)?,
            name: Component::new("settings.json").map_err(storage)?,
        };
        loop {
            let snapshot = self
                .task_files
                .inner
                .snapshot(&Query::File(key.clone()), 1)
                .await
                .map_err(storage)?;
            let mut settings: Value = snapshot
                .files
                .first()
                .map(|file| serde_json::from_slice(&file.bytes))
                .transpose()
                .map_err(storage)?
                .unwrap_or_else(|| json!({}));
            settings
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("settings must be an object".into()))?
                .insert("deleted".into(), json!(true));
            let bytes = serde_json::to_vec(&settings).map_err(storage)?;
            match self
                .task_files
                .inner
                .append(snapshot.generation, &FileValue::new(key.clone(), bytes))
                .await
            {
                Ok(_) => {
                    self.refresh_command_task(task).await?;
                    return Ok(true);
                }
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
    }
    #[inline(never)]
    fn queue_prompt<'a>(
        &'a self,
        operation: OperationId,
        target: Option<TaskId>,
        prompt: &'a str,
        settings: &'a Value,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Value>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            self.retain_prompt_input(
                operation,
                CommandResolution::Prompt {
                    text: prompt.into(),
                    source: None,
                    mentions: Vec::new(),
                },
            )
            .await?;
            self.retain_settings(operation, settings)?.await?;
            let task = accepted(self.submit_to(operation, target, prompt)?.await?, operation)?;
            self.refresh_command_task(task).await?;
            Ok(json!({"task":task,"selected_task":task,"admission_operation":operation}))
        })
    }
    #[inline(never)]
    pub(super) fn retain_settings<'a>(
        &'a self,
        operation: OperationId,
        settings: &'a Value,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            if !settings.is_object() {
                return Err(Error::Invalid("settings must be an object".into()));
            }
            if settings.get("deleted").and_then(Value::as_bool) == Some(true) {
                return Err(Error::Invalid("conversation was deleted".into()));
            }
            let bytes = serde_json::to_vec(settings).map_err(storage)?;
            if bytes.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid("settings exceed file bound".into()));
            }
            let task = crate::runtime::task_id_for_admission(operation);
            let key = FileKey::Ui {
                directory: Component::new(format!("task-{task}")).map_err(storage)?,
                name: Component::new("settings.json").map_err(storage)?,
            };
            // Publish the execution selection before admission can become runnable.
            // Reconciliation keeps the first saved binding for this operation.
            loop {
                let snapshot = self
                    .task_files
                    .inner
                    .snapshot(&Query::File(key.clone()), 1)
                    .await
                    .map_err(storage)?;
                if !snapshot.files.is_empty() {
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
            Ok(())
        })
    }
    async fn direct_tool(
        &self,
        operation: OperationId,
        parent: Option<TaskId>,
        tool: CommandTool,
        input: Value,
    ) -> Result<Value> {
        let name = match tool {
            CommandTool::Shell => crate::tools::shell::PLATFORM.name,
            CommandTool::Spawn => "spawn",
            CommandTool::WebSearch => "web_search",
            CommandTool::UrlFetch => "url_fetch",
        };
        let definition = self
            .tools()
            .get(name, None, None)?
            .ok_or_else(|| match tool {
                CommandTool::WebSearch | CommandTool::UrlFetch => Error::ToolRejected {
                    code: crate::wire::ErrorCode::Unavailable,
                    reason: "requested web capability is not configured",
                },
                _ => Error::Invalid(format!("tool {name} is not installed")),
            })?;
        let command = self
            .stage_command(
                operation,
                crate::filesystem::TOOL_TASK_COMMAND_KIND,
                &json!({"name":name,"revision":definition.definition.revision,"arguments":input}),
            )
            .await?;
        let definition = self
            .runtime
            .harness()
            .task::<Value, crate::tool::ToolResult>("acyclic.direct_tool@1")?;
        let input = json!({"command":command});
        let admission = inheritance::admit_prepared(
            &self.admission,
            Box::pin(
                self.prepare_inheritance(crate::runtime::task_id_for_admission(operation), parent),
            ),
            Box::pin(async {
                if let Some(parent) = parent {
                    let record = self.runtime.task_host().observe_admission(parent).await?;
                    self.runtime
                        .harness()
                        .durable_context(parent, record.operation_id, None)
                        .await?
                        .admit(operation, &definition, input)
                        .await
                } else {
                    self.runtime
                        .harness()
                        .admit(operation, &definition, input, None)
                        .await
                }
            }),
        )
        .await?;
        let task = accepted(admission, operation)?;
        let handle = self.runtime.harness().attach(task, &definition).await?;
        let commands = self.commands()?;
        #[cfg(any(test, feature = "native-execution"))]
        self.advance_with_children(task, &commands, self.task_files.clone(), None)
            .await?;
        #[cfg(not(any(test, feature = "native-execution")))]
        Box::pin(crate::package::harness::worker::advance(
            self, task, &commands,
        ))
        .await?;
        match handle.result().await? {
            crate::Outcome::Succeeded(result) => Ok(result.value),
            crate::Outcome::Failed { message } => Err(Error::TaskFailed { task, message }),
            crate::Outcome::Indeterminate { cause, .. } => Err(cause.into_error(operation)),
            crate::Outcome::Cancelled => Err(Error::Indeterminate(operation)),
        }
    }
    async fn stage_command(
        &self,
        operation: OperationId,
        kind: &str,
        value: &Value,
    ) -> Result<WorkflowCommand> {
        let bytes = serde_json::to_vec(value).map_err(storage)?;
        let publisher = FilesystemContentPublisher::new(
            self.files.clone(),
            &self.journal,
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?;
        let payload = publisher
            .stage_once(
                operation,
                &format!("inputs/{operation}.json"),
                &bytes,
                "application/json",
                "command input",
                crate::conversation::ContentAccess::default(),
            )?
            .await?
            .0;
        Ok(WorkflowCommand {
            operation_id: operation,
            kind: kind.into(),
            payload,
        })
    }
    async fn export(&self, operation: OperationId, target: Option<TaskId>) -> Result<Value> {
        let query = match target {
            Some(task) => Query::Task(Component::new(task.to_string()).map_err(storage)?),
            None => Query::Root,
        };
        let snapshot = self
            .task_files
            .inner
            .snapshot(&query, self.limits.context_messages)
            .await
            .map_err(storage)?;
        let records: Vec<_> = snapshot
            .files
            .iter()
            .filter(|file| !matches!(&file.key, FileKey::Ui { directory, .. } if directory.as_str() == "exports"))
            .map(|file| json!({"path":file.key.path(),"bytes":file.bytes.as_ref()}))
            .collect();
        let bytes = serde_json::to_vec(&json!({"generation":snapshot.generation,"files":records}))
            .map_err(storage)?;
        if bytes.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid("diagnostics exceed file bound".into()));
        }
        let key = FileKey::Ui {
            directory: Component::new("exports").map_err(storage)?,
            name: Component::new(format!("{operation}.json")).map_err(storage)?,
        };
        let path = key.path();
        self.task_files
            .inner
            .create(snapshot.generation, &FileValue::new(key, bytes))
            .await
            .map_err(storage)?;
        Ok(json!({"export":path,"files":records.len()}))
    }
}
pub(super) fn compact_key(task: TaskId) -> Result<FileKey> {
    Ok(FileKey::Ui {
        directory: Component::new(format!("task-{task}")).map_err(storage)?,
        name: Component::new("context.json").map_err(storage)?,
    })
}
fn required(target: Option<TaskId>) -> Result<TaskId> {
    target.ok_or_else(|| Error::Invalid("select a task first".into()))
}
pub(super) fn accepted<O>(
    admission: Admission<RuntimeTask<O>>,
    operation: OperationId,
) -> Result<TaskId> {
    match admission {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => Ok(task_id),
        Admission::Rejected { reason } => Err(Error::Conflict(reason)),
        _ => Err(Error::Indeterminate(operation)),
    }
}

pub(super) struct CommandOwner<'a>(pub &'a LocalHarness);
impl TaskCommandHost for CommandOwner<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context
                .durable_task_id()
                .ok_or_else(|| Error::Invalid("command task absent".into()))?;
            self.0.wait_command_control(task).await?;
            self.0
                .stock_commands(Some(task))
                .await?
                .execute(context, fence, command, payload)
                .await
        })
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let task = context.durable_task_id();
            self.0
                .stock_commands(task)
                .await?
                .reconcile(context, fence, command, payload)
                .await
        })
    }
}

#[cfg(test)]
mod dispatch_storage_tests {
    use super::*;

    fn retained_bytes<T>(_: impl FnOnce(&'static LocalHarness) -> T) -> usize {
        std::mem::size_of::<T>()
    }

    #[test]
    fn command_selection_retains_only_the_selected_operation() {
        let bytes = retained_bytes(|local| {
            local.dispatch_effect(
                OperationId::from_bytes([1; 16]),
                None,
                CommandResolution::Tool {
                    tool: CommandTool::Shell,
                    input: Value::Null,
                },
                &Value::Null,
            )
        });
        eprintln!("command dispatch retained bytes={bytes}");
        assert!(
            bytes <= 256,
            "command selection retained every operation's future"
        );
    }
}

#[cfg(test)]
mod polling_storage_tests {
    use super::*;
    fn retained<Args, T>(_: impl FnOnce(Args) -> T) -> usize {
        std::mem::size_of::<T>()
    }
    #[test]
    fn admission_boundaries_retain_only_owned_polling_handles() {
        let measured = [
            (
                "queue_prompt",
                retained(
                    |(local, text, settings): (
                        &'static LocalHarness,
                        &'static str,
                        &'static Value,
                    )| {
                        local.queue_prompt(OperationId::from_bytes([1; 16]), None, text, settings)
                    },
                ),
            ),
            (
                "retain_settings",
                retained(
                    |(local, _text, settings): (
                        &'static LocalHarness,
                        &'static str,
                        &'static Value,
                    )| {
                        local.retain_settings(OperationId::from_bytes([1; 16]), settings)
                    },
                ),
            ),
        ];
        eprintln!("admission retained bytes: {measured:?}");
        assert!(
            measured.into_iter().all(|(_, bytes)| bytes <= 512),
            "admission embeds concrete child futures"
        );
    }
}
