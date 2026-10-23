//! Native-host workspace effects over the shared task tree and git facade.
use super::*;
use crate::{
    runtime::{TaskContext, TaskStateProvider},
    task_volume::Query,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod event_tests;
mod events;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Workspace operations only. Native hosts retain every agent and tool process.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostAction {
    /// Translate a native lifecycle event into workspace coordination only.
    Hook {
        /// Native host name.
        host: String,
        /// Native lifecycle event.
        event: String,
        /// Native event payload, without transcripts or credentials.
        input: Value,
    },
    /// Resolve an already admitted task by its native working directory.
    Locate {
        /// Native tool working directory.
        cwd: PathBuf,
    },
    /// Admit or reopen the native root's stable task identity.
    Open {
        /// Native host name.
        host: String,
        /// Native root session identity.
        session: String,
    },
    /// Allocate an isolated native child through shared resource preparation.
    Fork {
        /// Authenticated parent task.
        parent: TaskId,
        /// Parent-local child reference.
        reference: String,
    },
    /// Reattach an existing native task's lazy view.
    Reopen {
        /// Existing native task.
        task: TaskId,
    },
    /// Capture completed native writes.
    Capture {
        /// Quiesced native task.
        task: TaskId,
    },
    /// Invoke the shared workspace git facade, including merge.
    Fs {
        /// Task selecting the workspace and grants.
        task: TaskId,
        /// Ordinary shared filesystem command.
        command: String,
    },
    /// Cancel shared task ownership. The native host kills its own agent.
    Cancel {
        /// Task whose ownership is cancelled.
        task: TaskId,
    },
    /// Read a bounded live snapshot of the selected task's own files.
    Query {
        /// Task selecting its private files.
        task: TaskId,
        /// Shared bounded file selector.
        query: Query,
    },
}

struct Attached {
    context: TaskContext,
    workspace: host_workspace::HostWorkspace,
}

/// Disposable view handles. Admissions, lineage, files and history remain in
/// the existing harness; this contains no durable index or execution registry.
pub struct HostWorkspaceOwner {
    local: Arc<LocalHarness>,
    views: BTreeMap<TaskId, Attached>,
}

impl HostWorkspaceOwner {
    /// Uses an already-running harness, with no provider or agent executor.
    pub fn new(local: Arc<LocalHarness>) -> Result<Self> {
        if local.agent.is_some() || !local.tools().definitions(None)?.is_empty() {
            return Err(Error::Invalid(
                "native host owner must have no agent or tools".into(),
            ));
        }
        Ok(Self {
            local,
            views: BTreeMap::new(),
        })
    }

    /// Dispatches a claimed workspace request. Calling this never executes a
    /// model, a native host, a shell command or another harness tool.
    pub async fn dispatch(&mut self, operation: OperationId, action: HostAction) -> Result<Value> {
        match action {
            HostAction::Hook { host, event, input } => {
                self.hook(operation, &host, &event, input).await
            }
            HostAction::Locate { cwd } => self.locate(&cwd).await,
            HostAction::Open { host, session } => {
                let operation = context::publication_operation(
                    b"acyclic.native-root/v1\0",
                    &serde_json::to_vec(&(&host, &session)).map_err(storage)?,
                );
                let task = self
                    .local
                    .admit_host_task(operation, &host, &session)
                    .await?;
                self.attach(task).await?;
                self.location(task)
            }
            HostAction::Fork { parent, reference } => {
                self.attach(parent).await?;
                self.quiescent(parent).await?;
                let parent = self.attached(parent)?;
                parent.workspace.capture(&parent.context, operation).await?;
                let task = self
                    .local
                    .admit_host_child(&parent.context, &reference)
                    .await?;
                self.attach(task).await?;
                self.location(task)
            }
            HostAction::Reopen { task } => {
                self.attach_retained(task, true).await?;
                self.location(task)
            }
            HostAction::Capture { task } => {
                self.attach(task).await?;
                self.quiescent(task).await?;
                let attached = self.attached(task)?;
                attached
                    .workspace
                    .capture(&attached.context, operation)
                    .await?;
                self.location(task)
            }
            HostAction::Fs { task, command } => {
                self.attach(task).await?;
                self.quiescent(task).await?;
                let attached = self.attached(task)?;
                serde_json::to_value(
                    attached
                        .workspace
                        .fs(&attached.context, operation, &command)
                        .await?,
                )
                .map_err(storage)
            }
            HostAction::Cancel { task } => {
                self.attach(task).await?;
                self.quiescent(task).await?;
                let attached = self.attached(task)?;
                attached
                    .workspace
                    .capture(&attached.context, operation)
                    .await?;
                self.local.runtime.task_host().cancel(task).await?;
                self.local.refresh_command_task(task).await?;
                self.views.remove(&task);
                Ok(json!({"task":task,"cancel_requested":true}))
            }
            HostAction::Query { task, query } => {
                self.attach(task).await?;
                let files = self.files(task).await?;
                let snapshot = files.snapshot(&query, 4096).await.map_err(storage)?;
                serde_json::to_value(snapshot).map_err(storage)
            }
        }
    }

    async fn files(
        &self,
        task: TaskId,
    ) -> Result<Arc<crate::task_volume::TaskFiles<acyclic_stream::LocalStream>>> {
        let context = &self.attached(task)?.context;
        Ok(self
            .local
            .task_files
            .for_created_task(context, context.scope())
            .await?
            .map_or_else(|| Arc::clone(&self.local.task_files), Arc::new))
    }

    fn attached(&self, task: TaskId) -> Result<&Attached> {
        self.views
            .get(&task)
            .ok_or_else(|| Error::NotFound("native task view".into()))
    }

    async fn require_native(&self, task: TaskId) -> Result<Option<TaskId>> {
        let admission = self
            .local
            .runtime
            .task_host()
            .observe_admission(task)
            .await?;
        if admission.task.name != "acyclic.native_host" || admission.task.version != "1" {
            return Err(Error::Unauthorized(
                "workspace request requires a native-host task".into(),
            ));
        }
        Ok(admission.parent)
    }

    fn attach(&mut self, task: TaskId) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        self.attach_retained(task, false)
    }

    fn attach_retained(
        &mut self,
        task: TaskId,
        retained: bool,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(attached) = self.views.get(&task) {
                attached.workspace.verify(&attached.context).await?;
                return Ok(());
            }
            let parent = self.require_native(task).await?;
            let context = self.local.bind_host_task(task).await?;
            let workspace = if let Some(parent) = parent {
                self.attach(parent).await?;
                let attached = self.attached(parent)?;
                if retained {
                    attached
                        .workspace
                        .reopen(&attached.context, &context)
                        .await?
                } else {
                    attached.workspace.fork(&attached.context, &context).await?
                }
            } else {
                let workspace = self
                    .local
                    .workspace()
                    .ok_or_else(|| Error::Unsupported("native hosts require a checkout".into()))?;
                let root = acyclic_fs::hold_root(&workspace.path)
                    .map_err(storage)?
                    .create_directories(Path::new(host_workspace::HOST_VIEW_ROOT))
                    .map_err(storage)?;
                host_workspace::HostWorkspace::attach(Arc::clone(workspace), &context, root).await?
            };
            self.views.insert(task, Attached { context, workspace });
            Ok(())
        })
    }

    fn location(&self, task: TaskId) -> Result<Value> {
        let attached = self
            .views
            .get(&task)
            .ok_or_else(|| Error::NotFound("native task view".into()))?;
        Ok(json!({"task":task,"cwd":attached.workspace.path()}))
    }
}
