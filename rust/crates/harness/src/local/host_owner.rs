//! Native-host workspace effects over the shared task tree and git facade.
use super::*;
use crate::{
    runtime::{TaskContext, TaskStateProvider},
    task_volume::Query,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

mod events;
mod sessions;
pub use sessions::SessionAction;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;

/// Workspace operations only. Native hosts retain every agent and tool process.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HostAction {
    /// Test-only snapshot of the view owner, using the shared OS sampler and
    /// N/2N oracle. It never samples or executes a native child process.
    #[cfg(feature = "test-support")]
    ResourceProbe {
        /// Task authorizing the query.
        task: TaskId,
        /// Retained child population at this measurement.
        agents: u64,
        /// Prior N snapshot: population, descriptors, threads and mounts.
        previous: Option<[u64; 4]>,
    },
    /// Discover native children from the shared input index, without histories.
    Sessions {
        /// Owning parent task.
        parent: TaskId,
        /// Last direct child observed; pages are bounded independently of history.
        after: Option<crate::task_volume::Component>,
    },
    /// Native session lifecycle over the ordinary child mailbox files.
    Session {
        /// Owning parent task.
        parent: TaskId,
        /// Parent-local child name.
        reference: String,
        /// File operation; never executes the host.
        action: SessionAction,
    },
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
    /// Publish an interrupt for the plugin that owns the native process tree.
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

impl HostAction {
    pub(super) fn is_observation(&self) -> bool {
        match self {
            Self::Sessions { .. }
            | Self::Locate { .. }
            | Self::Query { .. }
            | Self::Session {
                action: SessionAction::Observe,
                ..
            } => true,
            #[cfg(feature = "test-support")]
            Self::ResourceProbe { .. } => true,
            _ => false,
        }
    }
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
    #[cfg(test)]
    retirement_observer: Option<std::sync::mpsc::SyncSender<std::thread::ThreadId>>,
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
            #[cfg(test)]
            retirement_observer: None,
        })
    }

    /// Joins native view retirement while the coordination runtime can still
    /// service callbacks. The caller must release its other harness handles.
    pub async fn retire(self) -> Result<()> {
        let local = Arc::clone(&self.local);
        acyclic_native_runtime::spawn_blocking(move || drop(self))
            .await
            .map_err(storage)?;
        Arc::try_unwrap(local)
            .map_err(|_| Error::Conflict("native owner still has live harness handles".into()))?
            .disconnect()
            .await
    }

    /// Dispatches a claimed workspace request. Calling this never executes a
    /// model, a native host, a shell command or another harness tool.
    pub fn dispatch(
        &mut self,
        operation: OperationId,
        action: HostAction,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Value>> {
        // Every caller, including read-only observations, crosses the same
        // erased boundary. A caller's future cannot embed the complete task,
        // filesystem and publication state machines in its own layout.
        Box::pin(self.dispatch_action(operation, action))
    }

    async fn dispatch_action(
        &mut self,
        operation: OperationId,
        action: HostAction,
    ) -> Result<Value> {
        match action {
            #[cfg(feature = "test-support")]
            HostAction::ResourceProbe {
                task,
                agents,
                previous,
            } => self.resource_probe(task, agents, previous).await,
            HostAction::Sessions { parent, after } => {
                self.attach(parent).await?;
                let references = self
                    .files(parent)
                    .await?
                    .inner
                    .child_references_page(after.as_ref(), 32)
                    .await
                    .map_err(storage)?;
                serde_json::to_value(references).map_err(storage)
            }
            HostAction::Session {
                parent,
                reference,
                action,
            } => self.session(operation, parent, &reference, action).await,
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
            HostAction::Capture { task } => self.capture_native(operation, task).await,
            HostAction::Fs { task, command } => self.native_fs(operation, task, &command).await,
            HostAction::Cancel { task } => {
                self.require_native(task).await?;
                let admission = self
                    .local
                    .runtime
                    .task_host()
                    .observe_admission(task)
                    .await?;
                let parent = admission.parent.ok_or_else(|| {
                    Error::Unsupported("stop the native root through its host".into())
                })?;
                let reference = admission
                    .input
                    .get("session")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Error::Invalid("native child reference absent".into()))?;
                self.session(
                    operation,
                    parent,
                    reference,
                    SessionAction::Control {
                        control: crate::task_volume::Control::Interrupt,
                    },
                )
                .await
            }
            HostAction::Query { task, query } => {
                self.attach(task).await?;
                let files = self.files(task).await?;
                let snapshot = files.snapshot(&query, 4096).await.map_err(storage)?;
                serde_json::to_value(snapshot).map_err(storage)
            }
        }
    }

    #[cfg(feature = "test-support")]
    async fn resource_probe(
        &mut self,
        task: TaskId,
        agents: u64,
        previous: Option<[u64; 4]>,
    ) -> Result<Value> {
        use acyclic_native_runtime::testing::{sample_process_resources, scale::ViewResources};
        self.attach(task).await?;
        let process = sample_process_resources().map_err(storage)?;
        let current = ViewResources { agents, process };
        let exceeded = previous
            .map(|[agents, descriptors, threads, mounts]| {
                ViewResources {
                    agents,
                    process: acyclic_native_runtime::testing::ProcessResources {
                        descriptors,
                        threads,
                        mounts,
                        ..Default::default()
                    },
                }
                .exceeded(current)
            })
            .transpose()
            .map_err(storage)?;
        Ok(
            json!({"counts": [agents, process.descriptors, process.threads, process.mounts],
            "exceeded": exceeded}),
        )
    }

    async fn native_fs(
        &mut self,
        operation: OperationId,
        task: TaskId,
        command: &str,
    ) -> Result<Value> {
        self.attach(task).await?;
        let parsed = crate::tools::parse_fs_command(command)?;
        if let [name, reference] = parsed.arguments.as_slice()
            && name == "merge"
            && let Some(reference) = reference.strip_prefix("agents/")
        {
            let reference = crate::task_volume::Component::new(reference).map_err(storage)?;
            let lifecycle = self
                .files(task)
                .await?
                .inner
                .child_lifecycle(Some(&reference))
                .await
                .map_err(storage)?;
            for file in lifecycle.files {
                if matches!(
                    file.key,
                    crate::task_volume::FileKey::Child {
                        file: crate::task_volume::ChildFile::Status,
                        ..
                    }
                ) && !crate::task_volume::ChildStatus::parse(&file.bytes)
                    .map_err(storage)?
                    .settled
                {
                    return Err(Error::Conflict("native session is still running; wait for its captured result before merging".into()));
                }
            }
        }
        let attached = self.attached(task)?;
        serde_json::to_value(
            attached
                .workspace
                .fs(&attached.context, operation, command)
                .await?,
        )
        .map_err(storage)
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
                host_workspace::HostWorkspace::attach(Arc::clone(workspace), &context, &self.local.signed, root).await?
            };
            self.views.insert(task, Attached { context, workspace });
            Ok(())
        })
    }

    async fn capture_native(&mut self, operation: OperationId, task: TaskId) -> Result<Value> {
        self.attach(task).await?;
        let attached = self.attached(task)?;
        let _quiescence = if self.require_native(task).await?.is_some() {
            Some(
                super::host_process::HostProcessLease::acquire(
                    self.local.workspace_path().ok_or_else(|| {
                        Error::Invalid(
                            "step=capture native task: workspace absent; recovery: acyclic agents"
                                .into(),
                        )
                    })?,
                    task,
                    attached.workspace.path(),
                )?
                .quiescent(),
            )
        } else {
            None
        };
        attached
            .workspace
            .capture(&attached.context, operation)
            .await?;
        self.location(task)
    }

    fn location(&self, task: TaskId) -> Result<Value> {
        let attached = self
            .views
            .get(&task)
            .ok_or_else(|| Error::NotFound("native task view".into()))?;
        Ok(json!({"task":task,"cwd":attached.workspace.path()}))
    }
}

#[cfg(test)]
impl Drop for HostWorkspaceOwner {
    fn drop(&mut self) {
        if let Some(observer) = &self.retirement_observer {
            let _ = observer.send(std::thread::current().id());
        }
    }
}
