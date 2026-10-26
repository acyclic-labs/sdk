//! Native-host workspace effects over the native agent runtime and git facade.
use super::*;
use crate::agent::AgentPath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

mod agents;
mod events;
mod sessions;
pub use agents::{Control, TurnClaim};
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
        /// Agent authorizing the query.
        task: AgentPath,
        /// Retained child population at this measurement.
        agents: u64,
        /// Prior N snapshot: population, descriptors, threads and mounts.
        previous: Option<[u64; 4]>,
    },
    /// Discover spawned native children from the parent's conversation.
    Sessions {
        /// Owning parent agent.
        parent: AgentPath,
        /// Last conversation sequence observed; pages are bounded.
        after: Option<u64>,
    },
    /// Native session lifecycle as the child agent's inbox records.
    Session {
        /// Owning parent agent.
        parent: AgentPath,
        /// Parent-local child name.
        reference: String,
        /// Session operation; never executes the host.
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
    /// Resolve an attached agent by its native working directory.
    Locate {
        /// Native tool working directory.
        cwd: PathBuf,
    },
    /// Create or reopen the native root's stable agent.
    Open {
        /// Native host name.
        host: String,
        /// Native root session identity.
        session: String,
    },
    /// Reattach an existing native agent's lazy view.
    Reopen {
        /// Existing native agent.
        task: AgentPath,
    },
    /// Capture completed native writes.
    Capture {
        /// Quiesced native agent.
        task: AgentPath,
    },
    /// Invoke the shared workspace git facade, including merge.
    Fs {
        /// Agent selecting the workspace and grants.
        task: AgentPath,
        /// Ordinary shared filesystem command.
        command: String,
    },
    /// Publish an interrupt for the plugin that owns the native process tree.
    Cancel {
        /// Agent whose session is interrupted.
        task: AgentPath,
    },
    /// The agent's open children and their sessions.
    Agents {
        /// Parent agent.
        task: AgentPath,
    },
}

/// Disposable view handles over the native agent runtime; agents, lineage,
/// files and history are its commits, so this holds no durable index.
pub struct HostWorkspaceOwner {
    local: Arc<LocalHarness>,
    native: agents::NativeAgents,
    views: BTreeMap<AgentPath, host_workspace::HostWorkspace>,
    /// The native request files, whose retention keeps saved results.
    requests: Option<std::path::PathBuf>,
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
            native: agents::NativeAgents::new(&local)?,
            local,
            views: BTreeMap::new(),
            requests: None,
            #[cfg(test)]
            retirement_observer: None,
        })
    }

    /// Keeps each native request's saved result while its file under
    /// `requests` is retained.
    #[must_use]
    pub fn with_requests(mut self, requests: std::path::PathBuf) -> Self {
        self.requests = Some(requests);
        self
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

    /// Dispatches a workspace request. Calling this never executes a model, a
    /// native host, a shell command or another harness tool.
    pub fn dispatch(
        &mut self,
        operation: OperationId,
        action: HostAction,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Value>> {
        // Every caller, including read-only observations, crosses the same
        // erased boundary. A caller's future cannot embed the complete agent,
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
            } => self.resource_probe(&task, agents, previous).await,
            HostAction::Sessions { parent, after } => {
                self.attach(&parent).await?;
                self.sessions(&parent, after).await
            }
            HostAction::Session {
                parent,
                reference,
                action,
            } => self.session(operation, &parent, &reference, action).await,
            HostAction::Hook { host, event, input } => {
                self.hook(operation, &host, &event, input).await
            }
            HostAction::Locate { cwd } => self.locate(&cwd).await,
            HostAction::Open { host, session } => {
                let agent = self.native.open(&host, &session).await?;
                self.attach(&agent).await?;
                self.location(&agent)
            }
            HostAction::Reopen { task } => {
                self.attach(&task).await?;
                self.location(&task)
            }
            HostAction::Capture { task } => self.capture_native(operation, &task).await,
            HostAction::Fs { task, command } => self.native_fs(operation, &task, &command).await,
            HostAction::Cancel { task } => {
                if task.parent().is_none() {
                    return Err(Error::Unsupported(
                        "stop the native root through its host".into(),
                    ));
                }
                self.attach(&task).await?;
                self.native
                    .request(
                        &task,
                        operation,
                        &agents::Event::Control {
                            control: Control::Interrupt,
                        },
                    )
                    .await?;
                self.session_value(&task).await
            }
            HostAction::Agents { task } => {
                self.attach(&task).await?;
                let (state, _) = self.native.session(&task).await?;
                let mut agents = Vec::with_capacity(state.open_children.len());
                for child in &state.open_children {
                    let (_, session) = self.native.session(child).await?;
                    agents.push(json!({"task":child,"reference":session.reference,
                        "control":session.control,"settled":session.settled,
                        "running":session.claimed.is_some(),"pending":session.pending.len()}));
                }
                Ok(json!({ "agents": agents }))
            }
        }
    }

    #[cfg(feature = "test-support")]
    async fn resource_probe(
        &mut self,
        task: &AgentPath,
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
        task: &AgentPath,
        command: &str,
    ) -> Result<Value> {
        self.attach(task).await?;
        let parsed = crate::tools::parse_fs_command(command)?;
        if let [name, reference] = parsed.arguments.as_slice()
            && name == "merge"
            && let Some(reference) = reference.strip_prefix("agents/")
        {
            let (_, session) = self
                .native
                .session(&agents::child(task, reference)?)
                .await?;
            if session.claimed.is_some() || !session.pending.is_empty() {
                return Err(Error::Conflict(
                    "native session is still running; wait for its captured result before merging"
                        .into(),
                ));
            }
        }
        // A native request's saved result lives while its request file does.
        let request = self
            .requests
            .clone()
            .map(|requests| acyclic_fs::GitRequest {
                operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                retained: std::sync::Arc::new(move |request: acyclic_fs::OperationId| {
                    requests
                        .join(format!(
                            "{}.json",
                            OperationId::from_bytes(request.into_bytes())
                        ))
                        .exists()
                }),
            });
        serde_json::to_value(self.view(task)?.fs(operation, command, request).await?)
            .map_err(storage)
    }

    fn view(&self, agent: &AgentPath) -> Result<&host_workspace::HostWorkspace> {
        self.views
            .get(agent)
            .ok_or_else(|| Error::NotFound("native agent view".into()))
    }

    /// Attaches `agent`'s view, idempotently: a root views the checkout
    /// itself, a spawned child mounts its forked project under its parent.
    fn attach<'a>(
        &'a mut self,
        agent: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            if let Some(view) = self.views.get(agent) {
                return view.verify().await;
            }
            let (mut state, mut session) = self.native.session(agent).await?;
            if state.parent.is_some() && session.reference.is_none() {
                // The spawn committed but the child never ran its brief.
                self.native.resume(agent).await?;
                (state, session) = self.native.session(agent).await?;
            }
            let workspace = if let Some(parent) = &state.parent {
                self.attach(parent).await?;
                let reference = session.reference.as_deref().ok_or_else(|| {
                    Error::Invalid("native child has no recorded reference".into())
                })?;
                self.view(parent)?
                    .open_child(
                        agent,
                        &state.volumes,
                        self.native.view_scope(&state)?,
                        reference,
                    )
                    .await?
            } else {
                let workspace = self
                    .local
                    .workspace()
                    .ok_or_else(|| Error::Unsupported("native hosts require a checkout".into()))?;
                let root = acyclic_fs::hold_root(&workspace.path)
                    .map_err(storage)?
                    .create_directories(Path::new(host_workspace::HOST_VIEW_ROOT))
                    .map_err(storage)?;
                host_workspace::HostWorkspace::attach(
                    Arc::clone(workspace),
                    agent.clone(),
                    &self.local.signed,
                    root,
                )
                .await?
            };
            self.views.insert(agent.clone(), workspace);
            Ok(())
        })
    }

    async fn capture_native(&mut self, operation: OperationId, task: &AgentPath) -> Result<Value> {
        self.attach(task).await?;
        let view = self.view(task)?;
        let _quiescence = if task.parent().is_some() {
            Some(
                super::host_process::HostProcessLease::acquire(
                    self.local.workspace_path().ok_or_else(|| {
                        Error::Invalid(
                            "step=capture native agent: workspace absent; recovery: acyclic agents"
                                .into(),
                        )
                    })?,
                    task,
                    view.path(),
                )?
                .quiescent(),
            )
        } else {
            None
        };
        view.capture(operation).await?;
        self.location(task)
    }

    fn location(&self, task: &AgentPath) -> Result<Value> {
        Ok(json!({"task":task,"cwd":self.view(task)?.path()}))
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
