//! Native-host workspace effects over the native agent runtime and git facade.
use super::*;
use crate::agent::AgentPath;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

mod agents;
mod events;
mod sessions;
pub use agents::{Control, TurnClaim};
pub use sessions::SessionAction;
#[cfg(all(test, not(loom)))]
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
    /// The parent's open spawned children, by parent-local name.
    Sessions {
        /// Owning parent agent.
        parent: AgentPath,
    },
    /// Native session lifecycle as the child agent's inbox records.
    Session {
        /// Owning parent agent.
        parent: AgentPath,
        /// Parent-local child name.
        reference: String,
        /// Session operation; never executes the host.
        action: SessionAction,
        /// Also claim the session's next input under the physical fence of
        /// this same exchange, returned as `next`: null when the fence is
        /// held or nothing is claimable. A claim the fence proves unrun is
        /// handed out again under its exact identity.
        #[serde(default)]
        next: bool,
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
    /// The agent's open children and their sessions.
    Agents {
        /// Parent agent.
        task: AgentPath,
    },
}

/// One agent's view, attached once however many requests await it.
type View = Arc<tokio::sync::OnceCell<Arc<host_workspace::HostWorkspace>>>;

/// Disposable view handles over the native agent runtime; agents, lineage,
/// files and history are its commits, so this holds no durable index. Only
/// roots and children whose sessions run or wait on input stay resident.
///
/// Requests dispatch concurrently. Each one that changes an agent holds that
/// agent's lock, taken in one global order, so effects on one agent stay
/// serial while unrelated agents, reads and cancels never wait behind them.
pub struct HostWorkspaceOwner {
    local: Arc<LocalHarness>,
    native: agents::NativeAgents,
    views: std::sync::Mutex<BTreeMap<AgentPath, View>>,
    locks: AgentLocks,
    /// Serializes root creation, whose agent is not named before it.
    opening: tokio::sync::Mutex<()>,
    /// The native owner directory, whose `requests` files say which
    /// requests' clients have not yet read their reply.
    requests: Option<std::path::PathBuf>,
    #[cfg(test)]
    retirement_observer: Option<std::sync::mpsc::SyncSender<std::thread::ThreadId>>,
}

impl HostWorkspaceOwner {
    /// Admits one explicitly enrolled local volume through this host's Bound,
    /// retaining its original issuer and canonical filesystem provider.
    #[cfg(target_os = "linux")]
    pub async fn guest_session(
        &self,
        enrollment: crate::filesystem::hosting::LocalGuestEnrollment,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::native_mount::GuestSessionAdmission> {
        self.local.guest_session(enrollment, work).await
    }

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
            views: std::sync::Mutex::default(),
            locks: AgentLocks::default(),
            opening: tokio::sync::Mutex::new(()),
            requests: None,
            #[cfg(test)]
            retirement_observer: None,
        })
    }

    /// Keeps each native request's saved result until its reply under
    /// `directory`'s `results` is durable: the reply then answers a replay.
    #[must_use]
    pub fn with_requests(mut self, directory: std::path::PathBuf) -> Self {
        self.requests = Some(directory);
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
        &self,
        operation: OperationId,
        action: HostAction,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Value>> {
        // Every caller, including read-only observations, crosses the same
        // erased boundary. A caller's future cannot embed the complete agent,
        // filesystem and publication state machines in its own layout.
        // The original dispatch future owns this diagnostic lifetime. The key
        // is the same opaque identity the file bridge publishes and answers.
        Box::pin(acyclic_native_runtime::obs::waiting(
            "native-workspace-dispatch",
            operation,
            self.dispatch_action(operation, action),
        ))
    }

    async fn dispatch_action(&self, operation: OperationId, action: HostAction) -> Result<Value> {
        match action {
            #[cfg(feature = "test-support")]
            HostAction::ResourceProbe {
                task,
                agents,
                previous,
            } => self.resource_probe(&task, agents, previous).await,
            HostAction::Sessions { parent } => {
                self.attach(&parent).await?;
                let sessions = self.sessions(&parent).await;
                self.release_idle([parent], sessions).await
            }
            HostAction::Session {
                parent,
                reference,
                action,
                next,
            } => {
                let child = agents::child(&parent, &reference)?;
                let mut changed = vec![child.clone()];
                if matches!(action, SessionAction::Spawn { .. }) {
                    changed.push(parent.clone());
                }
                // An observed child's cwd stays resident for the claim that follows.
                let observe = matches!(action, SessionAction::Observe);
                let session = {
                    let _locks = self.locks.hold(changed).await;
                    self.session(operation, &parent, &reference, action, next)
                        .await
                };
                let child = (!observe).then_some(child);
                self.release_idle(child.into_iter().chain([parent]), session)
                    .await
            }
            HostAction::Hook { host, event, input } => {
                self.hook(operation, &host, &event, input).await
            }
            HostAction::Locate { cwd } => self.locate(&cwd).await,
            HostAction::Open { host, session } => {
                let agent = self.open(&host, &session).await?;
                self.attach(&agent).await?;
                self.location(&agent)
            }
            HostAction::Reopen { task } => {
                self.attach(&task).await?;
                self.location(&task)
            }
            HostAction::Capture { task } => {
                let _lock = self.locks.hold(vec![task.clone()]).await;
                self.capture_native(operation, &task).await
            }
            HostAction::Fs { task, command } => {
                let output = self.native_fs(operation, &task, &command).await;
                self.release_idle([task], output).await
            }
            HostAction::Agents { task } => {
                self.attach(&task).await?;
                let agents = self.agents(&task).await;
                self.release_idle([task], agents).await
            }
        }
    }

    /// Creates or replays a root session; root creation is serial.
    async fn open(&self, host: &str, session: &str) -> Result<AgentPath> {
        let _opening = self.opening.lock().await;
        self.native.open(host, session).await
    }

    async fn agents(&self, task: &AgentPath) -> Result<Value> {
        let (state, _) = self.native.session(task).await?;
        let mut agents = Vec::with_capacity(state.open_children.len());
        for child in &state.open_children {
            let (child_state, session) = self.native.session(child).await?;
            agents.push(json!({"task":child,"reference":session.reference,
                "control":session.control,"settled":session.settled,
                "running":session.claimed().is_some(),
                "pending":session.pending(&child_state)}));
        }
        Ok(json!({ "agents": agents }))
    }

    /// Retires each resident child view in `agents` whose session neither
    /// runs nor waits on input, after `result`'s request: a later request
    /// attaches it again lazily, so mounts and descriptors stay O(active
    /// sessions), not O(every agent this owner touched). Root views live
    /// until owner retirement. A view retires under its agent's lock and only
    /// while this table holds its sole reference, so a concurrent request
    /// never sees the same project mounted twice. Retirement joins off the
    /// coordination thread.
    async fn release_idle(
        &self,
        agents: impl IntoIterator<Item = AgentPath>,
        result: Result<Value>,
    ) -> Result<Value> {
        let mut idle = Vec::new();
        for agent in agents {
            if agent.parent().is_none() || !self.views()?.contains_key(&agent) {
                continue;
            }
            let _lock = self.locks.hold(vec![agent.clone()]).await;
            let (state, session) = self.native.session(&agent).await?;
            if session.claimed().is_some() || session.pending(&state) != 0 {
                continue;
            }
            let mut views = self.views()?;
            let sole = views.get(&agent).is_some_and(|view| {
                Arc::strong_count(view) == 1
                    && view.get().is_none_or(|view| Arc::strong_count(view) == 1)
            });
            if sole {
                idle.extend(views.remove(&agent));
            }
        }
        if !idle.is_empty() {
            acyclic_native_runtime::spawn_blocking(move || drop(idle))
                .await
                .map_err(storage)?;
        }
        result
    }

    #[cfg(feature = "test-support")]
    async fn resource_probe(
        &self,
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
        &self,
        operation: OperationId,
        task: &AgentPath,
        command: &str,
    ) -> Result<Value> {
        let parsed = crate::tools::parse_fs_command(command)?;
        let merged = match parsed.arguments.as_slice() {
            [name, reference] if name == "merge" => reference
                .strip_prefix("agents/")
                .map(|reference| agents::child(task, reference))
                .transpose()?,
            _ => None,
        };
        // A merge reads its child's settled project: it holds both locks.
        let changed = [Some(task.clone()), merged.clone()];
        let _locks = self
            .locks
            .hold(changed.into_iter().flatten().collect())
            .await;
        self.attach(task).await?;
        if let Some(child) = &merged {
            let (state, session) = self.native.session(child).await?;
            if session.claimed().is_some() || session.pending(&state) > 0 {
                return Err(Error::Conflict(
                    "native session is still running; wait for its captured result before merging"
                        .into(),
                ));
            }
        }
        // A native request's saved result lives until its client withdraws
        // the request, which it does once it has read the reply.
        let request = self
            .requests
            .clone()
            .map(|directory| acyclic_fs::GitRequest {
                operation: acyclic_fs::OperationId::from_bytes(operation.into_bytes()),
                unanswered: std::sync::Arc::new(move |request: acyclic_fs::OperationId| {
                    let file = format!("{}.json", OperationId::from_bytes(request.into_bytes()));
                    directory.join("requests").join(&file).exists()
                }),
            });
        serde_json::to_value(self.view(task)?.fs(operation, command, request).await?)
            .map_err(storage)
    }

    fn views(&self) -> Result<std::sync::MutexGuard<'_, BTreeMap<AgentPath, View>>> {
        self.views
            .lock()
            .map_err(|_| Error::Conflict("native view table poisoned".into()))
    }

    fn view(&self, agent: &AgentPath) -> Result<Arc<host_workspace::HostWorkspace>> {
        self.views()?
            .get(agent)
            .and_then(|view| view.get().cloned())
            .ok_or_else(|| Error::NotFound("native agent view".into()))
    }

    /// Every attached view, for cwd discovery.
    fn attached(&self) -> Result<Vec<(AgentPath, Arc<host_workspace::HostWorkspace>)>> {
        Ok(self
            .views()?
            .iter()
            .filter_map(|(agent, view)| Some((agent.clone(), Arc::clone(view.get()?))))
            .collect())
    }

    /// Attaches `agent`'s view, idempotently: a root views the checkout
    /// itself, a spawned child mounts its forked project under its parent.
    /// Concurrent requests share the one attachment in flight.
    fn attach<'a>(
        &'a self,
        agent: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let cell = Arc::clone(self.views()?.entry(agent.clone()).or_default());
            let mut opened = false;
            let view = cell
                .get_or_try_init(|| {
                    opened = true;
                    async { self.open_view(agent).await.map(Arc::new) }
                })
                .await?;
            if opened { Ok(()) } else { view.verify().await }
        })
    }

    async fn open_view(&self, agent: &AgentPath) -> Result<host_workspace::HostWorkspace> {
        let (mut state, mut session) = self.native.session(agent).await?;
        if state.parent.is_some() && session.reference.is_none() {
            // The spawn committed but the child never ran its brief.
            self.native.resume(agent).await?;
            (state, session) = self.native.session(agent).await?;
        }
        let workspace = if let Some(parent) = &state.parent {
            self.attach(parent).await?;
            let reference = session
                .reference
                .as_deref()
                .ok_or_else(|| Error::Invalid("native child has no recorded reference".into()))?;
            self.view(parent)?
                .open_child(
                    agent,
                    &state.volumes,
                    self.native.view_scope(&state).await?,
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
        Ok(workspace)
    }

    async fn capture_native(&self, operation: OperationId, task: &AgentPath) -> Result<Value> {
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

/// The lock table: one async lock per agent some request changes.
type LockTable = Arc<std::sync::Mutex<BTreeMap<AgentPath, Arc<tokio::sync::Mutex<()>>>>>;

/// One lock per agent a request changes, present only while a request holds
/// or awaits it.
#[derive(Default)]
struct AgentLocks(LockTable);

/// The locks one request holds; releasing them retires each unused one.
struct Held {
    locks: LockTable,
    held: Vec<(AgentPath, tokio::sync::OwnedMutexGuard<()>)>,
}

impl AgentLocks {
    /// Holds the lock of every agent in `agents`, in the one global agent
    /// order, so no two requests each hold a lock the other awaits.
    async fn hold(&self, agents: Vec<AgentPath>) -> Held {
        let agents = agents.into_iter().collect::<BTreeSet<_>>();
        let mut held = Held {
            locks: Arc::clone(&self.0),
            held: Vec::with_capacity(agents.len()),
        };
        for agent in agents {
            let lock = Arc::clone(
                self.0
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entry(agent.clone())
                    .or_default(),
            );
            held.held.push((agent, lock.lock_owned().await));
        }
        held
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut locks = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (agent, guard) in self.held.drain(..) {
            drop(guard);
            // Only the table refers to it: nothing holds or awaits the lock.
            if locks
                .get(&agent)
                .is_some_and(|lock| Arc::strong_count(lock) == 1)
            {
                locks.remove(&agent);
            }
        }
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
