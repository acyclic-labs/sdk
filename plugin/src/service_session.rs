//! Session ownership behind the service's existing actor and catalog.

use super::*;
use acyclic_harness::{
    Outcome, TaskId, conversation::FileRef, filesystem::FilesystemHost, runtime::DurableTaskHost,
};
use acyclic_stream::{LocalStream, LocalStreamLimits, StreamClient};

type PiRuntime = pi_runtime::PiTaskRuntime<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;

pub(crate) enum SessionController {
    HostManaged(Box<ControlPlane>),
    Pi(Box<PiSession>),
}

impl From<ControlPlane> for SessionController {
    fn from(control: ControlPlane) -> Self {
        Self::HostManaged(Box::new(control))
    }
}

impl SessionController {
    pub(crate) async fn open(
        directory: PathBuf,
        resources: &ServiceResources,
    ) -> Result<Self, String> {
        let (state, slots) = load_state(&directory)?;
        match &state.task_binding {
            PluginTaskBinding::HostManaged => resources
                .open_session_directory(directory)
                .await
                .map(Self::from),
            PluginTaskBinding::Pi(_) => PiSession::open(directory, state, slots, resources)
                .await
                .map(|session| Self::Pi(Box::new(session)))
                .map_err(|error| format!("Pi owning task runtime recovery failed: {error}")),
        }
    }

    pub(crate) fn state(&self) -> &AdapterState {
        match self {
            Self::HostManaged(control) => &control.state,
            Self::Pi(session) => &session.state,
        }
    }

    pub(crate) fn directory(&self) -> &Path {
        match self {
            Self::HostManaged(control) => &control.data,
            Self::Pi(session) => &session.directory,
        }
    }

    pub(crate) fn unflushed(&self) -> bool {
        match self {
            Self::HostManaged(control) => control.unflushed,
            Self::Pi(_) => false,
        }
    }

    pub(crate) async fn make_durable(&mut self) -> Result<(), String> {
        match self {
            Self::HostManaged(control) => control.make_durable().await,
            Self::Pi(_) => Ok(()),
        }
    }

    pub(crate) async fn dispatch(&mut self, request: ControlRequest) -> Result<Value, String> {
        match self {
            Self::HostManaged(control) => dispatch_session_request(control, request).await,
            Self::Pi(session) => session.dispatch(request).await,
        }
    }

    pub(crate) async fn close(self, deactivate: bool) -> Result<StateSlots, String> {
        match self {
            Self::HostManaged(control) => control.close(deactivate).await,
            Self::Pi(session) => session.close(deactivate).await,
        }
    }
}

pub(crate) struct PiSession {
    directory: PathBuf,
    state: AdapterState,
    slots: StateSlots,
    runtime: PiRuntime,
}

/// Public observations contain task identities and outcomes, never execution
/// fences, scheduler requirements or private journal credentials.
#[derive(Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum PiTaskStatus {
    Pending {
        task: TaskId,
    },
    Reserved {
        task: TaskId,
    },
    Unresolved {
        task: TaskId,
        error: String,
    },
    Outcome {
        task: TaskId,
        outcome: Box<Outcome<FileRef>>,
    },
}

impl From<pi_recovery::PiTaskObservation> for PiTaskStatus {
    fn from(observation: pi_recovery::PiTaskObservation) -> Self {
        use pi_recovery::PiTaskObservation;
        match observation {
            PiTaskObservation::Pending { task_id } => Self::Pending { task: task_id },
            PiTaskObservation::Reserved { lease } => Self::Reserved {
                task: TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
            },
            PiTaskObservation::Unresolved { lease, error } => Self::Unresolved {
                task: TaskId::from_bytes(lease.operation.operation_id.into_bytes()),
                error,
            },
            PiTaskObservation::Outcome { task_id, outcome } => Self::Outcome {
                task: task_id,
                outcome: Box::new(outcome),
            },
        }
    }
}

impl PiSession {
    async fn open(
        directory: PathBuf,
        state: AdapterState,
        slots: StateSlots,
        resources: &ServiceResources,
    ) -> Result<Self, String> {
        let PluginTaskBinding::Pi(binding) = &state.task_binding else {
            return Err("Pi session has no original task binding".to_owned());
        };
        if directory != resources.session_directory(&state.root_session_id) {
            return Err("Pi session directory differs from its original identity".to_owned());
        }
        let owner = pi_owner::PiOwnerConfig::load(&directory)
            .map_err(|error| format!("original owner: {error}"))?;
        owner.validate_root(binding)?;
        // Recovery never bootstraps a missing task provider. Original admission
        // authentication below still rejects an empty or unrelated history.
        let stream_path = directory.join("stream");
        if !stream_path.is_dir() {
            return Err("Pi original task provider is missing".to_owned());
        }
        let filesystem = Arc::new(
            FilesystemHost::new(
                resources.fs.clone(),
                owner.policy().journal.provider().clone(),
            )
            .map_err(display)?,
        );
        let stream = StreamClient::new(Arc::new(
            LocalStream::open(stream_path, LocalStreamLimits::default())
                .await
                .map_err(|error| format!("original task provider: {error}"))?,
        ));
        let (runtime, _) = PiRuntime::reopen(&owner, &state, stream, filesystem)
            .await
            .map_err(|error| format!("original task runtime: {error}"))?;
        Ok(Self {
            directory,
            state,
            slots,
            runtime,
        })
    }

    async fn dispatch(&self, request: ControlRequest) -> Result<Value, String> {
        if request.command == ControlCommand::Hook {
            let event = request.name.strip_prefix("pi:")
                .ok_or_else(|| "Pi session refuses another host's control".to_owned())?;
            let session = string(&request.arguments, "session_id")?;
            if session != self.state.root_session_id {
                return Err("Pi control differs from its original root session".to_owned());
            }
            match event {
                "SessionStart" => {
                    let selected = PathBuf::from(string(&request.arguments, "cwd")?)
                        .canonicalize().map_err(display)?;
                    if !self.state.roots.values().any(|root| root.path == selected) {
                        return Err("Pi root selection changed after original admission".to_owned());
                    }
                }
                "TaskStatus" => {}
                "TaskCancel" => {
                    // Authenticate every retained binding before the coordinator
                    // orders root cancellation and descendant cascade.
                    pi_recovery::observe_pi_session(&self.runtime.runtime, &self.state).await?;
                    let PluginTaskBinding::Pi(root) = &self.state.task_binding else {
                        return Err("Pi session has no original task binding".to_owned());
                    };
                    let task: TaskId = serde_json::from_value(
                        request.arguments.get("task").cloned()
                            .ok_or_else(|| "Pi cancellation requires its original task".to_owned())?,
                    ).map_err(display)?;
                    if task != root.task_id && !self.state.routes.values().any(|route| {
                        matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == task)
                    }) {
                        return Err("Pi cancellation target is outside its retained session".to_owned());
                    }
                    self.runtime.runtime.task_host().cancel(task).await.map_err(display)?;
                }
                _ => return Err("unknown Pi task control operation".to_owned()),
            }
        } else if request.command == ControlCommand::Agents {
            parse_read_only_arguments(&request.argv)?;
        } else {
            return Err("Pi workspace operations require their owning task runtime".to_owned());
        }
        let observations =
            pi_recovery::observe_pi_session(&self.runtime.runtime, &self.state).await?;
        let tasks: Vec<PiTaskStatus> = observations.into_iter().map(PiTaskStatus::from).collect();
        Ok(json!({"session": self.state.root_session_id, "tasks": tasks}))
    }

    async fn close(self, deactivate: bool) -> Result<StateSlots, String> {
        if deactivate {
            // Authenticate the entire retained graph before requesting the
            // existing coordinator's root cancellation and descendant cascade.
            pi_recovery::observe_pi_session(&self.runtime.runtime, &self.state).await?;
            let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
                return Err("Pi session has no original task binding".to_owned());
            };
            self.runtime
                .runtime
                .task_host()
                .cancel(binding.task_id)
                .await
                .map_err(display)?;
        }
        Ok(self.slots)
    }
}
