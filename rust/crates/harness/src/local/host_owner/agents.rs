//! The native host's agents: one A4 actor per native session, on the
//! harness's agent stream. A host process runs each session; its agent never
//! executes a model or a tool. A brief only names a spawned session; each
//! message (its first prompt included) opens a turn that waits for this
//! owner's hooks (claim, completion, control), taken ahead of the input queued
//! behind it, and settles as the shared conversation entry at the input's own
//! inbox sequence, so every host's committed reply reads it.
use super::*;
use crate::{
    agent::{
        AgentHost, AgentId, AgentPath, AgentRecord as _, AgentState, ConversationEntry,
        ForkAdopted, Hook, InboxRecord, IntentId, Message, Out, Queued, Sender, TurnDriver,
        TurnFuture, TurnInput, TurnStep, VolumeBinding, Wait,
    },
    conversation::{ContentAccess, ContentResidencyVerifier as _, FileRef, VolumeOwner},
    core::AuthorityRoot,
    executor::TurnOutput,
    filesystem::{
        AgentAuthority, FilesystemContentPublisher, FilesystemContentVerifier,
        FilesystemSpawnVolumes, PRIVATE_VOLUME, TurnAdmission, agent_authority,
    },
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_stream::{ReadRequest, StreamError, StreamProvider as _};
use futures::TryStreamExt as _;

mod steps;
use steps::{apply, idle, reject, start, waiting};

/// Host name of the inbox records this owner writes for a native session.
const HOST: &str = "native";
/// Bound of one session event, prompt or native resume reference.
pub(super) const MAXIMUM_BYTES: u64 = 65_536;
/// Bound of a recorded failure description.
const MAXIMUM_MESSAGE: usize = 4096;

/// Requested lifecycle of a native session.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    /// Run inputs as they arrive.
    #[default]
    Resume,
    /// Stop the running input; later inputs may resume the session.
    Interrupt,
    /// Stop the running input and drop queued inputs.
    Cancel,
    /// Stop and close the session.
    Archive,
}

/// The input a session's open turn runs: its brief or a message, held in the
/// turn's checkpoint until the native process that claimed it settles it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Running {
    /// Inbox sequence of the input; its settled conversation entry carries it.
    pub(super) inbox: u64,
    /// The brief or message.
    pub(super) input: TurnInput,
    /// The claim a native process runs it under.
    pub(super) claimed: Option<OperationId>,
}

/// The exact claim a native process executes under; returned by `Next` and
/// presented again to settle it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnClaim {
    /// The session's agent.
    pub agent: AgentPath,
    /// Claim operation.
    pub operation: OperationId,
    /// One-based turn number.
    pub turn: u64,
}

/// A native session's state: the open turn's checkpoint while one runs,
/// otherwise the output of its last conversation entry.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Session {
    /// Parent-local name of a spawned session, from its brief.
    pub(super) reference: Option<String>,
    /// Parent-local names of its children still open, in spawn order.
    pub(super) spawned: Vec<String>,
    /// The input its open turn runs.
    pub(super) running: Option<Running>,
    /// Settled turns.
    pub(super) settled: u64,
    /// The last settled turn's outcome.
    pub(super) result: Option<crate::Outcome<TurnOutput>>,
    /// Opaque native resume reference; never a transcript or credential.
    pub(super) native: Option<Value>,
    /// Requested lifecycle.
    pub(super) control: Control,
    /// The last request refused, so its sender learns why.
    pub(super) rejected: Option<Rejected>,
}

impl Session {
    /// The claim of the input its open turn runs.
    pub(super) fn claimed(&self) -> Option<OperationId> {
        self.running.as_ref().and_then(|running| running.claimed)
    }

    /// Unclaimed inputs of the agent in `state`: its open turn's, then
    /// those queued behind it.
    pub(super) fn pending(&self, state: &AgentState) -> usize {
        let queued = state
            .queued
            .iter()
            .filter(|queued| {
                matches!(
                    queued.input,
                    TurnInput::Brief { .. } | TurnInput::Message { .. }
                )
            })
            .count();
        usize::from(self.running.is_some() && self.claimed().is_none())
            + queued
            + usize::from(state.overflow.is_some())
    }
}

/// A refused request: a replayed or stale claim, completion or control.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rejected {
    /// The request's inbox identity.
    pub(super) id: IntentId,
    /// Why.
    pub(super) reason: String,
}

/// A spawned session's brief: its name in its parent and its first prompt,
/// which a replayed spawn must repeat exactly.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Brief {
    /// Parent-local name; the spawn intent.
    pub(super) reference: String,
    /// Initial prompt.
    pub(super) prompt: String,
}

/// A host-owner request, delivered to a session agent as a `Hook` event file.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Event {
    /// Spawn child session `reference` with its first input.
    Spawn {
        /// Parent-local name; the spawn intent.
        reference: String,
        /// Initial prompt.
        prompt: String,
    },
    /// Claim the next input.
    Claim,
    /// Settle the claimed input.
    Complete {
        /// The claim's operation.
        operation: OperationId,
        /// Native outcome.
        outcome: crate::Outcome<TurnOutput>,
    },
    /// Save the native resume reference.
    Native {
        /// Reference object.
        session: Value,
    },
    /// Change the requested lifecycle.
    Control {
        /// Requested state.
        control: Control,
    },
}

/// Content access of one agent over its own volumes.
struct Content {
    reader: Arc<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>>,
    writer: Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>,
}

/// Records native sessions; see the module documentation.
pub(super) struct HostDriver {
    stream: Arc<LocalStream>,
    files: Arc<LocalFilesystem>,
    issuer: AuthorityRoot,
}

impl HostDriver {
    fn content(&self, agent: &AgentState) -> Result<Content> {
        let AgentAuthority {
            scope,
            verifier,
            private,
            ..
        } = agent_authority(
            &self.issuer,
            &TurnAdmission::LocalRoot,
            &agent.path,
            &agent.volumes,
            &[],
        )?;
        Ok(Content {
            reader: FilesystemContentVerifier::new(
                Arc::clone(&self.files),
                &verifier,
                &scope,
                MAXIMUM_BYTES,
                None,
            )?,
            writer: FilesystemContentPublisher::new(
                Arc::clone(&self.files),
                &private.volume,
                &verifier,
                &scope,
                MAXIMUM_BYTES,
                None,
            )?,
        })
    }

    /// Reads one file the agent's own volumes hold.
    pub(super) async fn read(&self, agent: &AgentState, file: &FileRef) -> Result<Vec<u8>> {
        let content = self.content(agent)?;
        Ok(content
            .reader
            .read(file, MAXIMUM_BYTES, None, None)?
            .await?
            .to_vec())
    }

    /// Stages `bytes` once on the agent's private volume under `operation`.
    pub(super) async fn stage(
        &self,
        agent: &AgentState,
        operation: OperationId,
        bytes: &[u8],
    ) -> Result<FileRef> {
        let content = self.content(agent)?;
        Ok(content
            .writer
            .stage_once(
                operation,
                &format!("native/{operation}.json"),
                bytes,
                "application/json",
                "native",
                ContentAccess::default(),
            )?
            .await?
            .0)
    }

    /// The session after the agent's last step: its open turn's checkpoint,
    /// or the output of its own last conversation entry.
    pub(super) async fn session(&self, agent: &AgentState) -> Result<Session> {
        if let Some(turn) = &agent.turn {
            return serde_json::from_slice(&turn.checkpoint).map_err(storage);
        }
        if agent.conversation_tail <= agent.conversation_base {
            return Ok(Session::default());
        }
        let mut records = match self
            .stream
            .read(ReadRequest {
                path: agent.path.conversation().map_err(storage)?,
                from: agent.conversation_tail - 1,
                limit: 1,
            })
            .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let Some(record) = records.pop() else {
            return Err(Error::Conflict("native session entry is missing".into()));
        };
        let entry = ConversationEntry::decode(&record.value).map_err(storage)?;
        serde_json::from_slice(&self.read(agent, &entry.output).await?).map_err(storage)
    }

    async fn turn(&self, agent: &AgentState, input: Option<Queued>) -> Result<TurnStep> {
        let mut session = self.session(agent).await?;
        // A turn resumes only on its host's hooks, each of which is input.
        let Some(Queued { inbox, input }) = input else {
            return Ok(idle(Vec::new()));
        };
        let (mut outs, mut drops) = (Vec::new(), None);
        let settled = match &input {
            TurnInput::Brief { brief } => {
                let bytes = self.read(agent, brief).await?;
                let brief = serde_json::from_slice::<Brief>(&bytes).map_err(storage)?;
                session.reference = Some(brief.reference);
                None
            }
            TurnInput::Message { .. } => return start(session, inbox, input),
            TurnInput::ChildDone { done } => {
                session.spawned.retain(|reference| {
                    child(&agent.path, reference).ok().as_ref() != Some(&done.child)
                });
                None
            }
            // Native hosts decide their own tool approvals.
            TurnInput::Approval { .. } | TurnInput::ApprovalRequested { .. } => {
                return Ok(idle(Vec::new()));
            }
            TurnInput::Hook { host, id, event } if host == HOST => {
                let bytes = self.read(agent, event).await?;
                match serde_json::from_slice::<Event>(&bytes) {
                    Ok(event) => {
                        // A stop drops every input queued before it.
                        drops = matches!(
                            event,
                            Event::Control {
                                control: Control::Cancel | Control::Archive
                            }
                        )
                        .then_some(inbox);
                        apply(agent, &mut session, inbox, id, event, &mut outs)?
                    }
                    Err(error) => reject(&mut session, id, &error.to_string()),
                }
            }
            TurnInput::Hook { host, id, .. } => reject(
                &mut session,
                id,
                &format!("hook from {host} is not a native session event"),
            ),
        };
        let (inbox, input) = match settled {
            Some(running) => (running.inbox, running.input),
            // A request to the open turn is held in its checkpoint.
            None if session.running.is_some() => {
                return Ok(TurnStep {
                    drops,
                    ..waiting(&session, outs)?
                });
            }
            None => (inbox, input),
        };
        let bytes = serde_json::to_vec(&session).map_err(storage)?;
        let output = self.stage(agent, snapshot(agent, inbox), &bytes).await?;
        let entry = ConversationEntry {
            inbox,
            input,
            output,
        };
        outs.push(Out::Conversation(entry.encode().map_err(storage)?));
        Ok(TurnStep {
            drops,
            ..idle(outs)
        })
    }
}

/// Names the session snapshot output by the entry at inbox sequence `inbox`.
fn snapshot(agent: &AgentState, inbox: u64) -> OperationId {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.native-host.session.v1");
    hasher.update(agent.path.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&inbox.to_le_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    OperationId::from_bytes(bytes)
}

/// Spawned session `reference` of `parent`: the spawn intent names it.
pub(super) fn child(parent: &AgentPath, reference: &str) -> Result<AgentPath> {
    let intent = IntentId::try_from(reference).map_err(storage)?;
    parent
        .child(&AgentId::derive(&parent.id(), intent.as_str()))
        .map_err(storage)
}

/// The runtime's handle on the driver the owner also reads sessions through.
pub(super) struct Shared(Arc<HostDriver>);

impl TurnDriver for Shared {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> TurnFuture<'a> {
        Box::pin(async move {
            self.0
                .turn(agent, input)
                .await
                .map_err(|error| crate::agent::Error::Turn(Box::new(error)))
        })
    }
}

/// The native host's agent runtime, driven synchronously by its owner: every
/// request commits its inbox record, then drains the runtime, so the reply
/// observes the request's turn. It owns no thread or timer per agent.
pub(super) struct NativeAgents {
    host: Arc<AgentHost<LocalStream>>,
    driver: Arc<HostDriver>,
    volumes: Arc<dyn crate::agent::SpawnVolumes>,
    files: Arc<LocalFilesystem>,
    issuer: AuthorityRoot,
    bindings: [VolumeBinding; 2],
}

impl NativeAgents {
    pub(super) fn new(local: &LocalHarness) -> Result<Self> {
        let issuer = local.root_authority.clone().ok_or_else(|| {
            Error::Unsupported("native hosts require the harness's signing custody".into())
        })?;
        let stream = local.agent_stream.clone().ok_or_else(|| {
            Error::Unsupported("native hosts require the harness's agent stream".into())
        })?;
        let driver = Arc::new(HostDriver {
            stream: Arc::clone(&stream),
            files: local.files.clone(),
            issuer: issuer.clone(),
        });
        let within = ActorReference::new(b"local", b"native", b"hosts").map_err(storage)?;
        let volumes = FilesystemSpawnVolumes::new(
            local.files.clone(),
            issuer.clone(),
            TurnAdmission::LocalRoot,
            MAXIMUM_BYTES,
        );
        let volumes = if let Some(workspace) = local.workspace() {
            volumes.with_fork_cuts(Arc::new(crate::local::workspace::LazyForkCuts(Arc::clone(
                workspace,
            ))))
        } else {
            volumes
        };
        let volumes: Arc<dyn crate::agent::SpawnVolumes> = Arc::new(volumes);
        let host = Arc::new(
            AgentHost::with_spawn_volumes(
                stream,
                Shared(Arc::clone(&driver)),
                within,
                ActorLimits::new(60_000, 1 << 20, 1 << 16).map_err(storage)?,
                Arc::new(SystemClock),
                Arc::clone(&volumes),
            )
            .map_err(storage)?,
        );
        let bind = |name: &str, volume: &VolumeRef| VolumeBinding {
            name: name.to_owned(),
            volume: volume.clone(),
        };
        Ok(Self {
            host,
            driver,
            volumes,
            files: local.files.clone(),
            issuer,
            bindings: [
                bind("project", &local.project),
                bind("scratch", &local.scratch),
            ],
        })
    }

    /// Creates, or replays, root session `(host, session)` on the checkout's
    /// own project and scratch volumes, with a private volume of its own.
    pub(super) async fn open(&self, host: &str, session: &str) -> Result<AgentPath> {
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.native-root.v1");
        hasher.update(host.as_bytes());
        hasher.update(&[0]);
        hasher.update(session.as_bytes());
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        let agent = AgentId::from_bytes(bytes);
        let project = &self.bindings[0].volume;
        let private = VolumeRef::new(
            project.provider().clone(),
            format!("native-{agent}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes(bytes)),
            &mut None,
        )?;
        self.files.create_volume(&private).await?;
        let mut volumes = vec![VolumeBinding {
            name: PRIVATE_VOLUME.to_owned(),
            volume: private,
        }];
        volumes.extend(self.bindings.iter().cloned());
        let root = self
            .host
            .create(
                &IntentId::try_from(format!("native:{agent}")).map_err(storage)?,
                ForkAdopted {
                    agent,
                    parent: None,
                    forked_at: 0,
                    conversation_tail: 0,
                    volumes,
                    brief: None,
                    policy: None,
                },
            )
            .await
            .map_err(storage)?;
        self.drain().await?;
        Ok(root)
    }

    /// The committed state of `agent`.
    pub(super) async fn state(&self, agent: &AgentPath) -> Result<AgentState> {
        self.host.state(agent).await.map_err(storage)
    }

    /// The brief spawned `child` was created with.
    pub(super) async fn brief(&self, child: &AgentPath) -> Result<Brief> {
        let state = self.state(child).await?;
        let file = self
            .volumes
            .brief(child, &state.volumes)
            .await
            .map_err(storage)?
            .ok_or_else(|| Error::NotFound("spawned session has no brief".into()))?;
        let bytes = self.driver.read(&state, &file).await?;
        serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
    }

    /// `agent`'s session after its last turn.
    pub(super) async fn session(&self, agent: &AgentPath) -> Result<(AgentState, Session)> {
        let state = self.state(agent).await?;
        let session = self.driver.session(&state).await?;
        Ok((state, session))
    }

    pub(super) async fn read(&self, agent: &AgentState, file: &FileRef) -> Result<Vec<u8>> {
        self.driver.read(agent, file).await
    }

    /// Delivers `event` to `agent` as request `operation`, then runs its turn.
    /// Resending an operation replays its one inbox record.
    pub(super) async fn request(
        &self,
        agent: &AgentPath,
        operation: OperationId,
        event: &Event,
    ) -> Result<()> {
        let state = self.state(agent).await?;
        let bytes = serde_json::to_vec(event).map_err(storage)?;
        if bytes.len() as u64 > MAXIMUM_BYTES {
            return Err(Error::Invalid(
                "native session event exceeds its bound".into(),
            ));
        }
        let file = self.driver.stage(&state, operation, &bytes).await?;
        self.deliver(
            agent,
            InboxRecord::Hook(Hook {
                host: HOST.into(),
                id: IntentId::try_from(format!("native:{operation}")).map_err(storage)?,
                event: Box::new(file),
            }),
        )
        .await?;
        if let Event::Spawn { reference, .. } = event {
            // The spawn adopted its child in the same commit: run its brief.
            self.wake(&child(agent, reference)?)?;
            self.drain().await?;
        }
        Ok(())
    }

    /// Queues `prompt` as message `id` to `agent`, returning its inbox
    /// sequence. Its body is staged under `id`, so a replay commits nothing new
    /// and returns the same sequence.
    pub(super) async fn message(
        &self,
        agent: &AgentPath,
        id: &IntentId,
        prompt: &str,
    ) -> Result<u64> {
        let state = self.state(agent).await?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.native-host.message.v1");
        hasher.update(agent.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(id.as_str().as_bytes());
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        let body = self
            .driver
            .stage(&state, OperationId::from_bytes(bytes), prompt.as_bytes())
            .await?;
        self.deliver(
            agent,
            InboxRecord::Message(Message {
                from: Sender::Host { host: HOST.into() },
                id: id.clone(),
                body: Box::new(body),
            }),
        )
        .await
    }

    /// Where `agent`'s message `id` at inbox sequence `inbox` stands, through
    /// the shared committed reply; an answer carries the outcome its entry
    /// settled.
    pub(super) async fn reply(
        &self,
        agent: &AgentPath,
        id: &IntentId,
        inbox: u64,
    ) -> Result<Value> {
        Ok(
            match self.host.reply(agent, id, inbox).await.map_err(storage)? {
                crate::agent::Reply::Answered { sequence, entry } => {
                    let state = self.state(agent).await?;
                    let settled: Session =
                        serde_json::from_slice(&self.driver.read(&state, &entry.output).await?)
                            .map_err(storage)?;
                    json!({"kind": "answered", "sequence": sequence, "outcome": settled.result})
                }
                crate::agent::Reply::Pending => json!({ "kind": "pending" }),
                crate::agent::Reply::Dropped => json!({ "kind": "dropped" }),
            },
        )
    }

    /// Delivers `record` and runs `agent`'s turn on it, returning its inbox
    /// sequence; a turn that rejects the request (a stale claim or
    /// completion) fails it.
    async fn deliver(&self, agent: &AgentPath, record: InboxRecord) -> Result<u64> {
        let inbox = match self
            .host
            .send(agent, &Sender::Host { host: HOST.into() }, &record)
            .await
            .map_err(storage)?
        {
            crate::agent::Delivered::Closed => {
                return Err(Error::Conflict("native session is archived".into()));
            }
            crate::agent::Delivered::Committed { inbox } => inbox,
        };
        self.resume(agent).await?;
        let InboxRecord::Hook(Hook { id, .. }) = &record else {
            return Ok(inbox);
        };
        match self.session(agent).await?.1.rejected {
            Some(rejected) if rejected.id == *id => Err(Error::Conflict(rejected.reason)),
            _ => Ok(inbox),
        }
    }

    /// Runs `agent`'s outstanding turns, such as a spawned child's first
    /// turn on its brief that a stopped owner never ran.
    pub(super) async fn resume(&self, agent: &AgentPath) -> Result<()> {
        self.wake(agent)?;
        self.drain().await
    }

    fn wake(&self, agent: &AgentPath) -> Result<()> {
        self.host.host(agent).map_err(storage)
    }

    async fn drain(&self) -> Result<()> {
        match self.host.runtime().drain().await.into_iter().next() {
            None => Ok(()),
            Some((actor, error)) => Err(Error::Conflict(format!(
                "native session turn failed for {actor:?}: {error}"
            ))),
        }
    }

    /// `agent`'s workload over its own volumes, signed for its native views.
    pub(super) fn view_scope(&self, agent: &AgentState) -> Result<Scope> {
        // The agent's own grants, signed by the root issuer that admits views.
        let authority = agent_authority(
            &self.issuer,
            &TurnAdmission::LocalRoot,
            &agent.path,
            &agent.volumes,
            &[],
        )?;
        let mut grants = authority
            .grants
            .iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for binding in &agent.volumes {
            grants.push(binding.volume.directory_read_capability("", &mut None)?);
        }
        self.issuer.bootstrap_scope(
            "agent",
            &crate::Capabilities::new(grants),
            Some(crate::AgentId::from_bytes(agent.path.id().to_bytes())),
            &mut None,
        )
    }
}
