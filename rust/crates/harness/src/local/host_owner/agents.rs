//! The native host's agents: one A4 actor per native session, on the
//! harness's agent stream. A host process runs each session; its agent's
//! turns record that session (inputs, claims, results, controls) and spawn
//! its children, and never execute a model or a tool.
use super::*;
use crate::{
    agent::{
        AgentHost, AgentId, AgentOutcome, AgentPath, AgentState, ForkAdopted, Hook, InboxRecord,
        IntentId, Message, Out, Sender, TurnDriver, TurnFuture, TurnInput, TurnStep, VolumeBinding,
        Wait,
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
use std::collections::VecDeque;

/// Host name of the inbox records this owner writes for a native session.
const HOST: &str = "native";
/// Bound of one session event, prompt or native resume reference.
pub(super) const MAXIMUM_BYTES: u64 = 65_536;
/// Inputs a session queues before refusing another; its state is one record.
const MAXIMUM_PENDING: usize = 16;
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

/// One queued input: a brief or a message staged on the agent's private volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Input {
    /// Inbox sequence of the message (0 for the adoption brief).
    pub(super) inbox: u64,
    /// The prompt file.
    pub(super) prompt: FileRef,
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

/// A native session's state, carried whole by its last conversation entry.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Session {
    /// Parent-local name of a spawned session, from its brief.
    pub(super) reference: Option<String>,
    /// Inputs not yet claimed, oldest first.
    pub(super) pending: VecDeque<Input>,
    /// The claimed input and the operation that claimed it.
    pub(super) claimed: Option<(OperationId, Input)>,
    /// Settled turns.
    pub(super) settled: u64,
    /// The last settled turn's outcome.
    pub(super) result: Option<crate::Outcome<TurnOutput>>,
    /// Opaque native resume reference; never a transcript or credential.
    pub(super) native: Option<Value>,
    /// Requested lifecycle.
    pub(super) control: Control,
}

/// What one turn recorded.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Record {
    /// An input was queued.
    Input,
    /// A child session was spawned.
    Spawned {
        /// Parent-local name.
        reference: String,
        /// The child.
        child: AgentPath,
    },
    /// An input was claimed.
    Claimed,
    /// A claimed input settled.
    Settled,
    /// The native resume reference changed.
    Native,
    /// The requested lifecycle changed.
    Control,
    /// A child reached a terminal status.
    ChildDone {
        /// The child.
        child: AgentPath,
        /// Its outcome.
        outcome: AgentOutcome,
    },
    /// An event that no longer applies (a replayed or stale request).
    Rejected {
        /// Why.
        reason: String,
    },
}

/// One conversation entry of a native session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    /// The writing agent; a child's conversation starts with its parent's.
    pub(super) agent: AgentPath,
    /// What this turn recorded.
    pub(super) record: Record,
    /// The session after it.
    pub(super) session: Session,
}

/// A spawned session's brief: its name in its parent and its first input.
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

    /// The session after the agent's last turn.
    pub(super) async fn session(&self, agent: &AgentState) -> Result<Session> {
        let Some(from) = agent.conversation_tail.checked_sub(1) else {
            return Ok(Session::default());
        };
        Ok(self
            .entries(&agent.path, from, 1)
            .await?
            .pop()
            .filter(|(_, entry)| entry.agent == agent.path)
            .map(|(_, entry)| entry.session)
            .unwrap_or_default())
    }

    /// Up to `limit` entries of `agent`'s conversation from `from`, by sequence.
    pub(super) async fn entries(
        &self,
        agent: &AgentPath,
        from: u64,
        limit: u32,
    ) -> Result<Vec<(u64, Entry)>> {
        let records = match self
            .stream
            .read(ReadRequest {
                path: agent.conversation().map_err(storage)?,
                from,
                limit,
            })
            .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        // A child's conversation starts with its parent's: skip foreign entries.
        Ok(records
            .iter()
            .filter_map(|record| {
                serde_json::from_slice::<Entry>(&record.value)
                    .ok()
                    .map(|entry| (record.sequence, entry))
            })
            .collect())
    }

    async fn turn(&self, agent: &AgentState, input: Option<TurnInput>) -> Result<TurnStep> {
        let mut session = self.session(agent).await?;
        let mut outs = Vec::new();
        let record = match input {
            // Turns never continue, so no step resumes without input.
            None => return Ok(idle(Vec::new())),
            Some(TurnInput::Brief { brief }) => {
                let bytes = self.read(agent, &brief).await?;
                match serde_json::from_slice::<Brief>(&bytes) {
                    Ok(parsed) => {
                        session.reference = Some(parsed.reference);
                        queue(&mut session, 0, *brief)
                    }
                    Err(error) => Record::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Some(TurnInput::Message { body, inbox, .. }) => queue(&mut session, inbox, *body),
            Some(TurnInput::ChildDone { done }) => Record::ChildDone {
                child: done.child,
                outcome: done.outcome,
            },
            // Native hosts decide their own tool approvals.
            Some(TurnInput::Approval { .. } | TurnInput::ApprovalRequested { .. }) => {
                return Ok(idle(Vec::new()));
            }
            Some(TurnInput::Hook { host, event, .. }) if host == HOST => {
                let bytes = self.read(agent, &event).await?;
                match serde_json::from_slice::<Event>(&bytes) {
                    Ok(event) => apply(agent, &mut session, event, &mut outs)?,
                    Err(error) => Record::Rejected {
                        reason: error.to_string(),
                    },
                }
            }
            Some(TurnInput::Hook { host, .. }) => Record::Rejected {
                reason: format!("hook from {host} is not a native session event"),
            },
        };
        outs.push(Out::Conversation(
            serde_json::to_vec(&Entry {
                agent: agent.path.clone(),
                record,
                session,
            })
            .map_err(storage)?,
        ));
        Ok(idle(outs))
    }
}

fn idle(outs: Vec<Out>) -> TurnStep {
    TurnStep {
        checkpoint: None,
        outs,
        wait: Wait::Input,
    }
}

fn queue(session: &mut Session, inbox: u64, prompt: FileRef) -> Record {
    if session.pending.len() >= MAXIMUM_PENDING {
        return Record::Rejected {
            reason: "native session input queue is full".into(),
        };
    }
    session.pending.push_back(Input { inbox, prompt });
    Record::Input
}

fn rejected(reason: &str) -> Record {
    Record::Rejected {
        reason: reason.into(),
    }
}

/// Applies one owner request. Stale or replayed requests are recorded, never
/// failed: a failed turn would leave its input queued ahead of every later one.
fn apply(
    agent: &AgentState,
    session: &mut Session,
    event: Event,
    outs: &mut Vec<Out>,
) -> Result<Record> {
    Ok(match event {
        Event::Spawn { reference, prompt } => {
            let intent = IntentId::try_from(reference.as_str()).map_err(storage)?;
            let child = child(&agent.path, &reference)?;
            if agent.open_children.contains(&child) {
                rejected("native child already exists")
            } else {
                let brief = serde_json::to_vec(&Brief {
                    reference: reference.clone(),
                    prompt,
                })
                .map_err(storage)?;
                outs.push(Out::Spawn {
                    intent,
                    brief: Some(brief),
                    volumes: Vec::new(),
                });
                Record::Spawned { reference, child }
            }
        }
        Event::Claim if session.claimed.is_some() => rejected("native input already claimed"),
        Event::Claim if session.control != Control::Resume => rejected("native session is stopped"),
        Event::Claim => match session.pending.pop_front() {
            Some(input) => {
                // Named by its turn number, so a replayed claim names it again.
                session.claimed =
                    Some((OperationId::from_bytes(claim_bytes(agent, session)), input));
                Record::Claimed
            }
            None => rejected("native session has no pending input"),
        },
        Event::Complete { operation, outcome } => match &session.claimed {
            Some((claimed, _)) if *claimed == operation => {
                session.claimed = None;
                session.settled += 1;
                session.result = Some(bounded(outcome));
                Record::Settled
            }
            _ => rejected("native completion differs from its claim"),
        },
        Event::Native { session: native } => {
            session.native = Some(native);
            Record::Native
        }
        Event::Control { control }
            if session.control == Control::Archive && control != Control::Archive =>
        {
            rejected("native session is archived")
        }
        Event::Control { control } => {
            session.control = control;
            // An unstarted input owns no process: a stop settles it cancelled.
            if control != Control::Resume
                && session.claimed.is_none()
                && session.pending.pop_front().is_some()
            {
                session.settled += 1;
                session.result = Some(crate::Outcome::Cancelled);
            }
            if matches!(control, Control::Cancel | Control::Archive) {
                session.pending.clear();
            }
            Record::Control
        }
    })
}

/// Spawned session `reference` of `parent`: the spawn intent names it.
pub(super) fn child(parent: &AgentPath, reference: &str) -> Result<AgentPath> {
    let intent = IntentId::try_from(reference).map_err(storage)?;
    parent
        .child(&AgentId::derive(&parent.id(), intent.as_str()))
        .map_err(storage)
}

/// The claim identity of `agent`'s next turn: one per turn number, so a
/// replayed claim names the same operation.
fn claim_bytes(agent: &AgentState, session: &Session) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.native-host.claim.v1");
    hasher.update(agent.path.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&(session.settled + 1).to_le_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    bytes
}

fn bounded(outcome: crate::Outcome<TurnOutput>) -> crate::Outcome<TurnOutput> {
    let truncate = |text: String| -> String {
        let mut end = text.len().min(MAXIMUM_MESSAGE);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text[..end].to_owned()
    };
    match outcome {
        crate::Outcome::Succeeded(mut output) => {
            output.text = truncate(output.text);
            output.metadata = Value::Null;
            output.attachments.clear();
            crate::Outcome::Succeeded(output)
        }
        crate::Outcome::Failed { message } => crate::Outcome::Failed {
            message: truncate(message),
        },
        other => other,
    }
}

/// The runtime's handle on the driver the owner also reads sessions through.
pub(super) struct Shared(Arc<HostDriver>);

impl TurnDriver for Shared {
    fn step<'a>(
        &'a self,
        agent: &'a AgentState,
        _cursor: u64,
        input: Option<TurnInput>,
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
    host: AgentHost<LocalStream>,
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
        let volumes: Arc<dyn crate::agent::SpawnVolumes> =
            Arc::new(
                FilesystemSpawnVolumes::new(
                    local.files.clone(),
                    issuer.clone(),
                    TurnAdmission::LocalRoot,
                    MAXIMUM_BYTES,
                )
                .with_fork_cuts(Arc::new(crate::local::workspace::LazyForkCuts(
                    Arc::clone(local.workspace().ok_or_else(|| {
                        Error::Unsupported("native hosts require a checkout".into())
                    })?),
                ))),
            );
        let host = AgentHost::with_spawn_volumes(
            stream,
            Shared(Arc::clone(&driver)),
            within,
            ActorLimits::new(60_000, 1 << 20, 1 << 16).map_err(storage)?,
            Arc::new(SystemClock),
            Arc::clone(&volumes),
        )
        .map_err(storage)?;
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

    pub(super) async fn entries(
        &self,
        agent: &AgentPath,
        from: u64,
        limit: u32,
    ) -> Result<Vec<(u64, Entry)>> {
        self.driver.entries(agent, from, limit).await
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

    /// Queues `prompt` as message `operation` to `agent`.
    pub(super) async fn message(
        &self,
        agent: &AgentPath,
        operation: OperationId,
        prompt: &str,
    ) -> Result<()> {
        let state = self.state(agent).await?;
        let body = self
            .driver
            .stage(&state, operation, prompt.as_bytes())
            .await?;
        self.deliver(
            agent,
            InboxRecord::Message(Message {
                from: Sender::Host { host: HOST.into() },
                id: IntentId::try_from(format!("native:{operation}")).map_err(storage)?,
                body: Box::new(body),
            }),
        )
        .await
    }

    /// Delivers `record` and runs `agent`'s turn on it; a turn that rejects
    /// the record (a full queue, a stale claim) fails the request.
    async fn deliver(&self, agent: &AgentPath, record: InboxRecord) -> Result<()> {
        let before = self.state(agent).await?.conversation_tail;
        match self
            .host
            .send(agent, &Sender::Host { host: HOST.into() }, &record)
            .await
            .map_err(storage)?
        {
            crate::agent::Delivered::Closed => {
                return Err(Error::Conflict("native session is archived".into()));
            }
            crate::agent::Delivered::Committed { .. } => {}
        }
        self.resume(agent).await?;
        let tail = self.state(agent).await?.conversation_tail;
        if tail > before
            && let Some((_, entry)) = self.driver.entries(agent, tail - 1, 1).await?.pop()
            && entry.agent == *agent
            && let Record::Rejected { reason } = entry.record
        {
            return Err(Error::Conflict(reason));
        }
        Ok(())
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
