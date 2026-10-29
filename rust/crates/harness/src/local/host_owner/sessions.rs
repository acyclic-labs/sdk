//! Native session lifecycle: each request is one inbox record of the session's
//! agent, and every reply observes that agent's session after it. A prompt is
//! a message; its sender awaits it through the shared committed reply.
use super::agents::{Control, Event, Running, TurnClaim};
use super::*;
use crate::{
    Outcome,
    agent::{IntentId, TurnInput},
    executor::TurnOutput,
};

/// Bound of one native prompt.
const MAXIMUM_PROMPT: usize = 32_768;

/// Coordination only. Native session processes are owned by the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAction {
    /// Spawn the child with its initial input; the reply names the prompt.
    Spawn {
        /// Initial prompt.
        prompt: String,
    },
    /// Queue a follow-up input; the reply names the prompt.
    Send {
        /// Message body.
        prompt: String,
    },
    /// Claim the next input, after the preceding process is quiescent. A
    /// claimed input is refused: only a fenced handoff hands it out again.
    Next,
    /// Capture and settle a process that has exited, including failed writes.
    /// The native resume id is derived from the turn's own custodied event
    /// log, so a running turn never spends an exchange recording it.
    Complete {
        /// Exact original execution claim, returned by Next.
        claim: TurnClaim,
        /// Native outcome.
        outcome: Outcome<TurnOutput>,
    },
    /// Request a native-session lifecycle state.
    Control {
        /// Requested lifecycle state.
        control: Control,
    },
    /// Read the session and its native resume reference.
    Observe,
    /// Where prompt `id`, accepted at inbox sequence `inbox`, stands: the
    /// shared committed reply, with the outcome its settled turn recorded.
    Reply {
        /// The prompt's message identity, returned by Spawn or Send.
        id: IntentId,
        /// The prompt's inbox sequence, returned by Spawn or Send.
        inbox: u64,
    },
    /// After fencing the old process custody, settle an orphaned turn.
    Recover,
}

fn prompt_bound(prompt: &str) -> Result<()> {
    if prompt.is_empty() || prompt.len() > MAXIMUM_PROMPT {
        return Err(Error::Invalid("native prompt exceeds input bound".into()));
    }
    Ok(())
}

/// The native resume id an event names: a Codex thread, a Claude Code
/// session or a Pi session header.
fn native_session_id(event: &Value) -> Option<&str> {
    event
        .get("thread_id")
        .or_else(|| event.get("session_id"))
        .or_else(|| {
            (event.get("type").and_then(Value::as_str) == Some("session"))
                .then(|| event.get("id"))
                .flatten()
        })
        .and_then(Value::as_str)
}

/// The last valid native resume id a turn's event log names.
fn logged_session_id(log: &[u8]) -> Option<String> {
    log.split(|byte| *byte == b'\n')
        // Only lines that can name a session are decoded.
        .filter(|line| {
            line.windows(b"_id".len()).any(|window| window == b"_id")
                || line
                    .windows(b"session".len())
                    .any(|window| window == b"session")
        })
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .filter_map(|event| native_session_id(&event).map(str::to_owned))
        .filter(|id| valid_session_id(id))
        .last()
}

fn valid_session_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
}

/// The claim exchange fused onto request `operation`: its own inbox identity.
fn handoff(operation: OperationId) -> OperationId {
    crate::local::context::publication_operation(
        b"acyclic.native-handoff/v1\0",
        &operation.into_bytes(),
    )
}

/// The native session save derived at the settlement of claim `claim`.
fn derived_session(claim: OperationId) -> OperationId {
    crate::local::context::publication_operation(
        b"acyclic.native-session/v1\0",
        &claim.into_bytes(),
    )
}

impl HostWorkspaceOwner {
    pub(super) async fn session(
        &self,
        operation: OperationId,
        parent: &AgentPath,
        reference: &str,
        action: SessionAction,
        next: bool,
    ) -> Result<Value> {
        self.attach(parent).await?;
        let child = agents::child(parent, reference)?;
        if let SessionAction::Spawn { prompt } = &action {
            prompt_bound(prompt)?;
            host_workspace::child_reference(reference)?;
            let (state, _) = self.native.session(parent).await?;
            // Replaying a spawn is idempotent only for its own prompt.
            let brief = self.native.brief(&state, reference, prompt).await?;
            if !state.open_children.contains(&child) {
                // Host-authored parent work is the child's starting point.
                self.view(parent)?.capture(operation).await?;
                self.native
                    .request(
                        parent,
                        operation,
                        &Event::Spawn {
                            reference: reference.into(),
                            brief,
                        },
                    )
                    .await?;
            }
        }
        self.attach(&child).await?;
        let (mut accepted, mut fence) = (None, None);
        match action {
            SessionAction::Observe => {}
            // The first prompt is a message with the spawn's own identity, so
            // a replayed spawn names the same prompt at the same sequence.
            SessionAction::Spawn { prompt } => {
                let id = IntentId::try_from("native:spawn").map_err(storage)?;
                let inbox = self.native.message(&child, &id, &prompt).await?;
                accepted = Some(json!({"id": id, "inbox": inbox}));
            }
            SessionAction::Send { prompt } => {
                prompt_bound(&prompt)?;
                let id = IntentId::try_from(format!("native:{operation}")).map_err(storage)?;
                let inbox = self.native.message(&child, &id, &prompt).await?;
                accepted = Some(json!({"id": id, "inbox": inbox}));
            }
            SessionAction::Reply { id, inbox } => {
                accepted = Some(json!({"reply": self.native.reply(&child, &id, inbox).await?}));
            }
            SessionAction::Next => return self.claim(operation, &child, false).await,
            SessionAction::Complete { claim, outcome } => {
                fence = Some(
                    self.complete_session(operation, &child, &claim, outcome)
                        .await?,
                );
            }
            SessionAction::Control { control } => {
                if self.native.session(&child).await?.1.control == Control::Archive
                    && control != Control::Archive
                {
                    return Err(Error::Conflict("native session is archived".into()));
                }
                self.native
                    .request(&child, operation, &Event::Control { control })
                    .await?;
            }
            SessionAction::Recover => {
                let (state, session) = self.native.session(&child).await?;
                if let Some(claimed) = session.claimed() {
                    let claim = TurnClaim {
                        agent: state.path,
                        operation: claimed,
                        turn: session.settled + 1,
                    };
                    fence = Some(
                        self.complete_session(operation, &child, &claim, Outcome::Cancelled)
                            .await?,
                    );
                }
            }
        }
        // The accepted work rides this same exchange. A settlement keeps the
        // fence it captured under; any other action takes the child's fence
        // only when no process holds it, so a running child is never claimed.
        let work = if next {
            let fence = match fence {
                Some(fence) => Some(fence),
                None => self.fence(&child)?,
            };
            match fence {
                Some(_fence) => Some(self.claim(handoff(operation), &child, true).await?),
                None => Some(Value::Null),
            }
        } else {
            None
        };
        let mut value = self.session_value(&child).await?;
        if let Some(object) = value.as_object_mut() {
            if let Some(Value::Object(accepted)) = accepted {
                object.extend(accepted);
            }
            if let Some(work) = work {
                object.insert("next".into(), work);
            }
        }
        Ok(value)
    }

    /// The child's physical fence, when no live process holds it: every
    /// preceding owner's tree is fenced. `None` while a process holds it.
    fn fence(
        &self,
        child: &AgentPath,
    ) -> Result<Option<super::super::host_process::QuiescentHostProcess>> {
        let workspace = self
            .local
            .workspace_path()
            .ok_or_else(|| Error::Invalid("native fence has no workspace".into()))?;
        let view = self.view(child)?;
        match super::super::host_process::HostProcessLease::acquire(workspace, child, view.path()) {
            Ok(lease) => Ok(Some(lease.quiescent())),
            Err(Error::Conflict(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Claims the child's next input as request `operation`. A claimed input
    /// is refused unless `fenced`: its caller then holds the physical fence,
    /// which proves no process runs it, and receives it under its exact claim.
    async fn claim(
        &self,
        operation: OperationId,
        child: &AgentPath,
        fenced: bool,
    ) -> Result<Value> {
        let (_, session) = self.native.session(child).await?;
        if session.claimed().is_some() {
            if !fenced {
                return Err(Error::Conflict("native turn is already claimed".into()));
            }
        } else if session.control != Control::Resume || session.running.is_none() {
            return Ok(Value::Null);
        } else {
            self.native.request(child, operation, &Event::Claim).await?;
        }
        let (state, session) = self.native.session(child).await?;
        let Some(Running {
            input,
            claimed: Some(claimed),
            ..
        }) = &session.running
        else {
            return Err(Error::Conflict("native claim was not recorded".into()));
        };
        let TurnInput::Message { body, .. } = input else {
            return Err(Error::Conflict("native turn runs no prompt".into()));
        };
        let prompt = String::from_utf8(self.native.read(&state, body).await?).map_err(storage)?;
        let claim = TurnClaim {
            agent: child.clone(),
            operation: *claimed,
            turn: session.settled + 1,
        };
        Ok(
            json!({"task":child,"cwd":self.view(child)?.path(),"turn":claim.turn,
            "claim":claim,"prompt":prompt,"session":session.native}),
        )
    }

    /// Settles `claim` under the child's physical fence, which it returns so
    /// the next input can be claimed under the same fence.
    async fn complete_session(
        &self,
        operation: OperationId,
        child: &AgentPath,
        claim: &TurnClaim,
        outcome: Outcome<TurnOutput>,
    ) -> Result<super::super::host_process::QuiescentHostProcess> {
        if claim.agent != *child {
            return Err(Error::Conflict(
                "native completion selects another child".into(),
            ));
        }
        let view = self.view(child)?;
        let workspace = self
            .local
            .workspace_path()
            .ok_or_else(|| Error::Invalid("native completion has no workspace".into()))?;
        let quiescence =
            super::super::host_process::HostProcessLease::acquire(workspace, child, view.path())?
                .quiescent();
        quiescence.verify(child, view.path())?;
        let (_, session) = self.native.session(child).await?;
        match session.claimed() {
            Some(claimed) if claimed == claim.operation => {
                view.capture(operation).await?;
                // The turn's process recorded its whole event stream; the
                // settled conversation entry carries it, and the resume id
                // it last named is the session's.
                let events = match quiescence.events(claim.operation)? {
                    Some(bytes) => {
                        if let Some(id) = logged_session_id(&bytes)
                            && session
                                .native
                                .as_ref()
                                .and_then(|native| native.get("id"))
                                .and_then(Value::as_str)
                                != Some(id.as_str())
                        {
                            self.native
                                .request(
                                    child,
                                    derived_session(claim.operation),
                                    &Event::Native {
                                        session: json!({ "id": id }),
                                    },
                                )
                                .await?;
                        }
                        Some(
                            self.native
                                .stage_events(child, claim.operation, &bytes)
                                .await?,
                        )
                    }
                    None => None,
                };
                self.native
                    .request(
                        child,
                        operation,
                        &Event::Complete {
                            operation: claim.operation,
                            outcome,
                            events,
                        },
                    )
                    .await?;
                quiescence.retire_events(claim.operation)?;
            }
            // A replayed completion of a turn that already settled.
            _ if session.settled >= claim.turn => quiescence.retire_events(claim.operation)?,
            _ => {
                return Err(Error::Conflict(
                    "native completion differs from its original claim".into(),
                ));
            }
        }
        Ok(quiescence)
    }

    /// The plugin's view of one session.
    pub(super) async fn session_value(&self, child: &AgentPath) -> Result<Value> {
        let (state, session) = self.native.session(child).await?;
        let pending = session.pending(&state);
        Ok(json!({"task":child,"cwd":self.view(child)?.path(),
            "status":{"settled":session.claimed().is_none(),"turn":session.settled},
            "control":session.control,"events":session.events,"results":session.result.into_iter()
                .map(|outcome| json!({ "outcome": outcome })).collect::<Vec<_>>(),
            "pending":pending}))
    }

    /// The open children `parent` spawned, by parent-local name: the whole
    /// bounded set.
    pub(super) async fn sessions(&self, parent: &AgentPath) -> Result<Value> {
        let (state, session) = self.native.session(parent).await?;
        // A child whose report a stop dropped is closed all the same.
        let open = |reference: &&String| {
            agents::child(&state.path, reference)
                .is_ok_and(|path| state.open_children.contains(&path))
        };
        let references = session.spawned.iter().filter(open).collect::<Vec<_>>();
        Ok(json!({ "references": references }))
    }
}
