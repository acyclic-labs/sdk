//! Native session lifecycle: each request is one inbox record of the session's
//! agent, and every reply observes that agent's last conversation entry.
use super::agents::{Brief, Control, Event, TurnClaim};
use super::*;
use crate::{Outcome, executor::TurnOutput};

/// Bound of one native prompt.
const MAXIMUM_PROMPT: usize = 32_768;

/// Coordination only. Native session processes are owned by the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAction {
    /// Spawn the child with its initial input.
    Spawn {
        /// Initial prompt.
        prompt: String,
    },
    /// Queue a follow-up input.
    Send {
        /// Message body.
        prompt: String,
    },
    /// Claim the next input, after the preceding process is quiescent.
    Next,
    /// Capture and settle a process that has exited, including failed writes.
    Complete {
        /// Exact original execution claim, returned by Next.
        claim: TurnClaim,
        /// Native outcome.
        outcome: Outcome<TurnOutput>,
    },
    /// Save an opaque native resume id; never a transcript or credential.
    SaveSession {
        /// Native id.
        id: String,
    },
    /// Request a native-session lifecycle state.
    Control {
        /// Requested lifecycle state.
        control: Control,
    },
    /// Read the session and its native resume reference.
    Observe,
    /// After fencing the old process custody, settle an orphaned turn.
    Recover,
}

fn prompt_bound(prompt: &str) -> Result<()> {
    if prompt.is_empty() || prompt.len() > MAXIMUM_PROMPT {
        return Err(Error::Invalid("native prompt exceeds input bound".into()));
    }
    Ok(())
}

impl HostWorkspaceOwner {
    pub(super) async fn session(
        &mut self,
        operation: OperationId,
        parent: &AgentPath,
        reference: &str,
        action: SessionAction,
    ) -> Result<Value> {
        self.attach(parent).await?;
        let child = agents::child(parent, reference)?;
        if let SessionAction::Spawn { prompt } = &action {
            prompt_bound(prompt)?;
            host_workspace::child_reference(reference)?;
            let (state, _) = self.native.session(parent).await?;
            if state.open_children.contains(&child) {
                // Replaying a spawn is idempotent only for its own prompt.
                if self.native.brief(&child).await?.prompt != *prompt {
                    return Err(Error::Conflict(format!(
                        "session {reference} was already spawned with a different prompt"
                    )));
                }
            } else {
                // Host-authored parent work is the child's starting point.
                self.view(parent)?.capture(operation).await?;
                self.native
                    .request(
                        parent,
                        operation,
                        &Event::Spawn {
                            reference: reference.into(),
                            prompt: prompt.clone(),
                        },
                    )
                    .await?;
            }
        }
        self.attach(&child).await?;
        match action {
            SessionAction::Spawn { .. } | SessionAction::Observe => {}
            SessionAction::Send { prompt } => {
                prompt_bound(&prompt)?;
                self.native.message(&child, operation, &prompt).await?;
            }
            SessionAction::Next => return self.next_session(operation, &child).await,
            SessionAction::Complete { claim, outcome } => {
                self.complete_session(operation, &child, &claim, outcome)
                    .await?;
            }
            SessionAction::SaveSession { id } => {
                if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
                    return Err(Error::Invalid("native resume id invalid".into()));
                }
                let (_, session) = self.native.session(&child).await?;
                if session
                    .native
                    .as_ref()
                    .and_then(|native| native.get("id"))
                    .and_then(Value::as_str)
                    != Some(id.as_str())
                {
                    self.native
                        .request(
                            &child,
                            operation,
                            &Event::Native {
                                session: json!({ "id": id }),
                            },
                        )
                        .await?;
                }
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
                if let Some((claimed, _)) = session.claimed {
                    let claim = TurnClaim {
                        agent: state.path,
                        operation: claimed,
                        turn: session.settled + 1,
                    };
                    self.complete_session(operation, &child, &claim, Outcome::Cancelled)
                        .await?;
                }
            }
        }
        self.session_value(&child).await
    }

    async fn next_session(&self, operation: OperationId, child: &AgentPath) -> Result<Value> {
        let (_, session) = self.native.session(child).await?;
        if session.claimed.is_some() {
            return Err(Error::Conflict("native turn is already claimed".into()));
        }
        if session.control != Control::Resume || session.pending.is_empty() {
            return Ok(Value::Null);
        }
        self.native.request(child, operation, &Event::Claim).await?;
        let (state, session) = self.native.session(child).await?;
        let Some((claimed, input)) = &session.claimed else {
            return Err(Error::Conflict("native claim was not recorded".into()));
        };
        let bytes = self.native.read(&state, &input.prompt).await?;
        let prompt = if input.inbox == 0 {
            serde_json::from_slice::<Brief>(&bytes)
                .map_err(storage)?
                .prompt
        } else {
            String::from_utf8(bytes).map_err(storage)?
        };
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

    async fn complete_session(
        &self,
        operation: OperationId,
        child: &AgentPath,
        claim: &TurnClaim,
        outcome: Outcome<TurnOutput>,
    ) -> Result<()> {
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
        match &session.claimed {
            Some((claimed, _)) if *claimed == claim.operation => {
                view.capture(operation).await?;
                self.native
                    .request(
                        child,
                        operation,
                        &Event::Complete {
                            operation: claim.operation,
                            outcome,
                        },
                    )
                    .await
            }
            // A replayed completion of a turn that already settled.
            _ if session.settled >= claim.turn => Ok(()),
            _ => Err(Error::Conflict(
                "native completion differs from its original claim".into(),
            )),
        }
    }

    /// The plugin's view of one session.
    pub(super) async fn session_value(&self, child: &AgentPath) -> Result<Value> {
        let (_, session) = self.native.session(child).await?;
        Ok(json!({"task":child,"cwd":self.view(child)?.path(),
            "status":{"settled":session.claimed.is_none(),"turn":session.settled},
            "control":session.control,"results":session.result.into_iter()
                .map(|outcome| json!({ "outcome": outcome })).collect::<Vec<_>>(),
            "pending":session.pending.len()}))
    }

    /// Spawned children of `parent` recorded after conversation sequence
    /// `after`, one bounded page; the reply's `after` continues it.
    pub(super) async fn sessions(&self, parent: &AgentPath, after: Option<u64>) -> Result<Value> {
        let from = after.map_or(0, |after| after + 1);
        let entries = self.native.entries(parent, from, 32).await?;
        let next = entries.last().map(|(sequence, _)| *sequence).or(after);
        let references = entries
            .into_iter()
            .filter_map(|(_, entry)| match entry.record {
                agents::Record::Spawned { reference, .. } if entry.agent == *parent => {
                    Some(reference)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        Ok(json!({"after":next,"references":references}))
    }
}
