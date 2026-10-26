//! Native session mailboxes use the same files as `GraphCoder` child agents.
use super::*;
use crate::task_volume::{
    ChildFile, ChildObservation, ChildRequest, ChildResultWindow, ChildStatus, ChildTurn,
    ChildTurnClaim, ChildTurnPublication, Component, Control, FileKey, FileValue,
};
use crate::{Outcome, executor::TurnOutput};

/// Coordination only. Native session processes are owned by the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SessionAction {
    /// Admit a child with its immutable initial input.
    Spawn {
        /// Initial prompt.
        prompt: String,
    },
    /// Publish an immutable follow-up input.
    Send {
        /// Message body.
        prompt: String,
    },
    /// Claim the next input, after the preceding process is quiescent.
    Next,
    /// Capture and settle a process that has exited, including failed writes.
    Complete {
        /// Exact original execution claim, returned by Next.
        claim: ChildTurnClaim,
        /// Native outcome.
        outcome: Outcome<TurnOutput>,
    },
    /// Save an opaque native resume id; never a transcript or credential.
    SaveSession {
        /// Native id.
        id: String,
    },
    /// Publish a native-host lifecycle intent in the shared control file.
    Control {
        /// Requested lifecycle state.
        control: Control,
    },
    /// Read the live shared mailbox and native resume reference.
    Observe,
    /// After fencing the old process custody, settle an orphaned turn.
    Recover,
}

#[derive(Clone)]
struct Session {
    parent: TaskId,
    child: TaskId,
    reference: Component,
    observation: ChildObservation,
}

#[cfg(test)]
pub(super) async fn assert_lifecycle_projection(
    owner: &mut HostWorkspaceOwner,
    parent: TaskId,
    reference: &str,
) -> Result<()> {
    let admitted = owner
        .admit_session(
            OperationId::new(),
            parent,
            &Component::new(reference).map_err(storage)?,
            &SessionAction::Next,
        )
        .await?;
    assert!(
        admitted.observation.results.is_empty(),
        "claim admission must not project the previous answer"
    );
    assert!(admitted.observation.status.is_some());
    Ok(())
}

impl Session {
    fn key(&self, file: ChildFile) -> FileKey {
        FileKey::Child {
            reference: self.reference.clone(),
            file,
        }
    }
}

impl HostWorkspaceOwner {
    pub(super) async fn session(
        &mut self,
        operation: OperationId,
        parent: TaskId,
        reference: &str,
        action: SessionAction,
    ) -> Result<Value> {
        self.attach(parent).await?;
        let reference = Component::new(reference).map_err(storage)?;
        let session = self
            .admit_session(operation, parent, &reference, &action)
            .await?;
        let child = session.child;
        match action {
            SessionAction::Send { prompt } => {
                if prompt.is_empty() || prompt.len() > 32_768 {
                    return Err(Error::Invalid("native message exceeds input bound".into()));
                }
                self.publish(
                    parent,
                    FileValue::new(
                        session.key(ChildFile::Inbox(
                            Component::new(operation.to_string()).map_err(storage)?,
                        )),
                        prompt.into_bytes(),
                    ),
                )
                .await?;
            }
            SessionAction::SaveSession { id } => self.save_native_session(child, id).await?,
            SessionAction::Control { control } => {
                self.control_session(operation, &session, control).await?;
            }
            SessionAction::Next => return self.next_session(operation, &session).await,
            SessionAction::Complete { claim, outcome } => {
                self.complete_session(operation, &session, &claim, outcome)
                    .await?;
            }
            SessionAction::Recover
                if session
                    .observation
                    .status
                    .is_some_and(|status| !status.settled) =>
            {
                let original = session
                    .observation
                    .status_publication
                    .ok_or_else(|| Error::Conflict("native status has no atomic claim".into()))?;
                let Some(ChildTurn::InFlight { claim }) =
                    self.claim_session(original, &session).await?
                else {
                    return Err(Error::Conflict(
                        "native recovery differs from its claim".into(),
                    ));
                };
                self.complete_session(operation, &session, &claim, Outcome::Cancelled)
                    .await?;
            }
            SessionAction::Spawn { .. } | SessionAction::Observe | SessionAction::Recover => {
                return self.session_value(&session);
            }
        }
        let current = self
            .read_session(parent, session.reference, ChildResultWindow::Latest)
            .await?;
        self.session_value(&current)
    }

    async fn admit_session(
        &mut self,
        operation: OperationId,
        parent: TaskId,
        reference: &Component,
        action: &SessionAction,
    ) -> Result<Session> {
        if !matches!(action, SessionAction::Spawn { .. }) {
            // Mutations need the coherent lifecycle/claim cut, not a copy and
            // validation of the previous answer. Only observations expose it;
            // mutation replies project the new answer after publication.
            let window = if matches!(action, SessionAction::Observe | SessionAction::Recover) {
                ChildResultWindow::Latest
            } else {
                ChildResultWindow::None
            };
            let session = self.read_session(parent, reference.clone(), window).await?;
            self.attach_retained(session.child, true).await?;
            return Ok(session);
        }
        let files = self.files(parent).await?;
        let key = FileKey::Child {
            reference: reference.clone(),
            file: ChildFile::Request,
        };
        let request = files
            .snapshot(&Query::File(key), 1)
            .await
            .map_err(storage)?;
        let prompt = match action {
            SessionAction::Spawn { prompt } if request.files.is_empty() => prompt.clone(),
            _ => {
                ChildRequest::parse(
                    &request
                        .files
                        .first()
                        .ok_or_else(|| Error::NotFound("native child request absent".into()))?
                        .bytes,
                    65_536,
                )?
                .prompt
            }
        };
        if prompt.is_empty() || prompt.len() > 32_768 {
            return Err(Error::Invalid("native prompt exceeds input bound".into()));
        }
        if let SessionAction::Spawn { prompt: supplied } = action
            && supplied != &prompt
        {
            return Err(Error::Conflict(
                "child reference already has another prompt".into(),
            ));
        }
        if request.files.is_empty() {
            let attached = self.attached(parent)?;
            attached
                .workspace
                .capture(&attached.context, operation)
                .await?;
        }
        let child = if let Some(request) = request.files.first() {
            self.local.admit_child_task(&files, request).await?
        } else {
            self.local
                .admit_host_child_request(
                    &self.attached(parent)?.context,
                    reference.as_str(),
                    &prompt,
                )
                .await?
        };
        self.attach_retained(child, !request.files.is_empty())
            .await?;
        self.attached(parent)?
            .workspace
            .name_child(
                &self.attached(parent)?.context,
                &self.attached(child)?.context,
                &self.attached(child)?.workspace,
                reference.as_str(),
            )
            .await?;
        self.read_session(parent, reference.clone(), ChildResultWindow::Latest)
            .await
    }

    async fn read_session(
        &self,
        parent: TaskId,
        reference: Component,
        window: ChildResultWindow,
    ) -> Result<Session> {
        let files = self.files(parent).await?;
        let (child, observation) = self
            .local
            .observe_admitted_child(&files, &reference, parent, window, 1)
            .await?;
        Ok(Session {
            parent,
            child,
            reference,
            observation,
        })
    }

    async fn save_native_session(&self, child: TaskId, id: String) -> Result<()> {
        if id.is_empty() || id.len() > 256 || id.chars().any(char::is_control) {
            return Err(Error::Invalid("native resume id invalid".into()));
        }
        let key = super::events::context_key(child, "session")?;
        let previous = self
            .files(child)
            .await?
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        let previous = previous
            .files
            .first()
            .map(|file| serde_json::from_slice::<Value>(&file.bytes))
            .transpose()
            .map_err(storage)?;
        let session = previous
            .filter(|value| value.get("id").and_then(Value::as_str) == Some(id.as_str()))
            .unwrap_or_else(|| json!({"id":id}));
        self.publish(
            child,
            FileValue::new(key, serde_json::to_vec(&session).map_err(storage)?),
        )
        .await
    }

    async fn publication(&self, session: &Session) -> Result<ChildTurnPublication<'static>> {
        let owner = self
            .attached(session.parent)?
            .context
            .owned_mailbox
            .as_ref()
            .ok_or_else(|| Error::Unauthorized("native owner absent".into()))?;
        let (condition, deadline) = owner.file_publication_condition().await?;
        Ok(ChildTurnPublication {
            expected: session.observation.generation,
            conditions: vec![condition],
            deadline,
            work: None,
        })
    }

    async fn claim_session(
        &self,
        operation: OperationId,
        session: &Session,
    ) -> Result<Option<ChildTurn>> {
        self.files(session.parent)
            .await?
            .inner
            .claim_child_turn(
                &session.reference,
                session.child,
                operation,
                self.publication(session).await?,
            )
            .await
            .map_err(storage)
    }

    async fn control_session(
        &self,
        operation: OperationId,
        session: &Session,
        control: Control,
    ) -> Result<()> {
        // An unstarted input owns no process. Claim it atomically before control
        // blocks new claims; its immutable cancelled outcome uses that same claim.
        let unstarted = if session.observation.status.is_none()
            && matches!(
                control,
                Control::Interrupt | Control::Cancel | Control::Archive
            ) {
            match self.claim_session(operation, session).await? {
                Some(ChildTurn::Claimed { claim, .. }) => Some(claim),
                None => None,
                _ => {
                    return Err(Error::Conflict(
                        "unstarted native input already claimed".into(),
                    ));
                }
            }
        } else {
            None
        };
        self.publish(
            session.parent,
            FileValue::new(session.key(ChildFile::Control), control.bytes().to_vec()),
        )
        .await?;
        let current = self
            .read_session(
                session.parent,
                session.reference.clone(),
                ChildResultWindow::None,
            )
            .await?;
        if let Some(claim) = unstarted {
            self.complete_session(operation, &current, &claim, Outcome::Cancelled)
                .await?;
        } else if let Some(status) = session.observation.status.filter(|value| value.settled) {
            self.publish(
                session.parent,
                FileValue::new(
                    session.key(ChildFile::Status),
                    serde_json::to_vec(&ChildStatus {
                        state: current.observation.state,
                        ..status
                    })
                    .map_err(storage)?,
                ),
            )
            .await?;
        }
        Ok(())
    }

    async fn next_session(&self, operation: OperationId, session: &Session) -> Result<Value> {
        let Some(turn) = self.claim_session(operation, session).await? else {
            return Ok(Value::Null);
        };
        let (claim, input) = match turn {
            ChildTurn::Claimed { claim, input } => (claim, input),
            ChildTurn::InFlight { .. } => {
                return Err(Error::Conflict("native turn is already claimed".into()));
            }
            ChildTurn::Settled { .. } => return Ok(Value::Null),
        };
        let prompt = if matches!(
            input.key,
            FileKey::Child {
                file: ChildFile::Request,
                ..
            }
        ) {
            ChildRequest::parse(&input.bytes, 65_536)?.prompt
        } else {
            String::from_utf8(input.bytes.to_vec()).map_err(storage)?
        };
        let saved = self
            .files(session.child)
            .await?
            .snapshot(
                &Query::File(super::events::context_key(session.child, "session")?),
                1,
            )
            .await
            .map_err(storage)?;
        Ok(
            json!({"task":session.child,"cwd":self.attached(session.child)?.workspace.path(),
            "turn":claim.turn(),"claim":claim,"prompt":prompt,
            "session":saved.files.first().map(|file| serde_json::from_slice::<Value>(&file.bytes)).transpose().map_err(storage)?}),
        )
    }

    fn session_value(&self, session: &Session) -> Result<Value> {
        let observation = &session.observation;
        let results = observation
            .results
            .iter()
            .map(|file| serde_json::from_slice::<Value>(&file.bytes))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(storage)?;
        Ok(
            json!({"task":session.child,"cwd":self.attached(session.child)?.workspace.path(),"status":observation.status,"control":observation.control,"results":results,"pending":observation.pending_inputs}),
        )
    }

    async fn complete_session(
        &self,
        operation: OperationId,
        session: &Session,
        claim: &ChildTurnClaim,
        outcome: Outcome<TurnOutput>,
    ) -> Result<()> {
        if claim.task() != session.child || claim.reference() != &session.reference {
            return Err(Error::Conflict(
                "native completion selects another child".into(),
            ));
        }
        let attached = self.attached(session.child)?;
        let workspace = self
            .local
            .workspace_path()
            .ok_or_else(|| Error::Invalid("native completion has no workspace".into()))?;
        let quiescence = super::super::host_process::HostProcessLease::acquire(
            workspace,
            session.child,
            attached.workspace.path(),
        )?
        .quiescent();
        quiescence.verify(claim.task(), attached.workspace.path())?;
        match self.claim_session(claim.operation(), session).await? {
            Some(ChildTurn::InFlight { claim: original }) if &original == claim => {
                let attached = self.attached(session.child)?;
                attached
                    .workspace
                    .capture(&attached.context, operation)
                    .await?;
            }
            Some(ChildTurn::Settled {
                claim: original, ..
            }) if &original == claim => {}
            _ => {
                return Err(Error::Conflict(
                    "native completion differs from its original claim".into(),
                ));
            }
        }
        self.files(session.parent)
            .await?
            .inner
            .complete_child_turn(claim, outcome, self.publication(session).await?)
            .await
            .map_err(storage)?;
        Ok(())
    }
}
