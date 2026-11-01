//! The native turn's steps: opening a turn on its input, and applying the
//! owner's hooks to the session its checkpoint holds.
use super::*;

/// Opens the turn that runs `input` until a native process settles it; an
/// archived session starts none.
pub(super) fn start(mut session: Session, inbox: u64, input: TurnInput) -> Result<TurnStep> {
    if session.control == Control::Archive {
        return Ok(idle(Vec::new()));
    }
    session.running = Some(Running {
        inbox,
        input,
        claimed: None,
    });
    waiting(&session, Vec::new())
}

/// The open turn's step: `session` in its checkpoint, awaiting the next hook.
pub(super) fn waiting(session: &Session, outs: Vec<Out>) -> Result<TurnStep> {
    Ok(TurnStep {
        reserved_children: 0,
        checkpoint: Some(serde_json::to_vec(session).map_err(storage)?),
        outs,
        wait: Wait::Host(HOST.into()),
        drops: None,
    })
}

pub(super) fn idle(outs: Vec<Out>) -> TurnStep {
    TurnStep {
        reserved_children: 0,
        checkpoint: None,
        outs,
        wait: Wait::Input,
        drops: None,
    }
}

pub(super) fn reject(session: &mut Session, id: &IntentId, reason: &str) -> Option<Running> {
    session.rejected = Some(Rejected {
        id: id.clone(),
        reason: reason.into(),
    });
    None
}

/// Applies owner request `id` at inbox sequence `inbox`, returning the input it
/// settled. Stale or replayed requests are recorded, never failed: a failed
/// turn would leave its input queued ahead of every later one.
pub(super) fn apply(
    agent: &AgentState,
    session: &mut Session,
    inbox: u64,
    id: &IntentId,
    event: Event,
    outs: &mut Vec<Out>,
) -> Result<Option<Running>> {
    session.rejected = None;
    match event {
        Event::Spawn { reference, brief } => {
            let intent = IntentId::try_from(reference.as_str()).map_err(storage)?;
            if agent
                .open_children
                .contains(&child(&agent.path, &reference)?)
            {
                return Ok(reject(session, id, "native child already exists"));
            }
            outs.push(Out::Spawn {
                profile: None,
                intent,
                brief: Some(brief),
                volumes: Vec::new(),
            });
            session.spawned.push(reference);
        }
        Event::Claim => {
            let reason = match &session.running {
                None => Some("native session has no pending input"),
                Some(running) if running.claimed.is_some() => Some("native input already claimed"),
                Some(_) if session.control != Control::Resume => Some("native session is stopped"),
                Some(_) => None,
            };
            if let Some(reason) = reason {
                return Ok(reject(session, id, reason));
            }
            // Named by its turn number, so a replayed claim names it again.
            let claim = OperationId::from_bytes(claim_bytes(agent, session));
            if let Some(running) = &mut session.running {
                running.claimed = Some(claim);
            }
        }
        Event::Complete {
            operation,
            outcome,
            events,
        } => {
            if session.claimed() != Some(operation) {
                return Ok(reject(
                    session,
                    id,
                    "native completion differs from its claim",
                ));
            }
            session.settled += 1;
            session.result = Some(bounded(outcome));
            session.events = events;
            return Ok(session.running.take());
        }
        Event::Native { session: native } => session.native = Some(native),
        Event::Control { control }
            if session.control == Control::Archive && control != Control::Archive =>
        {
            return Ok(reject(session, id, "native session is archived"));
        }
        Event::Control { control } => {
            session.control = control;
            // An unstarted input owns no process: a stop settles it cancelled.
            if control != Control::Resume
                && session.running.is_some()
                && session.claimed().is_none()
            {
                session.settled += 1;
                session.result = Some(crate::Outcome::Cancelled);
                session.events = None;
                return Ok(session.running.take());
            }
        }
    }
    Ok(None)
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
