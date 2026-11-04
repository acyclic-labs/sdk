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

/// Applies owner request `id`, returning the input it settled. Stale or
/// replayed requests are recorded, never failed: a failed turn would leave its
/// input queued ahead of every later one.
pub(super) fn apply(
    agent: &AgentState,
    session: &mut Session,
    id: &IntentId,
    source: &FileRef,
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
                history: None,
            });
            session.spawned.push(reference);
        }
        Event::Claim {} => {
            let reason = match &session.running {
                None => Some("native session has no pending input"),
                Some(running) if running.claimed.is_some() => Some("native input already claimed"),
                Some(_) if session.control != Control::Resume => Some("native session is stopped"),
                Some(_) => None,
            };
            if let Some(reason) = reason {
                return Ok(reject(session, id, reason));
            }
            // The accepted request owns its claim, so its reply can be
            // reconstructed without admitting a competing request.
            let claim = claim_id(id);
            if let Some(running) = &mut session.running {
                running.claimed = Some(claim);
            }
        }
        Event::Complete {
            operation,
            outcome: _,
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
            session.result = Some(StoredOutcome::Complete(source.clone()));
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
                session.result = Some(StoredOutcome::Cancelled);
                session.events = None;
                return Ok(session.running.take());
            }
        }
    }
    Ok(None)
}
