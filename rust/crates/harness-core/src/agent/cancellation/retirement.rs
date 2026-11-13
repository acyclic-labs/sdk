//! Revalidate terminal facts through the actor's existing composition protocol.
use super::{Error, input_events};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{Checkpoint, Composer, Composition, GuestError, GuestFuture, Participant},
};
use acyclic_stream::{
    CommitCondition, CommitConflict, CommitOutcome, CommitRequest, Record, StreamError, StreamPath,
};

pub(in crate::agent) const RETIRE: &str = "acyclic.harness.agent.retire.v1";

pub(in crate::agent) fn retirement_participant(
    inner: Option<Participant>,
) -> Result<Participant, Error> {
    let inner = inner.map(|participant| (participant.kind, participant.plan.to_vec()));
    Ok(Participant {
        kind: RETIRE.into(),
        plan: crate::contract::canonical_json_bytes(&inner)
            .map_err(|_| Error::Record("retirement participant does not encode"))?
            .into(),
    })
}

pub(in crate::agent) fn open_retirement(
    composer: &dyn Composer,
    actor: &ActorReference,
    previous: Option<&Checkpoint>,
    delivered: Option<&Record>,
    next: &Checkpoint,
    participant: &Participant,
) -> Result<Box<dyn Composition>, GuestError> {
    let fail = |error: &str| GuestError::Handler(error.into());
    let inner: Option<(String, Vec<u8>)> = serde_json::from_slice(&participant.plan)
        .map_err(|_| fail("invalid retirement participant"))?;
    if crate::contract::canonical_json_bytes(&inner)
        .map_err(|_| fail("invalid retirement participant"))?
        != participant.plan.as_ref()
    {
        return Err(fail("retirement participant is not canonical"));
    }
    let head =
        super::super::checkpoint_head(next).map_err(|_| fail("invalid retirement checkpoint"))?;
    if head.state.path.id().as_str() != actor.id() {
        return Err(fail("retirement checkpoint names another actor"));
    }
    let path = input_events(&head.state.path).map_err(|_| fail("invalid input events path"))?;
    let inner = match inner {
        Some((kind, plan)) if kind != RETIRE => Some(composer.open(
            actor,
            previous,
            delivered,
            next,
            &Participant {
                kind,
                plan: plan.into(),
            },
        )?),
        Some(_) => return Err(fail("retirement participant cannot contain itself")),
        None => None,
    };
    Ok(Box::new(Retirement { path, inner }))
}

struct Retirement {
    path: StreamPath,
    inner: Option<Box<dyn Composition>>,
}

impl Composition for Retirement {
    fn revalidate(&self, conflicts: &[CommitConflict]) -> bool {
        let other = conflicts
            .iter()
            .filter(|conflict| {
                !matches!(conflict,
                    CommitConflict::Tail { path, .. } | CommitConflict::Exists { path }
                        | CommitConflict::Missing { path } if path == &self.path
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        !conflicts.is_empty()
            && (other.is_empty()
                || self
                    .inner
                    .as_ref()
                    .is_some_and(|inner| inner.revalidate(&other)))
    }

    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            if !request
                .effects
                .iter()
                .filter(|effect| !effect.soft)
                .flat_map(|effect| &effect.conditions)
                .any(|condition| {
                    matches!(condition,
                    CommitCondition::Tail { path, .. } | CommitCondition::Absent { path }
                        if path == &self.path)
                })
            {
                return Err(GuestError::Handler(
                    "retirement has no terminal-fact fence".into(),
                ));
            }
            match &mut self.inner {
                Some(inner) => inner.seal(request).await,
                None => Ok(request),
            }
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            match self.inner {
                Some(inner) => inner.settle(outcome).await,
                None => Ok(()),
            }
        })
    }
}
