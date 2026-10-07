//! Kani prototype for the selected Actors observation projection.
//!
//! The wire module is included directly from the current production Actors
//! crate.  The proof deliberately projects selected fields by reference and
//! never clones a whole wire message.  It makes no claim about transport,
//! arbitrary string semantics, ordering, or service behavior.

/// Current production Actors v1 generated wire types. `build.rs` extracts the
/// wire prefix from the production file and intentionally stops before the
/// generated tonic transport include.
pub mod production_wire {
    #![allow(missing_docs, clippy::all, clippy::pedantic)]
    include!(concat!(env!("OUT_DIR"), "/actors_wire_types.rs"));
}

/// Selected subscription observation values retained by the projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionProjection {
    pub delivered_cursor: u64,
    pub completed_cursor: u64,
    pub recoverable_cursor: u64,
    pub failed_cursor: Option<u64>,
}

/// Selected actor observation values retained by the projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActorProjection {
    pub subscription: Option<SubscriptionProjection>,
    pub checkpoint_unix_millis: Option<u64>,
    pub checkpoint_epoch: u64,
    pub configuration_revision: u64,
}

/// Selected mutation values retained by the projection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationProjection {
    pub expected_configuration_revision: u64,
    pub actual: Option<ActorProjection>,
}

fn project_subscription(
    source: &production_wire::SubscriptionObservation,
) -> SubscriptionProjection {
    SubscriptionProjection {
        delivered_cursor: source.delivered_cursor,
        completed_cursor: source.completed_cursor,
        recoverable_cursor: source.recoverable_cursor,
        failed_cursor: source.failed_cursor,
    }
}

/// Project selected production observation fields without cloning the wire.
pub fn project_actor(source: &production_wire::ActorObservation) -> ActorProjection {
    ActorProjection {
        subscription: source.subscriptions.first().map(project_subscription),
        checkpoint_unix_millis: source.checkpoint_unix_millis,
        checkpoint_epoch: source.checkpoint_epoch,
        configuration_revision: source.configuration_revision,
    }
}

/// Project optional actor presence from the production create response.
pub fn project_create_response(
    source: &production_wire::CreateActorResponse,
) -> Option<ActorProjection> {
    source.actor.as_ref().map(project_actor)
}

/// Project the expected revision and optional actual actor from production
/// update request/response values.
pub fn project_update(
    request: &production_wire::UpdateActorRequest,
    response: &production_wire::UpdateActorResponse,
) -> MutationProjection {
    MutationProjection {
        expected_configuration_revision: request.expected_configuration_revision,
        actual: response.actor.as_ref().map(project_actor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_projection_keeps_optional_presence() {
        let actor = production_wire::ActorObservation {
            subscriptions: vec![production_wire::SubscriptionObservation {
                failed_cursor: Some(0),
                ..Default::default()
            }],
            checkpoint_unix_millis: Some(0),
            ..Default::default()
        };
        assert_eq!(project_actor(&actor).subscription.unwrap().failed_cursor, Some(0));
        assert_eq!(project_actor(&actor).checkpoint_unix_millis, Some(0));
        assert_eq!(project_create_response(&production_wire::CreateActorResponse { actor: None }), None);
    }
}
