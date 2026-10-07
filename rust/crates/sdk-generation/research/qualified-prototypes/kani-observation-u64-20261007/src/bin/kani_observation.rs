use kani_observation_u64_20261007::{
    project_actor, project_create_response, project_update, production_wire,
};

fn main() {}

fn symbolic_subscription() -> production_wire::SubscriptionObservation {
    production_wire::SubscriptionObservation {
        delivered_cursor: kani::any(),
        completed_cursor: kani::any(),
        recoverable_cursor: kani::any(),
        failed_cursor: if kani::any() {
            Some(kani::any())
        } else {
            None
        },
        ..Default::default()
    }
}

fn symbolic_actor() -> production_wire::ActorObservation {
    production_wire::ActorObservation {
        subscriptions: vec![symbolic_subscription()],
        checkpoint_unix_millis: if kani::any() {
            Some(kani::any())
        } else {
            None
        },
        checkpoint_epoch: kani::any(),
        configuration_revision: kani::any(),
        ..Default::default()
    }
}

#[kani::proof]
fn actor_projection_preserves_selected_u64_and_option_fields() {
    let source = symbolic_actor();
    let projected = project_actor(&source);
    let projected_subscription = projected.subscription.expect("one symbolic subscription");
    let source_subscription = &source.subscriptions[0];

    assert!(projected_subscription.delivered_cursor == source_subscription.delivered_cursor);
    assert!(projected_subscription.completed_cursor == source_subscription.completed_cursor);
    assert!(projected_subscription.recoverable_cursor == source_subscription.recoverable_cursor);
    assert!(projected_subscription.failed_cursor == source_subscription.failed_cursor);
    assert!(projected.checkpoint_unix_millis == source.checkpoint_unix_millis);
    assert!(projected.checkpoint_epoch == source.checkpoint_epoch);
    assert!(projected.configuration_revision == source.configuration_revision);
}

#[kani::proof]
fn create_response_actor_optional_presence_is_preserved() {
    let present = kani::any();
    let actor = symbolic_actor();
    let response = production_wire::CreateActorResponse {
        actor: if present { Some(actor) } else { None },
    };
    let projected = project_create_response(&response);
    assert!(projected.is_some() == present);
    if present {
        let projected_actor = projected.expect("present actor");
        let source_actor = response.actor.as_ref().expect("present actor");
        assert!(projected_actor == project_actor(source_actor));
    }
    std::mem::forget(response);
}

#[kani::proof]
fn update_expected_revision_and_actual_projection_are_preserved() {
    let expected: u64 = kani::any();
    let actual_present = kani::any();
    let request = production_wire::UpdateActorRequest {
        expected_configuration_revision: expected,
        ..Default::default()
    };
    let response = production_wire::UpdateActorResponse {
        actor: if actual_present { Some(symbolic_actor()) } else { None },
    };
    let projected = project_update(&request, &response);
    assert!(projected.expected_configuration_revision == expected);
    assert!(projected.actual.is_some() == actual_present);
    if actual_present {
        let projected_actor = projected.actual.expect("present actor");
        let source_actor = response.actor.as_ref().expect("present actor");
        assert!(projected_actor.subscription == project_actor(source_actor).subscription);
        assert!(projected_actor.checkpoint_unix_millis == source_actor.checkpoint_unix_millis);
        assert!(projected_actor.checkpoint_epoch == source_actor.checkpoint_epoch);
        assert!(projected_actor.configuration_revision == source_actor.configuration_revision);
    }
    std::mem::forget(request);
    std::mem::forget(response);
}
