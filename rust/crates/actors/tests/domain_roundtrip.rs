//! Domain projections must preserve response wire values exactly.

use acyclic_actors::{
    domain::{ActorObservation, CreateActorResponse, InvokeActorResponse},
    wire,
};

fn observation() -> wire::ActorObservation {
    wire::ActorObservation {
        actor_id: "actor-1".into(),
        code_sha256: (1_u8..=32).collect::<Vec<_>>().into(),
        home_region: "eu-west".into(),
        state: 2,
        subscriptions: vec![wire::SubscriptionObservation {
            subscription_id: "events".into(),
            stream_path: "actors/actor-1/events".into(),
            state: 1,
            delivered_cursor: 11,
            completed_cursor: 22,
            recoverable_cursor: 33,
            placement_anchor: true,
            retry_count: 4,
            failure_code: "temporary".into(),
            failed_cursor: Some(44),
        }],
        checkpoint_unix_millis: Some(55),
        checkpoint_epoch: 66,
        configuration_revision: 77,
    }
}

#[test]
fn actor_observation_and_response_projection_round_trip_distinct_values() {
    let wire = observation();
    let typed = ActorObservation::try_from(wire.clone()).expect("valid observation");
    let round_trip: wire::ActorObservation = typed.into();
    assert_eq!(round_trip, wire);

    let response = wire::CreateActorResponse {
        actor: Some(wire.clone()),
    };
    let typed = CreateActorResponse::try_from(response.clone()).expect("valid response");
    let round_trip: wire::CreateActorResponse = typed.into();
    assert_eq!(round_trip, response);
}

#[test]
fn invoke_response_round_trips_distinct_wire_values() {
    let wire = wire::InvokeActorResponse {
        status: 418,
        body: vec![0, 1, 127, 128, 255].into(),
        headers: vec![
            wire::Header {
                name: "x-first".into(),
                value: "one".into(),
            },
            wire::Header {
                name: "x-second".into(),
                value: "two".into(),
            },
        ],
    };

    let typed: InvokeActorResponse = wire.clone().into();
    let round_trip: wire::InvokeActorResponse = typed.into();
    assert_eq!(round_trip, wire);
}
