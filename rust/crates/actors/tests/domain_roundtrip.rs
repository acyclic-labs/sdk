//! Domain projections must preserve response wire values exactly.

use acyclic_actors::{domain::InvokeActorResponse, wire};

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
