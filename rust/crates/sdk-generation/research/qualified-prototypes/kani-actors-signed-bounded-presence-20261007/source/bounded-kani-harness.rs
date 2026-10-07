// Narrow bounded-allocation production ingress obligations. These fixtures are
// concrete and consumed once; only u64 values and presence tags are symbolic.
#[kani::proof]
#[kani::unwind(1)]
fn subscription_observation_bounded_direct_u64_and_failed_cursor_presence() {
    let delivered: u64 = kani::any();
    let completed: u64 = kani::any();
    let recoverable: u64 = kani::any();
    let failed_value: u64 = kani::any();
    let failed_present: bool = kani::any();
    let failed = if failed_present { Some(failed_value) } else { None };
    let parsed = SubscriptionObservation::try_from(wire::SubscriptionObservation {
        subscription_id: String::from("subscription"),
        stream_path: String::from("stream"),
        state: 1,
        delivered_cursor: delivered,
        completed_cursor: completed,
        recoverable_cursor: recoverable,
        placement_anchor: true,
        retry_count: 2,
        failure_code: String::from("none"),
        failed_cursor: failed,
    })
    .expect("fixed bounded wire fixture is valid");
    assert_eq!(parsed.delivered_cursor(), delivered);
    assert_eq!(parsed.completed_cursor(), completed);
    assert_eq!(parsed.recoverable_cursor(), recoverable);
    assert_eq!(parsed.failed_cursor(), failed);
}

#[kani::proof]
#[kani::unwind(65)]
fn actor_observation_bounded_direct_u64_presence_and_fixed_digest() {
    let checkpoint: u64 = kani::any();
    let checkpoint_present: bool = kani::any();
    let expected_checkpoint = if checkpoint_present { Some(checkpoint) } else { None };
    let epoch: u64 = kani::any();
    let revision: u64 = kani::any();
    let parsed = ActorObservation::try_from(wire::ActorObservation {
        actor_id: String::from("actor"),
        code_sha256: vec![1; 32].into(),
        home_region: String::from("region"),
        state: 1,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: expected_checkpoint,
        checkpoint_epoch: epoch,
        configuration_revision: revision,
    })
    .expect("fixed bounded wire fixture is valid");
    assert_eq!(parsed.code_sha256().as_bytes(), &[1; 32]);
    assert_eq!(parsed.checkpoint_unix_millis(), expected_checkpoint);
    assert_eq!(parsed.checkpoint_epoch(), epoch);
    assert_eq!(parsed.configuration_revision(), revision);
}

#[kani::proof]
#[kani::unwind(1)]
fn actor_response_bounded_direct_optional_actor_presence() {
    let present: bool = kani::any();
    let actor = wire::ActorObservation {
        actor_id: String::from("actor"),
        code_sha256: vec![1; 32].into(),
        home_region: String::from("region"),
        state: 1,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: None,
        checkpoint_epoch: u64::MAX,
        configuration_revision: u64::MAX,
    };
    let parsed = CreateActorResponse::try_from(wire::CreateActorResponse {
        actor: if present { Some(actor) } else { None },
    })
    .expect("absent or fixed bounded actor response is valid");
    assert_eq!(parsed.actor().is_some(), present);
}
