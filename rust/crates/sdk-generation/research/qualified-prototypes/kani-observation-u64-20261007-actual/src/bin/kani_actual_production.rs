//! Kani harnesses bound directly to the current acyclic-actors production crate.
//!
//! These harnesses intentionally call the public production validators. They do
//! not reimplement a projection or copy the validator algorithm.

use acyclic_actors::{ContractError, validate_add_subscription, validate_create, validate_update};
use acyclic_actors::wire;

fn valid_create(limits: Option<wire::ActorLimits>) -> wire::CreateActorRequest {
    wire::CreateActorRequest {
        code_sha256: vec![1; 32],
        home_region: "eu".into(),
        bindings: Vec::new(),
        limits,
        subscriptions: Vec::new(),
        idempotency_key: "create-proof".into(),
    }
}

fn valid_update(limits: Option<wire::ActorLimits>, expected: u64) -> wire::UpdateActorRequest {
    wire::UpdateActorRequest {
        actor_id: "actor-proof".into(),
        code_sha256: vec![1; 32],
        bindings: Vec::new(),
        limits,
        expected_configuration_revision: expected,
        idempotency_key: "update-proof".into(),
    }
}

fn valid_subscription(start: Option<wire::subscription_start::Start>) -> wire::AddSubscriptionRequest {
    wire::AddSubscriptionRequest {
        actor_id: "actor-proof".into(),
        subscription: Some(wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart { start }),
            placement_anchor: false,
        }),
        idempotency_key: "subscription-proof".into(),
    }
}

/// Actual production property: for a present ActorLimits message, the public
/// production validator accepts exactly when every selected u64 limit is > 0.
#[kani::proof]
fn production_validate_create_limits() {
    let limits = wire::ActorLimits {
        handler_timeout_millis: kani::any(),
        memory_bytes: kani::any(),
        checkpoint_bytes: kani::any(),
    };
    let expected = limits.handler_timeout_millis > 0
        && limits.memory_bytes > 0
        && limits.checkpoint_bytes > 0;
    let result = validate_create(&valid_create(Some(limits)));
    kani::assert((result == Ok(())) == expected, "validate_create limit predicate");
}

/// Actual production property: Option presence is enforced by the public
/// validator; absent limits are rejected even when all symbolic values would
/// otherwise satisfy the positive predicate.
#[kani::proof]
fn production_validate_create_limits_presence() {
    let present: bool = kani::any();
    let limits = wire::ActorLimits {
        handler_timeout_millis: kani::any(),
        memory_bytes: kani::any(),
        checkpoint_bytes: kani::any(),
    };
    let request = valid_create(if present { Some(limits) } else { None });
    let result = validate_create(&request);
    if !present {
        kani::assert(result == Err(ContractError::InvalidArgument), "missing limits rejected");
    }
}

/// Actual production property: update validation reads the same full-width
/// limits predicate and ignores expected_configuration_revision at this layer.
#[kani::proof]
fn production_validate_update_limits_and_expected_revision() {
    let limits = wire::ActorLimits {
        handler_timeout_millis: kani::any(),
        memory_bytes: kani::any(),
        checkpoint_bytes: kani::any(),
    };
    let expected = limits.handler_timeout_millis > 0
        && limits.memory_bytes > 0
        && limits.checkpoint_bytes > 0;
    let revision: u64 = kani::any();
    let result = validate_update(&valid_update(Some(limits), revision));
    kani::assert((result == Ok(())) == expected, "validate_update limit predicate");
}

/// Actual production property: every full-width cursor variant accepted by
/// the production subscription validator is preserved through its oneof path.
#[kani::proof]
fn production_validate_add_subscription_cursor() {
    let cursor: u64 = kani::any();
    let result = validate_add_subscription(&valid_subscription(Some(
        wire::subscription_start::Start::Cursor(cursor),
    )));
    kani::assert(result == Ok(()), "all u64 subscription cursors accepted");
}

/// Single bounded milestone harness covering the production predicates used by
/// this prototype. Keeping this as one target avoids duplicate Kani processes.
#[kani::proof]
fn production_selected_properties() {
    let limits = wire::ActorLimits {
        handler_timeout_millis: kani::any(),
        memory_bytes: kani::any(),
        checkpoint_bytes: kani::any(),
    };
    let all_positive = limits.handler_timeout_millis > 0
        && limits.memory_bytes > 0
        && limits.checkpoint_bytes > 0;
    let create = validate_create(&valid_create(Some(limits)));
    kani::assert((create == Ok(())) == all_positive, "production create limits");

    let present: bool = kani::any();
    let create_with_presence = valid_create(if present { Some(limits) } else { None });
    let presence_result = validate_create(&create_with_presence);
    if !present {
        kani::assert(
            presence_result == Err(ContractError::InvalidArgument),
            "production create limits presence",
        );
    }

    let revision: u64 = kani::any();
    let update = validate_update(&valid_update(Some(limits), revision));
    kani::assert((update == Ok(())) == all_positive, "production update limits");

    let cursor: u64 = kani::any();
    let subscription = validate_add_subscription(&valid_subscription(Some(
        wire::subscription_start::Start::Cursor(cursor),
    )));
    kani::assert(subscription == Ok(()), "production cursor acceptance");
}

fn main() {}

