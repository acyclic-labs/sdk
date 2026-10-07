//! Negative control: intentionally false assertion over the unchanged production validator.
use acyclic_actors::validate_add_subscription;
use acyclic_actors::wire;

fn valid_subscription(cursor: u64) -> wire::AddSubscriptionRequest {
    wire::AddSubscriptionRequest {
        actor_id: "actor-proof".into(),
        subscription: Some(wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(cursor)),
            }),
            placement_anchor: false,
        }),
        idempotency_key: "subscription-proof".into(),
    }
}

#[kani::proof]
fn production_current_cursor_negative_mutation() {
    let cursor: u64 = kani::any();
    let result = validate_add_subscription(&valid_subscription(cursor));
    kani::assert(result == Err(acyclic_actors::ContractError::InvalidArgument), "intentional negative mutation");
}

fn main() {}