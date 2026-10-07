// Prepared against the current canonical source only.
// This file is a harness proposal, not a proof receipt and is not compiled here.
//
// It calls the exported production nominal type directly. The finite witness
// scope is deliberate: arbitrary-string mathematics is not claimed.

use acyclic_actors::domain::{ActorId, DomainError};

#[kani::proof]
#[kani::unwind(2)]
fn actor_id_nominal_fixed_witnesses_preserve_identity() {
    for raw in ["a", "actor-proof", "  "] {
        let raw = raw.to_owned();
        let parsed = ActorId::try_from(raw.clone()).expect("non-empty witness is valid");
        assert_eq!(parsed.as_str(), raw);
        let round_trip: String = parsed.into();
        assert_eq!(round_trip, raw);
    }
}

#[kani::proof]
#[kani::unwind(1)]
fn actor_id_nominal_rejects_empty() {
    assert_eq!(
        ActorId::try_from(String::new()),
        Err(DomainError::EmptyActorId)
    );
}

fn main() {}
