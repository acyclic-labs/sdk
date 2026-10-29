//! Schedule verification of the production authority participant.
#![cfg(loom)]
use acyclic_stream::{Revision, RevisionScope, StreamError};

#[path = "../src/lib/revision/participant.rs"]
mod participant;

#[test]
fn concurrent_replies_cannot_lower_a_retained_frontier() {
    participant::verify_concurrent_frontier(false);
}

#[test]
#[should_panic(expected = "a late reply lowered the required frontier")]
fn replacing_atomic_maximum_exposes_a_late_reply() {
    participant::verify_concurrent_frontier(true);
}
