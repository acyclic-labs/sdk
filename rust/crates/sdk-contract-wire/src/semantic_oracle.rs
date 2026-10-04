//! Rust-owned semantic expectations for executable qualification scenarios.
//!
//! These values are deliberately sparse.  An entry is published only after a
//! Rust fixture exercises the complete stateful sequence and its test asserts
//! the decoded values.  Generators must leave every other RPC pending rather
//! than infer observations from policy names or protobuf field names.

/// The canonical Actors sequence is CreateActor followed by UpdateActor.
///
/// The fixture creates `fixture-actor` at configuration revision 1, updates
/// it, and observes the same identity at revision 2 while its state remains
/// `ACTIVE`.  The JSON shape is the `observations` object in
/// `rpc-scenario-result.v1`.
pub const ACTORS_UPDATE_ACTOR_JSON: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[1,2],"status_trace":["ACTIVE"],"transitions":[]}"#;

/// Returns an exact expectation only for a scenario backed by a Rust fixture.
#[must_use]
pub fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.actors.v1.ActorsService/UpdateActor" => Some(ACTORS_UPDATE_ACTOR_JSON),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exercised_scenarios_have_expectations() {
        assert!(expectation_json("acyclic.actors.v1.ActorsService/UpdateActor").is_some());
        assert!(expectation_json("acyclic.actors.v1.ActorsService/CreateActor").is_none());
        assert!(expectation_json("acyclic.workers.v1.WorkersService/PublishVersion").is_none());
    }
}
