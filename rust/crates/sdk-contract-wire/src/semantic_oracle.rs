//! Rust-owned semantic expectations for executable qualification scenarios.
//!
//! Entries are published only after a Rust fixture exercises the complete
//! stateful sequence and its test asserts the decoded values. Generators must
//! leave every other RPC pending rather than infer observations from policy
//! names or protobuf field names.

mod actors;
mod inference;
mod machines;
mod objects;
mod stream;
mod workers;

/// The canonical Actors sequence is CreateActor followed by UpdateActor.
pub const ACTORS_UPDATE_ACTOR_JSON: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[1,2],"status_trace":["ACTIVE"],"transitions":[]}"#;

#[must_use]
pub fn expectation_json(rpc: &str) -> Option<&'static str> {
    actors::expectation_json(rpc)
        .or_else(|| inference::expectation_json(rpc))
        .or_else(|| machines::expectation_json(rpc))
        .or_else(|| workers::expectation_json(rpc))
        .or_else(|| objects::expectation_json(rpc))
        .or_else(|| stream::expectation_json(rpc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exercised_scenarios_have_expectations() {
        assert!(expectation_json("acyclic.actors.v1.ActorsService/UpdateActor").is_some());
        assert!(expectation_json("acyclic.actors.v1.ActorsService/CreateActor").is_some());
        assert!(expectation_json("inference.customer.v1.RunsService/Watch").is_some());
        assert!(expectation_json("acyclic.machines.v1.MachinesService/Create").is_some());
        assert!(expectation_json("acyclic.workers.v1.WorkersService/PublishVersion").is_none());
    }
}
