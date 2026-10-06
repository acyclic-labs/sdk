//! Exact semantic observations from `sdk-examples` ActorsFixture.

const CREATE: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[1],"status_trace":["ACTIVE"],"transitions":[]}"#;
const UPDATE: &str = super::ACTORS_UPDATE_ACTOR_JSON;
const INSPECT: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[2],"status_trace":["ACTIVE"],"transitions":[]}"#;
const ADD: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[2,3],"status_trace":["ACTIVE"],"transitions":[]}"#;
const REMOVE: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[3,4],"status_trace":["ACTIVE"],"transitions":[]}"#;
const RESUME: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[4,5],"status_trace":["ACTIVE"],"transitions":[]}"#;
const CHECKPOINT: &str = r#"{"identity_pairs":[{"field":"actor_id","request":"fixture-actor","response":"fixture-actor"}],"cursor_trace":[],"revision_trace":[5],"status_trace":["ACTIVE"],"transitions":[]}"#;
const INVOKE: &str = r#"{"identity_pairs":[],"cursor_trace":[],"revision_trace":[],"status_trace":["200"],"transitions":[]}"#;

pub(super) fn expectation_json(rpc: &str) -> Option<&'static str> {
    match rpc {
        "acyclic.actors.v1.ActorsService/CreateActor" => Some(CREATE),
        "acyclic.actors.v1.ActorsService/UpdateActor" => Some(UPDATE),
        "acyclic.actors.v1.ActorsService/InspectActor" => Some(INSPECT),
        "acyclic.actors.v1.ActorsService/AddSubscription" => Some(ADD),
        "acyclic.actors.v1.ActorsService/RemoveSubscription" => Some(REMOVE),
        "acyclic.actors.v1.ActorsService/ResumeSubscription" => Some(RESUME),
        "acyclic.actors.v1.ActorsService/CheckpointActor" => Some(CHECKPOINT),
        "acyclic.actors.v1.ActorsService/InvokeActor" => Some(INVOKE),
        _ => None,
    }
}
