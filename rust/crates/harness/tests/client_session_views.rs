use super::*;

#[test]
fn streaming_views_share_tool_identity_and_keep_provider_usage() {
    use acyclic_harness::{client::TaskFileViews, model::ModelEvent};
    let mut views = TaskFileViews::default();
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::Content {
                delta: "hel".into(),
            },
        )
        .unwrap();
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::Content { delta: "lo".into() },
        )
        .unwrap();
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::Reasoning {
                delta: "thinking".into(),
            },
        )
        .unwrap();
    views.tool_output(&owner(), "call", "first\n");
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::ToolCall(acyclic_harness::model::ToolCall::new(
                "call".into(),
                "bash".into(),
                serde_json::json!({"command":"pwd"}),
            )),
        )
        .unwrap();
    views.tool_output(&owner(), "call", "second\n");
    let usage = serde_json::json!({"input_tokens":12,"output_tokens":2});
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::Completed {
                metadata: usage.clone(),
            },
        )
        .unwrap();
    assert_eq!(
        views.transcripts["AGGREGATE_KIND_TASK:task"].streaming_text,
        "hello"
    );
    assert_eq!(
        views.transcripts["AGGREGATE_KIND_TASK:task"].reasoning,
        "thinking"
    );
    assert_eq!(
        views.tools["AGGREGATE_KIND_TASK:task"]["call"].output,
        tool_text("first\nsecond\n")
    );
    assert_eq!(views.tools["AGGREGATE_KIND_TASK:task"]["call"].name, "bash");
    assert_eq!(views.usage["AGGREGATE_KIND_TASK:task"], usage);
    views
        .model_event(
            &owner(),
            acyclic_harness::OperationId::from_bytes(7u128.to_be_bytes()),
            ModelEvent::Completed { metadata: usage },
        )
        .unwrap();
    assert_eq!(
        views.usage["AGGREGATE_KIND_TASK:task"],
        serde_json::json!({"input_tokens":24,"output_tokens":4})
    );
}
