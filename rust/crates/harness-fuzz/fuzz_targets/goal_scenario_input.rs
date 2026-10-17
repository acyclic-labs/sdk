use super::{goal_parser_input, goals};
use acyclic_harness::{
    model::ModelEvent,
    test_support::{ScriptedEvent, ScriptedTurn},
};

pub fn check(data: &[u8]) {
    let Ok(mut value) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    if let Some(object) = value.as_object_mut() {
        object.remove("driver");
    }
    let Ok(case) = goals::GoalCase::parse(value) else {
        return;
    };
    let canonical = serde_json::to_value(&case).unwrap();
    let parsed = goals::GoalCase::parse(canonical.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), canonical);
    if let Some(text) = case.seed.get("GOAL.md") {
        goal_parser_input::check(text.as_bytes()).unwrap();
    }
    // Inspect every rewrite independently of fixture expectations or earlier
    // invalid goals. Never execute an unbounded user script in the byte fuzzer.
    for turn in &case.turns {
        if let ScriptedTurn::Events(events) = turn {
            for event in events {
                if let ScriptedEvent::Event(ModelEvent::ToolCall(call)) = event
                    && matches!(
                        call.arguments
                            .get("path")
                            .and_then(serde_json::Value::as_str),
                        Some("GOAL.md" | ".volumes/scratch/GOAL.md")
                    )
                    && let Some(text) = call
                        .arguments
                        .get("text")
                        .and_then(serde_json::Value::as_str)
                {
                    goal_parser_input::check(text.as_bytes()).unwrap();
                }
            }
        }
    }
}
