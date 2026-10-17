use super::*;
use acyclic_harness::task_volume::Component;
use proptest::prelude::*;

fn scenario(index: usize, session: &str) -> Result<Scenario> {
    let scenarios: Vec<Scenario> = serde_json::from_str(include_str!("../external_hooks.json"))
        .map_err(|error| invalid(&error))?;
    let index = index
        .checked_rem(scenarios.len())
        .ok_or_else(|| Error::Invalid("hook scenarios are empty".into()))?;
    let mut selected = scenarios
        .into_iter()
        .nth(index)
        .ok_or_else(|| Error::Invalid("hook scenario is absent".into()))?;
    let field = if selected.host == ExternalHost::Pi {
        "sessionId"
    } else {
        "session_id"
    };
    set(&mut selected.input, field, json!(session))?;
    Ok(selected)
}

proptest! {
    // Property: every native hook normalizes into a valid immutable request
    // file and round trips without retaining unrelated native metadata.
    #[test]
    fn native_requests_round_trip_through_request_files(
        index in any::<usize>(),
        session in "[a-zA-Z0-9_-]{1,24}",
        metadata in ".{0,64}",
    ) {
        let mut selected = scenario(index, &session)?;
        let expected = request(selected.host, &selected.event, selected.input.clone())?;
        set(&mut selected.input, "environment", json!({"private": metadata}))?;
        set(&mut selected.input, "transcript_path", json!("/private/transcript"))?;
        let actual = request(selected.host, &selected.event, selected.input)?;
        prop_assert_eq!(&actual, &expected);
        let task = Component::new("task")?;
        let file = actual.to_file(task.clone())?;
        let mut forged = actual.clone();
        forged.operation_id = acyclic_harness::OperationId::from_bytes(
            forged.operation_id.into_bytes().map(|byte| byte ^ 1),
        );
        prop_assert!(matches!(forged.to_file(task.clone()),
            Err(acyclic_harness::task_volume::Error::InvalidSegment)));
        prop_assert_eq!(ExternalRequest::from_file(&task, &file)?, actual);
        prop_assert!(ExternalRequest::from_file(&Component::new("other-task")?, &file).is_err());
    }

    // Property: a tool retry keeps its operation despite changed arguments,
    // while native session identity remains part of the operation namespace.
    #[test]
    fn tool_retry_identity_is_independent_of_arguments(
        host in prop_oneof![Just(ExternalHost::Codex), Just(ExternalHost::ClaudeCode), Just(ExternalHost::Pi)],
        session in "[a-zA-Z0-9_-]{1,24}",
        call in "[a-zA-Z0-9_-]{1,24}",
        command in ".{0,64}",
    ) {
        let (event, mut input) = if host == ExternalHost::Pi {
            ("tool_call", json!({"sessionId":session,"toolCallId":call,"toolName":"bash","input":{"command":command}}))
        } else {
            ("PreToolUse", json!({"session_id":session,"tool_use_id":call,"tool_name":"Bash","tool_input":{"command":command}}))
        };
        let original = request(host, event, input.clone())?;
        let field = if host == ExternalHost::Pi { "input" } else { "tool_input" };
        set(&mut input, field, json!({"command":format!("{command} changed")}))?;
        let changed = request(host, event, input.clone())?;
        prop_assert_eq!(changed.operation_id, original.operation_id);
        prop_assert_ne!(changed.action, original.action);
        let field = if host == ExternalHost::Pi { "sessionId" } else { "session_id" };
        set(&mut input, field, json!(format!("{session}-other")))?;
        prop_assert_ne!(request(host, event, input)?.operation_id, original.operation_id);
    }

    // Property: two native spellings of the same identity field are rejected,
    // even when their values happen to agree.
    #[test]
    fn native_identity_aliases_cannot_be_ambiguous(
        index in any::<usize>(),
        session in "[a-zA-Z0-9_-]{1,24}",
    ) {
        let mut selected = scenario(index, &session)?;
        let alias = if selected.host == ExternalHost::Pi { "session_id" } else { "sessionId" };
        set(&mut selected.input, alias, json!(session))?;
        prop_assert!(request(selected.host, &selected.event, selected.input).is_err());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Host-executed calls retain their full arguments and immutable identity,
    // but can never become a prepared harness effect.
    #[test]
    fn host_execution_classification_preserves_authenticated_identity(
        host in prop_oneof![Just(ExternalHost::Codex), Just(ExternalHost::ClaudeCode), Just(ExternalHost::Pi)],
        session in "[a-zA-Z0-9_-]{1,24}", name in "[a-zA-Z0-9_-]{1,24}",
        arguments in ".{0,64}",
    ) {
        let event = if host == ExternalHost::Pi { "tool_call" } else { "PreToolUse" };
        let mut input = json!({"session_id":session,"toolCallId":"call","toolName":name,"input":{"value":arguments}});
        let harness = request(host,event,input.clone())?;
        input["hostExecuted"] = json!(true);
        let observed = request(host,event,input.clone())?;
        prop_assert_eq!(observed.operation_id, harness.operation_id);
        prop_assert_eq!(observed.task_id(), harness.task_id());
        prop_assert!(matches!(&observed.action, ExternalAction::HostTool {name: actual, arguments: value, ..}
            if actual == &name && value == &json!({"value":arguments})), "host arguments changed");
        prop_assert!(observed.tool_call(&acyclic_harness::tools::defaults()?).is_err());
        let task = Component::new(observed.task_id().to_string())?;
        let file = observed.to_file(task.clone())?;
        prop_assert_eq!(ExternalRequest::from_file(&task,&file)?, observed);
        input["hostExecuted"] = json!("true");
        prop_assert!(request(host,event,input).is_err());
    }
}
