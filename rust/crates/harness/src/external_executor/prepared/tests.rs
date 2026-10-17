use super::*;
use crate::external_executor::{ExternalHost, request};
use proptest::prelude::*;
use serde_json::json;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // The typed command projection is canonical, retains the native operation,
    // and rejects any field that could change its authority or executor kind.
    #[test]
    fn stock_commands_round_trip_without_accepting_an_executor_override(
        sequence in any::<u64>(),
        id in any::<[u8;16]>(),
        choice in 0usize..5,
        input in "[a-z]{0,32}",
    ) {
        let (name, arguments, kind) = match choice {
            0 => ("acyclic_spawn", json!({"name":"acyclic.command_session","version":"1","input":input}), TASK_ADMIT_COMMAND_KIND),
            1 => ("acyclic_receive", json!({"after":sequence}), MAIL_RECEIVE_TASK_COMMAND_KIND),
            2 => ("acyclic_wait", json!({"child":crate::TaskId::from_bytes(id)}), TASK_JOIN_COMMAND_KIND),
            3 => ("acyclic_cancel", json!({"child":crate::TaskId::from_bytes(id)}), TASK_CANCEL_COMMAND_KIND),
            _ => ("acyclic_inspect", json!({"task":crate::TaskId::from_bytes(id)}), TASK_INSPECT_COMMAND_KIND),
        };
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            let call = request(host, if host == ExternalHost::Pi {"tool_call"} else {"PreToolUse"},
                json!({"sessionId":"commands","toolCallId":"call","toolName":name,"input":arguments}))?;
            prop_assert_eq!(call.task_command()?, Some((kind, arguments.clone())));
            let tools = crate::tool::ToolRegistry::default();
            let validate = super::super::commands::validate_command;
            prop_assert_eq!(validate(&call, kind, &arguments, &tools)?, None);
            prop_assert!(validate(&call, TOOL_TASK_COMMAND_KIND, &arguments, &tools).is_err());
            let mut forged = arguments.clone();
            forged["kind"] = json!(TOOL_TASK_COMMAND_KIND);
            prop_assert!(validate(&call, kind, &forged, &tools).is_err());
            let encoded = serde_json::to_vec(&call).unwrap();
            let mut replay: ExternalRequest = serde_json::from_slice(&encoded).unwrap();
            prop_assert_eq!(replay.task_command()?, call.task_command()?);
            prop_assert_eq!(replay.operation_id, call.operation_id);
            if let ExternalAction::RunTool {arguments, ..} = &mut replay.action {
                arguments["kind"] = json!(TOOL_TASK_COMMAND_KIND);
            }
            prop_assert!(replay.task_command().is_err());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // The declared process identity, not its label or parent turn, selects the
    // same attachment/completion/wait. Changed targets remain immutable retries.
    #[test]
    fn separate_native_sessions_keep_their_own_identity_through_start_and_stop(
        parent in "[a-z]{1,16}", child in "[a-z]{1,16}", label in "[a-z]{1,16}",
    ) {
        let task = crate::task_volume::Component::new("task")?;
        for host in [ExternalHost::Codex,ExternalHost::ClaudeCode,ExternalHost::Pi] {
            for child_host in [ExternalHost::Codex,ExternalHost::ClaudeCode,ExternalHost::Pi] {
                let target = crate::external_executor::ChildSession {
                    host:child_host, session_id:format!("fresh-{child}"), caller:None,
                };
                let input = json!({"sessionId":format!("parent-{parent}"),"agentId":label,
                    "parentAgentId":"parent-caller","childSession":target});
                let start = request(host,if host == ExternalHost::Pi {"subagent_start"} else {"SubagentStart"},input.clone())?;
                let stop = request(host,if host == ExternalHost::Pi {"subagent_stop"} else {"SubagentStop"},input.clone())?;
                let attached = request(child_host,if child_host == ExternalHost::Pi {"session_start"} else {"SessionStart"},
                    json!({"sessionId":target.session_id,"cwd":"/project"}))?;
                let completed = request(child_host,if child_host == ExternalHost::Pi {"session_shutdown"} else {"SessionEnd"},
                    json!({"sessionId":target.session_id}))?;
                prop_assert_eq!(start.child_attachment("/project")?,attached.clone());
                prop_assert_eq!(stop.child_attachment("/project")?,attached.clone());
                prop_assert_eq!(stop.child_completion()?,completed);
                let expected = serde_json::to_value(TaskJoinCommand {child:attached.task_id()})?;
                prop_assert_eq!(stop.task_command()?,Some((TASK_JOIN_COMMAND_KIND,expected)));
                let mut changed = input;
                changed["childSession"]["session_id"] = json!(format!("other-{child}"));
                let retry = request(host,if host == ExternalHost::Pi {"subagent_start"} else {"SubagentStart"},changed)?;
                prop_assert_eq!(retry.operation_id,start.operation_id);
                let original = start.to_file(task.clone())?;
                let changed = retry.to_file(task.clone())?;
                prop_assert_eq!(&original.key,&changed.key);
                prop_assert_ne!(&original.bytes,&changed.bytes);
                prop_assert_eq!(crate::external_executor::ExternalRequest::from_file(&task,&original)?,start);
            }
        }
    }

    // Invalid or ambiguous child identity never acquires a request identity;
    // nested authority fields are not forwarded into the child's attachment.
    #[test]
    fn child_session_fields_are_typed_and_unambiguous(session in "[a-z]{1,24}") {
        let target = json!({"host":"pi","session_id":session,"caller":null});
        let mut input = json!({"sessionId":"parent","agentId":"label","childSession":target});
        for invalid in [Value::Null,json!({"host":"pi","session_id":"","caller":null}),
            json!({"host":"pi","session_id":format!("{session}\0"),"caller":null}),
            json!({"host":"pi","session_id":session,"caller":""}),
            json!({"host":"pi","session_id":session,"caller":null,"grants":["*"]}),
        ] {
            input["childSession"] = invalid;
            prop_assert!(request(ExternalHost::Pi,"subagent_start",input.clone()).is_err());
            prop_assert!(request(ExternalHost::Pi,"subagent_stop",input.clone()).is_err());
        }
        input["childSession"] = target.clone();
        input["child_session"] = target;
        prop_assert!(request(ExternalHost::Pi,"subagent_start",input).is_err());
    }
}

#[test]
fn every_exposed_command_has_a_typed_preparation() -> Result<()> {
    for definition in crate::external_executor::command_catalog::definitions() {
        let error = request(
            ExternalHost::Pi,
            "tool_call",
            json!({
                "sessionId":"catalog", "toolCallId":"call", "toolName":definition.name,"input":{}
            }),
        )?
        .task_command();
        assert!(error.is_err(), "command must validate its required fields");
        assert_eq!(definition.input_schema["additionalProperties"], false);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // A child start and stop select the same session regardless of parent turn;
    // the child receives its own caller identity, never its parent's identity.
    #[test]
    fn native_child_identity_is_stable_across_start_stop_and_turns(
        session in "[a-z]{1,24}", child in "[a-z]{1,24}", turn in "[a-z]{1,24}",
    ) {
        for host in [ExternalHost::Codex, ExternalHost::ClaudeCode, ExternalHost::Pi] {
            let start = request(host, if host == ExternalHost::Pi {"subagent_start"} else {"SubagentStart"},
                json!({"sessionId":session,"agentId":child,"parentAgentId":"parent","turnId":turn}))?;
            let stop = request(host, if host == ExternalHost::Pi {"subagent_stop"} else {"SubagentStop"},
                json!({"sessionId":session,"agentId":child,"parentAgentId":"parent"}))?;
            let attached = request(host, if host == ExternalHost::Pi {"session_start"} else {"SessionStart"},
                json!({"sessionId":session,"agentId":child,"cwd":"/project","turnId":turn}))?;
            prop_assert_eq!(start.child_attachment("/project")?, attached.clone());
            prop_assert_eq!(stop.child_attachment("/project")?, attached.clone());
            let completed = request(host, if host == ExternalHost::Pi {"session_shutdown"} else {"SessionEnd"},
                json!({"sessionId":session,"agentId":child}))?;
            prop_assert_eq!(stop.child_completion()?, completed);
            prop_assert!(start.child_completion().is_err());
            let mut changed = stop.clone();
            let mut identity = stop.operation_id.into_bytes();
            identity[0] ^= 1;
            changed.operation_id = crate::OperationId::from_bytes(identity);
            prop_assert!(changed.child_completion().is_err());
            let expected = serde_json::to_value(TaskJoinCommand {child:attached.task_id()}).unwrap();
            prop_assert_eq!(stop.task_command()?, Some((TASK_JOIN_COMMAND_KIND, expected)));
            prop_assert!(super::super::commands::validate_command(&stop, super::super::intake::LIFECYCLE,
                &serde_json::to_value(&stop).unwrap(), &crate::tool::ToolRegistry::default())?.is_some());
        }
    }
}
