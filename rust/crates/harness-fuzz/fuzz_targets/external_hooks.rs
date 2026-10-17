#![no_main]
//! Native hooks normalize into the same bounded request files as actor clients.

use acyclic_harness::{
    external_executor::{
        CommandSessionMachine, ExternalHost, ExternalRequest, HookBinding, hook_answer, hooks,
        request,
    },
    task_volume::{Component, FileKey, FileValue, TaskFile},
    workflow::ResumableMachine,
};
use libfuzzer_sys::fuzz_target;
use serde::Deserialize;

#[derive(Deserialize)]
struct Hook {
    host: ExternalHost,
    event: String,
    input: serde_json::Value,
}

fn file(
    task: Component,
    request: &ExternalRequest,
    bytes: Vec<u8>,
) -> acyclic_harness::task_volume::Result<FileValue> {
    Ok(FileValue::new(
        FileKey::Task {
            task,
            file: TaskFile::Request(Component::new(request.operation_id.to_string())?),
        },
        bytes,
    ))
}

fn check(input: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    if input.len() > acyclic_stream::MAX_RECORD_BYTES {
        return Ok(());
    }
    acyclic_harness::test_support::fuzz_local_owner_input(input);
    acyclic_harness::test_support::fuzz_codex_hook_parent(input);
    acyclic_harness::test_support::fuzz_native_child_binding(input);
    acyclic_harness::test_support::fuzz_native_launch(input);
    acyclic_harness::test_support::fuzz_native_context_file(input);
    acyclic_harness::test_support::fuzz_native_file_input(input);
    if let Ok(executable) = std::str::from_utf8(input)
        && !executable.contains('\0')
    {
        let executable = format!("{executable}/acyclic");
        #[cfg(windows)]
        let expected = executable.replace('\\', "/");
        #[cfg(not(windows))]
        let expected = executable.clone();
        for (event, groups) in acyclic_harness::external_executor::claude_hooks(&executable) {
            let command = groups[0]["hooks"][0]["command"].as_str().unwrap();
            let arguments =
                acyclic_harness::tools::parse_command(command).expect("quoted hook command");
            assert_eq!(
                arguments,
                [expected.as_str(), "__hook", "claude-code", event]
            );
            assert!(acyclic_harness::external_executor::owns_claude_hook(
                event, &groups[0]
            ));
        }
    }
    if let Ok(group) = serde_json::from_slice::<serde_json::Value>(input) {
        for spec in hooks(ExternalHost::ClaudeCode) {
            acyclic_harness::external_executor::owns_claude_hook(spec.event, &group);
        }
    }
    let task = Component::new("task")?;
    if let Ok(arguments) = serde_json::from_slice::<serde_json::Value>(input) {
        if let Ok(result) = acyclic_harness::external_executor::tool_result(arguments.clone()) {
            assert_eq!(result["isError"], false);
            assert!(result["content"].is_array());
        }
    }
    if let Ok((name, arguments, key, bytes)) =
        serde_json::from_slice::<(String, serde_json::Value, FileKey, Vec<u8>)>(input)
    {
        let file = FileValue::new(key, bytes);
        if let Ok(request) =
            acyclic_harness::external_executor::native_tool_request(&name, arguments, &file, None)
        {
            assert_eq!(
                request.to_file(match &file.key {
                    FileKey::Task { task, .. } => task.clone(),
                    _ => return Ok(()),
                })?,
                file
            );
        }
    }
    if let Ok((state, input)) =
        serde_json::from_slice::<(serde_json::Value, serde_json::Value)>(input)
    {
        let machine = CommandSessionMachine::new();
        if let Ok(initial) = machine.initialize(&input) {
            let emitted = machine.transition(&initial, &serde_json::Value::Null)?;
            assert_eq!(emitted.commands.len(), 1);
        }
        if let Ok(next) = machine.transition(&state, &input) {
            assert!(next.commands.len() <= 1);
            if state["stage"] == "waiting" && input["commands"][0]["value"].get("Failed").is_some()
            {
                assert!(matches!(
                    next.status,
                    acyclic_harness::workflow::MachineStatus::Suspended
                ));
                assert_eq!(next.state["stage"], "idle");
            }
            assert_eq!(machine.transition(&state, &input)?, next);
            let bytes = serde_json::to_vec(&next.state)?;
            let decoded: serde_json::Value = serde_json::from_slice(&bytes)?;
            assert_eq!(decoded, next.state);
        }
    }
    for host in [
        ExternalHost::Codex,
        ExternalHost::ClaudeCode,
        ExternalHost::Pi,
    ] {
        for spec in hooks(host) {
            if let Ok(answer) = hook_answer(host, spec.event, input) {
                let encoded = serde_json::to_vec(&answer)?;
                assert_eq!(hook_answer(host, spec.event, &encoded)?, answer);
                assert!(answer.is_object());
                assert!(answer.get("Failed").is_none());
            }
        }
    }
    if let Ok(binding) = HookBinding::from_bytes(input) {
        let encoded = serde_json::to_vec(&binding)?;
        let decoded = HookBinding::from_bytes(&encoded)?;
        assert_eq!(decoded, binding);
        if let HookBinding::Platform {
            requests, results, ..
        } = decoded
        {
            assert_ne!(requests, results);
        }
    }
    if let Ok(path) = std::str::from_utf8(input)
        && let Ok(key) = FileKey::parse(path)
    {
        assert_eq!(FileKey::parse(&key.path())?, key);
    }
    if let Ok(hook) = serde_json::from_slice::<Hook>(input) {
        acyclic_harness::test_support::fuzz_native_hook_request(hook.host, &hook.event, hook.input);
    }
    if let Ok(hook) = serde_json::from_slice::<Hook>(input)
        && let Ok(request) = request(hook.host, &hook.event, hook.input)
    {
        acyclic_harness::test_support::fuzz_external_command(&request);
        if let acyclic_harness::external_executor::ExternalAction::RunTool {
            name, arguments, ..
        } = &request.action
        {
            if let Some(mut arguments) = arguments.as_object().cloned() {
                // Native input supplies seed data; the production normalizer
                // derives its request identity before the handoff is decoded.
                if !arguments.contains_key("_acyclic") {
                    if let Ok(file) = request.to_file(task.clone()) {
                        arguments.insert("_acyclic".into(), serde_json::json!(file.key.path()));
                        let decoded = acyclic_harness::external_executor::native_tool_request(
                            name,
                            serde_json::Value::Object(arguments.clone()),
                            &file,
                            None,
                        )?;
                        assert_eq!(decoded, request);
                        let workspace = "0".repeat(64);
                        arguments.insert(
                            "_acyclic".into(),
                            serde_json::json!(format!(
                                "workspaces/{workspace}/{}",
                                file.key.path()
                            )),
                        );
                        assert!(
                            acyclic_harness::external_executor::native_tool_request(
                                name,
                                serde_json::Value::Object(arguments.clone()),
                                &file,
                                None,
                            )
                            .is_err()
                        );
                        let decoded = acyclic_harness::external_executor::native_tool_request(
                            name,
                            serde_json::Value::Object(arguments),
                            &file,
                            Some(&workspace),
                        )?;
                        assert_eq!(decoded, request);
                    }
                }
            }
        }
        if let Ok(call) = request.tool_call(&acyclic_harness::tools::defaults()?) {
            assert!(!call.revision.is_empty());
        }
        match request.to_file(task.clone()) {
            Ok(file) => assert_eq!(ExternalRequest::from_file(&task, &file)?, request),
            Err(acyclic_harness::task_volume::Error::LimitExceeded) => {}
            Err(error) => return Err(error.into()),
        }
    }
    if let Ok(request) = serde_json::from_slice::<ExternalRequest>(input) {
        acyclic_harness::test_support::fuzz_external_command(&request);
        if let Ok(call) = request.tool_call(&acyclic_harness::tools::defaults()?) {
            let bytes = serde_json::to_vec(&call)?;
            assert_eq!(
                serde_json::from_slice::<acyclic_harness::filesystem::ToolTaskCommand>(&bytes)?,
                call
            );
        }
        let original = file(task.clone(), &request, input.to_vec())?;
        if let Ok(accepted) = ExternalRequest::from_file(&task, &original) {
            assert_eq!(accepted, request);
            let bytes = serde_json::to_vec(&accepted)?;
            assert_eq!(
                ExternalRequest::from_file(&task, &file(task.clone(), &accepted, bytes)?)?,
                accepted
            );
            assert!(ExternalRequest::from_file(&Component::new("other-task")?, &original).is_err());
        }
    }
    Ok(())
}

fuzz_target!(|input: &[u8]| {
    let result = check(input);
    assert!(result.is_ok(), "hook request file invariant: {result:?}");
});
