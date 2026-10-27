//! The public default catalog follows the native host shell contract.
use acyclic_harness_core::Result;

#[test]
fn default_tool_names_and_shell_contract_match_the_host() -> Result<()> {
    let registry = acyclic_harness_core::tools::defaults()?;
    let names = registry
        .definitions(None)?
        .into_iter()
        .map(|tool| tool.name)
        .collect::<std::collections::BTreeSet<_>>();
    let shell = "shell";
    let expected = [
        shell, "fs", "read", "write", "edit", "spawn", "send", "wait", "archive",
    ]
    .map(String::from)
    .into_iter()
    .collect();
    assert_eq!(names, expected);
    for old_name in ["bash", "powershell"] {
        assert!(!names.contains(old_name));
        assert!(registry.get(old_name, None, None)?.is_none());
    }
    let tool = registry.get(shell, None, None)?.unwrap();
    let schema = &tool.definition.input_schema;
    assert_eq!(schema["required"], serde_json::json!(["command"]));
    assert!(schema["properties"].get("cwd").is_some());
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["properties"]["timeout"]["maximum"], 4_294_967);
    assert!(schema["properties"]["command"].get("maxLength").is_none());
    assert!(schema["properties"]["cwd"].get("maxLength").is_none());
    assert!(tool.hook_names.is_empty());
    assert!(tool.definition.description.contains(if cfg!(windows) {
        "PowerShell syntax"
    } else {
        "bash syntax"
    }));
    Ok(())
}

#[test]
fn native_inputs_are_not_narrowed_by_remote_machine_limits() -> Result<()> {
    use acyclic_harness_core::tools::ShellInput;
    for timeout in [None, Some(120), Some(4_294_967)] {
        ShellInput {
            command: "#".repeat(32_768),
            cwd: Some("a/".repeat(4096)),
            timeout,
        }
        .validate()?;
    }
    ShellInput {
        command: "sleep 90".into(),
        cwd: None,
        timeout: Some(120),
    }
    .validate()?;
    Ok(())
}
