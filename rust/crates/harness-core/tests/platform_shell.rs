//! The public default catalog is the same on every host.
use acyclic_harness_core::Result;

#[test]
fn default_tool_names_and_shell_contract_are_host_independent() -> Result<()> {
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
    assert!(schema["properties"].get("target").is_some());
    assert_eq!(schema["additionalProperties"], false);
    assert!(tool.hook_names.is_empty());
    // One description on every OS: each target's language comes from the roster.
    assert_eq!(
        tool.definition.description,
        acyclic_harness_core::tools::shell::DESCRIPTION
    );
    for host_specific in ["PowerShell", "bash"] {
        assert!(!tool.definition.description.contains(host_specific));
    }
    Ok(())
}
