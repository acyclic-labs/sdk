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
    assert_eq!(
        tool.hook_names,
        if cfg!(windows) {
            &["PowerShell", "Bash", "shell"]
        } else {
            &["Bash", "PowerShell", "shell"]
        }
    );
    assert!(tool.definition.description.contains(if cfg!(windows) {
        "PowerShell syntax"
    } else {
        "bash syntax"
    }));
    Ok(())
}
