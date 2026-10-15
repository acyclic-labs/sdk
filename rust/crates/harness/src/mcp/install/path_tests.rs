use super::tests::{Remote, advertised, server};
use super::*;
use crate::mcp::config::Dialect;
use proptest::prelude::*;
use serde_json::json;

// Property: changing only ambient variables preserves revisions; resolving the same
// configured name to a different absolute executable changes them. Resolution is idempotent.
proptest! {
    #[test]
    fn explicit_binding_ignores_ambient_environment_and_pins_path(
        first_term in "[a-z]{0,20}", second_term in "[a-z]{0,20}", argument in "[a-z ]{0,24}",
    ) {
        let executable = std::env::current_exe().unwrap();
        let root = tempfile::tempdir_in(executable.parent().unwrap()).unwrap();
        let a = root.path().join("a");
        let b = root.path().join("b");
        std::fs::create_dir(&a).unwrap();
        std::fs::create_dir(&b).unwrap();
        let name = if cfg!(windows) { "mcp-fixture.exe" } else { "mcp-fixture" };
        std::fs::hard_link(&executable, a.join(name)).unwrap();
        std::fs::hard_link(&executable, b.join(name)).unwrap();
        let config = server(json!({"command":"mcp-fixture", "args":[argument], "env":{"KEY":"explicit"}}), Dialect::Codex).unwrap();
        let command = |directory: &Path, term: &str| native::command_for(&config, root.path(), |key| match key {
            "PATH" => Some(directory.as_os_str().to_owned()), "TERM" => Some(term.into()), _ => None,
        }).unwrap();
        let original = command(&a, &first_term);
        let ambient = command(&a, &second_term);
        let replaced = command(&b, &first_term);
        prop_assert!(Path::new(original.get_program()).is_absolute());
        prop_assert_eq!(std::fs::canonicalize(original.get_program()).unwrap(), Path::new(original.get_program()));
        let revision = |command| {
            let binding = native::command_binding(command, &config);
            let (tools, _) = register("fixture".into(), &config, Connected {
                transport: Arc::new(Remote::default()), tools: advertised(&["read"]), binding,
            }, McpClientLimits::default(), &|| Ok(())).unwrap();
            tools.get("mcp.fixture.read", None, None).unwrap().unwrap().definition.revision.clone()
        };
        prop_assert_eq!(revision(&original), revision(&ambient));
        prop_assert_ne!(revision(&original), revision(&replaced));
    }
}
