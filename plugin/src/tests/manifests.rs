//! One native manifest activates discovery and authenticated hooks together.
use acyclic_harness::external_executor::{ExternalHost, hooks};
use serde_json::Value;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn host_manifests_route_every_event_to_the_common_client() -> Result {
    let manifest: Value = serde_json::from_str(include_str!("../../.codex-plugin/plugin.json"))?;
    assert_eq!(manifest["hooks"], "./hooks/hooks.json");
    assert_eq!(manifest["mcpServers"], "./mcp.json");
    assert!(
        !std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("plugin.json")
            .exists()
    );
    let codex: Value = serde_json::from_str(include_str!("../../hooks/hooks.json"))?;
    let pi: Value = serde_json::from_str(include_str!("../../pi/package.json"))?;
    let claude: Value =
        serde_json::from_str(include_str!("../../claude/.claude-plugin/plugin.json"))?;
    assert_eq!(
        claude,
        acyclic_harness::external_executor::claude_manifest(env!("CARGO_PKG_VERSION"))
    );
    for spec in hooks(ExternalHost::Codex) {
        let hook = &codex["hooks"][spec.event][0]["hooks"][0];
        assert_eq!(hook["type"], "command");
        assert!(
            hook["command"]
                .as_str()
                .is_some_and(|command| command.ends_with(&format!("__hook codex {}", spec.event)))
        );
        assert_eq!(hook["timeout"].as_u64(), Some(spec.timeout_seconds()));
    }
    for spec in hooks(ExternalHost::Pi) {
        assert_eq!(
            pi["acyclic"]["hooks"][spec.event]["args"],
            serde_json::json!(["__hook", "pi", spec.event])
        );
    }
    let mcp: Value = serde_json::from_str(include_str!("../../mcp.json"))?;
    assert_eq!(mcp["mcpServers"]["acyclic"]["type"], "stdio");
    assert_eq!(mcp["mcpServers"]["acyclic"]["command"], "acyclic-plugin");
    assert_eq!(
        mcp["mcpServers"]["acyclic"]["args"],
        serde_json::json!(["__tools"])
    );
    assert_eq!(
        mcp["mcpServers"]["acyclic"]["env_vars"],
        serde_json::json!(["ACYCLIC_HARNESS_BINDING", "ACYCLIC_LOCAL_OWNER_DIR", "XDG_DATA_HOME", "LOCALAPPDATA"])
    );
    for file in ["../src/main.rs", "../pi/index.ts"] {
        let source = std::fs::read_to_string(
            std::path::Path::new(file)
                .strip_prefix("../")
                .map(|path| std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))?,
        )?;
        assert!(source.lines().count() <= 300);
        for old in [
            "ConcurrentServiceControl",
            "ControlEndpoint",
            "thread_stack_size",
            "setTimeout",
            "while (true)",
        ] {
            assert!(!source.contains(old), "retired owner in {file}: {old}");
        }
    }
    Ok(())
}
