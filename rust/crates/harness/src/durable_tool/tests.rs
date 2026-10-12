use super::*;

use super::*;
use crate::workflow::{MachineIdentity, ResumableMachine};
use serde_json::json;

struct TestMachine {
    identity: MachineIdentity,
}

impl ResumableMachine for TestMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    fn state_schema(&self) -> &Value {
        static SCHEMA: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
        SCHEMA.get_or_init(|| json!({"type": "object"}))
    }

    fn initialize(&self, _input: &Value) -> Result<Value> {
        Ok(json!({}))
    }

    fn transition(&self, state: &Value, _input: &Value) -> Result<MachineTransition> {
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }
}

fn definition(name: &str, revision: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        revision: revision.into(),
        description: "test resumable tool".into(),
        input_schema: json!({"type": "object"}),
        output_schema: json!({"type": "object"}),
        projection_schema: crate::tool::json_projection_schema(json!({"type": "object"})),
    }
}

fn tool(name: &str, revision: &str, machine_name: &str) -> ResumableTool {
    ResumableTool {
        definition: definition(name, revision),
        machine: Arc::new(TestMachine {
            identity: MachineIdentity {
                name: machine_name.into(),
                version: revision.into(),
                digest: [7; 32],
            },
        }),
    }
}

#[test]
fn resumable_tool_requires_machine_identity_to_match_definition() {
    assert!(matches!(
        tool("test.echo", "1", "test.other").validate(None),
        Err(Error::Conflict(_))
    ));
}

#[test]
fn resumable_registry_rejects_ambiguous_exact_revisions() -> Result<()> {
    let mut registry = ResumableToolRegistry::default();
    registry.register(tool("test.echo", "1", "test.echo"))?;
    assert!(matches!(
        registry.register(tool("test.echo", "1", "test.echo")),
        Err(Error::Conflict(message)) if message.contains("already registered")
    ));
    Ok(())
}
