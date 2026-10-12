use super::*;

pub(super) struct CreatedControlMachine {
    pub(super) identity: MachineIdentity,
    pub(super) schema: Value,
}

impl ResumableMachine for CreatedControlMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        Ok(input.clone())
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        if state.get("emitted") == Some(&Value::Bool(true)) {
            return Ok(MachineTransition {
                state: state.clone(),
                commands: Vec::new(),
                status: MachineStatus::Completed {
                    value: serde_json::json!({"ok":true}),
                },
            });
        }
        if let Some(command) = input.get("created_command") {
            let command = serde_json::from_value(command.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            return Ok(MachineTransition {
                state: serde_json::json!({"emitted":true}),
                commands: vec![command],
                status: MachineStatus::Suspended,
            });
        }
        Ok(MachineTransition {
            state: state.clone(),
            commands: Vec::new(),
            status: MachineStatus::Suspended,
        })
    }
}
