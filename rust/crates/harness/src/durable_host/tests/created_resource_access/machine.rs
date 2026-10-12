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
        let (state, commands, status) = if state.get("emitted") == Some(&Value::Bool(true)) {
            (
                state.clone(),
                Vec::new(),
                MachineStatus::Completed {
                    value: serde_json::json!({"ok":true}),
                },
            )
        } else if let Some(command) = input.get("created_command") {
            (
                serde_json::json!({"emitted":true}),
                vec![
                    serde_json::from_value(command.clone())
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                ],
                MachineStatus::Suspended,
            )
        } else {
            (state.clone(), Vec::new(), MachineStatus::Suspended)
        };
        Ok(MachineTransition {
            state,
            commands,
            status,
        })
    }
}
