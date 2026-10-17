use super::{ExternalAction, ExternalHost, ExternalRequest};
use crate::{
    Error, Result,
    filesystem::ToolTaskCommand,
    tool::{ToolRegistry, files::WriteFileInput},
};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Shell {
    #[serde(alias = "cmd")]
    command: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    #[serde(alias = "file_path")]
    path: String,
    content: String,
}

impl ExternalRequest {
    /// Pins one registered tool revision and translates native command/write
    /// arguments into the harness's existing schemas. Other registered tools
    /// already use their exact declared schema; unknown options are rejected.
    pub fn tool_call(&self, tools: &ToolRegistry) -> Result<ToolTaskCommand> {
        self.tool_call_at(tools, None)
    }

    pub(super) fn tool_call_at(
        &self,
        tools: &ToolRegistry,
        revision: Option<&str>,
    ) -> Result<ToolTaskCommand> {
        let ExternalAction::RunTool {
            name, arguments, ..
        } = &self.action
        else {
            return Err(Error::Invalid("request is not a native tool call".into()));
        };
        let (name, arguments) = match (self.host, name.as_str()) {
            (ExternalHost::Codex, "exec_command") | (ExternalHost::ClaudeCode, "Bash") => {
                let input: Shell = decode(arguments)?;
                ("bash", json!({"command":input.command}))
            }
            (ExternalHost::ClaudeCode, "Write") | (ExternalHost::Pi, "write")
                if arguments.get("text").is_none() =>
            {
                let input: Write = decode(arguments)?;
                crate::conversation::validate_content_path(&input.path, &mut None)?;
                if crate::conversation::is_internal_path(&input.path) {
                    return Err(Error::Unauthorized(
                        "native write targets internal storage".into(),
                    ));
                }
                let display_name = input
                    .path
                    .rsplit('/')
                    .next()
                    .ok_or_else(|| Error::Invalid("native write path is empty".into()))?
                    .to_owned();
                let input = WriteFileInput {
                    path: input.path,
                    text: input.content,
                    media_type: "text/plain".into(),
                    display_name,
                };
                (
                    "write",
                    serde_json::to_value(input)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
            }
            _ => (name.as_str(), arguments.clone()),
        };
        let tool = tools
            .get(name, revision, None)?
            .ok_or_else(|| Error::Unsupported("native tool is not registered".into()))?;
        crate::executor::validate_json_schema_value(
            &tool.definition.input_schema,
            &arguments,
            "native tool input",
            &mut None,
        )?;
        Ok(ToolTaskCommand {
            name: tool.definition.name.clone(),
            revision: tool.definition.revision.clone(),
            arguments,
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(input: &Value) -> Result<T> {
    T::deserialize(input).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests;
