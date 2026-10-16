//! Host bindings for direct, user-submitted tool requests. Never used for model calls.
use super::CommandTool;
use crate::{Error, Result};
use serde_json::{Value, json};
use std::time::Duration;

/// Execute an explicitly submitted host tool using the shared bounded native
/// process lifecycle. The workspace and search endpoint come from config files.
pub async fn execute_host_command(
    tool: CommandTool,
    input: &Value,
    settings: &Value,
    maximum_bytes: usize,
) -> Result<Value> {
    let key = match tool {
        CommandTool::Bash => "command",
        CommandTool::WebSearch => "query",
        CommandTool::UrlFetch => "url",
        CommandTool::Spawn => {
            return Err(Error::Unsupported(
                "spawn requires the composed task runtime".into(),
            ));
        }
    };
    let argument = input
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && !text.contains('\0'))
        .ok_or_else(|| Error::Invalid("invalid direct tool input".into()))?;
    if input.as_object().is_none_or(|object| object.len() != 1)
        || maximum_bytes == 0
        || argument.len() > maximum_bytes
    {
        return Err(Error::Invalid(
            "direct tool input exceeds its schema/bound".into(),
        ));
    }
    let mut command = match tool {
        CommandTool::Bash => {
            return Err(Error::Invalid(
                "shell requires the selected task executor".into(),
            ));
        }
        CommandTool::UrlFetch | CommandTool::WebSearch => {
            let mut command = std::process::Command::new("curl");
            command.args([
                "--fail",
                "--show-error",
                "--silent",
                "--max-time",
                "30",
                "--max-filesize",
                &maximum_bytes.to_string(),
            ]);
            if tool == CommandTool::WebSearch {
                let endpoint = settings
                    .get("search_endpoint")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Error::Unsupported(
                            "configure the web search endpoint in settings.json".into(),
                        )
                    })?;
                command.args([
                    "--get",
                    "--data-urlencode",
                    &format!("q={argument}"),
                    "--",
                    endpoint,
                ]);
            } else {
                command.args(["--", argument]);
            }
            command
        }
        CommandTool::Spawn => unreachable!(),
    };
    let output = acyclic_native_runtime::run_blocking_io(
        move || {
            acyclic_native_runtime::process_output(
                &mut command,
                Duration::from_secs(30),
                maximum_bytes,
            )
        },
        None,
    )
    .map_err(|error| Error::Storage(error.to_string()))?
    .await
    .map_err(|error| Error::Storage(error.to_string()))?
    .map_err(|error| Error::Storage(error.to_string()))?;
    Ok(
        json!({"status":output.status.code(),"stdout":String::from_utf8_lossy(&output.stdout),"stderr":String::from_utf8_lossy(&output.stderr)}),
    )
}
