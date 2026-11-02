//! The plugin hook protocol's stdin: `{request, config}` (`BeforeHook` in
//! `sys/types`). A tool call becomes a normalized `Request` (its action, and
//! the command or path the action names) so one hook judges every provider's
//! tools alike.
use super::*;
use crate::tool::Tool;

/// `subject`'s capability: the tool it executes as, else its own name.
pub(super) fn capability<'a>(subject: &'a str, tool: Option<&'a Tool>) -> &'a str {
    tool.map(|tool| tool.executor.capability().unwrap_or(tool))
        .map_or(subject, |tool| tool.definition.name.as_str())
}

/// A call of `tool`, which runs as `capability`, with `input`.
pub(super) fn request(tool: &str, capability: &str, input: &Value) -> Value {
    let text = |names: &[&str]| {
        names
            .iter()
            .find_map(|name| input.get(*name).and_then(Value::as_str))
    };
    let (action, named) = match capability {
        name if name == crate::tools::shell::PLATFORM.name => {
            (
                "exec",
                input
                    .get("command")
                    .and_then(|command| match command {
                        Value::String(command) => Some(command.clone()),
                        // Plain words stay readable to command matchers; JSON quoting
                        // preserves every other literal argument boundary. This
                        // display form is matched, never executed as a shell.
                        Value::Array(args) if args.iter().all(Value::is_string) => Some(
                            args.iter()
                                .map(|arg| match arg {
                                    Value::String(word)
                                        if !word.is_empty()
                                            && word.bytes().all(|byte| {
                                                byte.is_ascii_alphanumeric()
                                                    || b"_./:-".contains(&byte)
                                            }) =>
                                    {
                                        word.clone()
                                    }
                                    _ => arg.to_string(),
                                })
                                .collect::<Vec<_>>()
                                .join(" "),
                        ),
                        _ => None,
                    })
                    .map(|command| ("command", command)),
            )
        }
        "read" | "write" | "edit" => (
            capability,
            text(&["path", "file_path"]).map(|path| ("path", path.to_owned())),
        ),
        name if name.starts_with("mcp.") => ("mcp", None),
        _ => ("other", None),
    };
    let mut request = serde_json::json!({
        "event": "tool", "from": "self", "tool": tool, "input": input, "action": "other",
    });
    // An action whose command or path is missing is judged as `other`.
    match named {
        Some((field, value)) => {
            request["action"] = action.into();
            request[field] = value.into();
        }
        None if action == "mcp" => request["action"] = action.into(),
        None => {}
    }
    request
}

/// What a plugin hook with `config` reads for `input`, whose tool input a
/// rewrite earlier in the chain may have replaced with `rewritten`.
pub(super) fn encode(
    input: HookInput<'_>,
    event: HookEvent,
    config: &str,
    (rewritten, registry): (Option<&Value>, &ToolRegistry),
) -> Result<Vec<u8>> {
    let subject = input.subject(event);
    let tool = registry.get(subject, None, None)?;
    let judged = rewritten.or(input.tool_input()).unwrap_or(&Value::Null);
    let config: &serde_json::value::RawValue = serde_json::from_str(config)
        .map_err(|error| Error::Invalid(format!("plugin hook settings: {error}")))?;
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "request": request(subject, capability(subject, tool), judged),
        "config": config,
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalized_calls_keep_guard_actions_and_literal_arguments() {
        let request = request(
            "shell",
            crate::tools::shell::PLATFORM.name,
            &serde_json::json!({"command": ["git", "push", "--force", "a b", "$(id)"]}),
        );
        assert_eq!(request["action"], "exec");
        assert_eq!(request["command"], r#"git push --force "a b" "$(id)""#);
        assert_eq!(
            super::request("mcp.docs.search", "mcp.docs.search", &Value::Null)["action"],
            "mcp"
        );
    }
}
