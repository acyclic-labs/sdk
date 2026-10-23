//! Location selection only; the native host still authorizes and executes tools.
use crate::{Error, Result};
use serde_json::Value;
use std::path::Path;

/// Selects a native tool's task view without changing its permission decision.
/// Content and edit bodies are preserved. Paths outside the checkout remain
/// subject to the native host's ordinary permissions and sandbox.
pub fn route_tool(name: &str, mut input: Value, root: &Path, view: &Path) -> Result<Value> {
    let root = root.to_string_lossy();
    let view = view.to_string_lossy();
    let map = |text: &str| root_references(text, &root, &view);
    let path = |text: &str| {
        if Path::new(text).is_absolute() {
            map(text)
        } else {
            Path::new(view.as_ref())
                .join(text)
                .to_string_lossy()
                .into_owned()
        }
    };
    if name == "apply_patch" {
        let patch = if input.is_string() {
            &mut input
        } else {
            input
                .get_mut("command")
                .ok_or_else(|| Error::Invalid("patch has no command".into()))?
        };
        let text = patch
            .as_str()
            .ok_or_else(|| Error::Invalid("patch has no command".into()))?;
        *patch = Value::String(
            text.split('\n')
                .map(|line| {
                    for prefix in [
                        "*** Add File: ",
                        "*** Update File: ",
                        "*** Delete File: ",
                        "*** Move to: ",
                        "*** Copy to: ",
                    ] {
                        if let Some(value) = line.strip_prefix(prefix) {
                            return format!("{prefix}{}", path(value));
                        }
                    }
                    line.to_owned()
                })
                .collect::<Vec<_>>()
                .join("\n"),
        );
        return Ok(input);
    }
    let object = input
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("native tool input must be an object".into()))?;
    match name {
        "exec_command" | "shell_command" => {
            let cwd = object
                .get("workdir")
                .and_then(Value::as_str)
                .map_or_else(|| view.to_string(), path);
            object.insert("workdir".into(), Value::String(cwd));
        }
        "Bash" | "bash" | "PowerShell" => {
            let text = object
                .get("command")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("native shell input has no command".into()))?;
            let command = text;
            let command = if name == "PowerShell" {
                format!(
                    "Set-Location -LiteralPath '{}'; {command}",
                    view.replace('\'', "''")
                )
            } else {
                format!("cd -- '{}' && {command}", view.replace('\'', "'\"'\"'"))
            };
            object.insert("command".into(), Value::String(command));
        }
        "Read" | "Write" | "Edit" | "NotebookEdit" | "Glob" | "Grep" | "read" | "write"
        | "edit" | "view_image" => {
            for field in ["file_path", "path", "notebook_path", "cwd"] {
                if let Some(text) = object.get(field).and_then(Value::as_str) {
                    object.insert(field.into(), Value::String(path(text)));
                }
            }
            if matches!(name, "Glob" | "Grep") && !object.contains_key("path") {
                object.insert("path".into(), Value::String(view.to_string()));
            }
        }
        _ => {}
    }
    Ok(input)
}

fn comparable(text: &str) -> String {
    if cfg!(windows) {
        text.replace('/', "\\").to_ascii_lowercase()
    } else {
        text.to_owned()
    }
}

fn root_references(text: &str, root: &str, view: &str) -> String {
    let source = comparable(text);
    let root = comparable(root);
    let selected = comparable(view);
    let continues = |c: char| c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | '~');
    let separator = |c| c == '/' || cfg!(windows) && c == '\\';
    let mut result = String::with_capacity(text.len());
    let mut copied = 0;
    let mut index = 0;
    while let Some(c) = text.get(index..).and_then(|text| text.chars().next()) {
        let boundary = text
            .get(..index)
            .and_then(|text| text.chars().next_back())
            .is_none_or(|c| !continues(c) && !separator(c));
        let names = |name: &str| {
            let end = index + name.len();
            (boundary
                && source.get(index..end) == Some(name)
                && text
                    .get(end..)
                    .and_then(|s| s.chars().next())
                    .is_none_or(|c| separator(c) || !continues(c)))
            .then_some(end)
        };
        if let Some(end) = names(&selected) {
            index = end;
        } else if let Some(end) = names(&root) {
            result.push_str(text.get(copied..index).unwrap_or_default());
            result.push_str(view);
            copied = end;
            index = end;
        } else {
            index += c.len_utf8();
        }
    }
    result.push_str(text.get(copied..).unwrap_or_default());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn native_paths_move_but_content_and_outside_permissions_do_not() -> Result<()> {
        let root = std::env::temp_dir().join("native-root");
        let view = root.join(".volumes/agents/child");
        let root_text = root.to_string_lossy();
        let original = json!({"file_path":"file.txt","content":format!("{root_text}/literal"),"permission":"ask"});
        let routed = route_tool("Write", original.clone(), &root, &view)?;
        assert_eq!(
            routed.get("file_path").unwrap_or(&Value::Null),
            view.join("file.txt").to_string_lossy().as_ref()
        );
        assert_eq!(routed.get("content"), original.get("content"));
        assert_eq!(routed.get("permission"), Some(&json!("ask")));
        assert!(routed.get("permissionDecision").is_none());
        let outside = root.with_file_name("outside");
        assert_eq!(
            route_tool("Read", json!({"file_path":outside}), &root, &view)?
                .get("file_path")
                .cloned()
                .ok_or_else(|| Error::Invalid("fixture path missing".into()))?,
            json!(outside)
        );
        let shell = format!(
            "echo '{root_text}/a' '{root_text}-other/a' '{}'/b",
            view.display()
        );
        let result = route_tool("exec_command", json!({"cmd":shell}), &root, &view)?;
        assert_eq!(
            result.get("cmd"),
            Some(&json!(shell)),
            "shell literals are content, not path arguments"
        );
        let bash = route_tool("Bash", json!({"command":shell}), &root, &view)?;
        assert!(
            bash["command"]
                .as_str()
                .unwrap_or_default()
                .ends_with(&shell)
        );
        assert_eq!(
            result.get("workdir").unwrap_or(&Value::Null),
            view.to_string_lossy().as_ref()
        );
        assert!(
            result
                .get("cmd")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("fixture command missing".into()))?
                .contains(&format!("{root_text}-other/a"))
        );
        assert!(
            !result
                .get("cmd")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("fixture command missing".into()))?
                .contains("child/.volumes")
        );
        assert_ne!(
            routed, original,
            "negative control: unmodified native input selects the parent"
        );
        Ok(())
    }

    #[test]
    fn patch_headers_select_the_view_and_patch_bodies_remain_literal() -> Result<()> {
        let root = std::env::temp_dir().join("patch-root");
        let view = root.join(".volumes/agents/child");
        let body = format!("+literal {}/a", root.display());
        let patch = format!("*** Begin Patch\n*** Add File: a\n{body}\n*** End Patch");
        for input in [json!(patch), json!({"command":patch})] {
            let result = route_tool("apply_patch", input, &root, &view)?;
            let result = result
                .as_str()
                .or_else(|| result.get("command").and_then(Value::as_str))
                .ok_or_else(|| Error::Invalid("fixture patch missing".into()))?;
            assert!(result.contains(&format!("*** Add File: {}", view.join("a").display())));
            assert!(result.contains(&body));
        }
        Ok(())
    }
}
