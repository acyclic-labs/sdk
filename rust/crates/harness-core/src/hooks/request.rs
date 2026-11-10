//! The plugin hook protocol's stdin: `{request, config}` (`BeforeHook` in
//! `sys/types`). A tool call becomes a normalized `Request` (its action, and
//! the command or path the action names) so one hook judges every provider's
//! tools alike. Atomic edits expose `paths`, including both ends of each move;
//! single-file operations expose `path`.
use super::*;
use crate::tool::Tool;

/// `subject`'s capability: the tool it executes as, else its own name.
pub(super) fn capability<'a>(subject: &'a str, tool: Option<&'a Tool>) -> &'a str {
    tool.map(|tool| tool.executor.capability().unwrap_or(tool))
        .map_or(subject, |tool| tool.definition.name.as_str())
}

/// A call of `tool`, which runs as `capability`, with `input`.
pub(super) fn request(tool: &str, capability: &str, input: &Value) -> Value {
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
            input
                .get("path")
                .and_then(Value::as_str)
                .map(|path| ("path", path.to_owned())),
        ),
        "web_search" => ("search", None),
        "url_fetch" => ("web", None),
        name if name.starts_with("mcp.") => ("mcp", None),
        _ => ("other", None),
    };
    let mut request = serde_json::json!({
        "event": "tool", "from": "self", "tool": tool, "action": "other",
    });
    // An action whose command or path is missing is judged as `other`.
    match named {
        Some((field, value)) => {
            request["action"] = action.into();
            request[field] = value.into();
        }
        None if matches!(action, "search" | "web" | "mcp") => request["action"] = action.into(),
        None => {}
    }
    request
}

/// Normalize the executor's actual canonical arguments, using the same adapter
/// as policy and dispatch. Raw aliases never compete with the selected template.
pub(super) fn normalized(
    input: HookInput<'_>,
    event: HookEvent,
    rewritten: Option<&Value>,
    registry: &ToolRegistry,
) -> Result<Value> {
    let subject = input.subject(event);
    let tool = registry.get(subject, None, None)?;
    let judged = rewritten.or(input.tool_input()).unwrap_or(&Value::Null);
    let canonical = match (tool, input) {
        (Some(tool), HookInput::Tool { invocation, .. }) => match rewritten {
            None => tool.executor.canonical(invocation, None)?,
            Some(arguments) => tool.executor.canonical(
                &crate::tool::ToolInvocation {
                    operation_id: invocation.operation_id,
                    call_id: invocation.call_id.clone(),
                    name: invocation.name.clone(),
                    arguments: arguments.clone(),
                },
                None,
            )?,
        },
        (Some(tool), HookInput::Fields(_)) if tool.executor.capability().is_some() => {
            return Err(Error::Invalid(
                "native hook matching requires the original tool invocation".into(),
            ));
        }
        _ => None,
    };
    let arguments = canonical.as_ref().map_or(judged, |call| &call.arguments);
    let implementation = canonical.as_ref().map_or(subject, |call| &call.name);
    let mut request = if implementation == "apply_patch" {
        // Use the publication parser, never a second interpretation of headers
        // or a model-supplied decoy path. Rewrites traverse this same boundary.
        let patch = crate::tools::variants::patch_input(arguments)?;
        let changes = crate::tool::patch::parse_document(patch, &mut None)?;
        let paths = changes
            .iter()
            .flat_map(|change| std::iter::once(change.path()).chain(change.move_to()))
            .map(|path| Value::String(path.to_owned()))
            .collect();
        let mut request = serde_json::json!({
            "event": "tool", "from": "self", "tool": subject, "action": "edit",
        });
        request["paths"] = Value::Array(paths);
        request
    } else {
        request(subject, capability(subject, tool), arguments)
    };
    // Hooks may rewrite the model's input, so their input field retains it;
    // only normalized action/command/path come from the effective executor.
    request["input"] = judged.clone();
    Ok(request)
}

/// What a plugin hook with `config` reads for `input`, whose tool input a
/// rewrite earlier in the chain may have replaced with `rewritten`.
pub(super) fn encode(
    input: HookInput<'_>,
    event: HookEvent,
    config: &str,
    (rewritten, registry): (Option<&Value>, &ToolRegistry),
) -> Result<Vec<u8>> {
    let config: &serde_json::value::RawValue = serde_json::from_str(config)
        .map_err(|error| Error::Invalid(format!("plugin hook settings: {error}")))?;
    let mut bytes = serde_json::to_vec(&serde_json::json!({
        "request": normalized(input, event, rewritten, registry)?,
        "config": config,
    }))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Exercises the original patch decoder, native adapter and file-hook boundary.
/// Inputs are operator-authored V4A bytes, including malformed documents.
#[cfg(all(feature = "test-support", feature = "filesystem"))]
pub fn check_native_patch_paths(input: &[u8]) {
    let Ok(patch) = std::str::from_utf8(input) else {
        return;
    };
    static REGISTRY: std::sync::OnceLock<ToolRegistry> = std::sync::OnceLock::new();
    let registry = REGISTRY.get_or_init(|| {
        let mut registry = crate::tools::defaults().unwrap();
        crate::tools::native::NativeTools::new(crate::model::NativeToolProfile::Openai)
            .unwrap()
            .bind(&mut registry)
            .unwrap();
        registry
    });
    let call = crate::tool::ToolInvocation {
        operation_id: crate::OperationId::from_bytes([7; 16]),
        call_id: "original-patch".into(),
        name: "apply_patch".into(),
        arguments: serde_json::json!({"input":patch, "path":"public/decoy"}),
    };
    let actual = normalized(
        HookInput::Tool {
            invocation: &call,
            response: None,
            session_id: None,
        },
        HookEvent::PreToolUse,
        None,
        registry,
    );
    match (crate::tool::patch::parse_document(patch, &mut None), actual) {
        (Ok(changes), Ok(actual)) => {
            let expected: Vec<_> = changes
                .iter()
                .flat_map(|change| std::iter::once(change.path()).chain(change.move_to()))
                .collect();
            assert_eq!(actual["action"], "edit", "patch lost its file guard action");
            assert_eq!(
                actual["paths"],
                serde_json::json!(expected),
                "patch lost an affected guard path"
            );
            assert!(
                actual.get("path").is_none(),
                "model path competed with patch paths"
            );
            let guard = Requests::compile(&RequestMatch {
                action: vec!["edit".into()],
                path: Some("^private/".into()),
                command: None,
            })
            .unwrap();
            assert_eq!(
                guard.matches(&actual),
                expected.iter().any(|path| path.starts_with("private/")),
                "affected paths bypassed or spuriously selected their guard"
            );
        }
        (Err(_), Err(_)) => {}
        (expected, actual) => {
            panic!("patch decoder/hook admission differ: {expected:?} / {actual:?}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "filesystem")]
    #[tokio::test]
    async fn atomic_edit_guards_cover_every_path_and_rewritten_move_destination() -> Result<()> {
        let root = tempfile::tempdir().map_err(Error::process_io)?;
        let captured = root.path().join("request.json");
        let literal = |text: &str| format!("'{}'", text.replace('\'', "'\\''"));
        let powershell = |text: &str| format!("'{}'", text.replace('\'', "''"));
        let output = captured.to_string_lossy();
        // These are the actual plugin programs. Capture their real stdin and
        // exit through the public blocking protocol, on either target OS.
        let guard = HookSpec {
            run: HookRun::Plugin {
                instance: "private-edits".into(),
                sh: Some(format!(
                    "cat > {}; echo 'patch guard denied' >&2; exit 2",
                    literal(&output)
                )),
                ps1: Some(format!(
                    "[IO.File]::WriteAllText({}, [Console]::In.ReadToEnd()); [Console]::Error.WriteLine('patch guard denied'); exit 2",
                    powershell(&output)
                )),
                config: "{}".into(),
                matcher: Some(RequestMatch {
                    action: vec!["edit".into()],
                    path: Some("^private/".into()),
                    command: None,
                }),
            },
            timeout_ms: 5_000,
            failure: HookFailure::Block,
        };
        let mut registry = crate::tools::defaults()?;
        crate::tools::native::NativeTools::new(crate::model::NativeToolProfile::Openai)?
            .bind(&mut registry)?;
        let invocation = |patch: &str| crate::tool::ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "original-patch".into(),
            name: "apply_patch".into(),
            arguments: serde_json::json!({"input":patch, "path":"public/decoy"}),
        };
        let hooks = |rules: Vec<HookSpec>| -> Result<Hooks> {
            let mut hooks = Hooks::default();
            hooks.add_rules([HookRule {
                on: HookEvent::PreToolUse,
                matcher: String::new(),
                hooks: rules,
            }])?;
            Ok(hooks)
        };
        let preamble = "*** Begin Patch\n*** Add File: public/first\n+first\n";
        for (operation, expected) in [
            (
                "*** Add File: private/added\n+new\n",
                vec!["public/first", "private/added"],
            ),
            (
                "*** Update File: private/edited\n-old\n+new\n",
                vec!["public/first", "private/edited"],
            ),
            (
                "*** Delete File: private/deleted\n",
                vec!["public/first", "private/deleted"],
            ),
            (
                "*** Update File: private/source\n*** Move to: public/destination\n",
                vec!["public/first", "private/source", "public/destination"],
            ),
            (
                "*** Update File: public/source\n*** Move to: private/destination\n",
                vec!["public/first", "public/source", "private/destination"],
            ),
        ] {
            let patch = format!("{preamble}{operation}*** End Patch\n");
            let call = invocation(&patch);
            let input = HookInput::Tool {
                invocation: &call,
                response: None,
                session_id: None,
            };
            let result = hooks(vec![guard.clone()])?
                .run(
                    HookEvent::PreToolUse,
                    root.path(),
                    root.path(),
                    input,
                    &registry,
                )
                .await?;
            assert!(
                result.blocks(),
                "atomic edit bypassed its file guard: {operation}"
            );
            assert!(result.diagnostics.is_empty(), "{result:?}");
            let bytes = std::fs::read(&captured).map_err(Error::process_io)?;
            let received: Value = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(received["request"]["action"], "edit");
            assert_eq!(received["request"]["paths"], serde_json::json!(expected));
            assert!(received["request"].get("path").is_none());
            assert_eq!(received["request"]["input"], call.arguments);
        }
        let public = "*** Begin Patch\n*** Update File: public/source\n*** Move to: public/destination\n*** End Patch\n";
        let call = invocation(public);
        let input = HookInput::Tool {
            invocation: &call,
            response: None,
            session_id: None,
        };
        assert!(
            !hooks(vec![guard.clone()])?
                .run(
                    HookEvent::PreToolUse,
                    root.path(),
                    root.path(),
                    input,
                    &registry
                )
                .await?
                .blocks()
        );
        // An initially skipped guard must judge the final rewritten patch too.
        let private = public.replace("public/destination", "private/rewritten");
        let rewrite = |arguments: Value| {
            let decision = serde_json::json!({"allow":true,"input":arguments}).to_string();
            HookSpec {
                run: HookRun::Plugin {
                    instance: "rewrite".into(),
                    sh: Some(format!(
                        "cat >/dev/null; printf '%s' {}",
                        literal(&decision)
                    )),
                    ps1: Some(format!(
                        "$null = [Console]::In.ReadToEnd(); [Console]::Out.Write({})",
                        powershell(&decision)
                    )),
                    config: "{}".into(),
                    matcher: None,
                },
                timeout_ms: 5_000,
                failure: HookFailure::Block,
            }
        };
        let result = hooks(vec![
            guard.clone(),
            rewrite(serde_json::json!({"input":private})),
        ])?
        .run(
            HookEvent::PreToolUse,
            root.path(),
            root.path(),
            input,
            &registry,
        )
        .await?;
        assert!(
            result.blocks(),
            "rewritten move bypassed its destination guard"
        );
        assert!(result.diagnostics.is_empty(), "{result:?}");
        let received: Value =
            serde_json::from_slice(&std::fs::read(&captured).map_err(Error::process_io)?)
                .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            received["request"]["paths"],
            serde_json::json!(["public/source", "private/rewritten"])
        );
        // Invalid rewrites block in either authority order, without starting
        // a guard process or losing the model's ability to issue another call.
        for arguments in [
            serde_json::json!({"input":"*** Begin Patch\n*** Add File: private/unfinished\n+new\n"}),
            serde_json::json!({"input":7}),
        ] {
            for rules in [
                vec![guard.clone(), rewrite(arguments.clone())],
                vec![rewrite(arguments.clone()), guard.clone()],
            ] {
                std::fs::remove_file(&captured)
                    .or_else(|error| {
                        if error.kind() == std::io::ErrorKind::NotFound {
                            Ok(())
                        } else {
                            Err(error)
                        }
                    })
                    .map_err(Error::process_io)?;
                let result = hooks(rules)?
                    .run(
                        HookEvent::PreToolUse,
                        root.path(),
                        root.path(),
                        input,
                        &registry,
                    )
                    .await
                    .expect("malformed rewritten patch aborted the hook chain");
                assert!(
                    result.blocks(),
                    "malformed rewritten patch escaped its guard"
                );
                assert!(
                    result
                        .blocked
                        .as_ref()
                        .is_some_and(|reason| !reason.is_empty())
                );
                assert!(result.updated_input.is_none());
                assert!(result.diagnostics.is_empty(), "{result:?}");
                assert!(!captured.exists(), "invalid patch started a guard process");
            }
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn native_file_guards_match_the_executed_path_after_each_rewrite() -> Result<()> {
        let mut registry = crate::tools::defaults()?;
        crate::tools::native::NativeTools::new(crate::model::NativeToolProfile::Google)?
            .bind(&mut registry)?;
        let guard = Requests::compile(&RequestMatch {
            action: vec!["read".into(), "write".into(), "edit".into()],
            path: Some("^private/".into()),
            command: None,
        })?;
        for name in ["read_file", "write_file", "replace"] {
            let invocation = crate::tool::ToolInvocation {
                operation_id: crate::OperationId::from_bytes([7; 16]),
                call_id: "original-call".into(),
                name: name.into(),
                arguments: serde_json::json!({
                    "path": "public/decoy", "file_path": "private/actual",
                    "content": "text", "old_string": "old", "new_string": "new"
                }),
            };
            let input = HookInput::Tool {
                invocation: &invocation,
                response: None,
                session_id: None,
            };
            let original = normalized(input, HookEvent::PreToolUse, None, &registry)?;
            assert_eq!(original["path"], "private/actual", "{name}");
            assert_eq!(original["input"], invocation.arguments);
            assert!(guard.matches(&original), "{name}");
            let mut rewritten = invocation.arguments.clone();
            rewritten["file_path"] = "public/actual".into();
            rewritten["path"] = "private/decoy".into();
            let changed = normalized(input, HookEvent::PreToolUse, Some(&rewritten), &registry)?;
            assert_eq!(changed["path"], "public/actual", "{name}");
            assert!(!guard.matches(&changed), "{name}");
        }
        Ok(())
    }

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
