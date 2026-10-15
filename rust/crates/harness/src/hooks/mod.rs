//! Configured command hooks shared by native harness sessions.

use crate::{Error, Result, tool::ToolRegistry};
use regex_lite::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub(crate) mod imports;
use imports::Document;
mod spec;
pub use spec::{HookFailure, HookRule, HookRun, HookSpec};

/// Lifecycle points with command-hook support.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookEvent {
    /// A session was opened, resumed, forked, or compacted.
    SessionStart,
    /// A tool is about to execute.
    PreToolUse,
    /// A tool returned successfully.
    PostToolUse,
    /// The model is about to finish its turn.
    Stop,
}

impl HookEvent {
    fn name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
        }
    }
}

struct Binding {
    event: HookEvent,
    matcher: Option<Matcher>,
    commands: Vec<Arc<HookSpec>>,
}

enum Matcher {
    Exact(String),
    Pattern(Regex),
}

impl Matcher {
    fn matches(&self, name: &str) -> bool {
        match self {
            Self::Exact(expected) => expected == name,
            Self::Pattern(pattern) => pattern.is_match(name),
        }
    }
}

/// Validated hooks, with matchers compiled once during configuration loading.
#[derive(Default)]
pub struct Hooks {
    bindings: Vec<Binding>,
}

impl Hooks {
    /// Adds a Claude settings file or a Codex hooks.json document atomically.
    pub fn add_json(&mut self, document: &[u8]) -> Result<()> {
        let document = serde_json::from_slice(document)
            .map_err(|error| Error::Invalid(format!("hook configuration: {error}")))?;
        self.add_document(document)
    }

    /// Adds the inline hooks table from a Codex config.toml document.
    pub fn add_toml(&mut self, document: &str) -> Result<()> {
        let document: Document = toml::from_str(document)
            .map_err(|error| Error::Invalid(format!("hook configuration: {error}")))?;
        self.add_document(document)
    }

    /// Installs normalized rules atomically, compiling each group selector once.
    pub fn add_rules(&mut self, rules: impl IntoIterator<Item = HookRule>) -> Result<()> {
        let mut bindings = Vec::new();
        for group in rules {
            let event = group.on;
            let matcher =
                if group.matcher.is_empty() || group.matcher == "*" || event == HookEvent::Stop {
                    None
                } else if group.matcher.chars().any(|character| {
                    matches!(
                        character,
                        '.' | '^'
                            | '$'
                            | '*'
                            | '+'
                            | '?'
                            | '('
                            | ')'
                            | '['
                            | ']'
                            | '{'
                            | '}'
                            | '|'
                            | '\\'
                    )
                }) {
                    Some(Matcher::Pattern(Regex::new(&group.matcher).map_err(
                        |error| Error::Invalid(format!("hook matcher: {error}")),
                    )?))
                } else {
                    Some(Matcher::Exact(group.matcher))
                };
            for hook in &group.hooks {
                hook.validate()?;
            }
            bindings.push(Binding {
                event,
                matcher,
                commands: group.hooks.into_iter().map(Arc::new).collect(),
            });
        }
        self.bindings.extend(bindings);
        Ok(())
    }

    fn commands<'a>(
        &'a self,
        event: HookEvent,
        input: &'a Value,
        registry: &'a ToolRegistry,
    ) -> Result<impl Iterator<Item = &'a Arc<HookSpec>>> {
        let subject = match event {
            HookEvent::SessionStart => input.get("source"),
            HookEvent::PreToolUse | HookEvent::PostToolUse => input.get("tool_name"),
            HookEvent::Stop => None,
        }
        .and_then(Value::as_str)
        .unwrap_or("");
        let tool = if matches!(event, HookEvent::PreToolUse | HookEvent::PostToolUse) {
            registry.get(subject, None, None)?
        } else {
            None
        };
        Ok(self
            .bindings
            .iter()
            .filter(move |binding| {
                binding.event == event
                    && binding.matcher.as_ref().is_none_or(|matcher| {
                        matcher.matches(subject)
                            || tool.is_some_and(|tool| {
                                matcher.matches(&tool.definition.name)
                                    || tool.hook_names.iter().any(|name| matcher.matches(name))
                            })
                    })
            })
            .flat_map(|binding| &binding.commands))
    }

    async fn run_commands<F, Fut>(
        &self,
        event: HookEvent,
        input: &Value,
        registry: &ToolRegistry,
        mut execute: F,
    ) -> Result<HookOutput>
    where
        F: FnMut(&Arc<HookSpec>) -> Fut,
        Fut: Future<Output = Result<(Option<i32>, Vec<u8>, Vec<u8>)>>,
    {
        let results =
            futures::future::join_all(self.commands(event, input, registry)?.map(|hook| {
                let execution = execute(hook);
                async move { (hook.failure, execution.await) }
            }))
            .await;
        let mut output = HookOutput::default();
        for (failure, result) in results {
            output.apply(event, failure, result);
        }
        Ok(output)
    }
}

/// Applied hook decisions and context for the harness loop.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct HookOutput {
    /// Reason the guarded action was blocked.
    pub blocked: Option<String>,
    /// A pre-tool hook requested human approval, with its explanation.
    pub approval: Option<String>,
    /// Extra context to include in the next model request.
    pub additional_context: Vec<String>,
    /// Replacement arguments returned by a pre-tool hook.
    pub updated_input: Option<Value>,
    /// Command failures, including explicit failure-policy blocks.
    pub diagnostics: Vec<String>,
}

impl HookOutput {
    fn apply(
        &mut self,
        event: HookEvent,
        policy: HookFailure,
        result: Result<(Option<i32>, Vec<u8>, Vec<u8>)>,
    ) {
        let block_failure = policy == HookFailure::Block && event != HookEvent::Stop;
        let observed = match result {
            Ok((code, _, stderr)) if code != Some(0) && (code != Some(2) || block_failure) => {
                Err(exit_error(code, &stderr))
            }
            Ok((code, stdout, stderr)) => self.observe(event, code, &stdout, &stderr),
            Err(error) => Err(error),
        };
        if let Err(error) = observed {
            let diagnostic = format!("{} hook failed: {error}", event.name());
            if block_failure {
                self.blocked.get_or_insert_with(|| diagnostic.clone());
            }
            self.diagnostics.push(diagnostic);
        }
    }

    fn observe(
        &mut self,
        event: HookEvent,
        code: Option<i32>,
        stdout: &[u8],
        stderr: &[u8],
    ) -> Result<()> {
        if code == Some(2) {
            if matches!(event, HookEvent::PreToolUse | HookEvent::Stop) {
                self.blocked
                    .get_or_insert_with(|| String::from_utf8_lossy(stderr).into_owned());
            } else if event == HookEvent::PostToolUse {
                self.additional_context
                    .push(String::from_utf8_lossy(stderr).into_owned());
            } else {
                return Err(exit_error(code, stderr));
            }
            return Ok(());
        }
        let text = std::str::from_utf8(stdout)
            .map_err(|error| Error::Invalid(error.to_string()))?
            .trim();
        if text.is_empty() {
            return Ok(());
        }
        let mut value = match serde_json::from_str::<Value>(text) {
            Ok(value) => value,
            Err(error) => {
                if text.starts_with('{')
                    || text.starts_with('[') && event != HookEvent::SessionStart
                {
                    return Err(Error::Invalid(format!(
                        "hook output JSON is invalid: {error}"
                    )));
                }
                if event == HookEvent::SessionStart {
                    self.additional_context.push(text.into());
                }
                return Ok(());
            }
        };
        validate_output(&value, event)?;
        let mut specific = value
            .get_mut("hookSpecificOutput")
            .map(Value::take)
            .unwrap_or(Value::Null);
        if value.get("continue").and_then(Value::as_bool) == Some(false) {
            self.blocked.get_or_insert_with(|| {
                take_string(&mut value, "stopReason").unwrap_or_else(|| "Stopped by hook".into())
            });
        }
        if event == HookEvent::PostToolUse
            && value.get("decision").and_then(Value::as_str) == Some("block")
            && let Some(reason) = take_string(&mut value, "reason")
        {
            self.additional_context.push(reason);
        }
        if matches!(event, HookEvent::PreToolUse | HookEvent::Stop)
            && (value.get("decision").and_then(Value::as_str) == Some("block")
                || specific.get("permissionDecision").and_then(Value::as_str) == Some("deny"))
        {
            self.blocked.get_or_insert_with(|| {
                take_string(&mut specific, "permissionDecisionReason")
                    .or_else(|| take_string(&mut value, "reason"))
                    .unwrap_or_else(|| "Blocked by hook".into())
            });
        }
        if event == HookEvent::PreToolUse
            && specific.get("permissionDecision").and_then(Value::as_str) == Some("ask")
        {
            self.approval.get_or_insert_with(|| {
                take_string(&mut specific, "permissionDecisionReason")
                    .unwrap_or_else(|| "Approval requested by hook".into())
            });
        }
        if let Some(context) = take_string(&mut specific, "additionalContext") {
            self.additional_context.push(context);
        }
        if event == HookEvent::PreToolUse
            && let Some(input) = specific
                .as_object_mut()
                .and_then(|fields| fields.remove("updatedInput"))
        {
            self.updated_input = Some(input);
        }
        Ok(())
    }
}

fn exit_error(code: Option<i32>, stderr: &[u8]) -> Error {
    Error::Process {
        kind: "exit",
        message: format!("exit {code:?}: {}", String::from_utf8_lossy(stderr)),
    }
}

fn validate_output(value: &Value, event: HookEvent) -> Result<()> {
    if !value.is_object() {
        return Err(Error::Invalid(
            "hook structured output must be an object".into(),
        ));
    }
    validate_strings(value, &["stopReason", "decision", "reason"])?;
    if value
        .get("continue")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(Error::Invalid("hook continue must be a boolean".into()));
    }
    if value
        .get("decision")
        .and_then(Value::as_str)
        .is_some_and(|decision| !matches!(decision, "approve" | "block"))
    {
        return Err(Error::Invalid("unknown hook decision".into()));
    }
    if let Some(specific) = value.get("hookSpecificOutput") {
        if !specific.is_object() {
            return Err(Error::Invalid(
                "hookSpecificOutput must be an object".into(),
            ));
        }
        validate_strings(
            specific,
            &[
                "hookEventName",
                "permissionDecision",
                "permissionDecisionReason",
                "additionalContext",
            ],
        )?;
        if specific
            .get("hookEventName")
            .and_then(Value::as_str)
            .is_some_and(|name| name != event.name())
        {
            return Err(Error::Invalid("hook output names another event".into()));
        }
        if specific
            .get("permissionDecision")
            .and_then(Value::as_str)
            .is_some_and(|decision| !matches!(decision, "allow" | "ask" | "deny"))
        {
            return Err(Error::Invalid("unknown hook permissionDecision".into()));
        }
        if specific
            .get("updatedInput")
            .is_some_and(|input| !input.is_object())
        {
            return Err(Error::Invalid("hook updatedInput must be an object".into()));
        }
    }
    Ok(())
}

fn validate_strings(value: &Value, names: &[&str]) -> Result<()> {
    for name in names {
        if value.get(name).is_some_and(|value| !value.is_string()) {
            return Err(Error::Invalid(format!("hook {name} must be a string")));
        }
    }
    Ok(())
}

fn take_string(value: &mut Value, name: &str) -> Option<String> {
    let Value::String(text) = value.as_object_mut()?.remove(name)? else {
        return None;
    };
    Some(text)
}

mod native;
#[cfg(test)]
mod tests;
