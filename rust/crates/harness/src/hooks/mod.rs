//! Configured command hooks shared by native harness sessions.

use crate::{Error, Result, tool::ToolRegistry};
use regex_lite::Regex;
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub(crate) mod imports;
use imports::Document;
mod snapshot;
mod spec;
pub use spec::{HookEvent, HookFailure, HookRule, HookRun, HookSpec};

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
    /// Media type for normalized rules retained as a configuration file.
    pub const MEDIA_TYPE: &'static str = "application/vnd.acyclic.hooks+json";

    /// Whether the selected configuration contains any event bindings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

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

mod output;
pub use output::HookOutput;

mod native;
#[cfg(test)]
mod tests;
