//! Configured command hooks shared by native harness sessions.

use crate::{Error, Result, tool::ToolRegistry};
use regex_lite::Regex;
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub(crate) mod imports;
pub(crate) use imports::Document;
mod budget;
#[cfg(feature = "filesystem")]
#[doc(hidden)]
pub mod effects;
#[cfg(test)]
mod mcp_identity_tests;
mod plan;
mod snapshot;
mod spec;
pub(crate) use spec::HookSite;
pub use spec::{HookEvent, HookFailure, HookRule, HookRun, HookSpec};
mod input;
pub use input::HookInput;

#[cfg(feature = "filesystem")]
pub(crate) struct BoundHooks {
    pub(crate) rules: Hooks,
    pub(crate) manifest: [u8; 32],
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
    pub(crate) fn append(&mut self, other: Self) {
        self.bindings.extend(other.bindings);
    }
    /// Media type for normalized rules retained as a configuration file.
    pub const MEDIA_TYPE: &'static str = "application/vnd.acyclic.hooks+json";

    /// Whether the selected configuration contains any event bindings.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Whether any command fires at a site the stock execution loop owns.
    pub(crate) fn has_execution_hooks(&self) -> bool {
        self.bindings.iter().any(|binding| {
            let execution = match binding.event.site() {
                HookSite::Prompt => false,
                HookSite::BeforeTool | HookSite::AfterTool | HookSite::TurnEnd => true,
            };
            execution && !binding.commands.is_empty()
        })
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
            let matcher = if group.matcher.is_empty()
                || group.matcher == "*"
                || matches!(event, HookEvent::UserPromptSubmit | HookEvent::Stop)
            {
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

    pub(crate) fn commands<'a, 'input>(
        &'a self,
        event: HookEvent,
        input: impl Into<HookInput<'input>>,
        registry: &'a ToolRegistry,
    ) -> Result<impl Iterator<Item = &'a Arc<HookSpec>> + Clone> {
        let subject = input.into().subject(event);
        #[cfg(any(test, feature = "test-support"))]
        let subject = if SUBSTITUTE_HOOK_NAME.with(std::cell::Cell::get) {
            "mcp__docs__search"
        } else {
            subject
        };
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

    #[doc(hidden)]
    pub async fn run_commands<'a, F, Fut>(
        &self,
        event: HookEvent,
        input: impl Into<HookInput<'a>>,
        registry: &ToolRegistry,
        maximum_bytes: u64,
        mut execute: F,
    ) -> Result<HookOutput>
    where
        F: FnMut(&Arc<HookSpec>, usize) -> Fut,
        Fut: Future<Output = Result<(Option<i32>, Vec<u8>, Vec<u8>)>>,
    {
        let input = input.into();
        let commands = self.commands(event, input, registry)?;
        let count = commands.clone().count();
        let quota = capture_quota(maximum_bytes, count);
        let mut budget = budget::Budget::new(maximum_bytes)?;
        let results = futures::future::join_all(commands.map(|hook| {
            let execution = execute(hook, quota);
            async move { (hook.failure, execution.await) }
        }))
        .await;
        let mut output = HookOutput::default();
        for (failure, result) in results {
            budget.apply(&mut output, event, failure, result);
        }
        budget.finish(&mut output);
        Ok(output)
    }
}

// A static partition keeps total live capture storage bounded without a lock,
// scheduling-dependent output truncation, or delaying concurrent hook dispatch.
fn capture_quota(maximum: u64, count: usize) -> usize {
    usize::try_from(maximum / (count.max(1) as u64))
        .unwrap_or(usize::MAX)
        .min(65_536)
}

pub(crate) fn report(event: HookEvent, output: &HookOutput) {
    for failure in &output.diagnostics {
        tracing::warn!(hook_event = event.name(), cause = ?failure.cause,
            exit_code = failure.cause.exit_code(), blocking = failure.blocking,
            error = %failure.cause.message(), "configured hook failed");
    }
}

mod output;
pub use output::{HookDiagnostic, HookOutput};

mod native;
#[doc(hidden)]
pub use native::HookCheck;
// Negative-control seam for the facade's `mcp_identity_tests`: substitutes the
// hook subject.
#[cfg(any(test, feature = "test-support"))]
thread_local! {
    #[doc(hidden)]
    pub static SUBSTITUTE_HOOK_NAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod identity_test_support;
