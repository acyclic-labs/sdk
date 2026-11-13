//! Configured command hooks shared by native harness sessions.

use crate::{Error, Result, tool::ToolRegistry};
use regex_lite::Regex;
use serde_json::Value;
use std::{future::Future, sync::Arc};

pub(crate) mod imports;
pub(crate) use imports::Document;
mod budget;
#[doc(hidden)]
pub mod effects;
#[cfg(test)]
mod mcp_identity_tests;
mod plan;
mod snapshot;
mod spec;
pub(crate) use spec::HookSite;
pub use spec::{HookEvent, HookFailure, HookRule, HookRun, HookSpec, RequestMatch};
mod chain;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use chain::model as chain_model;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod codec_fuzz;
mod input;
mod request;
pub use input::HookInput;
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub use request::check_native_patch_paths;

pub(crate) struct BoundHooks {
    pub(crate) rules: Hooks,
    pub(crate) manifest: [u8; 32],
}

fn shared<T>(value: T, io: Option<&crate::filesystem::PreparationIo>) -> Result<Arc<T>> {
    if let Some(io) = io {
        let bytes = acyclic_fs::NativeMaterializationWork::shared_allocation_bytes::<T>().map_err(
            |_| {
                crate::executor::stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Invalid,
                    "shared hook layout overflow",
                )
            },
        )?;
        io.retain_owned(bytes)?;
        io.allocation_attempt(bytes)?;
    }
    Ok(Arc::new(value))
}
#[derive(Debug)]
struct Binding {
    event: HookEvent,
    matcher: Option<Matcher>,
    commands: Vec<(Arc<HookSpec>, Option<Requests>)>,
}

/// A compiled [`RequestMatch`].
#[derive(Debug)]
struct Requests {
    actions: Vec<String>,
    command: Option<Regex>,
    path: Option<Regex>,
}

impl Requests {
    fn compile(
        matcher: &RequestMatch,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Self> {
        let pattern = |pattern: &Option<String>| {
            pattern
                .as_deref()
                .map(Regex::new)
                .transpose()
                .map_err(|error| Error::Invalid(format!("plugin hook matcher: {error}")))
        };
        let mut actions = Vec::new();
        for action in &matcher.action {
            let action = crate::executor::stock_string_copy(action, io)?;
            crate::executor::stock_vec_push(&mut actions, action, io)?;
        }
        Ok(Self {
            actions,
            command: pattern(&matcher.command)?,
            path: pattern(&matcher.path)?,
        })
    }

    /// Whether a plugin hook's request (`request::request`) is selected.
    fn matches(&self, request: &Value) -> bool {
        let field = |name| request.get(name).and_then(Value::as_str);
        let selects = |pattern: &Option<Regex>, name| {
            pattern
                .as_ref()
                .is_none_or(|pattern| field(name).is_some_and(|text| pattern.is_match(text)))
        };
        (self.actions.is_empty()
            || field("action").is_some_and(|action| self.actions.iter().any(|a| a == action)))
            && selects(&self.command, "command")
            && self.path.as_ref().is_none_or(|pattern| {
                field("path").is_some_and(|path| pattern.is_match(path))
                    || request
                        .get("paths")
                        .and_then(Value::as_array)
                        .is_some_and(|paths| {
                            paths
                                .iter()
                                .filter_map(Value::as_str)
                                .any(|path| pattern.is_match(path))
                        })
            })
    }
}

#[derive(Debug)]
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
#[derive(Clone, Debug, Default)]
pub struct Hooks {
    bindings: Vec<Arc<Binding>>,
    custody: Vec<Arc<acyclic_fs::NativeAllocation>>,
}

impl Hooks {
    /// Appends already validated rules in authority order, sharing their compiled matchers.
    pub fn append(&mut self, other: Self) {
        self.bindings.extend(other.bindings);
        self.custody.extend(other.custody);
    }

    pub(crate) fn retain_native(&mut self, custody: Arc<acyclic_fs::NativeAllocation>) {
        self.custody.push(custody);
    }

    /// Whether any command fires at a site the stock execution loop owns.
    pub(crate) fn has_execution_hooks(&self) -> bool {
        self.bindings.iter().any(|binding| {
            let execution = match binding.event.site() {
                HookSite::Prompt | HookSite::File | HookSite::Stream => false,
                HookSite::BeforeTool | HookSite::AfterTool | HookSite::TurnEnd => true,
            };
            execution && !binding.commands.is_empty()
        })
    }

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
        self.add_groups(
            rules
                .into_iter()
                .map(|group| (group.on, group.matcher, group.hooks)),
            None,
        )
    }

    pub(crate) fn add_rule_with_io(
        &mut self,
        event: HookEvent,
        matcher: String,
        hooks: impl IntoIterator<Item = HookSpec>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        self.add_groups([(event, matcher, hooks)], io)
    }

    fn add_groups<I: IntoIterator<Item = HookSpec>>(
        &mut self,
        groups: impl IntoIterator<Item = (HookEvent, String, I)>,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        let mut bindings = Vec::new();
        for (event, pattern, hooks) in groups {
            let matcher = if pattern.is_empty()
                || pattern == "*"
                || matches!(event, HookEvent::UserPromptSubmit | HookEvent::Stop)
            {
                None
            } else if pattern.chars().any(|character| {
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
                Some(Matcher::Pattern(Regex::new(&pattern).map_err(|error| {
                    Error::Invalid(format!("hook matcher: {error}"))
                })?))
            } else {
                Some(Matcher::Exact(pattern))
            };
            let mut commands = Vec::new();
            for hook in hooks {
                hook.validate()?;
                let requests = match &hook.run {
                    HookRun::Plugin {
                        matcher: Some(matcher),
                        ..
                    } => Some(Requests::compile(matcher, io)?),
                    _ => None,
                };
                let hook = shared(hook, io)?;
                crate::executor::stock_vec_push(&mut commands, (hook, requests), io)?;
            }
            let binding = shared(
                Binding {
                    event,
                    matcher,
                    commands,
                },
                io,
            )?;
            crate::executor::stock_vec_push(&mut bindings, binding, io)?;
        }
        let capacity = self
            .bindings
            .len()
            .checked_add(bindings.len())
            .ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "hook count overflow",
                )
            })?;
        crate::executor::stock_vec_reserve(&mut self.bindings, capacity, io)?;
        let bindings = crate::executor::stock_into_iter(bindings, io)?;
        self.bindings.extend(bindings);
        Ok(())
    }

    fn candidates<'a, 'input>(
        &'a self,
        event: HookEvent,
        input: impl Into<HookInput<'input>>,
        registry: &'a ToolRegistry,
    ) -> Result<impl Iterator<Item = (&'a Arc<HookSpec>, Option<&'a Requests>)> + Clone> {
        let input = input.into();
        let subject = input.subject(event);
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
            .flat_map(|binding| &binding.commands)
            .map(|(hook, requests)| (hook, requests.as_ref())))
    }

    pub(crate) fn commands<'a, 'input>(
        &'a self,
        event: HookEvent,
        input: impl Into<HookInput<'input>>,
        registry: &'a ToolRegistry,
    ) -> Result<impl Iterator<Item = &'a Arc<HookSpec>> + Clone> {
        let input = input.into();
        let request = Arc::new(request::normalized(input, event, None, registry)?);
        Ok(self
            .candidates(event, input, registry)?
            .filter(move |(_, matcher)| matcher.is_none_or(|matcher| matcher.matches(&request)))
            .map(|(hook, _)| hook))
    }

    /// The outcome of `event`'s hooks after a run interrupted before its
    /// outcome was recorded: each matching hook reports a failure, which
    /// blocks exactly where its failure policy blocks.
    #[doc(hidden)]
    pub fn interrupted<'a>(
        &self,
        event: HookEvent,
        input: impl Into<HookInput<'a>>,
        registry: &ToolRegistry,
    ) -> Result<HookOutput> {
        let mut output = HookOutput::default();
        for hook in self.commands(event, input, registry)? {
            output.diagnostics.push(HookDiagnostic {
                cause: crate::ProcessError::Io {
                    exit_code: None,
                    message: "hook was interrupted before its outcome was recorded".into(),
                },
                blocking: hook.failure.blocks(event),
            });
        }
        Ok(output)
    }

    #[doc(hidden)]
    pub async fn run_commands<'a, F, Fut>(
        &self,
        event: HookEvent,
        input: impl Into<HookInput<'a>>,
        registry: &ToolRegistry,
        maximum_bytes: impl Into<Option<u64>>,
        mut execute: F,
    ) -> Result<HookOutput>
    where
        F: FnMut(&Arc<HookSpec>, Option<usize>, Option<&Value>) -> Fut,
        Fut: Future<Output = Result<(Option<i32>, Vec<u8>, Vec<u8>)>>,
    {
        let input = input.into();
        let maximum_bytes = maximum_bytes.into();
        if event.site() == HookSite::BeforeTool {
            let hooks: Vec<_> = self.candidates(event, input, registry)?.collect();
            let quota = maximum_bytes.map(|maximum| capture_quota(maximum, hooks.len()));
            let original = input.tool_input().unwrap_or(&Value::Null);
            let reason = match chain::judge(
                &hooks,
                event,
                original,
                (quota, maximum_bytes),
                &|matcher, current| match matcher {
                    None => Ok(true),
                    Some(matcher) => Ok(matcher.matches(&request::normalized(
                        input,
                        event,
                        Some(current),
                        registry,
                    )?)),
                },
                &mut execute,
            )
            .await
            {
                Ok(output) => return Ok(output),
                // Invalid tool arguments, including a hook's rewrite, reject
                // this call through the ordinary blocking outcome.
                Err(Error::Invalid(reason)) => reason,
                Err(Error::ToolRejected {
                    code: crate::wire::ErrorCode::Invalid,
                    reason,
                }) => reason.into(),
                Err(error) => return Err(error),
            };
            return Ok(HookOutput {
                blocked: Some(reason),
                ..HookOutput::default()
            });
        }
        let commands = self.commands(event, input, registry)?;
        let quota = maximum_bytes.map(|maximum| capture_quota(maximum, commands.clone().count()));
        let mut budget = budget::Budget::new(maximum_bytes)?;
        let results = futures::future::join_all(commands.map(|hook| {
            let execution = execute(hook, quota, None);
            async move { (hook, execution.await) }
        }))
        .await;
        let mut output = HookOutput::default();
        for (hook, result) in results {
            budget.apply(&mut output, event, &**hook, result);
        }
        budget.finish(&mut output);
        Ok(output)
    }
}

// A static partition keeps total live capture storage bounded without a lock,
// scheduling-dependent output truncation, or delaying concurrent hook dispatch.
fn capture_quota(maximum: u64, count: usize) -> usize {
    usize::try_from(maximum / (count.max(1) as u64)).unwrap_or(usize::MAX)
}

pub(crate) fn report(event: HookEvent, output: &HookOutput) {
    for failure in &output.diagnostics {
        tracing::warn!(hook_event = event.name(), cause = ?failure.cause,
            exit_code = failure.cause.exit_code(), blocking = failure.blocking,
            error = %failure.cause.message(), "configured hook failed");
    }
}

mod output;
pub(crate) use output::ToolHookDecision;
pub use output::{HookDiagnostic, HookOutput};

mod native;
#[doc(hidden)]
pub use native::HookCheck;
pub use native::HookExecution;
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
