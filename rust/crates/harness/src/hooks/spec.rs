use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Lifecycle points with command-hook support.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HookEvent {
    /// A session was opened, resumed, forked, or compacted.
    SessionStart,
    /// A user prompt was admitted and is about to reach the model.
    UserPromptSubmit,
    /// A tool is about to execute.
    PreToolUse,
    /// A tool returned successfully.
    PostToolUse,
    /// The model is about to finish its turn.
    Stop,
}

/// The single runtime boundary that fires an event. Every firing site
/// dispatches exactly `HookEvent::at(site)`, so declaring an event without
/// choosing its site in [`HookEvent::site`] does not compile, and an event
/// with a site runs wherever that site runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HookSite {
    /// Model command preparation, before the first model request of a turn.
    Prompt,
    /// After a tool call is admitted and before it is dispatched.
    BeforeTool,
    /// After a tool result is durably recorded.
    AfterTool,
    /// A model step finished without requesting a tool.
    TurnEnd,
}

impl HookEvent {
    /// Every event, in declaration order.
    pub(crate) const ALL: [Self; 5] = [
        Self::SessionStart,
        Self::UserPromptSubmit,
        Self::PreToolUse,
        Self::PostToolUse,
        Self::Stop,
    ];

    pub(crate) const fn site(self) -> HookSite {
        match self {
            Self::SessionStart | Self::UserPromptSubmit => HookSite::Prompt,
            Self::PreToolUse => HookSite::BeforeTool,
            Self::PostToolUse => HookSite::AfterTool,
            Self::Stop => HookSite::TurnEnd,
        }
    }

    /// Events fired at `site`, in declaration order.
    pub(crate) fn at(site: HookSite) -> impl Iterator<Item = Self> {
        Self::ALL
            .into_iter()
            .filter(move |event| event.site() == site)
    }

    /// The one event fired at a single-event site.
    pub(crate) fn only(site: HookSite) -> Result<Self> {
        let mut events = Self::at(site);
        match (events.next(), events.next()) {
            (Some(event), None) => Ok(event),
            _ => Err(Error::Invalid("hook site fires several events".into())),
        }
    }

    const fn ordinal(self) -> usize {
        match self {
            Self::SessionStart => 0,
            Self::UserPromptSubmit => 1,
            Self::PreToolUse => 2,
            Self::PostToolUse => 3,
            Self::Stop => 4,
        }
    }

    /// Plain, non-JSON stdout is model context for prompt-scoped events.
    pub(crate) fn plain_context(self) -> bool {
        matches!(self, Self::SessionStart | Self::UserPromptSubmit)
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
        }
    }
}

// `ordinal` is exhaustive and injective, so `ALL` lists each event once.
const _: () = {
    let [first, second, third, fourth, fifth] = HookEvent::ALL;
    assert!(
        first.ordinal() == 0
            && second.ordinal() == 1
            && third.ordinal() == 2
            && fourth.ordinal() == 3
            && fifth.ordinal() == 4
    );
};

/// An event selector and its commands, sharing one compiled matcher.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookRule {
    /// Lifecycle point at which these commands run.
    pub on: HookEvent,
    /// Exact identifier, regular expression, or empty wildcard.
    pub matcher: String,
    /// Commands in deterministic result-merge order.
    pub hooks: Vec<HookSpec>,
}

/// Normalized command data consumed by the single hook runner.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookSpec {
    /// Native executable or script passed to the shared shell selector.
    pub run: HookRun,
    /// Positive execution deadline in milliseconds.
    pub timeout_ms: u64,
    /// How execution and output failures affect the caller.
    pub failure: HookFailure,
}

/// Finite native command vocabulary, independent of imported settings formats.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum HookRun {
    /// Executes an argv vector directly.
    Executable {
        /// Program resolved by the native process provider.
        executable: String,
        /// Individual arguments without shell interpretation.
        argv: Vec<String>,
    },
    /// Executes a script through the shared native shell selector.
    Shell {
        /// Complete script, passed as one shell argument.
        command: String,
        /// Explicit shell, or the platform default.
        shell: Option<String>,
    },
}

/// A command failure always records a diagnostic; blocking is explicit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookFailure {
    /// Report the failure and continue with the other hooks and caller.
    #[default]
    Report,
    /// Report the failure and block the guarded action, without aborting other hooks.
    Block,
}

impl HookSpec {
    pub(super) fn validate(&self) -> Result<()> {
        let (command, arguments_valid) = match &self.run {
            HookRun::Executable { executable, argv } => (
                executable,
                argv.iter().all(|argument| !argument.contains('\0')),
            ),
            HookRun::Shell { command, shell } => {
                if shell.as_deref().is_some_and(|shell| {
                    !matches!(shell, "bash" | "sh" | "powershell" | "pwsh" | "cmd")
                }) {
                    return Err(Error::Invalid("unknown hook shell".into()));
                }
                (command, true)
            }
        };
        if command.trim().is_empty()
            || command.contains('\0')
            || !arguments_valid
            || self.timeout_ms == 0
        {
            return Err(Error::Invalid(
                "hook command and timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "spec/properties.rs"]
mod properties;
