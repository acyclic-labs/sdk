pub use crate::config::{HookEvent, RequestMatch};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// The single runtime boundary that fires an event. Every firing site
/// dispatches exactly `HookEvent::at(site)`, so declaring an event without
/// choosing its site in [`HookEvent::site`] does not compile, and an event
/// with a site runs wherever that site runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HookSite {
    /// Model command preparation, before the first model request of a turn.
    Prompt,
    /// After a tool call is admitted and before it is dispatched.
    BeforeTool,
    /// After a tool result is durably recorded.
    AfterTool,
    /// A model step finished without requesting a tool.
    TurnEnd,
    /// A durable volume publication record.
    File,
    /// A durable followed stream record.
    Stream,
}

impl HookEvent {
    /// Every event, in declaration order.
    /// Every event, in declaration order.
    pub const ALL: [Self; 7] = [
        Self::SessionStart,
        Self::UserPromptSubmit,
        Self::PreToolUse,
        Self::PostToolUse,
        Self::Stop,
        Self::FileChanged,
        Self::StreamRecord,
    ];

    /// The site that fires this event.
    pub const fn site(self) -> HookSite {
        match self {
            Self::SessionStart | Self::UserPromptSubmit => HookSite::Prompt,
            Self::PreToolUse => HookSite::BeforeTool,
            Self::PostToolUse => HookSite::AfterTool,
            Self::Stop => HookSite::TurnEnd,
            Self::FileChanged => HookSite::File,
            Self::StreamRecord => HookSite::Stream,
        }
    }

    /// Events fired at `site`, in declaration order.
    /// Events fired at `site`, in declaration order.
    pub fn at(site: HookSite) -> impl Iterator<Item = Self> {
        Self::ALL
            .into_iter()
            .filter(move |event| event.site() == site)
    }

    /// The one event fired at a single-event site.
    /// The one event fired at a single-event site.
    pub fn only(site: HookSite) -> Result<Self> {
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
            Self::FileChanged => 5,
            Self::StreamRecord => 6,
        }
    }

    /// Plain, non-JSON stdout is model context for prompt-scoped events.
    pub(crate) fn plain_context(self) -> bool {
        matches!(self, Self::SessionStart | Self::UserPromptSubmit)
    }

    #[doc(hidden)]
    pub fn name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::UserPromptSubmit => "UserPromptSubmit",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
            Self::FileChanged => "FileChanged",
            Self::StreamRecord => "StreamRecord",
        }
    }
}

// `ordinal` is exhaustive and injective, so `ALL` lists each event once.
const _: () = {
    let [first, second, third, fourth, fifth, sixth, seventh] = HookEvent::ALL;
    assert!(
        first.ordinal() == 0
            && second.ordinal() == 1
            && third.ordinal() == 2
            && fourth.ordinal() == 3
            && fifth.ordinal() == 4
            && sixth.ordinal() == 5
            && seventh.ordinal() == 6
    );
};

/// An event selector and its commands, sharing one compiled matcher.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct HookSpec {
    /// Native executable or script passed to the shared shell selector.
    pub run: HookRun,
    /// Optional positive caller execution deadline in milliseconds.
    pub timeout_ms: Option<u64>,
    /// How execution and output failures affect the caller.
    pub failure: HookFailure,
}

/// Finite native command vocabulary, independent of imported settings formats.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
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
    /// A plugin's `hooks/before-<event>.{ts,sh,ps1}`, pinned by their exact
    /// bytes at the enabling agent's admitted generation. A TypeScript hook
    /// runs on every target in the shared settings evaluator: its default
    /// export is called with `{request, config}` and returns the decision.
    /// Otherwise the target runs the script form it can: `sh` on POSIX,
    /// PowerShell on Windows, reading `{request, config}` on stdin and
    /// writing the decision on stdout.
    Plugin {
        /// The instance: enabling agent, contributing entry and hook path.
        instance: String,
        /// `before-<event>.ts` and every module it statically imports, as
        /// `(resolved path, source)`, the hook itself first. Empty when the
        /// plugin ships scripts instead.
        ts: Vec<(String, String)>,
        /// `before-<event>.sh`, if the plugin ships one.
        sh: Option<String>,
        /// `before-<event>.ps1`, if the plugin ships one.
        ps1: Option<String>,
        /// The plugin's settings, as JSON.
        config: String,
        /// Which requests run it (plugin.ts `matcher`); others skip it
        /// without running anything.
        matcher: Option<RequestMatch>,
    },
}

/// A command failure always records a diagnostic; blocking is explicit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(rename_all = "snake_case")]
pub enum HookFailure {
    /// Report the failure and continue with the other hooks. A failed before-tool
    /// hook still refuses tool admission; other lifecycle sites may continue.
    #[default]
    Report,
    /// Report the failure and block the guarded action, without aborting other hooks.
    Block,
}

impl HookFailure {
    /// Whether a failure under this policy blocks `event`. A `Stop` failure
    /// never blocks: blocking a stop asks the model to continue a finished
    /// turn.
    pub(crate) fn blocks(self, event: HookEvent) -> bool {
        self == Self::Block && event != HookEvent::Stop
    }
}

impl HookSpec {
    /// A plugin hook speaks the plugin protocol: `{request, config}` in and
    /// a decision out. Every other hook speaks its host's.
    pub(crate) fn plugin(&self) -> Option<(&str, &str)> {
        match &self.run {
            HookRun::Plugin {
                instance, config, ..
            } => Some((instance, config)),
            HookRun::Executable { .. } | HookRun::Shell { .. } => None,
        }
    }

    /// Validates the rule against the admitted hook contract.
    pub fn validate(&self) -> Result<()> {
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
            HookRun::Plugin {
                ts,
                sh,
                ps1,
                config,
                ..
            } => {
                // A plugin hook that cannot run denies its request.
                if self.failure != HookFailure::Block {
                    return Err(Error::Invalid("a plugin hook's failure blocks".into()));
                }
                serde_json::from_str::<serde::de::IgnoredAny>(config)
                    .map_err(|error| Error::Invalid(format!("plugin hook settings: {error}")))?;
                // One form judges a request on every target, never two that
                // could disagree.
                if !ts.is_empty() && (sh.is_some() || ps1.is_some()) {
                    return Err(Error::Invalid(
                        "a plugin hook ships TypeScript or scripts, not both".into(),
                    ));
                }
                let script = ts
                    .first()
                    .map(|(_, source)| source)
                    .or(sh.as_ref())
                    .or(ps1.as_ref())
                    .ok_or_else(|| Error::Invalid("a plugin hook has a script".into()))?;
                (
                    script,
                    ts.iter()
                        .flat_map(|(path, source)| [path, source])
                        .chain(sh)
                        .chain(ps1)
                        .all(|text| !text.trim().is_empty() && !text.contains('\0')),
                )
            }
        };
        if command.trim().is_empty()
            || command.contains('\0')
            || !arguments_valid
            || self.timeout_ms == Some(0)
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
