use crate::{Error, Result};
use serde::{Deserialize, Serialize};

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
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::SessionStart => "SessionStart",
            Self::PreToolUse => "PreToolUse",
            Self::PostToolUse => "PostToolUse",
            Self::Stop => "Stop",
        }
    }
}

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
