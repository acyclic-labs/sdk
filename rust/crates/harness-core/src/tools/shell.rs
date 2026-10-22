//! Platform shell contracts shared by model tools, policy routing and execution.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

/// Platform shell data shared by registration, execution and client routing.
pub struct ShellSpec {
    /// Model-visible tool and approval-policy name.
    pub name: &'static str,
    /// Model guidance for the host's command language.
    pub description: &'static str,
    /// Approval and lifecycle hook aliases.
    pub hook_names: &'static [&'static str],
    /// Native executables in preference order; resolved once at host startup.
    pub executables: &'static [&'static str],
    /// Non-interactive arguments preceding the source text.
    pub arguments: &'static [&'static str],
    /// Host initialization prepended to the command source.
    pub command_prefix: &'static str,
}

/// Windows commands must use the native filesystem namespace.
#[cfg(windows)]
pub const PLATFORM: ShellSpec = ShellSpec {
    name: "powershell",
    description: "Run native PowerShell in the calling task's composed workspace view, using PowerShell 7 (pwsh) when available, otherwise Windows PowerShell 5.1. Use PowerShell syntax and Windows paths. Runs with -NoProfile -NonInteractive; writes outside the task view are not captured. No OS sandbox in release 1",
    hook_names: &["PowerShell"],
    executables: &["pwsh.exe", "powershell.exe"],
    arguments: &["-NoProfile", "-NonInteractive", "-Command"],
    command_prefix: "[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false); $OutputEncoding = [Console]::OutputEncoding; ",
};

/// Unix hosts retain the bash command language.
#[cfg(not(windows))]
pub const PLATFORM: ShellSpec = ShellSpec {
    name: "bash",
    description: "Run a bash command in the calling task's composed workspace view. Use bash syntax and Unix paths. No OS sandbox in release 1; writes outside the task view are not captured",
    hook_names: &["Bash"],
    executables: &["bash"],
    arguments: &["--noprofile", "--norc", "-c"],
    command_prefix: "",
};

/// A command in the calling task's view; workspace selection is host-owned.
/// Release 1 uses no OS sandbox: commands can reach the host filesystem.
/// Writes outside the task view are not captured. The cwd must remain in the view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ShellInput {
    /// Source for the platform shell, inheriting the host environment.
    #[schemars(length(min = 1))]
    pub command: String,
    /// Optional directory relative to the task view. Parent traversal is forbidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Positive timeout in seconds. Omission uses 300 seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 4294967))]
    pub timeout: Option<u32>,
}

impl ShellInput {
    /// Validates the command and relative directory before any native I/O.
    pub fn validate(&self) -> Result<()> {
        let cwd = Path::new(self.cwd.as_deref().unwrap_or(""));
        if self.command.is_empty()
            || self.command.contains('\0')
            || self.cwd.as_ref().is_some_and(|cwd| cwd.contains('\0'))
            || cwd
                .components()
                .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::Invalid(
                "invalid shell command, cwd or timeout".into(),
            ));
        }
        self.timeout_ms()?;
        Ok(())
    }

    pub(crate) fn timeout_ms(&self) -> Result<u32> {
        self.timeout
            .unwrap_or(300)
            .checked_mul(1000)
            .filter(|timeout| *timeout > 0)
            .ok_or_else(|| Error::Invalid("shell timeout is outside the native clock range".into()))
    }
}

#[cfg(test)]
mod tests;
