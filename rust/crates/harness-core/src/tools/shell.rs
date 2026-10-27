//! Platform shell contracts shared by model tools, policy routing and execution.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub use acyclic_host_platform::shell::{PLATFORM, ShellSpec};

/// A command in the calling task's view; workspace selection is host-owned.
/// Release 1 uses no OS sandbox: commands can reach the host filesystem.
/// Writes outside the task view are not captured. The cwd must remain in the view.
#[derive(
    Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema, crate::HeapLayout,
)]
#[serde(deny_unknown_fields)]
pub struct ShellInput {
    /// At most 16 KiB of UTF-8 source, inheriting the host environment.
    #[schemars(length(min = 1, max = 16384))]
    pub command: String,
    /// Optional directory relative to the task view. Parent traversal is forbidden.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(max = 4096))]
    pub cwd: Option<String>,
    /// Positive timeout in seconds, at most 60. Omission uses 60 seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 60))]
    pub timeout: Option<u32>,
}

impl ShellInput {
    /// Validates the command and relative directory before any native I/O.
    pub fn validate(&self) -> Result<()> {
        let cwd = Path::new(self.cwd.as_deref().unwrap_or(""));
        if self.command.is_empty()
            || self.command.len() > 16_384
            || self.command.contains('\0')
            || self
                .cwd
                .as_ref()
                .is_some_and(|cwd| cwd.len() > 4096 || cwd.contains('\0'))
            || cwd
                .components()
                .any(|part| !matches!(part, Component::Normal(_) | Component::CurDir))
        {
            return Err(Error::Invalid(
                "shell input admission failed; recovery: supply 1..16384 NUL-free source bytes and a relative path within 4096 bytes without parent traversal".into(),
            ));
        }
        self.timeout_ms()?;
        Ok(())
    }

    pub(crate) fn timeout_ms(&self) -> Result<u32> {
        let seconds = self.timeout.unwrap_or(60);
        if !(1..=60).contains(&seconds) {
            return Err(Error::Invalid("shell timeout admission failed; recovery: choose 1..60 seconds or omit timeout for 60 seconds".into()));
        }
        Ok(seconds * 1000)
    }
}

#[cfg(test)]
mod tests;
